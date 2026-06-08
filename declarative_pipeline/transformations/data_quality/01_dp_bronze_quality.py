"""
Declarative Bronze Quality Layer.

Computes object-level DQ metrics for bronze tables (row counts, null rates,
duplicate counts) in a declarative style using DLT, and aligns them with the
central dq_rule_catalog for observability.
"""

import dlt
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from pyspark.sql import functions as F


@dlt.view(
    name="dq_bronze_key_config",
    comment="Key configuration for bronze DQ metrics",
)
def dq_bronze_key_config():
    """
    Configuration view that defines the key columns per bronze object.

    This allows the metric computation to stay generic and driven by metadata
    instead of hard-coding keys for each table.
    """
    data = [
        ("bronze_store_sales", ["ss_item_sk", "ss_ticket_number", "ss_sold_date_sk", "ss_store_sk"]),
        ("bronze_date_dim", ["d_date_sk", "d_date_id"]),
        ("bronze_item", ["i_item_sk", "i_item_id"]),
        ("bronze_store", ["s_store_sk", "s_store_id"]),
        ("bronze_customer", ["c_customer_sk", "c_customer_id"]),
    ]

    rows = [(name, cols) for name, cols in data]

    schema = StructType(
        [
            StructField("object_name", StringType(), False),
            StructField("key_cols", ArrayType(StringType()), False),
        ]
    )

    return spark.createDataFrame(rows, schema)


def aggregate_metrics(df, object_name, object_type, key_cols):
    """
    Compute row counts, per-key null stats, and duplicate counts for a table.

    The duplicate_row_count is defined as the sum over (count - 1) for all key
    groups with count > 1, i.e. the number of extra rows beyond unique keys.
    """
    duplicate_count = (
        df.groupBy(*[F.col(c) for c in key_cols])
        .count()
        .where(F.col("count") > 1)
        .agg(
            F.coalesce(
                F.sum(F.col("count") - 1),
                F.lit(0),
            ).alias("duplicate_row_count")
        )
    )

    # Base metric: total row count.
    metric_exprs = [F.count(F.lit(1)).alias("row_count")]

    # For each key column, compute null count and null percentage.
    for idx, col_name in enumerate(key_cols, start=1):
        metric_exprs.append(
            F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(
                f"null_key_{idx}_count"
            )
        )
        metric_exprs.append(
            F.round(
                100.0
                * F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0))
                / F.when(F.count(F.lit(1)) == 0, None).otherwise(F.count(F.lit(1))),
                2,
            ).alias(f"pct_null_key_{idx}")
        )

    return (
        df.agg(*metric_exprs)
        .crossJoin(duplicate_count)
        .withColumn("observed_at", F.current_timestamp())
        .withColumn("paradigm", F.lit("declarative"))
        .withColumn("object_name", F.lit(object_name))
        .withColumn("object_type", F.lit(object_type))
    )


@dlt.view(
    name="dq_bronze_dp_metrics_overview",
    comment="Declarative bronze object-level metrics",
)
def dq_bronze_dp_metrics_overview():
    """
    Compute metrics for all configured bronze objects and union them.

    The result is one row per object_name with standardized metric columns,
    which can be joined to the declarative rule catalog for observability.
    """
    config = dlt.read("dq_bronze_key_config")

    metrics_dfs = []

    # Note: collect() is acceptable here because the key config is tiny metadata.
    for row in config.collect():
        object_name = row["object_name"]
        key_cols = row["key_cols"]

        df = aggregate_metrics(
            dlt.read(object_name),
            object_name,
            "RAW_MODEL",
            key_cols,
        )

        metrics_dfs.append(df)

    # Union all per-object metric frames into a single DataFrame.
    result = metrics_dfs[0]
    for df in metrics_dfs[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    return result


@dlt.view(
    name="dq_bronze_dp_observability",
    comment="Declarative bronze observability summary aligned with unified dq_rule_catalog metadata",
)
def dq_bronze_dp_observability():
    """
    Join bronze metrics with the dq_rule_catalog to provide a rule-aware view.

    This aligns computed metrics (row_count, nulls, duplicates) with the
    declarative rule metadata for the bronze layer.
    """
    m = dlt.read("dq_bronze_dp_metrics_overview")
    c = dlt.read("dq_rule_catalog").where(F.col("layer") == "bronze")
    return m.join(c, on="object_name", how="inner")


@dlt.table(
    name="dq_bronze_dp_metrics_overview_pub",
    comment="Persisted bronze DQ metrics overview for UC/BI and comparison",
)
def dq_bronze_dp_observability_pub():
    """Materialized version of the bronze metrics overview for external consumption."""
    return dlt.read("dq_bronze_dp_metrics_overview")


@dlt.table(
    name="dq_bronze_dp_observability_pub",
    comment="Persisted bronze DQ observability (metrics + rule metadata) for UC/BI and comparison",
)
def dq_bronze_dp_observability_pub():
    """Materialized version of the bronze observability join (metrics + rules)."""
    return dlt.read("dq_bronze_dp_observability")