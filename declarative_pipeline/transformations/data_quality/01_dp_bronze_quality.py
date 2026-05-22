"""
Declarative Bronze Qualtity Layer

"""
import dlt
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType
from pyspark.sql import functions as F

@dlt.view(
    name="dq_bronze_key_config",
    comment="Key configuration for bronze DQ metrics"
)
def dq_bronze_key_config():
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
    duplicate_count = (
        df.groupBy(*[F.col(c) for c in key_cols])
        .count()
        .where(F.col("count") > 1)
        .agg(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicate_row_count"))
    )

    metric_exprs = [F.count(F.lit(1)).alias("row_count")]

    for idx, col_name in enumerate(key_cols, start=1):
        metric_exprs.append(
            F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(f"null_key_{idx}_count")
        )
        metric_exprs.append(
            F.round(
                100.0 * F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0))
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


@dlt.view(name="dq_bronze_dp_metrics_overview", comment="Declarative bronze object-level metrics")
def dq_bronze_dp_metrics_overview():
    config = dlt.read("dq_bronze_key_config")

    metrics_dfs = []

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

    result = metrics_dfs[0]
    for df in metrics_dfs[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    return result


@dlt.view(name="dq_bronze_dp_observability", comment="Declarative bronze observability summary aligned with unified dq_rule_catalog metadata")
def dq_bronze_dp_observability():
    m = dlt.read("dq_bronze_dp_metrics_overview")
    c = dlt.read("dq_rule_catalog").where(F.col("layer") == "bronze")
    return m.join(c, on="object_name", how="inner")

#catalog
@dlt.table(
    name="dq_bronze_dp_metrics_overview_pub",
    comment="Persistierte Bronze DQ Observability für UC/BI und Vergleich"
)
def dq_bronze_dp_observability_pub():
    return dlt.read("dq_bronze_dp_metrics_overview")

@dlt.table(
    name="dq_bronze_dp_observability_pub",
    comment="Persistierte Bronze DQ Observability für UC/BI und Vergleich"
)
def dq_bronze_dp_observability_pub():
    return dlt.read("dq_bronze_dp_observability")
