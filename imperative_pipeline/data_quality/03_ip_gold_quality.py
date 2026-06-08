"""
Gold Data Quality Layer - pure PySpark implementation.

Implements business-facing data quality checks on gold dimensions, facts,
and aggregates by producing:
- a row-level issues table for specific rule violations
- a metrics table with table-level KPI counts per gold object.
"""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# Target Unity Catalog location for all gold-layer DQ artifacts.
CATALOG = "workspace"
SCHEMA = "imperative"


def get_spark() -> SparkSession:
    """Return the active Spark session or create one if none exists."""
    return SparkSession.builder.getOrCreate()


def fq_table(table_name: str) -> str:
    """Build the fully qualified table name within the configured catalog/schema."""
    return f"{CATALOG}.{SCHEMA}.{table_name}"


def write_delta_table(df: DataFrame, table_name: str) -> None:
    """Write a DataFrame as a managed Delta table, replacing any existing definition."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(fq_table(table_name))
    )


def count_if(condition):
    """
    Count rows that satisfy a given boolean condition.

    Implemented via SUM over a CASE expression to keep the API composable.
    """
    return F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))).cast("bigint")


def issue_frame(
    table_name: str,
    rule_id: str,
    rule_group: str,
    key_exprs: list,
    reason_expr,
    condition,
) -> DataFrame:
    """
    Generic helper to build a rule-specific issue frame for a gold table.

    It standardizes the issue schema across rules:
    - issue_ts, paradigm, object_name, rule_id, rule_group
    - up to four key columns (key_1..key_4)
    - issue_reason describing the failure.
    """
    return (
        spark.table(fq_table(table_name))
        .filter(condition)
        .select(
            F.current_timestamp().alias("issue_ts"),
            F.lit("imperative").alias("paradigm"),
            F.lit(table_name).alias("object_name"),
            F.lit(rule_id).alias("rule_id"),
            F.lit(rule_group).alias("rule_group"),
            key_exprs[0].cast("string").alias("key_1"),
            key_exprs[1].cast("string").alias("key_2"),
            key_exprs[2].cast("string").alias("key_3"),
            key_exprs[3].cast("string").alias("key_4"),
            reason_expr.alias("issue_reason"),
        )
    )


def build_gold_issues() -> DataFrame:
    """
    Build a unified issues table for gold-layer DQ rules.

    Rules focus on business-validity of dimensions, facts, and aggregated
    marts (e.g. negative profits, missing business keys).
    """
    null_string = F.lit(None).cast("string")
    frames = [
        # IP_GOLD_001 – BUSINESS_VALUE: negative pricing in item dimension.
        issue_frame(
            "gold_dim_item",
            "IP_GOLD_001",
            "BUSINESS_VALUE",
            [F.col("item_key"), F.col("item_id"), null_string, null_string],
            F.when(F.col("current_price") < 0, "NEGATIVE_CURRENT_PRICE")
            .when(F.col("wholesale_cost") < 0, "NEGATIVE_WHOLESALE_COST")
            .otherwise("UNKNOWN"),
            (F.col("current_price") < 0) | (F.col("wholesale_cost") < 0),
        ),
        # IP_GOLD_002 – BUSINESS_KEY: store ID and name must be present.
        issue_frame(
            "gold_dim_store",
            "IP_GOLD_002",
            "BUSINESS_KEY",
            [F.col("store_key"), F.col("store_id"), F.col("store_name"), null_string],
            F.when(F.col("store_id").isNull(), "NULL_STORE_ID")
            .when(F.col("store_name").isNull(), "NULL_STORE_NAME")
            .otherwise("UNKNOWN"),
            F.col("store_id").isNull() | F.col("store_name").isNull(),
        ),
        # IP_GOLD_003 – BUSINESS_KEY: customer ID and email must be present.
        issue_frame(
            "gold_dim_customer",
            "IP_GOLD_003",
            "BUSINESS_KEY",
            [
                F.col("customer_key"),
                F.col("customer_id"),
                F.col("email_address"),
                null_string,
            ],
            F.when(F.col("customer_id").isNull(), "NULL_CUSTOMER_ID")
            .when(F.col("email_address").isNull(), "NULL_EMAIL_ADDRESS")
            .otherwise("UNKNOWN"),
            F.col("customer_id").isNull() | F.col("email_address").isNull(),
        ),
        # IP_GOLD_004 – MEASURE_VALIDITY: net profit in the fact table must not be negative.
        issue_frame(
            "gold_fact_store_sales",
            "IP_GOLD_004",
            "MEASURE_VALIDITY",
            [
                F.col("ticket_number"),
                F.col("item_key"),
                F.col("store_key"),
                F.col("customer_key"),
            ],
            F.lit("NEGATIVE_NET_PROFIT"),
            F.col("net_profit") < 0,
        ),
        # IP_GOLD_005 – BUSINESS_KEY: mandatory foreign keys in the fact table.
        issue_frame(
            "gold_fact_store_sales",
            "IP_GOLD_005",
            "BUSINESS_KEY",
            [
                F.col("ticket_number"),
                F.col("item_key"),
                F.col("store_key"),
                F.col("customer_key"),
            ],
            F.when(F.col("sold_date_key").isNull(), "NULL_SOLD_DATE_KEY")
            .when(F.col("item_key").isNull(), "NULL_ITEM_KEY")
            .when(F.col("store_key").isNull(), "NULL_STORE_KEY")
            .when(F.col("customer_key").isNull(), "NULL_CUSTOMER_KEY")
            .otherwise("UNKNOWN"),
            F.col("sold_date_key").isNull()
            | F.col("item_key").isNull()
            | F.col("store_key").isNull()
            | F.col("customer_key").isNull(),
        ),
        # IP_GOLD_006 – BUSINESS_KEY: mandatory attributes in the top-items aggregate mart.
        issue_frame(
            "gold_top_items_month",
            "IP_GOLD_006",
            "BUSINESS_KEY",
            [F.col("year"), F.col("month"), F.col("item_key"), F.col("item_id")],
            F.when(F.col("year").isNull(), "NULL_YEAR")
            .when(F.col("month").isNull(), "NULL_MONTH")
            .when(F.col("item_key").isNull(), "NULL_ITEM_KEY")
            .otherwise("UNKNOWN"),
            F.col("year").isNull()
            | F.col("month").isNull()
            | F.col("item_key").isNull(),
        ),
    ]
    return reduce(DataFrame.unionByName, frames)


def metrics_frame(
    table_name: str,
    object_type: str,
    issue_condition,
    negative_condition,
    null_business_key_condition,
) -> DataFrame:
    """
    Build table-level DQ metrics for a given gold table.

    Metrics include:
    - row_count
    - issue_count: rows matching the combined issue_condition
    - negative_value_count: rows violating measure sign expectations
    - null_business_key_count: rows missing key business identifiers.
    """
    return spark.table(fq_table(table_name)).agg(
        F.current_timestamp().alias("metric_ts"),
        F.lit("imperative").alias("paradigm"),
        F.lit(table_name).alias("object_name"),
        F.lit(object_type).alias("object_type"),
        F.count(F.lit(1)).cast("bigint").alias("row_count"),
        count_if(issue_condition).alias("issue_count"),
        count_if(negative_condition).alias("negative_value_count"),
        count_if(null_business_key_condition).alias("null_business_key_count"),
        F.lit("post_load_validation").alias("validation_mode"),
        F.lit("table_from_dataframe_writer").alias("source_layer"),
    )


def build_gold_metrics() -> DataFrame:
    """
    Compute metrics snapshots for all gold tables.

    This provides a compact, table-level view of business rule violations at
    the curated layer, suitable for monitoring dashboards or alerts.
    """
    false_condition = F.lit(False)
    frames = [
        metrics_frame(
            "gold_dim_date",
            "DELTA_TABLE",
            false_condition,
            false_condition,
            false_condition,
        ),
        metrics_frame(
            "gold_dim_item",
            "DELTA_TABLE",
            (F.col("current_price") < 0) | (F.col("wholesale_cost") < 0),
            (F.col("current_price") < 0) | (F.col("wholesale_cost") < 0),
            false_condition,
        ),
        metrics_frame(
            "gold_dim_store",
            "DELTA_TABLE",
            F.col("store_id").isNull() | F.col("store_name").isNull(),
            false_condition,
            F.col("store_id").isNull() | F.col("store_name").isNull(),
        ),
        metrics_frame(
            "gold_dim_customer",
            "DELTA_TABLE",
            F.col("customer_id").isNull() | F.col("email_address").isNull(),
            false_condition,
            F.col("customer_id").isNull() | F.col("email_address").isNull(),
        ),
        metrics_frame(
            "gold_fact_store_sales",
            "DELTA_TABLE",
            (F.col("net_profit") < 0)
            | F.col("sold_date_key").isNull()
            | F.col("item_key").isNull()
            | F.col("store_key").isNull()
            | F.col("customer_key").isNull(),
            F.col("net_profit") < 0,
            F.col("sold_date_key").isNull()
            | F.col("item_key").isNull()
            | F.col("store_key").isNull()
            | F.col("customer_key").isNull(),
        ),
        metrics_frame(
            "gold_top_items_month",
            "DELTA_TABLE",
            F.col("year").isNull()
            | F.col("month").isNull()
            | F.col("item_key").isNull(),
            false_condition,
            F.col("year").isNull()
            | F.col("month").isNull()
            | F.col("item_key").isNull(),
        ),
    ]
    return reduce(DataFrame.unionByName, frames)


def main() -> None:
    """Build and persist the gold-level issues and metrics tables."""
    issues = build_gold_issues()
    write_delta_table(issues, "dq_gold_ip_issues")
    print(f"Wrote {fq_table('dq_gold_ip_issues')}: {issues.count()} rows")

    metrics = build_gold_metrics()
    write_delta_table(metrics, "dq_gold_ip_metrics")
    print(f"Wrote {fq_table('dq_gold_ip_metrics')}: {metrics.count()} rows")


# Create the Spark session once at module level so all functions can reuse it.
spark = get_spark()


if __name__ == "__main__":
    main()