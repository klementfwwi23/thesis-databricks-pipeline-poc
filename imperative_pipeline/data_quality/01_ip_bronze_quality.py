"""
Bronze Data Quality Layer - pure PySpark implementation

"""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


CATALOG = "workspace"
SCHEMA = "imperative"


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def fq_table(table_name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table_name}"


def write_delta_table(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(fq_table(table_name))
    )


def count_if(condition):
    return F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))).cast("bigint")


def pct_null(column_name: str):
    return F.round(
        F.lit(100.0)
        * count_if(F.col(column_name).isNull())
        / F.when(F.count(F.lit(1)) == 0, F.lit(None)).otherwise(F.count(F.lit(1))),
        2,
    ).cast("double")


def duplicate_row_count(df: DataFrame, key_columns: list[str]) -> int:
    duplicate_rows = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .select(F.sum(F.col("count") - F.lit(1)).cast("bigint").alias("duplicate_row_count"))
        .collect()
    )
    if not duplicate_rows or duplicate_rows[0]["duplicate_row_count"] is None:
        return 0
    return int(duplicate_rows[0]["duplicate_row_count"])


def bronze_metrics(
    table_name: str,
    key_columns: list[str],
    duplicate_key_columns: list[str],
) -> DataFrame:
    df = spark.table(fq_table(table_name))
    duplicate_count = duplicate_row_count(df, duplicate_key_columns)

    null_count_columns = []
    pct_columns = []
    for idx in range(4):
        if idx < len(key_columns):
            key_column = key_columns[idx]
            null_count_columns.append(count_if(F.col(key_column).isNull()).alias(f"null_key_{idx + 1}_count"))
            pct_columns.append(pct_null(key_column).alias(f"pct_null_key_{idx + 1}"))
        else:
            null_count_columns.append(F.lit(None).cast("bigint").alias(f"null_key_{idx + 1}_count"))
            pct_columns.append(F.lit(None).cast("double").alias(f"pct_null_key_{idx + 1}"))

    return df.agg(
        F.current_timestamp().alias("metric_ts"),
        F.lit("imperative").alias("paradigm"),
        F.lit(table_name).alias("object_name"),
        F.lit("RAW_TABLE").alias("object_type"),
        F.count(F.lit(1)).cast("bigint").alias("row_count"),
        *null_count_columns,
        *pct_columns,
        F.lit(duplicate_count).cast("bigint").alias("duplicate_row_count"),
        F.lit("explicit_post_load_monitoring").alias("validation_mode"),
    )


def main() -> None:
    metric_frames = [
        bronze_metrics(
            "bronze_store_sales",
            ["ss_item_sk", "ss_ticket_number", "ss_sold_date_sk", "ss_store_sk"],
            ["ss_item_sk", "ss_ticket_number", "ss_sold_date_sk", "ss_store_sk"],
        ),
        bronze_metrics("bronze_date_dim", ["d_date_sk", "d_date_id"], ["d_date_sk", "d_date_id"]),
        bronze_metrics("bronze_item", ["i_item_sk", "i_item_id"], ["i_item_sk", "i_item_id"]),
        bronze_metrics("bronze_store", ["s_store_sk", "s_store_id"], ["s_store_sk", "s_store_id"]),
        bronze_metrics("bronze_customer", ["c_customer_sk", "c_customer_id"], ["c_customer_sk", "c_customer_id"]),
    ]

    metrics = reduce(DataFrame.unionByName, metric_frames)
    write_delta_table(metrics, "dq_bronze_ip_metrics_overview")
    print(f"Wrote {fq_table('dq_bronze_ip_metrics_overview')}: {metrics.count()} rows")


spark = get_spark()

if __name__ == "__main__":
    main()
