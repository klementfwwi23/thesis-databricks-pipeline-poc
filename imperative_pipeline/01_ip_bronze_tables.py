"""
Imperative Bronze Layer

"""

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


def bronze_from_source(source_table: str) -> DataFrame:
    return (
        spark.table(source_table)
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_table", F.lit(source_table))
    )


def main() -> None:
    bronze_tables = [
        ("samples.tpcds_sf1.store_sales", "bronze_store_sales"),
        ("samples.tpcds_sf1.date_dim", "bronze_date_dim"),
        ("samples.tpcds_sf1.item", "bronze_item"),
        ("samples.tpcds_sf1.store", "bronze_store"),
        ("samples.tpcds_sf1.customer", "bronze_customer"),
    ]

    for source_table, target_table in bronze_tables:
        df = bronze_from_source(source_table)
        write_delta_table(df, target_table)
        row_count = spark.table(fq_table(target_table)).count()
        print(f"Wrote {fq_table(target_table)}: {row_count} rows")


spark = get_spark()

if __name__ == "__main__":
    main()
