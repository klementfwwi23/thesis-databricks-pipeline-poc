"""
Imperative Bronze Layer.

This job reads source tables, adds basic ingestion metadata, and writes the
result as managed Delta tables in the bronze layer.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# Target Unity Catalog location for all bronze tables written by this job.
CATALOG = "workspace"
SCHEMA = "imperative"


def get_spark() -> SparkSession:
    """Return the active Spark session or create one if none exists."""
    return SparkSession.builder.getOrCreate()


def fq_table(table_name: str) -> str:
    """Build the fully qualified table name for the target schema."""
    return f"{CATALOG}.{SCHEMA}.{table_name}"


def write_delta_table(df: DataFrame, table_name: str) -> None:
    """Write a DataFrame as a Delta table, replacing an existing table if needed."""
    (
        df.write.format("delta")
        .mode("overwrite")
        # Allow schema updates when the source structure changes.
        .option("overwriteSchema", "true")
        .saveAsTable(fq_table(table_name))
    )


def bronze_from_source(source_table: str) -> DataFrame:
    """Load a source table and enrich it with technical bronze metadata."""
    return (
        spark.table(source_table)
        # Record when the data was ingested into the bronze layer.
        .withColumn("ingestion_timestamp", F.current_timestamp())
        # Preserve the origin of the record for lineage and debugging.
        .withColumn("source_table", F.lit(source_table))
    )


def main() -> None:
    """Ingest predefined source tables into bronze Delta tables."""
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

        # Read back the written table to confirm the write and report its size.
        row_count = spark.table(fq_table(target_table)).count()
        print(f"Wrote {fq_table(target_table)}: {row_count} rows")


# Create the Spark session once at module level so helper functions can reuse it.
spark = get_spark()


if __name__ == "__main__":
    main()