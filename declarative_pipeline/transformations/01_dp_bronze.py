"""
Declarative Bronze Layer

Ingests raw TPC-DS sample tables into bronze DLT tables and adds
technical ingestion metadata, providing the raw foundation for
subsequent silver and gold transformations.
"""


import dlt
from pyspark.sql import functions as F



def bronze_source(source_name: str):
    # Generic helper for raw ingestion:
    # reads a source table once and adds technical ingestion metadata.
    return (
        spark.read.table(source_name)
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_table", F.lit(source_name))
    )



@dlt.table(name="bronze_store_sales", comment="Bronze layer - Raw store sales data from samples.tpcds_sf1000")
def bronze_store_sales():
    # Raw store_sales landing table; no business transformations applied.
    return bronze_source("samples.tpcds_sf1.store_sales")



@dlt.table(name="bronze_date_dim", comment="Bronze layer - Raw date dimension data from samples.tpcds_sf1000")
def bronze_date_dim():
    # Raw date_dim landing table used as the basis for downstream dimensions.
    return bronze_source("samples.tpcds_sf1.date_dim")



@dlt.table(name="bronze_item", comment="Bronze layer - Raw item data from samples.tpcds_sf1000")
def bronze_item():
    # Raw item landing table preserving source structure.
    return bronze_source("samples.tpcds_sf1.item")



@dlt.table(name="bronze_store", comment="Bronze layer - Raw store data from samples.tpcds_sf1000")
def bronze_store():
    # Raw store landing table capturing store attributes as delivered by the source.
    return bronze_source("samples.tpcds_sf1.store")



@dlt.table(name="bronze_customer", comment="Bronze layer - Raw customer data from samples.tpcds_sf1000")
def bronze_customer():
    # Raw customer landing table for downstream enrichment in silver/gold.
    return bronze_source("samples.tpcds_sf1.customer")