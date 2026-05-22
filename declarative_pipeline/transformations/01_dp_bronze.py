"""
Declarative Bronze Layer

"""

import dlt
from pyspark.sql import functions as F


def bronze_source(source_name: str):
    return (
        spark.read.table(source_name)
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_table", F.lit(source_name))
    )


@dlt.table(name="bronze_store_sales", comment="Bronze layer - Raw store sales data from samples.tpcds_sf1000")
def bronze_store_sales():
    return bronze_source("samples.tpcds_sf1.store_sales")


@dlt.table(name="bronze_date_dim", comment="Bronze layer - Raw date dimension data from samples.tpcds_sf1000")
def bronze_date_dim():
    return bronze_source("samples.tpcds_sf1.date_dim")


@dlt.table(name="bronze_item", comment="Bronze layer - Raw item data from samples.tpcds_sf1000")
def bronze_item():
    return bronze_source("samples.tpcds_sf1.item")


@dlt.table(name="bronze_store", comment="Bronze layer - Raw store data from samples.tpcds_sf1000")
def bronze_store():
    return bronze_source("samples.tpcds_sf1.store")


@dlt.table(name="bronze_customer", comment="Bronze layer - Raw customer data from samples.tpcds_sf1000")
def bronze_customer():
    return bronze_source("samples.tpcds_sf1.customer")
