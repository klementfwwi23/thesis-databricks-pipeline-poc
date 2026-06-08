"""
Declarative Rule Catalog.

Defines a unified, declarative catalog of data quality rules across
Bronze, Silver, and Gold layers and exposes it as Delta Live Tables
views/tables to be consumed by pipelines.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


# Central list of all DQ rules used by the declarative pipelines.
# Each tuple encodes:
# (rule_id, layer, object_name, rule_group, rule_expression,
#  rule_type, validation_mode, severity, owner)
RULES = [
    ("BRZ_SS_KEY_01", "bronze", "bronze_store_sales", "KEY_COMPLETENESS",
     "ss_item_sk IS NOT NULL", "MONITOR",
     "passive observability on raw model", "LOW", "data-engineering"),
    ("BRZ_SS_KEY_02", "bronze", "bronze_store_sales", "KEY_COMPLETENESS",
     "ss_ticket_number IS NOT NULL", "MONITOR",
     "passive observability on raw model", "LOW", "data-engineering"),
    ("BRZ_SS_KEY_03", "bronze", "bronze_store_sales", "KEY_COMPLETENESS",
     "ss_sold_date_sk IS NOT NULL", "MONITOR",
     "passive observability on raw model", "LOW", "data-engineering"),
    ("BRZ_SS_KEY_04", "bronze", "bronze_store_sales", "KEY_COMPLETENESS",
     "ss_store_sk IS NOT NULL", "MONITOR",
     "passive observability on raw model", "LOW", "data-engineering"),
    ("BRZ_SS_DUP_01", "bronze", "bronze_store_sales", "DUPLICATE_MONITORING",
     "duplicate(ss_item_sk, ss_ticket_number, ss_sold_date_sk, ss_store_sk)",
     "MONITOR", "passive observability on raw model", "MEDIUM",
     "data-engineering"),
    ("BRZ_DATE_KEY_01", "bronze", "bronze_date_dim", "KEY_COMPLETENESS",
     "d_date_sk IS NOT NULL", "MONITOR",
     "passive observability on raw model", "LOW", "data-engineering"),
    ("BRZ_DATE_KEY_02", "bronze", "bronze_date_dim", "KEY_COMPLETENESS",
     "d_date_id IS NOT NULL", "MONITOR",
     "passive observability on raw model", "LOW", "data-engineering"),
    ("BRZ_DATE_DUP_01", "bronze", "bronze_date_dim", "DUPLICATE_MONITORING",
     "duplicate(d_date_sk, d_date_id)", "MONITOR",
     "passive observability on raw model", "MEDIUM", "data-engineering"),
    ("BRZ_ITEM_KEY_01", "bronze", "bronze_item", "KEY_COMPLETENESS",
     "i_item_sk IS NOT NULL", "MONITOR",
     "passive observability on raw model", "LOW", "data-engineering"),
    ("BRZ_ITEM_KEY_02", "bronze", "bronze_item", "KEY_COMPLETENESS",
     "i_item_id IS NOT NULL", "MONITOR",
     "passive observability on raw model", "LOW", "data-engineering"),
    ("BRZ_ITEM_DUP_01", "bronze", "bronze_item", "DUPLICATE_MONITORING",
     "duplicate(i_item_sk, i_item_id)", "MONITOR",
     "passive observability on raw model", "MEDIUM", "data-engineering"),
    ("BRZ_STORE_KEY_01", "bronze", "bronze_store", "KEY_COMPLETENESS",
     "s_store_sk IS NOT NULL", "MONITOR",
     "passive observability on raw model", "LOW", "data-engineering"),
    ("BRZ_STORE_KEY_02", "bronze", "bronze_store", "KEY_COMPLETENESS",
     "s_store_id IS NOT NULL", "MONITOR",
     "passive observability on raw model", "LOW", "data-engineering"),
    ("BRZ_STORE_DUP_01", "bronze", "bronze_store", "DUPLICATE_MONITORING",
     "duplicate(s_store_sk, s_store_id)", "MONITOR",
     "passive observability on raw model", "MEDIUM", "data-engineering"),
    ("BRZ_CUST_KEY_01", "bronze", "bronze_customer", "KEY_COMPLETENESS",
     "c_customer_sk IS NOT NULL", "MONITOR",
     "passive observability on raw model", "LOW", "data-engineering"),
    ("BRZ_CUST_KEY_02", "bronze", "bronze_customer", "KEY_COMPLETENESS",
     "c_customer_id IS NOT NULL", "MONITOR",
     "passive observability on raw model", "LOW", "data-engineering"),
    ("BRZ_CUST_DUP_01", "bronze", "bronze_customer", "DUPLICATE_MONITORING",
     "duplicate(c_customer_sk, c_customer_id)", "MONITOR",
     "passive observability on raw model", "MEDIUM", "data-engineering"),

    # Silver-layer rules: enforced expectations on materialized models.
    ("DP_SILVER_001", "silver", "silver_date_dim", "KEY_VALIDITY",
     "d_date_sk IS NOT NULL", "EXPECT",
     "EXPECT constraint on materialized model", "HIGH", "model-owner"),
    ("DP_SILVER_002", "silver", "silver_date_dim", "KEY_VALIDITY",
     "d_date IS NOT NULL", "EXPECT",
     "EXPECT constraint on materialized model", "HIGH", "model-owner"),
    ("DP_SILVER_003", "silver", "silver_date_dim", "DOMAIN_VALIDITY",
     "d_year BETWEEN 1900 AND 2100", "EXPECT",
     "EXPECT constraint on materialized model", "MEDIUM", "model-owner"),
    ("DP_SILVER_004", "silver", "silver_date_dim", "DOMAIN_VALIDITY",
     "d_moy BETWEEN 1 AND 12", "EXPECT",
     "EXPECT constraint on materialized model", "MEDIUM", "model-owner"),
    ("DP_SILVER_005", "silver", "silver_item", "KEY_VALIDITY",
     "i_item_sk IS NOT NULL AND i_item_id IS NOT NULL", "EXPECT",
     "EXPECT constraint on materialized model", "HIGH", "model-owner"),
    ("DP_SILVER_006", "silver", "silver_item", "BUSINESS_VALUE",
     "i_current_price IS NOT NULL AND i_current_price >= 0", "EXPECT",
     "EXPECT constraint on materialized model", "HIGH", "model-owner"),
    ("DP_SILVER_007", "silver", "silver_item", "BUSINESS_VALUE",
     "i_wholesale_cost >= 0 OR i_wholesale_cost IS NULL", "EXPECT",
     "EXPECT constraint on materialized model", "MEDIUM", "model-owner"),
    ("DP_SILVER_008", "silver", "silver_store", "KEY_VALIDITY",
     "s_store_sk IS NOT NULL AND s_store_id IS NOT NULL "
     "AND s_store_name IS NOT NULL",
     "EXPECT", "EXPECT constraint on materialized model", "HIGH",
     "model-owner"),
    ("DP_SILVER_009", "silver", "silver_customer", "KEY_VALIDITY",
     "c_customer_sk IS NOT NULL AND c_customer_id IS NOT NULL", "EXPECT",
     "EXPECT constraint on materialized model", "HIGH", "model-owner"),
    ("DP_SILVER_010", "silver", "silver_store_sales", "KEY_VALIDITY",
     "ss_ticket_number IS NOT NULL AND ss_item_sk IS NOT NULL", "EXPECT",
     "EXPECT constraint on materialized model", "HIGH", "model-owner"),
    ("DP_SILVER_011", "silver", "silver_store_sales", "BUSINESS_VALUE",
     "ss_quantity > 0", "EXPECT",
     "EXPECT constraint on materialized model", "HIGH", "model-owner"),
    ("DP_SILVER_012", "silver", "silver_store_sales", "BUSINESS_VALUE",
     "(ss_sales_price >= 0 OR ss_sales_price IS NULL) "
     "AND (ss_list_price >= 0 OR ss_list_price IS NULL) "
     "AND (ss_wholesale_cost >= 0 OR ss_wholesale_cost IS NULL) "
     "AND (ss_net_paid >= 0 OR ss_net_paid IS NULL)",
     "EXPECT", "EXPECT constraint on materialized model", "HIGH",
     "model-owner"),
    ("DP_SILVER_013", "silver", "silver_store_sales", "REFERENTIAL_VALIDITY",
     "rows that remain in the model satisfy declarative referential filtering",
     "OBSERVE", "observed model state after EXPECT enforcement", "MEDIUM",
     "model-owner"),

    # Gold-layer rules: business-owner facing expectations on curated views.
    ("DP_GOLD_000", "gold", "gold_dim_date", "BUSINESS_KEY",
     "date_key IS NOT NULL AND calendar_date IS NOT NULL", "EXPECT",
     "EXPECT constraint on materialized view", "HIGH", "business-owner"),
    ("DP_GOLD_001", "gold", "gold_dim_item", "BUSINESS_VALUE",
     "current_price >= 0 AND (wholesale_cost >= 0 OR wholesale_cost IS NULL)",
     "EXPECT", "EXPECT constraint on materialized view", "HIGH",
     "business-owner"),
    ("DP_GOLD_002", "gold", "gold_dim_store", "BUSINESS_KEY",
     "store_id IS NOT NULL AND store_name IS NOT NULL", "EXPECT",
     "EXPECT constraint on materialized view", "HIGH", "business-owner"),
    ("DP_GOLD_003", "gold", "gold_dim_customer", "BUSINESS_KEY",
     "customer_id IS NOT NULL AND email_address IS NOT NULL", "EXPECT",
     "EXPECT constraint on materialized view", "HIGH", "business-owner"),
    ("DP_GOLD_004", "gold", "gold_fact_store_sales", "MEASURE_VALIDITY",
     "net_profit >= 0", "EXPECT",
     "EXPECT constraint on materialized view", "HIGH", "business-owner"),
    ("DP_GOLD_005", "gold", "gold_fact_store_sales", "BUSINESS_KEY",
     "sold_date_key IS NOT NULL AND item_key IS NOT NULL "
     "AND store_key IS NOT NULL AND customer_key IS NOT NULL",
     "EXPECT", "EXPECT constraint on materialized view", "HIGH",
     "business-owner"),
    ("DP_GOLD_006", "gold", "gold_top_items_month", "BUSINESS_KEY",
     "year IS NOT NULL AND month IS NOT NULL AND item_key IS NOT NULL",
     "EXPECT", "EXPECT constraint on materialized view", "HIGH",
     "business-owner"),
]

# Explicit schema for the rule catalog to keep the DLT contract stable.
SCHEMA = StructType([
    StructField("rule_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("object_name", StringType(), False),
    StructField("rule_group", StringType(), False),
    StructField("rule_expression", StringType(), False),
    StructField("rule_type", StringType(), False),
    StructField("validation_mode", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("owner", StringType(), False),
])


@dlt.view(
    name="dq_rule_catalog",
    comment=(
        "Unified declarative data quality rule catalog for Bronze / Silver / "
        "Gold (single source of truth)"
    ),
)
def dq_rule_catalog():
    """
    Base view exposing the in-memory RULES list as a structured DataFrame.

    This view is used as the authoritative source for all declarative DQ rules
    and can be joined to pipeline metadata for introspection.
    """
    return spark.createDataFrame(RULES, SCHEMA)


@dlt.table(
    name="dp_rule_catalog_pub",
    comment=(
        "Unified declarative data quality rule catalog for Bronze / Silver / "
        "Gold (single source of truth)"
    ),
)
def rule_catalog_tbl():
    """
    Published Delta table version of the rule catalog.

    Materializes dq_rule_catalog so that downstream consumers (e.g. BI tools,
    governance processes) can query the rule definitions directly.
    """
    return dlt.read("dq_rule_catalog")