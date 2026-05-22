"""
Declarative Gold Qualtity Layer

"""

import dlt
from pyspark.sql import functions as F


def gold_catalog_rule(rule_id: str):
    return dlt.read("dq_rule_catalog").where(F.col("rule_id") == rule_id)


@dlt.view(name="dq_gold_dp_issues", comment="Declarative gold DQ issues observed on materialized gold models using unified dq_rule_catalog metadata")
def dq_gold_dp_issues():
    i1 = gold_catalog_rule("DP_GOLD_001").crossJoin(
        dlt.read("gold_dim_item")
        .where((F.col("current_price") < 0) | (F.col("wholesale_cost") < 0))
        .select(
            F.current_timestamp().alias("observed_at"), F.lit("declarative").alias("paradigm"),
            F.col("item_key").cast("string").alias("key_1"), F.col("item_id").cast("string").alias("key_2"),
            F.lit(None).cast("string").alias("key_3"), F.lit(None).cast("string").alias("key_4"),
            F.when(F.col("current_price") < 0, F.lit("NEGATIVE_CURRENT_PRICE")).when(F.col("wholesale_cost") < 0, F.lit("NEGATIVE_WHOLESALE_COST")).otherwise(F.lit("UNKNOWN")).alias("issue_reason")
        )
    )
    i2 = gold_catalog_rule("DP_GOLD_002").crossJoin(
        dlt.read("gold_dim_store")
        .where(F.col("store_id").isNull() | F.col("store_name").isNull())
        .select(
            F.current_timestamp().alias("observed_at"), F.lit("declarative").alias("paradigm"),
            F.col("store_key").cast("string").alias("key_1"), F.col("store_id").cast("string").alias("key_2"),
            F.col("store_name").cast("string").alias("key_3"), F.lit(None).cast("string").alias("key_4"),
            F.when(F.col("store_id").isNull(), F.lit("NULL_STORE_ID")).when(F.col("store_name").isNull(), F.lit("NULL_STORE_NAME")).otherwise(F.lit("UNKNOWN")).alias("issue_reason")
        )
    )
    i3 = gold_catalog_rule("DP_GOLD_003").crossJoin(
        dlt.read("gold_dim_customer")
        .where(F.col("customer_id").isNull() | F.col("email_address").isNull())
        .select(
            F.current_timestamp().alias("observed_at"), F.lit("declarative").alias("paradigm"),
            F.col("customer_key").cast("string").alias("key_1"), F.col("customer_id").cast("string").alias("key_2"),
            F.col("email_address").cast("string").alias("key_3"), F.lit(None).cast("string").alias("key_4"),
            F.when(F.col("customer_id").isNull(), F.lit("NULL_CUSTOMER_ID")).when(F.col("email_address").isNull(), F.lit("NULL_EMAIL_ADDRESS")).otherwise(F.lit("UNKNOWN")).alias("issue_reason")
        )
    )
    i4 = gold_catalog_rule("DP_GOLD_004").crossJoin(
        dlt.read("gold_fact_store_sales")
        .where(F.col("net_profit") < 0)
        .select(
            F.current_timestamp().alias("observed_at"), F.lit("declarative").alias("paradigm"),
            F.col("ticket_number").cast("string").alias("key_1"), F.col("item_key").cast("string").alias("key_2"),
            F.col("store_key").cast("string").alias("key_3"), F.col("customer_key").cast("string").alias("key_4"),
            F.lit("NEGATIVE_NET_PROFIT").alias("issue_reason")
        )
    )
    i5 = gold_catalog_rule("DP_GOLD_005").crossJoin(
        dlt.read("gold_fact_store_sales")
        .where(F.col("sold_date_key").isNull() | F.col("item_key").isNull() | F.col("store_key").isNull() | F.col("customer_key").isNull())
        .select(
            F.current_timestamp().alias("observed_at"), F.lit("declarative").alias("paradigm"),
            F.col("ticket_number").cast("string").alias("key_1"), F.col("item_key").cast("string").alias("key_2"),
            F.col("store_key").cast("string").alias("key_3"), F.col("customer_key").cast("string").alias("key_4"),
            F.when(F.col("sold_date_key").isNull(), F.lit("NULL_SOLD_DATE_KEY")).when(F.col("item_key").isNull(), F.lit("NULL_ITEM_KEY")).when(F.col("store_key").isNull(), F.lit("NULL_STORE_KEY")).when(F.col("customer_key").isNull(), F.lit("NULL_CUSTOMER_KEY")).otherwise(F.lit("UNKNOWN")).alias("issue_reason")
        )
    )
    i6 = gold_catalog_rule("DP_GOLD_006").crossJoin(
        dlt.read("gold_top_items_month")
        .where(F.col("year").isNull() | F.col("month").isNull() | F.col("item_key").isNull())
        .select(
            F.current_timestamp().alias("observed_at"), F.lit("declarative").alias("paradigm"),
            F.col("year").cast("string").alias("key_1"), F.col("month").cast("string").alias("key_2"),
            F.col("item_key").cast("string").alias("key_3"), F.col("item_id").cast("string").alias("key_4"),
            F.when(F.col("year").isNull(), F.lit("NULL_YEAR")).when(F.col("month").isNull(), F.lit("NULL_MONTH")).when(F.col("item_key").isNull(), F.lit("NULL_ITEM_KEY")).otherwise(F.lit("UNKNOWN")).alias("issue_reason")
        )
    )
    result = i1.unionByName(i2).unionByName(i3).unionByName(i4).unionByName(i5).unionByName(i6)
    return result.select("observed_at", "paradigm", "object_name", "rule_id", "rule_group", "key_1", "key_2", "key_3", "key_4", "issue_reason")


@dlt.view(name="dq_gold_dp_metrics", comment="Declarative gold DQ observability metrics aligned with unified dq_rule_catalog metadata")
def dq_gold_dp_metrics():
    metrics = [
        dlt.read("gold_dim_date").agg(F.count(F.lit(1)).alias("row_count"), F.sum(F.when(F.col("date_key").isNull() | F.col("calendar_date").isNull(), 1).otherwise(0)).alias("issue_count")).withColumn("negative_value_count", F.lit(0)).withColumn("null_business_key_count", F.col("issue_count")).withColumn("object_name", F.lit("gold_dim_date")).withColumn("object_type", F.lit("MATERIALIZED_VIEW")).withColumn("source_layer", F.lit("mv_from_silver")),
        dlt.read("gold_dim_item").agg(F.count(F.lit(1)).alias("row_count"), F.sum(F.when((F.col("current_price") < 0) | (F.col("wholesale_cost") < 0), 1).otherwise(0)).alias("issue_count")).withColumn("negative_value_count", F.col("issue_count")).withColumn("null_business_key_count", F.lit(0)).withColumn("object_name", F.lit("gold_dim_item")).withColumn("object_type", F.lit("MATERIALIZED_VIEW")).withColumn("source_layer", F.lit("mv_from_silver")),
        dlt.read("gold_dim_store").agg(F.count(F.lit(1)).alias("row_count"), F.sum(F.when(F.col("store_id").isNull() | F.col("store_name").isNull(), 1).otherwise(0)).alias("issue_count")).withColumn("negative_value_count", F.lit(0)).withColumn("null_business_key_count", F.col("issue_count")).withColumn("object_name", F.lit("gold_dim_store")).withColumn("object_type", F.lit("MATERIALIZED_VIEW")).withColumn("source_layer", F.lit("mv_from_silver")),
        dlt.read("gold_dim_customer").agg(F.count(F.lit(1)).alias("row_count"), F.sum(F.when(F.col("customer_id").isNull() | F.col("email_address").isNull(), 1).otherwise(0)).alias("issue_count")).withColumn("negative_value_count", F.lit(0)).withColumn("null_business_key_count", F.col("issue_count")).withColumn("object_name", F.lit("gold_dim_customer")).withColumn("object_type", F.lit("MATERIALIZED_VIEW")).withColumn("source_layer", F.lit("mv_from_silver")),
        dlt.read("gold_fact_store_sales").agg(F.count(F.lit(1)).alias("row_count"), F.sum(F.when((F.col("net_profit") < 0) | F.col("sold_date_key").isNull() | F.col("item_key").isNull() | F.col("store_key").isNull() | F.col("customer_key").isNull(), 1).otherwise(0)).alias("issue_count"), F.sum(F.when(F.col("net_profit") < 0, 1).otherwise(0)).alias("negative_value_count"), F.sum(F.when(F.col("sold_date_key").isNull() | F.col("item_key").isNull() | F.col("store_key").isNull() | F.col("customer_key").isNull(), 1).otherwise(0)).alias("null_business_key_count")).withColumn("object_name", F.lit("gold_fact_store_sales")).withColumn("object_type", F.lit("MATERIALIZED_VIEW")).withColumn("source_layer", F.lit("mv_from_silver")),
        dlt.read("gold_top_items_month").agg(F.count(F.lit(1)).alias("row_count"), F.sum(F.when(F.col("year").isNull() | F.col("month").isNull() | F.col("item_key").isNull(), 1).otherwise(0)).alias("issue_count")).withColumn("negative_value_count", F.lit(0)).withColumn("null_business_key_count", F.col("issue_count")).withColumn("object_name", F.lit("gold_top_items_month")).withColumn("object_type", F.lit("MATERIALIZED_VIEW")).withColumn("source_layer", F.lit("mv_from_silver")),
    ]
    result = metrics[0]
    for df in metrics[1:]:
        result = result.unionByName(df)
    result = result.withColumn("metric_ts", F.current_timestamp()).withColumn("paradigm", F.lit("declarative"))
    return result.join(dlt.read("dq_rule_catalog").where(F.col("layer") == "gold"), on="object_name", how="inner").select(
        "metric_ts", "paradigm", "object_name", "object_type", "row_count", "issue_count", "negative_value_count", "null_business_key_count", "validation_mode", "source_layer", "rule_id", "rule_group", "rule_expression", "rule_type", "severity", "owner"
    )

#catalog
@dlt.table(
    name="dq_gold_dp_issues_pub",
    comment="Persistierte Gold DQ Issues für UC/BI und Vergleich"
)
def dq_gold_dp_issues_pub():
    return dlt.read("dq_gold_dp_issues")

@dlt.table(
    name="dq_gold_dp_metrics_pub",
    comment="Persistierte Gold DQ Issues für UC/BI und Vergleich"
)
def dq_gold_dp_metrics_pub():
    return dlt.read("dq_gold_dp_metrics")
