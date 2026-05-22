"""
Declarative Bronze Silver Layer

"""

import dlt
from pyspark.sql import functions as F


def catalog_rule(rule_id: str):
    return (
        dlt.read("dq_rule_catalog")
        .where(F.col("rule_id") == rule_id)
        .select(
            "rule_id",
            "layer",
            "object_name",
            "rule_group",
            "rule_expression",
            "rule_type",
            "validation_mode",
            "severity",
            "owner",
        )
        .withColumn("paradigm", F.lit("declarative"))
    )


def enrich_with_rule(df, rule_id: str):
    return df.crossJoin(catalog_rule(rule_id))


@dlt.view(name="dq_silver_dp_issues", comment="Observed residual issues on declarative silver models derived from unified dq_rule_catalog metadata")
def dq_silver_dp_issues():
    issues_1 = enrich_with_rule(
        dlt.read("silver_date_dim")
        .where(F.col("d_date_sk").isNull() | F.col("d_date").isNull() | F.col("d_year").isNull() | F.col("d_moy").isNull())
        .select(
            F.current_timestamp().alias("observed_at"),
            F.col("d_date_sk").cast("string").alias("key_1"),
            F.col("d_date_id").cast("string").alias("key_2"),
            F.lit(None).cast("string").alias("key_3"),
            F.lit(None).cast("string").alias("key_4"),
            F.when(F.col("d_date_sk").isNull(), F.lit("NULL_DATE_SK"))
             .when(F.col("d_date").isNull(), F.lit("NULL_DATE"))
             .when(F.col("d_year").isNull(), F.lit("NULL_YEAR"))
             .when(F.col("d_moy").isNull(), F.lit("NULL_MONTH"))
             .otherwise(F.lit("UNKNOWN")).alias("issue_reason"),
        ),
        "DP_SILVER_001",
    )
    issues_2 = enrich_with_rule(
        dlt.read("silver_item")
        .where(F.col("i_current_price").isNull() | (F.col("i_current_price") < 0) | (F.col("i_wholesale_cost") < 0))
        .select(
            F.current_timestamp().alias("observed_at"),
            F.col("i_item_sk").cast("string").alias("key_1"),
            F.col("i_item_id").cast("string").alias("key_2"),
            F.lit(None).cast("string").alias("key_3"),
            F.lit(None).cast("string").alias("key_4"),
            F.when(F.col("i_current_price").isNull(), F.lit("NULL_CURRENT_PRICE"))
             .when(F.col("i_current_price") < 0, F.lit("NEGATIVE_CURRENT_PRICE"))
             .when(F.col("i_wholesale_cost") < 0, F.lit("NEGATIVE_WHOLESALE_COST"))
             .otherwise(F.lit("UNKNOWN")).alias("issue_reason"),
        ),
        "DP_SILVER_006",
    )
    issues_3 = enrich_with_rule(
        dlt.read("silver_store")
        .where(F.col("s_store_sk").isNull() | F.col("s_store_id").isNull() | F.col("s_store_name").isNull())
        .select(
            F.current_timestamp().alias("observed_at"),
            F.col("s_store_sk").cast("string").alias("key_1"),
            F.col("s_store_id").cast("string").alias("key_2"),
            F.col("s_store_name").cast("string").alias("key_3"),
            F.lit(None).cast("string").alias("key_4"),
            F.when(F.col("s_store_sk").isNull(), F.lit("NULL_STORE_SK"))
             .when(F.col("s_store_id").isNull(), F.lit("NULL_STORE_ID"))
             .when(F.col("s_store_name").isNull(), F.lit("NULL_STORE_NAME"))
             .otherwise(F.lit("UNKNOWN")).alias("issue_reason"),
        ),
        "DP_SILVER_008",
    )
    issues_4 = enrich_with_rule(
        dlt.read("silver_customer")
        .where(F.col("c_customer_sk").isNull() | F.col("c_customer_id").isNull())
        .select(
            F.current_timestamp().alias("observed_at"),
            F.col("c_customer_sk").cast("string").alias("key_1"),
            F.col("c_customer_id").cast("string").alias("key_2"),
            F.lit(None).cast("string").alias("key_3"),
            F.lit(None).cast("string").alias("key_4"),
            F.when(F.col("c_customer_sk").isNull(), F.lit("NULL_CUSTOMER_SK"))
             .when(F.col("c_customer_id").isNull(), F.lit("NULL_CUSTOMER_ID"))
             .otherwise(F.lit("UNKNOWN")).alias("issue_reason"),
        ),
        "DP_SILVER_009",
    )
    issues_5 = enrich_with_rule(
        dlt.read("silver_store_sales")
        .where(F.col("ss_ticket_number").isNull() | F.col("ss_item_sk").isNull() | F.col("ss_quantity").isNull() | (F.col("ss_quantity") <= 0))
        .select(
            F.current_timestamp().alias("observed_at"),
            F.col("ss_ticket_number").cast("string").alias("key_1"),
            F.col("ss_item_sk").cast("string").alias("key_2"),
            F.col("ss_store_sk").cast("string").alias("key_3"),
            F.col("ss_customer_sk").cast("string").alias("key_4"),
            F.when(F.col("ss_ticket_number").isNull(), F.lit("NULL_TICKET_NUMBER"))
             .when(F.col("ss_item_sk").isNull(), F.lit("NULL_ITEM_SK"))
             .when(F.col("ss_quantity").isNull(), F.lit("NULL_QUANTITY"))
             .when(F.col("ss_quantity") <= 0, F.lit("INVALID_QUANTITY"))
             .otherwise(F.lit("UNKNOWN")).alias("issue_reason"),
        ),
        "DP_SILVER_010",
    )
    issues_6 = enrich_with_rule(
        dlt.read("silver_store_sales")
        .where((F.col("ss_sales_price") < 0) | (F.col("ss_list_price") < 0) | (F.col("ss_wholesale_cost") < 0) | (F.col("ss_net_paid") < 0))
        .select(
            F.current_timestamp().alias("observed_at"),
            F.col("ss_ticket_number").cast("string").alias("key_1"),
            F.col("ss_item_sk").cast("string").alias("key_2"),
            F.col("ss_store_sk").cast("string").alias("key_3"),
            F.col("ss_customer_sk").cast("string").alias("key_4"),
            F.when(F.col("ss_sales_price") < 0, F.lit("NEGATIVE_SALES_PRICE"))
             .when(F.col("ss_list_price") < 0, F.lit("NEGATIVE_LIST_PRICE"))
             .when(F.col("ss_wholesale_cost") < 0, F.lit("NEGATIVE_WHOLESALE_COST"))
             .when(F.col("ss_net_paid") < 0, F.lit("NEGATIVE_NET_PAID"))
             .otherwise(F.lit("UNKNOWN")).alias("issue_reason"),
        ),
        "DP_SILVER_012",
    )
    return issues_1.unionByName(issues_2).unionByName(issues_3).unionByName(issues_4).unionByName(issues_5).unionByName(issues_6).select(
        "observed_at", "paradigm", "object_name", "rule_id", "rule_group", "key_1", "key_2", "key_3", "key_4", "issue_reason"
    )


@dlt.view(
    name="dq_silver_dp_rule_status",
    comment="Rule status view derived from unified dq_rule_catalog for silver",
)
def dq_silver_dp_rule_status():
    rules = (
        dlt.read("dq_rule_catalog")
        .where(F.col("layer") == "silver")
        .where(F.col("rule_type") == "EXPECT")
    )

    outputs = []

    for row in rules.collect():
        rule_id = row["rule_id"]
        object_name = row["object_name"]
        expr = row["rule_expression"]

        df = dlt.read(object_name).where(F.expr(expr))

        agg = df.agg(F.count(F.lit(1)).alias("violating_rows")).withColumn(
            "rule_id", F.lit(rule_id)
        )

        enriched = (
            agg.join(catalog_rule(rule_id), on="rule_id", how="inner")
            .withColumn("observed_at", F.current_timestamp())
            .withColumn(
                "rule_status",
                F.when(F.col("violating_rows") == 0, F.lit("PASS")).otherwise(
                    F.lit("FAIL")
                ),
            )
        )

        outputs.append(enriched)

    result = outputs[0]
    for df_out in outputs[1:]:
        result = result.unionByName(df_out)

    return result.select(
        "observed_at",
        "paradigm",
        "layer",
        "object_name",
        "rule_id",
        "rule_group",
        "rule_status",
        "violating_rows",
    )

#catalog 
@dlt.table(
    name="dq_silver_dp_issues_pub",
    comment="Persistierte Silver DQ Issues für UC/BI und Vergleich"
)
def dq_silver_dp_issues_pub():
    return dlt.read("dq_silver_dp_issues")

@dlt.table(
    name="dq_silver_dp_rule_status_pub",
    comment="Persistierte Silver DQ Issues für UC/BI und Vergleich"
)
def dq_silver_dp_rule_status_pub():
    return dlt.read("dq_silver_dp_rule_status")