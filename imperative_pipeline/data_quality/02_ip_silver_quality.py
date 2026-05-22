"""
Silver Data Quality Layer

"""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
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


def empty_string_col(name: str):
    return F.lit(None).cast("string").alias(name)


def build_store_sales_quarantine() -> DataFrame:
    source = spark.table(fq_table("bronze_store_sales"))
    window = (
        Window
        .partitionBy("ss_ticket_number", "ss_item_sk")
        .orderBy(F.col("ingestion_timestamp").desc())
    )

    deduplicated = (
        source
        .filter(
            F.col("ss_ticket_number").isNotNull()
            & F.col("ss_item_sk").isNotNull()
        )
        .select(
            F.col("ss_sold_date_sk").cast("int").alias("ss_sold_date_sk"),
            F.col("ss_sold_time_sk").cast("int").alias("ss_sold_time_sk"),
            F.col("ss_item_sk").cast("int").alias("ss_item_sk"),
            F.col("ss_customer_sk").cast("int").alias("ss_customer_sk"),
            F.col("ss_cdemo_sk").cast("int").alias("ss_cdemo_sk"),
            F.col("ss_hdemo_sk").cast("int").alias("ss_hdemo_sk"),
            F.col("ss_addr_sk").cast("int").alias("ss_addr_sk"),
            F.col("ss_store_sk").cast("int").alias("ss_store_sk"),
            F.col("ss_promo_sk").cast("int").alias("ss_promo_sk"),
            F.col("ss_ticket_number").cast("bigint").alias("ss_ticket_number"),
            F.col("ss_quantity").cast("int").alias("ss_quantity"),
            F.col("ss_wholesale_cost").cast("decimal(7,2)").alias("ss_wholesale_cost"),
            F.col("ss_list_price").cast("decimal(7,2)").alias("ss_list_price"),
            F.col("ss_sales_price").cast("decimal(7,2)").alias("ss_sales_price"),
            F.col("ss_ext_discount_amt").cast("decimal(7,2)").alias("ss_ext_discount_amt"),
            F.col("ss_ext_sales_price").cast("decimal(7,2)").alias("ss_ext_sales_price"),
            F.col("ss_ext_wholesale_cost").cast("decimal(7,2)").alias("ss_ext_wholesale_cost"),
            F.col("ss_ext_list_price").cast("decimal(7,2)").alias("ss_ext_list_price"),
            F.col("ss_ext_tax").cast("decimal(7,2)").alias("ss_ext_tax"),
            F.col("ss_coupon_amt").cast("decimal(7,2)").alias("ss_coupon_amt"),
            F.col("ss_net_paid").cast("decimal(7,2)").alias("ss_net_paid"),
            F.col("ss_net_paid_inc_tax").cast("decimal(7,2)").alias("ss_net_paid_inc_tax"),
            F.col("ss_net_profit").cast("decimal(7,2)").alias("ss_net_profit"),
            "ingestion_timestamp",
            "source_table",
        )
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .alias("d")
    )

    date_dim = spark.table(fq_table("silver_date_dim")).select("d_date_sk").alias("dt")
    item = spark.table(fq_table("silver_item")).select("i_item_sk").alias("i")
    store = spark.table(fq_table("silver_store")).select("s_store_sk").alias("s")
    customer = spark.table(fq_table("silver_customer")).select("c_customer_sk").alias("c")

    checked = (
        deduplicated
        .join(date_dim, F.col("d.ss_sold_date_sk") == F.col("dt.d_date_sk"), "left")
        .join(item, F.col("d.ss_item_sk") == F.col("i.i_item_sk"), "left")
        .join(store, F.col("d.ss_store_sk") == F.col("s.s_store_sk"), "left")
        .join(customer, F.col("d.ss_customer_sk") == F.col("c.c_customer_sk"), "left")
        # Flags für Referenzialität
        .withColumn(
            "has_valid_date_fk",
            F.when(
                F.col("d.ss_sold_date_sk").isNull() | F.col("dt.d_date_sk").isNotNull(),
                1
            ).otherwise(0),
        )
        .withColumn(
            "has_valid_item_fk",
            F.when(F.col("i.i_item_sk").isNotNull(), 1).otherwise(0),
        )
        .withColumn(
            "has_valid_store_fk",
            F.when(
                F.col("d.ss_store_sk").isNull() | F.col("s.s_store_sk").isNotNull(),
                1
            ).otherwise(0),
        )
        .withColumn(
            "has_valid_customer_fk",
            F.when(
                F.col("d.ss_customer_sk").isNull() | F.col("c.c_customer_sk").isNotNull(),
                1
            ).otherwise(0),
        )
    )

    base_cols = [
        "ss_sold_date_sk",
        "ss_sold_time_sk",
        "ss_quantity",
        "ss_sales_price",
        "ss_list_price",
        "ss_wholesale_cost",
        "ss_net_paid",
        "ss_net_profit",
        "ingestion_timestamp",
        "source_table",
    ]

    # DP_SILVER_010 – KEY_VALIDITY: Ticket & Item not null
    rule_010 = (
        checked
        .filter(
            F.col("d.ss_ticket_number").isNull() | F.col("d.ss_item_sk").isNull()
        )
        .select(
            F.current_timestamp().alias("issue_ts"),
            F.lit("imperative").alias("paradigm"),
            F.lit("silver_store_sales").alias("object_name"),
            F.lit("IP_SILVER_010").alias("rule_id"),
            F.lit("KEY_VALIDITY").alias("rule_group"),
            F.col("d.ss_ticket_number").cast("string").alias("key_1"),
            F.col("d.ss_item_sk").cast("string").alias("key_2"),
            F.col("d.ss_store_sk").cast("string").alias("key_3"),
            F.col("d.ss_customer_sk").cast("string").alias("key_4"),
            F.when(F.col("d.ss_ticket_number").isNull(), "NULL_TICKET_NUMBER")
             .when(F.col("d.ss_item_sk").isNull(), "NULL_ITEM_SK")
             .otherwise("UNKNOWN")
             .alias("issue_reason"),
            *[F.col("d." + c) for c in base_cols],
        )
    )

    # DP_SILVER_011 – BUSINESS_VALUE: ss_quantity > 0
    rule_011 = (
        checked
        .filter(
            F.col("d.ss_quantity").isNull() | (F.col("d.ss_quantity") <= 0)
        )
        .select(
            F.current_timestamp().alias("issue_ts"),
            F.lit("imperative").alias("paradigm"),
            F.lit("silver_store_sales").alias("object_name"),
            F.lit("IP_SILVER_011").alias("rule_id"),
            F.lit("BUSINESS_VALUE").alias("rule_group"),
            F.col("d.ss_ticket_number").cast("string").alias("key_1"),
            F.col("d.ss_item_sk").cast("string").alias("key_2"),
            F.col("d.ss_store_sk").cast("string").alias("key_3"),
            F.col("d.ss_customer_sk").cast("string").alias("key_4"),
            F.when(F.col("d.ss_quantity").isNull(), "NULL_QUANTITY")
             .when(F.col("d.ss_quantity") <= 0, "INVALID_QUANTITY")
             .otherwise("UNKNOWN")
             .alias("issue_reason"),
            *[F.col("d." + c) for c in base_cols],
        )
    )

    # DP_SILVER_012 – BUSINESS_VALUE: Preise/Beträge >= 0 oder NULL
    rule_012 = (
        checked
        .filter(
            (F.col("d.ss_sales_price") < 0)
            | (F.col("d.ss_list_price") < 0)
            | (F.col("d.ss_wholesale_cost") < 0)
            | (F.col("d.ss_net_paid") < 0)
        )
        .select(
            F.current_timestamp().alias("issue_ts"),
            F.lit("imperative").alias("paradigm"),
            F.lit("silver_store_sales").alias("object_name"),
            F.lit("IP_SILVER_012").alias("rule_id"),
            F.lit("BUSINESS_VALUE").alias("rule_group"),
            F.col("d.ss_ticket_number").cast("string").alias("key_1"),
            F.col("d.ss_item_sk").cast("string").alias("key_2"),
            F.col("d.ss_store_sk").cast("string").alias("key_3"),
            F.col("d.ss_customer_sk").cast("string").alias("key_4"),
            F.when(F.col("d.ss_sales_price") < 0, "NEGATIVE_SALES_PRICE")
             .when(F.col("d.ss_list_price") < 0, "NEGATIVE_LIST_PRICE")
             .when(F.col("d.ss_wholesale_cost") < 0, "NEGATIVE_WHOLESALE_COST")
             .when(F.col("d.ss_net_paid") < 0, "NEGATIVE_NET_PAID")
             .otherwise("UNKNOWN")
             .alias("issue_reason"),
            *[F.col("d." + c) for c in base_cols],
        )
    )

    # DP_SILVER_013 – REFERENTIAL_VALIDITY: FK-Checks
    rule_013 = (
        checked
        .filter(
            (F.col("has_valid_date_fk") == 0)
            | (F.col("has_valid_item_fk") == 0)
            | (F.col("has_valid_store_fk") == 0)
            | (F.col("has_valid_customer_fk") == 0)
        )
        .select(
            F.current_timestamp().alias("issue_ts"),
            F.lit("imperative").alias("paradigm"),
            F.lit("silver_store_sales").alias("object_name"),
            F.lit("IP_SILVER_013").alias("rule_id"),
            F.lit("REFERENTIAL_VALIDITY").alias("rule_group"),
            F.col("d.ss_ticket_number").cast("string").alias("key_1"),
            F.col("d.ss_item_sk").cast("string").alias("key_2"),
            F.col("d.ss_store_sk").cast("string").alias("key_3"),
            F.col("d.ss_customer_sk").cast("string").alias("key_4"),
            F.when(F.col("has_valid_date_fk") == 0, "INVALID_DATE_FK")
             .when(F.col("has_valid_item_fk") == 0, "INVALID_ITEM_FK")
             .when(F.col("has_valid_store_fk") == 0, "INVALID_STORE_FK")
             .when(F.col("has_valid_customer_fk") == 0, "INVALID_CUSTOMER_FK")
             .otherwise("UNKNOWN")
             .alias("issue_reason"),
            *[F.col("d." + c) for c in base_cols],
        )
    )

    return (
        rule_010
        .unionByName(rule_011)
        .unionByName(rule_012)
        .unionByName(rule_013)
    )


def issue_frame(table_name: str, rule_id: str, rule_group: str, key_exprs: list, reason_expr, condition) -> DataFrame:
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


def build_silver_issues() -> DataFrame:
    null_string = F.lit(None).cast("string")
    frames = [
        issue_frame(
            "silver_date_dim",
            "IP_SILVER_001",
            "KEY_VALIDITY",
            [F.col("d_date_sk"), F.col("d_date_id"), null_string, null_string],
            F.when(F.col("d_date_sk").isNull(), "NULL_DATE_SK")
            .when(F.col("d_date").isNull(), "NULL_DATE")
            .when(F.col("d_year").isNull(), "NULL_YEAR")
            .when(F.col("d_moy").isNull(), "NULL_MONTH")
            .otherwise("UNKNOWN"),
            F.col("d_date_sk").isNull() | F.col("d_date").isNull() | F.col("d_year").isNull() | F.col("d_moy").isNull(),
        ),
        issue_frame(
            "silver_item",
            "IP_SILVER_006",
            "BUSINESS_VALUE",
            [F.col("i_item_sk"), F.col("i_item_id"), null_string, null_string],
            F.when(F.col("i_current_price").isNull(), "NULL_CURRENT_PRICE")
            .when(F.col("i_current_price") < 0, "NEGATIVE_CURRENT_PRICE")
            .when(F.col("i_wholesale_cost") < 0, "NEGATIVE_WHolesale_COST")
            .otherwise("UNKNOWN"),
            F.col("i_current_price").isNull() | (F.col("i_current_price") < 0) | (F.col("i_wholesale_cost") < 0),
        ),
        issue_frame(
            "silver_store",
            "IP_SILVER_008",
            "KEY_VALIDITY",
            [F.col("s_store_sk"), F.col("s_store_id"), F.col("s_store_name"), null_string],
            F.when(F.col("s_store_sk").isNull(), "NULL_STORE_SK")
            .when(F.col("s_store_id").isNull(), "NULL_STORE_ID")
            .when(F.col("s_store_name").isNull(), "NULL_STORE_NAME")
            .otherwise("UNKNOWN"),
            F.col("s_store_sk").isNull() | F.col("s_store_id").isNull() | F.col("s_store_name").isNull(),
        ),
        issue_frame(
            "silver_customer",
            "IP_SILVER_009",
            "KEY_VALIDITY",
            [F.col("c_customer_sk"), F.col("c_customer_id"), null_string, null_string],
            F.when(F.col("c_customer_sk").isNull(), "NULL_CUSTOMER_SK")
            .when(F.col("c_customer_id").isNull(), "NULL_CUSTOMER_ID")
            .otherwise("UNKNOWN"),
            F.col("c_customer_sk").isNull() | F.col("c_customer_id").isNull(),
        ),
        issue_frame(
            "silver_store_sales",
            "IP_SILVER_010",
            "KEY_VALIDITY",
            [F.col("ss_ticket_number"), F.col("ss_item_sk"), F.col("ss_store_sk"), F.col("ss_customer_sk")],
            F.when(F.col("ss_ticket_number").isNull(), "NULL_TICKET_NUMBER")
            .when(F.col("ss_item_sk").isNull(), "NULL_ITEM_SK")
            .when(F.col("ss_quantity").isNull(), "NULL_QUANTITY")
            .when(F.col("ss_quantity") <= 0, "INVALID_QUANTITY")
            .otherwise("UNKNOWN"),
            F.col("ss_ticket_number").isNull() | F.col("ss_item_sk").isNull() | F.col("ss_quantity").isNull() | (F.col("ss_quantity") <= 0),
        ),
        issue_frame(
            "silver_store_sales",
            "IP_SILVER_012",
            "BUSINESS_VALUE",
            [F.col("ss_ticket_number"), F.col("ss_item_sk"), F.col("ss_store_sk"), F.col("ss_customer_sk")],
            F.when(F.col("ss_sales_price") < 0, "NEGATIVE_SALES_PRICE")
            .when(F.col("ss_list_price") < 0, "NEGATIVE_LIST_PRICE")
            .when(F.col("ss_wholesale_cost") < 0, "NEGATIVE_WHOLESALE_COST")
            .when(F.col("ss_net_paid") < 0, "NEGATIVE_NET_PAID")
            .otherwise("UNKNOWN"),
            (F.col("ss_sales_price") < 0) | (F.col("ss_list_price") < 0) | (F.col("ss_wholesale_cost") < 0) | (F.col("ss_net_paid") < 0),
        ),
    ]
    return reduce(DataFrame.unionByName, frames)


def main() -> None:
    quarantine = build_store_sales_quarantine()
    write_delta_table(quarantine, "dq_silver_ip_store_sales_quarantine")
    print(f"Wrote {fq_table('dq_silver_ip_store_sales_quarantine')}: {quarantine.count()} rows")

    issues = build_silver_issues()
    write_delta_table(issues, "dq_silver_ip_issues")
    print(f"Wrote {fq_table('dq_silver_ip_issues')}: {issues.count()} rows")


spark = get_spark()

if __name__ == "__main__":
    main()
