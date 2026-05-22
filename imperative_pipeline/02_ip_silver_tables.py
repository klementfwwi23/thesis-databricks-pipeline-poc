"""
Imperative Silver Layer - pure PySpark implementation

"""

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


def trim_null(col_name: str):
    trimmed = F.trim(F.col(col_name))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def latest_by(df: DataFrame, partition_cols: list[str]) -> DataFrame:
    window = Window.partitionBy(*partition_cols).orderBy(F.col("ingestion_timestamp").desc())
    return df.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")


def build_silver_date_dim() -> DataFrame:
    staged = spark.table(fq_table("bronze_date_dim")).select(
        F.col("d_date_sk").cast("int").alias("d_date_sk"),
        "d_date_id",
        F.col("d_date").cast("date").alias("d_date"),
        F.col("d_month_seq").cast("int").alias("d_month_seq"),
        F.col("d_week_seq").cast("int").alias("d_week_seq"),
        F.col("d_quarter_seq").cast("int").alias("d_quarter_seq"),
        F.col("d_year").cast("int").alias("d_year"),
        F.col("d_dow").cast("int").alias("d_dow"),
        F.col("d_moy").cast("int").alias("d_moy"),
        F.col("d_dom").cast("int").alias("d_dom"),
        F.col("d_qoy").cast("int").alias("d_qoy"),
        F.col("d_fy_year").cast("int").alias("d_fy_year"),
        F.col("d_fy_quarter_seq").cast("int").alias("d_fy_quarter_seq"),
        F.col("d_fy_week_seq").cast("int").alias("d_fy_week_seq"),
        F.trim("d_day_name").alias("d_day_name"),
        F.trim("d_quarter_name").alias("d_quarter_name"),
        "d_holiday",
        "d_weekend",
        "d_following_holiday",
        F.col("d_first_dom").cast("int").alias("d_first_dom"),
        F.col("d_last_dom").cast("int").alias("d_last_dom"),
        F.col("d_same_day_ly").cast("int").alias("d_same_day_ly"),
        F.col("d_same_day_lq").cast("int").alias("d_same_day_lq"),
        "d_current_day",
        "d_current_week",
        "d_current_month",
        "d_current_quarter",
        "d_current_year",
        F.concat(F.col("d_year").cast("string"), F.lit("-"), F.lpad(F.col("d_moy").cast("string"), 2, "0")).alias("year_month"),
        F.when(F.col("d_weekend") == "Y", F.lit(True)).otherwise(F.lit(False)).alias("is_weekend"),
        "ingestion_timestamp",
        "source_table",
    )

    return latest_by(staged, ["d_date_sk"]).filter(
        (F.col("d_date_sk").isNotNull())
        & (F.col("d_date_sk") > 0)
        & (F.col("d_date").isNotNull())
        & (F.col("d_year").isNotNull())
        & (F.col("d_moy").isNotNull())
        & (F.col("d_year").between(1900, 2100))
        & (F.col("d_moy").between(1, 12))
        & (F.col("d_dom").between(1, 31))
        & (F.col("d_dow").between(0, 6))
        & (F.col("d_qoy").between(1, 4))
    )


def build_silver_item() -> DataFrame:
    staged = spark.table(fq_table("bronze_item")).select(
        F.col("i_item_sk").cast("int").alias("i_item_sk"),
        trim_null("i_item_id").alias("i_item_id"),
        F.col("i_rec_start_date").cast("date").alias("i_rec_start_date"),
        F.col("i_rec_end_date").cast("date").alias("i_rec_end_date"),
        trim_null("i_item_desc").alias("i_item_desc"),
        F.col("i_current_price").cast("decimal(7,2)").alias("i_current_price"),
        F.col("i_wholesale_cost").cast("decimal(7,2)").alias("i_wholesale_cost"),
        F.col("i_brand_id").cast("int").alias("i_brand_id"),
        trim_null("i_brand").alias("i_brand"),
        F.col("i_class_id").cast("int").alias("i_class_id"),
        trim_null("i_class").alias("i_class"),
        F.col("i_category_id").cast("int").alias("i_category_id"),
        trim_null("i_category").alias("i_category"),
        F.col("i_manufact_id").cast("int").alias("i_manufact_id"),
        trim_null("i_manufact").alias("i_manufact"),
        trim_null("i_size").alias("i_size"),
        trim_null("i_formulation").alias("i_formulation"),
        trim_null("i_color").alias("i_color"),
        trim_null("i_units").alias("i_units"),
        trim_null("i_container").alias("i_container"),
        F.col("i_manager_id").cast("int").alias("i_manager_id"),
        trim_null("i_product_name").alias("i_product_name"),
        "ingestion_timestamp",
        "source_table",
    )

    return latest_by(staged, ["i_item_sk"]).filter(
        (F.col("i_item_sk").isNotNull())
        & (F.col("i_item_sk") > 0)
        & (F.col("i_item_id").isNotNull())
        & (F.col("i_current_price").isNotNull())
        & (F.col("i_current_price") >= 0)
        & ((F.col("i_wholesale_cost") >= 0) | F.col("i_wholesale_cost").isNull())
    )


def build_silver_store() -> DataFrame:
    staged = spark.table(fq_table("bronze_store")).select(
        F.col("s_store_sk").cast("int").alias("s_store_sk"),
        trim_null("s_store_id").alias("s_store_id"),
        F.col("s_rec_start_date").cast("date").alias("s_rec_start_date"),
        F.col("s_rec_end_date").cast("date").alias("s_rec_end_date"),
        F.col("s_closed_date_sk").cast("int").alias("s_closed_date_sk"),
        trim_null("s_store_name").alias("s_store_name"),
        F.col("s_number_employees").cast("int").alias("s_number_employees"),
        F.col("s_floor_space").cast("int").alias("s_floor_space"),
        trim_null("s_hours").alias("s_hours"),
        trim_null("s_manager").alias("s_manager"),
        F.col("s_market_id").cast("int").alias("s_market_id"),
        trim_null("s_geography_class").alias("s_geography_class"),
        trim_null("s_market_desc").alias("s_market_desc"),
        trim_null("s_market_manager").alias("s_market_manager"),
        F.col("s_division_id").cast("int").alias("s_division_id"),
        trim_null("s_division_name").alias("s_division_name"),
        F.col("s_company_id").cast("int").alias("s_company_id"),
        trim_null("s_company_name").alias("s_company_name"),
        trim_null("s_street_number").alias("s_street_number"),
        trim_null("s_street_name").alias("s_street_name"),
        trim_null("s_street_type").alias("s_street_type"),
        trim_null("s_suite_number").alias("s_suite_number"),
        trim_null("s_city").alias("s_city"),
        trim_null("s_county").alias("s_county"),
        trim_null("s_state").alias("s_state"),
        trim_null("s_zip").alias("s_zip"),
        trim_null("s_country").alias("s_country"),
        F.col("s_gmt_offset").cast("decimal(5,2)").alias("s_gmt_offset"),
        F.col("s_tax_precentage").cast("decimal(5,2)").alias("s_tax_precentage"),
        "ingestion_timestamp",
        "source_table",
    )

    return latest_by(staged, ["s_store_sk"]).filter(
        (F.col("s_store_sk").isNotNull())
        & (F.col("s_store_sk") > 0)
        & (F.col("s_store_id").isNotNull())
        & (F.col("s_store_name").isNotNull())
        & ((F.col("s_number_employees") >= 0) | F.col("s_number_employees").isNull())
        & ((F.col("s_floor_space") >= 0) | F.col("s_floor_space").isNull())
    )


def build_silver_customer() -> DataFrame:
    staged = spark.table(fq_table("bronze_customer")).select(
        F.col("c_customer_sk").cast("int").alias("c_customer_sk"),
        trim_null("c_customer_id").alias("c_customer_id"),
        F.col("c_current_cdemo_sk").cast("int").alias("c_current_cdemo_sk"),
        F.col("c_current_hdemo_sk").cast("int").alias("c_current_hdemo_sk"),
        F.col("c_current_addr_sk").cast("int").alias("c_current_addr_sk"),
        F.col("c_first_shipto_date_sk").cast("int").alias("c_first_shipto_date_sk"),
        F.col("c_first_sales_date_sk").cast("int").alias("c_first_sales_date_sk"),
        trim_null("c_salutation").alias("c_salutation"),
        trim_null("c_first_name").alias("c_first_name"),
        trim_null("c_last_name").alias("c_last_name"),
        "c_preferred_cust_flag",
        F.col("c_birth_day").cast("int").alias("c_birth_day"),
        F.col("c_birth_month").cast("int").alias("c_birth_month"),
        F.col("c_birth_year").cast("int").alias("c_birth_year"),
        trim_null("c_birth_country").alias("c_birth_country"),
        trim_null("c_login").alias("c_login"),
        trim_null("c_email_address").alias("c_email_address"),
        F.col("c_last_review_date_sk").cast("int").alias("c_last_review_date_sk"),
        F.col("c_last_review_date").cast("int").alias("c_last_review_date"),
        "ingestion_timestamp",
        "source_table",
    )

    return latest_by(staged, ["c_customer_sk"]).filter(
        (F.col("c_customer_sk").isNotNull())
        & (F.col("c_customer_sk") > 0)
        & (F.col("c_customer_id").isNotNull())
        & (F.col("c_birth_year").between(1900, 2100) | F.col("c_birth_year").isNull())
        & (F.col("c_birth_month").between(1, 12) | F.col("c_birth_month").isNull())
        & (F.col("c_birth_day").between(1, 31) | F.col("c_birth_day").isNull())
    )


def build_silver_store_sales() -> DataFrame:
    staged = spark.table(fq_table("bronze_store_sales")).select(
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

    latest = latest_by(staged, ["ss_ticket_number", "ss_item_sk"]).alias("l")
    date_dim = spark.table(fq_table("silver_date_dim")).select("d_date_sk").alias("d")
    item = spark.table(fq_table("silver_item")).select("i_item_sk").alias("i")
    store = spark.table(fq_table("silver_store")).select("s_store_sk").alias("s")
    customer = spark.table(fq_table("silver_customer")).select("c_customer_sk").alias("c")

    valid_fk = (
        latest.join(date_dim, F.col("l.ss_sold_date_sk") == F.col("d.d_date_sk"), "left")
        .join(item, F.col("l.ss_item_sk") == F.col("i.i_item_sk"), "left")
        .join(store, F.col("l.ss_store_sk") == F.col("s.s_store_sk"), "left")
        .join(customer, F.col("l.ss_customer_sk") == F.col("c.c_customer_sk"), "left")
        .filter(
            (F.col("l.ss_sold_date_sk").isNull() | F.col("d.d_date_sk").isNotNull())
            & F.col("i.i_item_sk").isNotNull()
            & (F.col("l.ss_store_sk").isNull() | F.col("s.s_store_sk").isNotNull())
            & (F.col("l.ss_customer_sk").isNull() | F.col("c.c_customer_sk").isNotNull())
        )
        .select("l.*")
    )

    return valid_fk.filter(
        (F.col("ss_ticket_number").isNotNull())
        & (F.col("ss_item_sk").isNotNull())
        & (F.col("ss_item_sk") > 0)
        & (F.col("ss_quantity") > 0)
        & ((F.col("ss_sales_price") >= 0) | F.col("ss_sales_price").isNull())
        & ((F.col("ss_list_price") >= 0) | F.col("ss_list_price").isNull())
        & ((F.col("ss_wholesale_cost") >= 0) | F.col("ss_wholesale_cost").isNull())
        & ((F.col("ss_net_paid") >= 0) | F.col("ss_net_paid").isNull())
    )


def main() -> None:
    table_builders = [
        ("silver_date_dim", build_silver_date_dim),
        ("silver_item", build_silver_item),
        ("silver_store", build_silver_store),
        ("silver_customer", build_silver_customer),
        ("silver_store_sales", build_silver_store_sales),
    ]

    for table_name, builder in table_builders:
        df = builder()
        write_delta_table(df, table_name)
        print(f"Wrote {fq_table(table_name)}: {spark.table(fq_table(table_name)).count()} rows")


spark = get_spark()

if __name__ == "__main__":
    main()
