"""
Declarative Silver Layer

Transforms bronze tables into standardized, de-duplicated and validated
silver models by applying DLT expectations, type casting, trimming, and
referential filtering against dimension tables.
"""


import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window



def trim_to_null(column_name: str):
    # Normalizes empty strings to NULL while trimming surrounding whitespace.
    return F.when(F.trim(F.col(column_name)) == "", F.lit(None)).otherwise(F.trim(F.col(column_name)))



def latest_by_keys(df, keys):
    # Keeps only the latest record per logical key based on ingestion_timestamp.
    window_spec = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(F.col("ingestion_timestamp").desc())
    return df.withColumn("rn", F.row_number().over(window_spec)).where(F.col("rn") == 1).drop("rn")



@dlt.table(name="silver_date_dim", comment="Silver layer - standardized and validated date dimension (declarative)")
@dlt.expect_or_drop("DP_SILVER_001", "d_date_sk IS NOT NULL")
@dlt.expect_or_drop("DP_SILVER_002", "d_date IS NOT NULL")
@dlt.expect_or_drop("DP_SILVER_003", "d_year BETWEEN 1900 AND 2100")
@dlt.expect_or_drop("DP_SILVER_004", "d_moy BETWEEN 1 AND 12")
@dlt.expect_or_drop("valid_day_of_month", "d_dom BETWEEN 1 AND 31")
@dlt.expect_or_drop("valid_day_of_week", "d_dow BETWEEN 0 AND 6")
@dlt.expect_or_drop("valid_quarter", "d_qoy BETWEEN 1 AND 4")
@dlt.expect("valid_holiday_flag", "d_holiday IN ('Y', 'N')")
@dlt.expect("valid_weekend_flag", "d_weekend IN ('Y', 'N')")
def silver_date_dim():
    # Type-casts and derives standardized date attributes, then keeps the latest record per d_date_sk.
    staged = (
        dlt.read("bronze_date_dim")
        .select(
            F.col("d_date_sk").cast("int").alias("d_date_sk"),
            F.col("d_date_id"),
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
            F.trim(F.col("d_day_name")).alias("d_day_name"),
            F.trim(F.col("d_quarter_name")).alias("d_quarter_name"),
            F.col("d_holiday"),
            F.col("d_weekend"),
            F.col("d_following_holiday"),
            F.col("d_first_dom").cast("int").alias("d_first_dom"),
            F.col("d_last_dom").cast("int").alias("d_last_dom"),
            F.col("d_same_day_ly").cast("int").alias("d_same_day_ly"),
            F.col("d_same_day_lq").cast("int").alias("d_same_day_lq"),
            F.col("d_current_day"),
            F.col("d_current_week"),
            F.col("d_current_month"),
            F.col("d_current_quarter"),
            F.col("d_current_year"),
            F.concat(F.col("d_year").cast("string"), F.lit("-"), F.lpad(F.col("d_moy").cast("string"), 2, "0")).alias("year_month"),
            F.when(F.col("d_weekend") == "Y", F.lit(True)).otherwise(F.lit(False)).alias("is_weekend"),
            F.col("ingestion_timestamp"),
            F.col("source_table"),
        )
    )
    return latest_by_keys(staged, ["d_date_sk"])



@dlt.table(name="silver_item", comment="Silver layer - standardized and validated item dimension (declarative)")
@dlt.expect_or_drop("DP_SILVER_005", "i_item_sk IS NOT NULL")
@dlt.expect_or_drop("pk_item_sk_positive", "i_item_sk > 0")
@dlt.expect_or_drop("DP_SILVER_006", "i_current_price IS NOT NULL AND i_current_price >= 0")
@dlt.expect_or_drop("DP_SILVER_007", "i_wholesale_cost >= 0 OR i_wholesale_cost IS NULL")
@dlt.expect_or_drop("nn_item_id", "i_item_id IS NOT NULL")
@dlt.expect("valid_date_range", "i_rec_end_date IS NULL OR i_rec_end_date >= i_rec_start_date")
def silver_item():
    # Cleans string attributes, enforces price rules, and resolves latest version per item key.
    staged = (
        dlt.read("bronze_item")
        .select(
            F.col("i_item_sk").cast("int").alias("i_item_sk"),
            trim_to_null("i_item_id").alias("i_item_id"),
            F.col("i_rec_start_date").cast("date").alias("i_rec_start_date"),
            F.col("i_rec_end_date").cast("date").alias("i_rec_end_date"),
            trim_to_null("i_item_desc").alias("i_item_desc"),
            F.col("i_current_price").cast("decimal(7,2)").alias("i_current_price"),
            F.col("i_wholesale_cost").cast("decimal(7,2)").alias("i_wholesale_cost"),
            F.col("i_brand_id").cast("int").alias("i_brand_id"),
            trim_to_null("i_brand").alias("i_brand"),
            F.col("i_class_id").cast("int").alias("i_class_id"),
            trim_to_null("i_class").alias("i_class"),
            F.col("i_category_id").cast("int").alias("i_category_id"),
            trim_to_null("i_category").alias("i_category"),
            F.col("i_manufact_id").cast("int").alias("i_manufact_id"),
            trim_to_null("i_manufact").alias("i_manufact"),
            trim_to_null("i_size").alias("i_size"),
            trim_to_null("i_formulation").alias("i_formulation"),
            trim_to_null("i_color").alias("i_color"),
            trim_to_null("i_units").alias("i_units"),
            trim_to_null("i_container").alias("i_container"),
            F.col("i_manager_id").cast("int").alias("i_manager_id"),
            trim_to_null("i_product_name").alias("i_product_name"),
            F.col("ingestion_timestamp"),
            F.col("source_table"),
        )
    )
    return latest_by_keys(staged, ["i_item_sk"])



@dlt.table(name="silver_store", comment="Silver layer - standardized and validated store dimension (declarative)")
@dlt.expect_or_drop("DP_SILVER_008", "s_store_sk IS NOT NULL")
@dlt.expect_or_drop("pk_store_sk_positive", "s_store_sk > 0")
@dlt.expect_or_drop("DP_SILVER_008_b", "s_store_id IS NOT NULL AND s_store_name IS NOT NULL")
@dlt.expect_or_drop("valid_employees", "s_number_employees >= 0 OR s_number_employees IS NULL")
@dlt.expect_or_drop("valid_floor_space", "s_floor_space >= 0 OR s_floor_space IS NULL")
@dlt.expect("valid_tax_pct", "s_tax_precentage BETWEEN 0 AND 1 OR s_tax_precentage IS NULL")
@dlt.expect("valid_gmt_offset", "s_gmt_offset BETWEEN -12 AND 14 OR s_gmt_offset IS NULL")
@dlt.expect("valid_date_range", "s_rec_end_date IS NULL OR s_rec_end_date >= s_rec_start_date")
def silver_store():
    # Standardizes store attributes and applies basic sanity checks before keeping the latest version per store.
    staged = (
        dlt.read("bronze_store")
        .select(
            F.col("s_store_sk").cast("int").alias("s_store_sk"),
            trim_to_null("s_store_id").alias("s_store_id"),
            F.col("s_rec_start_date").cast("date").alias("s_rec_start_date"),
            F.col("s_rec_end_date").cast("date").alias("s_rec_end_date"),
            F.col("s_closed_date_sk").cast("int").alias("s_closed_date_sk"),
            trim_to_null("s_store_name").alias("s_store_name"),
            F.col("s_number_employees").cast("int").alias("s_number_employees"),
            F.col("s_floor_space").cast("int").alias("s_floor_space"),
            trim_to_null("s_hours").alias("s_hours"),
            trim_to_null("s_manager").alias("s_manager"),
            F.col("s_market_id").cast("int").alias("s_market_id"),
            trim_to_null("s_geography_class").alias("s_geography_class"),
            trim_to_null("s_market_desc").alias("s_market_desc"),
            trim_to_null("s_market_manager").alias("s_market_manager"),
            F.col("s_division_id").cast("int").alias("s_division_id"),
            trim_to_null("s_division_name").alias("s_division_name"),
            F.col("s_company_id").cast("int").alias("s_company_id"),
            trim_to_null("s_company_name").alias("s_company_name"),
            trim_to_null("s_street_number").alias("s_street_number"),
            trim_to_null("s_street_name").alias("s_street_name"),
            trim_to_null("s_street_type").alias("s_street_type"),
            trim_to_null("s_suite_number").alias("s_suite_number"),
            trim_to_null("s_city").alias("s_city"),
            trim_to_null("s_county").alias("s_county"),
            trim_to_null("s_state").alias("s_state"),
            trim_to_null("s_zip").alias("s_zip"),
            trim_to_null("s_country").alias("s_country"),
            F.col("s_gmt_offset").cast("decimal(5,2)").alias("s_gmt_offset"),
            F.col("s_tax_precentage").cast("decimal(5,2)").alias("s_tax_precentage"),
            F.col("ingestion_timestamp"),
            F.col("source_table"),
        )
    )
    return latest_by_keys(staged, ["s_store_sk"])



@dlt.table(name="silver_customer", comment="Silver layer - standardized and validated customer dimension (declarative)")
@dlt.expect_or_drop("DP_SILVER_009", "c_customer_sk IS NOT NULL")
@dlt.expect_or_drop("pk_customer_sk_positive", "c_customer_sk > 0")
@dlt.expect_or_drop("DP_SILVER_009_b", "c_customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_birth_year", "c_birth_year BETWEEN 1900 AND 2100 OR c_birth_year IS NULL")
@dlt.expect_or_drop("valid_birth_month", "c_birth_month BETWEEN 1 AND 12 OR c_birth_month IS NULL")
@dlt.expect_or_drop("valid_birth_day", "c_birth_day BETWEEN 1 AND 31 OR c_birth_day IS NULL")
def silver_customer():
    # Cleans customer attributes and enforces basic key and birth-date validity before deduplication.
    staged = (
        dlt.read("bronze_customer")
        .select(
            F.col("c_customer_sk").cast("int").alias("c_customer_sk"),
            trim_to_null("c_customer_id").alias("c_customer_id"),
            F.col("c_current_cdemo_sk").cast("int").alias("c_current_cdemo_sk"),
            F.col("c_current_hdemo_sk").cast("int").alias("c_current_hdemo_sk"),
            F.col("c_current_addr_sk").cast("int").alias("c_current_addr_sk"),
            F.col("c_first_shipto_date_sk").cast("int").alias("c_first_shipto_date_sk"),
            F.col("c_first_sales_date_sk").cast("int").alias("c_first_sales_date_sk"),
            trim_to_null("c_salutation").alias("c_salutation"),
            trim_to_null("c_first_name").alias("c_first_name"),
            trim_to_null("c_last_name").alias("c_last_name"),
            F.col("c_preferred_cust_flag"),
            F.col("c_birth_day").cast("int").alias("c_birth_day"),
            F.col("c_birth_month").cast("int").alias("c_birth_month"),
            F.col("c_birth_year").cast("int").alias("c_birth_year"),
            trim_to_null("c_birth_country").alias("c_birth_country"),
            trim_to_null("c_login").alias("c_login"),
            trim_to_null("c_email_address").alias("c_email_address"),
            F.col("c_last_review_date_sk").cast("int").alias("c_last_review_date_sk"),
            F.col("c_last_review_date").cast("int").alias("c_last_review_date"),
            F.col("ingestion_timestamp"),
            F.col("source_table"),
        )
    )
    return latest_by_keys(staged, ["c_customer_sk"])



@dlt.table(name="silver_store_sales", comment="Silver layer - standardized, deduplicated and referentially filtered store sales fact (declarative)")
@dlt.expect_or_drop("DP_SILVER_010", "ss_ticket_number IS NOT NULL")
@dlt.expect_or_drop("DP_SILVER_010_b", "ss_item_sk IS NOT NULL")
@dlt.expect_or_drop("fk_item_sk_positive", "ss_item_sk > 0")
@dlt.expect_or_drop("DP_SILVER_011", "ss_quantity > 0")
@dlt.expect_or_drop("non_negative_sales_price", "ss_sales_price >= 0 OR ss_sales_price IS NULL")
@dlt.expect_or_drop("non_negative_list_price", "ss_list_price >= 0 OR ss_list_price IS NULL")
@dlt.expect_or_drop("non_negative_wholesale_cost", "ss_wholesale_cost >= 0 OR ss_wholesale_cost IS NULL")
@dlt.expect_or_drop("non_negative_net_paid", "ss_net_paid >= 0 OR ss_net_paid IS NULL")
@dlt.expect_or_drop("sales_price_reasonable", "ss_sales_price <= ss_list_price * 1.5 OR ss_sales_price IS NULL OR ss_list_price IS NULL")
def silver_store_sales():
    # Standardizes fact schema, resolves latest records, and filters to rows with valid dimensional references.
    staged = (
        dlt.read("bronze_store_sales")
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
            F.col("ingestion_timestamp"),
            F.col("source_table"),
        )
    )

    # Deduplicate by ticket + item, then enforce referential integrity against silver dimensions.
    latest_only = latest_by_keys(staged, ["ss_ticket_number", "ss_item_sk"])
    d = dlt.read("silver_date_dim").select(F.col("d_date_sk"))
    i = dlt.read("silver_item").select(F.col("i_item_sk"))
    s = dlt.read("silver_store").select(F.col("s_store_sk"))
    c = dlt.read("silver_customer").select(F.col("c_customer_sk"))

    return (
        latest_only.alias("l")
        .join(d.alias("d"), F.col("l.ss_sold_date_sk") == F.col("d.d_date_sk"), "left")
        .join(i.alias("i"), F.col("l.ss_item_sk") == F.col("i.i_item_sk"), "left")
        .join(s.alias("s"), F.col("l.ss_store_sk") == F.col("s.s_store_sk"), "left")
        .join(c.alias("c"), F.col("l.ss_customer_sk") == F.col("c.c_customer_sk"), "left")
        # Allow NULL date/store/customer keys only if there is no corresponding dimension row,
        # while requiring a valid item reference for all facts.
        .where((F.col("l.ss_sold_date_sk").isNull()) | (F.col("d.d_date_sk").isNotNull()))
        .where(F.col("i.i_item_sk").isNotNull())
        .where((F.col("l.ss_store_sk").isNull()) | (F.col("s.s_store_sk").isNotNull()))
        .where((F.col("l.ss_customer_sk").isNull()) | (F.col("c.c_customer_sk").isNotNull()))
        .select("l.*")
    )