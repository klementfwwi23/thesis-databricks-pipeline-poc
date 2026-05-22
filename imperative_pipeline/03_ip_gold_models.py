"""
Imperative Gold Layer

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


def nullif_zero(column):
    return F.when(column == 0, F.lit(None)).otherwise(column)


def build_gold_dim_date() -> DataFrame:
    return spark.table(fq_table("silver_date_dim")).select(
        F.col("d_date_sk").alias("date_key"),
        F.col("d_date_id").alias("date_id"),
        F.col("d_date").alias("calendar_date"),
        F.col("d_year").alias("year"),
        F.col("d_qoy").alias("quarter"),
        F.col("d_moy").alias("month"),
        F.col("d_dom").alias("day_of_month"),
        F.col("d_dow").alias("day_of_week"),
        F.col("d_day_name").alias("day_name"),
        F.col("d_quarter_name").alias("quarter_name"),
        F.col("d_fy_year").alias("fiscal_year"),
        F.col("d_fy_quarter_seq").alias("fiscal_quarter_seq"),
        F.col("d_fy_week_seq").alias("fiscal_week_seq"),
        F.col("d_month_seq").alias("month_seq"),
        F.col("d_week_seq").alias("week_seq"),
        F.col("d_quarter_seq").alias("quarter_seq"),
        "year_month",
        F.concat(F.lit("Q"), F.col("d_qoy").cast("string"), F.lit(" "), F.col("d_year").cast("string")).alias("quarter_year"),
        F.when(F.col("d_holiday") == "Y", True).otherwise(False).alias("is_holiday"),
        "is_weekend",
        F.when(F.col("d_following_holiday") == "Y", True).otherwise(False).alias("is_following_holiday"),
        F.when(F.col("d_current_day") == "Y", True).otherwise(False).alias("is_current_day"),
        F.when(F.col("d_current_week") == "Y", True).otherwise(False).alias("is_current_week"),
        F.when(F.col("d_current_month") == "Y", True).otherwise(False).alias("is_current_month"),
        F.when(F.col("d_current_quarter") == "Y", True).otherwise(False).alias("is_current_quarter"),
        F.when(F.col("d_current_year") == "Y", True).otherwise(False).alias("is_current_year"),
        F.col("d_same_day_ly").alias("same_day_last_year_key"),
        F.col("d_same_day_lq").alias("same_day_last_quarter_key"),
        F.col("ingestion_timestamp").alias("source_timestamp"),
    )


def build_gold_dim_item() -> DataFrame:
    return spark.table(fq_table("silver_item")).select(
        F.col("i_item_sk").alias("item_key"),
        F.col("i_item_id").alias("item_id"),
        F.col("i_item_desc").alias("item_description"),
        F.col("i_product_name").alias("product_name"),
        F.col("i_current_price").alias("current_price"),
        F.col("i_wholesale_cost").alias("wholesale_cost"),
        F.when(F.col("i_current_price") < 10, "Low (< 10)")
        .when(F.col("i_current_price") < 50, "Medium (10-50)")
        .when(F.col("i_current_price") < 100, "High (50-100)")
        .otherwise("Premium (>= 100)")
        .alias("price_category"),
        F.round((F.col("i_current_price") - F.col("i_wholesale_cost")) / nullif_zero(F.col("i_current_price")) * 100, 2).alias("margin_pct"),
        F.col("i_brand_id").alias("brand_id"),
        F.col("i_brand").alias("brand_name"),
        F.col("i_manufact_id").alias("manufacturer_id"),
        F.col("i_manufact").alias("manufacturer_name"),
        F.col("i_category_id").alias("category_id"),
        F.col("i_category").alias("category_name"),
        F.col("i_class_id").alias("class_id"),
        F.col("i_class").alias("class_name"),
        F.col("i_size").alias("size"),
        F.col("i_color").alias("color"),
        F.col("i_units").alias("unit_of_measure"),
        F.col("i_container").alias("container_type"),
        F.col("i_formulation").alias("formulation"),
        F.col("i_manager_id").alias("manager_id"),
        F.col("i_rec_start_date").alias("valid_from"),
        F.col("i_rec_end_date").alias("valid_to"),
        F.when(F.col("i_rec_end_date").isNull(), True).otherwise(False).alias("is_current"),
        F.col("ingestion_timestamp").alias("source_timestamp"),
    )


def build_gold_dim_store() -> DataFrame:
    source = spark.table(fq_table("silver_store"))
    return source.select(
        F.col("s_store_sk").alias("store_key"),
        F.col("s_store_id").alias("store_id"),
        F.col("s_store_name").alias("store_name"),
        F.col("s_number_employees").alias("number_of_employees"),
        F.col("s_floor_space").alias("floor_space_sqft"),
        F.col("s_hours").alias("operating_hours"),
        F.col("s_manager").alias("store_manager"),
        F.when(F.col("s_number_employees") < 100, "Small (< 100)")
        .when(F.col("s_number_employees") < 200, "Medium (100-200)")
        .when(F.col("s_number_employees") < 300, "Large (200-300)")
        .otherwise("Very Large (>= 300)")
        .alias("store_size_category"),
        F.col("s_market_id").alias("market_id"),
        F.col("s_market_desc").alias("market_description"),
        F.col("s_market_manager").alias("market_manager"),
        F.col("s_geography_class").alias("geography_class"),
        F.col("s_company_id").alias("company_id"),
        F.col("s_company_name").alias("company_name"),
        F.col("s_division_id").alias("division_id"),
        F.col("s_division_name").alias("division_name"),
        F.concat_ws(" ", "s_street_number", "s_street_name", "s_street_type").alias("street_address"),
        F.col("s_suite_number").alias("suite_number"),
        F.col("s_city").alias("city"),
        F.col("s_county").alias("county"),
        F.col("s_state").alias("state"),
        F.col("s_zip").alias("zip_code"),
        F.col("s_country").alias("country"),
        F.concat_ws(", ", F.concat_ws(" ", "s_street_number", "s_street_name", "s_street_type", "s_suite_number"), "s_city", "s_state", "s_zip", "s_country").alias("full_address"),
        F.col("s_gmt_offset").alias("gmt_offset"),
        F.col("s_tax_precentage").alias("tax_percentage"),
        F.col("s_rec_start_date").alias("valid_from"),
        F.col("s_rec_end_date").alias("valid_to"),
        F.col("s_closed_date_sk").alias("closed_date_key"),
        F.when(F.col("s_rec_end_date").isNull() & F.col("s_closed_date_sk").isNull(), True).otherwise(False).alias("is_currently_open"),
        F.col("ingestion_timestamp").alias("source_timestamp"),
    )


def build_gold_dim_customer() -> DataFrame:
    age = F.year(F.current_date()) - F.col("c_birth_year")
    return spark.table(fq_table("silver_customer")).select(
        F.col("c_customer_sk").alias("customer_key"),
        F.col("c_customer_id").alias("customer_id"),
        F.concat_ws(" ", "c_salutation", "c_first_name", "c_last_name").alias("full_name"),
        F.col("c_salutation").alias("salutation"),
        F.col("c_first_name").alias("first_name"),
        F.col("c_last_name").alias("last_name"),
        F.col("c_email_address").alias("email_address"),
        F.col("c_login").alias("login"),
        F.col("c_birth_day").alias("birth_day"),
        F.col("c_birth_month").alias("birth_month"),
        F.col("c_birth_year").alias("birth_year"),
        F.col("c_birth_country").alias("birth_country"),
        F.when(F.col("c_birth_year").isNull(), "Unknown")
        .when(age < 25, "Gen Z (< 25)")
        .when(age < 40, "Millennials (25-39)")
        .when(age < 55, "Gen X (40-54)")
        .when(age < 75, "Baby Boomers (55-74)")
        .otherwise("Silent Generation (75+)")
        .alias("age_group"),
        F.when(F.col("c_birth_year").isNull(), F.lit(None)).otherwise(age).alias("current_age"),
        F.when(F.col("c_preferred_cust_flag") == "Y", True).otherwise(False).alias("is_preferred_customer"),
        F.col("c_current_cdemo_sk").alias("current_demographics_key"),
        F.col("c_current_hdemo_sk").alias("current_household_demographics_key"),
        F.col("c_current_addr_sk").alias("current_address_key"),
        F.col("c_first_shipto_date_sk").alias("first_shipment_date_key"),
        F.col("c_first_sales_date_sk").alias("first_sales_date_key"),
        F.col("c_last_review_date_sk").alias("last_review_date_key"),
        F.col("c_last_review_date").alias("last_review_date"),
        F.col("ingestion_timestamp").alias("source_timestamp"),
    )


def build_gold_fact_store_sales() -> DataFrame:
    discount_ratio = (F.col("ss_list_price") - F.col("ss_sales_price")) / nullif_zero(F.col("ss_list_price"))
    return spark.table(fq_table("silver_store_sales")).select(
        F.col("ss_sold_date_sk").alias("sold_date_key"),
        F.col("ss_sold_time_sk").alias("sold_time_key"),
        F.col("ss_item_sk").alias("item_key"),
        F.col("ss_customer_sk").alias("customer_key"),
        F.col("ss_store_sk").alias("store_key"),
        F.col("ss_promo_sk").alias("promotion_key"),
        F.col("ss_cdemo_sk").alias("customer_demographics_key"),
        F.col("ss_hdemo_sk").alias("household_demographics_key"),
        F.col("ss_addr_sk").alias("address_key"),
        F.col("ss_ticket_number").alias("ticket_number"),
        F.col("ss_quantity").alias("quantity_sold"),
        F.col("ss_wholesale_cost").alias("unit_wholesale_cost"),
        F.col("ss_list_price").alias("unit_list_price"),
        F.col("ss_sales_price").alias("unit_sales_price"),
        F.col("ss_ext_wholesale_cost").alias("total_wholesale_cost"),
        F.col("ss_ext_list_price").alias("total_list_price"),
        F.col("ss_ext_sales_price").alias("total_sales_price"),
        F.col("ss_ext_discount_amt").alias("total_discount_amount"),
        F.col("ss_ext_tax").alias("total_tax_amount"),
        F.col("ss_coupon_amt").alias("coupon_amount"),
        F.col("ss_net_paid").alias("net_paid"),
        F.col("ss_net_paid_inc_tax").alias("net_paid_including_tax"),
        F.col("ss_net_profit").alias("net_profit"),
        F.round(discount_ratio * 100, 2).alias("discount_percentage"),
        F.round(F.col("ss_net_profit") / nullif_zero(F.col("ss_ext_sales_price")) * 100, 2).alias("profit_margin_percentage"),
        F.round(F.col("ss_ext_sales_price") / nullif_zero(F.col("ss_quantity")), 2).alias("revenue_per_unit"),
        F.when(discount_ratio == 0, "No Discount")
        .when(discount_ratio <= 0.10, "Small (0-10%)")
        .when(discount_ratio <= 0.25, "Medium (10-25%)")
        .when(discount_ratio <= 0.50, "Large (25-50%)")
        .otherwise("Very Large (> 50%)")
        .alias("discount_category"),
        F.col("ingestion_timestamp").alias("source_timestamp"),
    )


def build_gold_top_items_month() -> DataFrame:
    fact = spark.table(fq_table("gold_fact_store_sales")).alias("f")
    date = spark.table(fq_table("gold_dim_date")).alias("d")
    item = spark.table(fq_table("gold_dim_item")).alias("i")

    joined_sales = (
        fact.join(date, F.col("f.sold_date_key") == F.col("d.date_key"), "inner")
        .join(item, F.col("f.item_key") == F.col("i.item_key"), "inner")
        .select(
            F.col("d.year").alias("year"),
            F.col("d.month").alias("month"),
            F.col("d.year_month").alias("year_month"),
            F.col("i.item_key").alias("item_key"),
            F.col("i.item_id").alias("item_id"),
            F.col("i.product_name").alias("product_name"),
            F.col("i.category_name").alias("category_name"),
            F.col("f.total_sales_price").alias("total_sales_price"),
            F.col("f.quantity_sold").alias("quantity_sold"),
            F.col("f.net_profit").alias("net_profit"),
        )
    )

    item_month_sales = (
        joined_sales
        .groupBy(
            "year",
            "month",
            "year_month",
            "item_key",
            "item_id",
            "product_name",
            "category_name",
        )
        .agg(
            F.sum("total_sales_price").alias("total_revenue"),
            F.sum("quantity_sold").alias("total_quantity_sold"),
            F.sum("net_profit").alias("total_profit"),
        )
    )

    window = Window.partitionBy("year", "month").orderBy(F.col("total_revenue").desc(), F.col("total_quantity_sold").desc())
    return (
        item_month_sales.withColumn("revenue_rank", F.row_number().over(window))
        .filter(F.col("revenue_rank") <= 10)
        .select(
            "year",
            "month",
            "year_month",
            "item_key",
            "item_id",
            "product_name",
            "category_name",
            "total_revenue",
            "total_quantity_sold",
            "total_profit",
            "revenue_rank",
        )
    )


def main() -> None:
    table_builders = [
        ("gold_dim_date", build_gold_dim_date),
        ("gold_dim_item", build_gold_dim_item),
        ("gold_dim_store", build_gold_dim_store),
        ("gold_dim_customer", build_gold_dim_customer),
        ("gold_fact_store_sales", build_gold_fact_store_sales),
        ("gold_top_items_month", build_gold_top_items_month),
    ]

    for table_name, builder in table_builders:
        df = builder()
        write_delta_table(df, table_name)
        print(f"Wrote {fq_table(table_name)}: {spark.table(fq_table(table_name)).count()} rows")


spark = get_spark()

if __name__ == "__main__":
    main()
