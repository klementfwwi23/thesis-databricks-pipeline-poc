"""
Declarative Gold Layer

"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dlt.table(name="gold_dim_date", comment="Gold dimension: business-ready calendar attributes")
def gold_dim_date():
    return (
        dlt.read("silver_date_dim")
        .select(
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
            F.col("year_month"),
            F.concat(F.lit("Q"), F.col("d_qoy").cast("string"), F.lit(" "), F.col("d_year").cast("string")).alias("quarter_year"),
            F.when(F.col("d_holiday") == "Y", F.lit(True)).otherwise(F.lit(False)).alias("is_holiday"),
            F.col("is_weekend"),
            F.when(F.col("d_following_holiday") == "Y", F.lit(True)).otherwise(F.lit(False)).alias("is_following_holiday"),
            F.when(F.col("d_current_day") == "Y", F.lit(True)).otherwise(F.lit(False)).alias("is_current_day"),
            F.when(F.col("d_current_week") == "Y", F.lit(True)).otherwise(F.lit(False)).alias("is_current_week"),
            F.when(F.col("d_current_month") == "Y", F.lit(True)).otherwise(F.lit(False)).alias("is_current_month"),
            F.when(F.col("d_current_quarter") == "Y", F.lit(True)).otherwise(F.lit(False)).alias("is_current_quarter"),
            F.when(F.col("d_current_year") == "Y", F.lit(True)).otherwise(F.lit(False)).alias("is_current_year"),
            F.col("d_same_day_ly").alias("same_day_last_year_key"),
            F.col("d_same_day_lq").alias("same_day_last_quarter_key"),
            F.col("ingestion_timestamp").alias("source_timestamp"),
        )
    )


@dlt.table(name="gold_dim_item", comment="Gold dimension: item attributes for product and pricing analysis")
def gold_dim_item():
    return (
        dlt.read("silver_item")
        .select(
            F.col("i_item_sk").alias("item_key"),
            F.col("i_item_id").alias("item_id"),
            F.col("i_item_desc").alias("item_description"),
            F.col("i_product_name").alias("product_name"),
            F.col("i_current_price").alias("current_price"),
            F.col("i_wholesale_cost").alias("wholesale_cost"),
            F.when(F.col("i_current_price") < 10, F.lit("Low (< 10)"))
             .when(F.col("i_current_price") < 50, F.lit("Medium (10-50)"))
             .when(F.col("i_current_price") < 100, F.lit("High (50-100)"))
             .otherwise(F.lit("Premium (>= 100)"))
             .alias("price_category"),
            F.round(((F.col("i_current_price") - F.col("i_wholesale_cost")) / F.when(F.col("i_current_price") == 0, None).otherwise(F.col("i_current_price"))) * 100, 2).alias("margin_pct"),
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
            F.when(F.col("i_rec_end_date").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_current"),
            F.col("ingestion_timestamp").alias("source_timestamp"),
        )
    )


@dlt.table(name="gold_dim_store", comment="Gold dimension: store attributes for regional and organizational analysis")
def gold_dim_store():
    return (
        dlt.read("silver_store")
        .select(
            F.col("s_store_sk").alias("store_key"),
            F.col("s_store_id").alias("store_id"),
            F.col("s_store_name").alias("store_name"),
            F.col("s_number_employees").alias("number_of_employees"),
            F.col("s_floor_space").alias("floor_space_sqft"),
            F.col("s_hours").alias("operating_hours"),
            F.col("s_manager").alias("store_manager"),
            F.when(F.col("s_number_employees") < 100, F.lit("Small (< 100)"))
             .when(F.col("s_number_employees") < 200, F.lit("Medium (100-200)"))
             .when(F.col("s_number_employees") < 300, F.lit("Large (200-300)"))
             .otherwise(F.lit("Very Large (>= 300)"))
             .alias("store_size_category"),
            F.col("s_market_id").alias("market_id"),
            F.col("s_market_desc").alias("market_description"),
            F.col("s_market_manager").alias("market_manager"),
            F.col("s_geography_class").alias("geography_class"),
            F.col("s_company_id").alias("company_id"),
            F.col("s_company_name").alias("company_name"),
            F.col("s_division_id").alias("division_id"),
            F.col("s_division_name").alias("division_name"),
            F.concat_ws(" ", F.col("s_street_number"), F.col("s_street_name"), F.col("s_street_type")).alias("street_address"),
            F.col("s_suite_number").alias("suite_number"),
            F.col("s_city").alias("city"),
            F.col("s_county").alias("county"),
            F.col("s_state").alias("state"),
            F.col("s_zip").alias("zip_code"),
            F.col("s_country").alias("country"),
            F.concat_ws(", ",
                F.concat_ws(" ", F.col("s_street_number"), F.col("s_street_name"), F.col("s_street_type"), F.col("s_suite_number")),
                F.col("s_city"), F.col("s_state"), F.col("s_zip"), F.col("s_country")
            ).alias("full_address"),
            F.col("s_gmt_offset").alias("gmt_offset"),
            F.col("s_tax_precentage").alias("tax_percentage"),
            F.col("s_rec_start_date").alias("valid_from"),
            F.col("s_rec_end_date").alias("valid_to"),
            F.col("s_closed_date_sk").alias("closed_date_key"),
            F.when(F.col("s_rec_end_date").isNull() & F.col("s_closed_date_sk").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_currently_open"),
            F.col("ingestion_timestamp").alias("source_timestamp"),
        )
    )


@dlt.table(name="gold_dim_customer", comment="Gold dimension: customer attributes for segmentation and customer analytics")
def gold_dim_customer():
    current_year = F.year(F.current_date())
    age = F.when(F.col("c_birth_year").isNull(), F.lit(None)).otherwise(current_year - F.col("c_birth_year"))
    return (
        dlt.read("silver_customer")
        .select(
            F.col("c_customer_sk").alias("customer_key"),
            F.col("c_customer_id").alias("customer_id"),
            F.concat_ws(" ", F.col("c_salutation"), F.col("c_first_name"), F.col("c_last_name")).alias("full_name"),
            F.col("c_salutation").alias("salutation"),
            F.col("c_first_name").alias("first_name"),
            F.col("c_last_name").alias("last_name"),
            F.col("c_email_address").alias("email_address"),
            F.col("c_login").alias("login"),
            F.col("c_birth_day").alias("birth_day"),
            F.col("c_birth_month").alias("birth_month"),
            F.col("c_birth_year").alias("birth_year"),
            F.col("c_birth_country").alias("birth_country"),
            F.when(F.col("c_birth_year").isNull(), F.lit("Unknown"))
             .when(current_year - F.col("c_birth_year") < 25, F.lit("Gen Z (< 25)"))
             .when(current_year - F.col("c_birth_year") < 40, F.lit("Millennials (25-39)"))
             .when(current_year - F.col("c_birth_year") < 55, F.lit("Gen X (40-54)"))
             .when(current_year - F.col("c_birth_year") < 75, F.lit("Baby Boomers (55-74)"))
             .otherwise(F.lit("Silent Generation (75+)"))
             .alias("age_group"),
            age.alias("current_age"),
            F.when(F.col("c_preferred_cust_flag") == "Y", F.lit(True)).otherwise(F.lit(False)).alias("is_preferred_customer"),
            F.col("c_current_cdemo_sk").alias("current_demographics_key"),
            F.col("c_current_hdemo_sk").alias("current_household_demographics_key"),
            F.col("c_current_addr_sk").alias("current_address_key"),
            F.col("c_first_shipto_date_sk").alias("first_shipment_date_key"),
            F.col("c_first_sales_date_sk").alias("first_sales_date_key"),
            F.col("c_last_review_date_sk").alias("last_review_date_key"),
            F.col("c_last_review_date").alias("last_review_date"),
            F.col("ingestion_timestamp").alias("source_timestamp"),
        )
    )


@dlt.table(name="gold_fact_store_sales", comment="Gold fact: central store sales fact table with analytical measures")
def gold_fact_store_sales():
    discount_ratio = (F.col("ss_list_price") - F.col("ss_sales_price")) / F.when(F.col("ss_list_price") == 0, None).otherwise(F.col("ss_list_price"))
    return (
        dlt.read("silver_store_sales")
        .select(
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
            F.coalesce(F.round(discount_ratio * 100, 2), F.lit(0)).alias("discount_percentage"),
            F.coalesce(F.round(F.col("ss_net_profit") / F.when(F.col("ss_ext_sales_price") == 0, None).otherwise(F.col("ss_ext_sales_price")) * 100, 2), F.lit(0)).alias("profit_margin_percentage"),
            F.coalesce(F.round(F.col("ss_ext_sales_price") / F.when(F.col("ss_quantity") == 0, None).otherwise(F.col("ss_quantity")), 2), F.lit(0)).alias("revenue_per_unit"),
            F.when(F.col("ss_list_price").isNull() | (F.col("ss_list_price") == 0), F.lit("No List Price"))
             .when(discount_ratio == 0, F.lit("No Discount"))
             .when(discount_ratio <= 0.10, F.lit("Small (0-10%)"))
             .when(discount_ratio <= 0.25, F.lit("Medium (10-25%)"))
             .when(discount_ratio <= 0.50, F.lit("Large (25-50%)"))
             .otherwise(F.lit("Very Large (> 50%)"))
             .alias("discount_category"),
            F.col("ingestion_timestamp").alias("source_timestamp"),
        )
    )


@dlt.table(name="gold_top_items_month", comment="Gold aggregation: monthly top-selling items by revenue")
def gold_top_items_month():
    ranked = (
        dlt.read("gold_fact_store_sales").alias("f")
        .join(dlt.read("gold_dim_date").alias("d"), F.col("f.sold_date_key") == F.col("d.date_key"), "inner")
        .join(dlt.read("gold_dim_item").alias("i"), F.col("f.item_key") == F.col("i.item_key"), "inner")
        .groupBy("d.year", "d.month", "d.year_month", "i.item_key", "i.item_id", "i.product_name", "i.category_name")
        .agg(
            F.sum("f.quantity_sold").alias("total_quantity_sold"),
            F.sum("f.total_sales_price").alias("total_revenue"),
            F.sum("f.net_profit").alias("total_profit"),
        )
    )
    window_spec = Window.partitionBy("year", "month").orderBy(F.col("total_revenue").desc(), F.col("total_quantity_sold").desc())
    return (
        ranked.withColumn("revenue_rank", F.row_number().over(window_spec))
        .where(F.col("revenue_rank") <= 10)
        .withColumn("aggregation_timestamp", F.current_timestamp())
    )
