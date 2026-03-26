from datetime import date, datetime
from decimal import Decimal

from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    countDistinct,
    dayofmonth,
    lit,
    lower,
    month,
    row_number,
    sum as spark_sum,
    to_date,
    weekofyear,
    when,
    year,
)

from src.common.db_config import (
    get_jdbc_properties,
    get_jdbc_url,
    get_sqlalchemy_url,
)


def _normalize_scalar(value):
    if value is None:
        return None

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    if isinstance(value, (Decimal, date, datetime)):
        return value

    return value


def _df_to_rows(pd_df, columns):
    rows = []
    for row in pd_df[columns].itertuples(index=False, name=None):
        rows.append(tuple(_normalize_scalar(v) for v in row))
    return rows


def _bulk_insert_dataframe(engine, schema, table, pd_df, columns, page_size=1000):
    if pd_df.empty:
        print(f"Skipping {schema}.{table}: dataframe is empty.")
        return

    rows = _df_to_rows(pd_df, columns)
    insert_sql = f"""
        INSERT INTO {schema}.{table} ({", ".join(columns)})
        VALUES %s
    """

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            execute_values(cur, insert_sql, rows, page_size=page_size)
        raw_conn.commit()
    finally:
        raw_conn.close()


def _bulk_insert_spark_df(engine, schema, table, spark_df, columns, page_size=1000):
    insert_sql = f"""
        INSERT INTO {schema}.{table} ({", ".join(columns)})
        VALUES %s
    """

    raw_conn = engine.raw_connection()
    batch = []

    try:
        with raw_conn.cursor() as cur:
            for row in spark_df.select(*columns).toLocalIterator():
                batch.append(tuple(_normalize_scalar(v) for v in row))

                if len(batch) >= page_size:
                    execute_values(cur, insert_sql, batch, page_size=page_size)
                    raw_conn.commit()
                    batch = []

            if batch:
                execute_values(cur, insert_sql, batch, page_size=page_size)
                raw_conn.commit()
    finally:
        raw_conn.close()


def build_datamarts():
    spark = (
        SparkSession.builder
        .appName("DatamartBuilder")
        .master("local[1]")
        .config("spark.jars", "/opt/spark/jars/postgresql.jar")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "3g")
        .getOrCreate()
    )

    jdbc_url = get_jdbc_url()
    jdbc_props = get_jdbc_properties()

    print("Reading core tables from PostgreSQL...")

    orders = spark.read.jdbc(
        url=jdbc_url,
        table="core.orders",
        properties=jdbc_props,
    )

    order_items = spark.read.jdbc(
        url=jdbc_url,
        table="core.order_items",
        properties=jdbc_props,
    )

    order_drivers = spark.read.jdbc(
        url=jdbc_url,
        table="core.order_drivers",
        properties=jdbc_props,
    )

    stores = spark.read.jdbc(
        url=jdbc_url,
        table="core.stores",
        properties=jdbc_props,
    )

    items = spark.read.jdbc(
        url=jdbc_url,
        table="core.items",
        properties=jdbc_props,
    )

    print("Normalizing source tables...")

    orders = (
        orders
        .withColumn("order_discount", coalesce(col("order_discount"), lit(0.0)))
        .withColumn("delivery_cost", coalesce(col("delivery_cost"), lit(0.0)))
        .withColumn("report_date", to_date(col("created_at")))
        .filter(col("report_date").isNotNull())
    )

    order_items = (
        order_items
        .withColumn("item_discount", coalesce(col("item_discount"), lit(0.0)))
        .withColumn("item_canceled_quantity", coalesce(col("item_canceled_quantity"), lit(0)))
        .withColumn("item_quantity", coalesce(col("item_quantity"), lit(0)))
        .withColumn("item_price", coalesce(col("item_price"), lit(0.0)))
    )

    stores = stores.select(
        "store_id",
        "store_address",
        "store_city",
    )

    items = items.select(
        "item_id",
        "item_title",
        "item_category",
    )

    orders_enriched = (
        orders.alias("o")
        .join(stores.alias("s"), on="store_id", how="left")
        .withColumn("city", coalesce(col("o.delivery_city"), col("s.store_city"), lit("UNKNOWN")))
        .withColumn("report_year", year(col("report_date")))
        .withColumn("report_month", month(col("report_date")))
        .withColumn("report_day", dayofmonth(col("report_date")))
        .select(
            "order_id",
            "user_id",
            "store_id",
            "store_address",
            "city",
            "created_at",
            "delivered_at",
            "canceled_at",
            "order_cancellation_reason",
            "order_discount",
            "delivery_cost",
            "report_date",
            "report_year",
            "report_month",
            "report_day",
        )
    )

    print("Building dm_orders...")

    order_items_enriched = (
        order_items.alias("oi")
        .join(
            orders_enriched.alias("o"),
            on="order_id",
            how="inner",
        )
        .withColumn(
            "gross_line_amount",
            col("item_quantity") * col("item_price")
        )
        .withColumn(
            "net_quantity",
            when(
                (col("item_quantity") - col("item_canceled_quantity")) > 0,
                col("item_quantity") - col("item_canceled_quantity"),
            ).otherwise(lit(0))
        )
        .withColumn(
            "revenue_line_amount",
            col("net_quantity")
            * col("item_price")
            * (lit(1.0) - col("item_discount") / lit(100.0))
            * (lit(1.0) - col("order_discount") / lit(100.0))
        )
    )

    order_financials = (
        order_items_enriched
        .groupBy("order_id")
        .agg(
            spark_sum("gross_line_amount").alias("gross_amount_items"),
            spark_sum("revenue_line_amount").alias("revenue_amount_items"),
        )
    )

    driver_stats = (
        order_drivers
        .groupBy("order_id")
        .agg(
            countDistinct("driver_id").alias("driver_count")
        )
        .withColumn(
            "has_driver_change",
            when(col("driver_count") > 1, lit(1)).otherwise(lit(0))
        )
    )

    active_drivers = (
        order_drivers.alias("od")
        .join(
            orders_enriched.select("order_id", "report_date", "city", "store_id").alias("o"),
            on="order_id",
            how="inner",
        )
        .groupBy("report_date", "city", "store_id")
        .agg(
            countDistinct("driver_id").alias("active_drivers_cnt")
        )
    )

    orders_fact = (
        orders_enriched.alias("o")
        .join(order_financials.alias("f"), on="order_id", how="left")
        .join(driver_stats.alias("d"), on="order_id", how="left")
        .withColumn("gross_amount_items", coalesce(col("gross_amount_items"), lit(0.0)))
        .withColumn("revenue_amount_items", coalesce(col("revenue_amount_items"), lit(0.0)))
        .withColumn("has_driver_change", coalesce(col("has_driver_change"), lit(0)))
        .withColumn("gross_amount_order", col("gross_amount_items"))
        .withColumn("revenue_amount_order", col("revenue_amount_items") + col("delivery_cost"))
        .withColumn("profit_amount_order", col("revenue_amount_items"))
        .withColumn(
            "is_delivered",
            when(col("delivered_at").isNotNull(), lit(1)).otherwise(lit(0))
        )
        .withColumn(
            "is_canceled",
            when(col("canceled_at").isNotNull(), lit(1)).otherwise(lit(0))
        )
        .withColumn(
            "is_canceled_after_delivery",
            when(
                col("canceled_at").isNotNull() & col("delivered_at").isNotNull(),
                lit(1)
            ).otherwise(lit(0))
        )
        .withColumn(
            "is_canceled_service_fault",
            when(
                lower(coalesce(col("order_cancellation_reason"), lit(""))).rlike(
                    "service|сервис|курьер|driver|store|shop|магазин|delivery|доставка"
                ),
                lit(1)
            ).otherwise(lit(0))
        )
    )

    dm_orders = (
        orders_fact
        .groupBy(
            "report_date",
            "report_year",
            "report_month",
            "report_day",
            "city",
            "store_id",
            "store_address",
        )
        .agg(
            spark_sum("gross_amount_order").alias("gross_amount"),
            spark_sum("revenue_amount_order").alias("revenue_amount"),
            spark_sum("profit_amount_order").alias("profit_amount"),
            countDistinct("order_id").alias("orders_created_cnt"),
            spark_sum("is_delivered").alias("orders_delivered_cnt"),
            spark_sum("is_canceled").alias("orders_canceled_cnt"),
            spark_sum("is_canceled_after_delivery").alias("orders_canceled_after_delivery_cnt"),
            spark_sum("is_canceled_service_fault").alias("orders_canceled_service_fault_cnt"),
            countDistinct("user_id").alias("buyers_cnt"),
            avg("revenue_amount_order").alias("avg_check_amount"),
            spark_sum("has_driver_change").alias("orders_with_driver_change_cnt"),
        )
        .alias("b")
        .join(
            active_drivers.alias("a"),
            on=["report_date", "city", "store_id"],
            how="left",
        )
        .withColumn("active_drivers_cnt", coalesce(col("active_drivers_cnt"), lit(0)))
        .withColumn(
            "orders_per_buyer",
            when(col("buyers_cnt") > 0, col("orders_created_cnt") / col("buyers_cnt"))
            .otherwise(lit(0.0))
        )
        .withColumn(
            "revenue_per_buyer",
            when(col("buyers_cnt") > 0, col("revenue_amount") / col("buyers_cnt"))
            .otherwise(lit(0.0))
        )
        .select(
            "report_date",
            "report_year",
            "report_month",
            "report_day",
            "city",
            "store_id",
            "store_address",
            "gross_amount",
            "revenue_amount",
            "profit_amount",
            "orders_created_cnt",
            "orders_delivered_cnt",
            "orders_canceled_cnt",
            "orders_canceled_after_delivery_cnt",
            "orders_canceled_service_fault_cnt",
            "buyers_cnt",
            "avg_check_amount",
            "orders_per_buyer",
            "revenue_per_buyer",
            "orders_with_driver_change_cnt",
            "active_drivers_cnt",
        )
        .filter(col("store_id").isNotNull())
    )

    print("Building dm_items...")

    item_fact = (
        order_items.alias("oi")
        .join(
            orders_enriched.select(
                "order_id",
                "store_id",
                "store_address",
                "city",
                "report_date",
                "report_year",
                "report_month",
                "report_day",
                "order_discount",
            ).alias("o"),
            on="order_id",
            how="inner",
        )
        .join(items.alias("i"), on="item_id", how="left")
        .withColumn("report_week", weekofyear(col("report_date")))
        .withColumn(
            "item_gross_amount_calc",
            col("item_quantity")
            * col("item_price")
            * (lit(1.0) - col("item_discount") / lit(100.0))
            * (lit(1.0) - col("order_discount") / lit(100.0))
        )
        .withColumn(
            "has_item_cancellation",
            when(col("item_canceled_quantity") > 0, lit(1)).otherwise(lit(0))
        )
    )

    dm_items = (
        item_fact
        .groupBy(
            "report_date",
            "report_year",
            "report_month",
            "report_day",
            "report_week",
            "city",
            "store_id",
            "store_address",
            "item_category",
            "item_id",
            "item_title",
        )
        .agg(
            spark_sum("item_gross_amount_calc").alias("item_gross_amount"),
            spark_sum("item_quantity").alias("ordered_quantity"),
            spark_sum("item_canceled_quantity").alias("canceled_quantity"),
            countDistinct("order_id").alias("orders_with_item_cnt"),
            spark_sum("has_item_cancellation").alias("orders_with_item_cancellation_cnt"),
        )
    )

    daily_popularity_window = Window.partitionBy(
        "report_date", "city", "store_id"
    ).orderBy(col("ordered_quantity").desc(), col("item_id").asc())

    daily_reverse_window = Window.partitionBy(
        "report_date", "city", "store_id"
    ).orderBy(col("ordered_quantity").asc(), col("item_id").asc())

    weekly_popularity_window = Window.partitionBy(
        "report_year", "report_week", "city", "store_id"
    ).orderBy(col("ordered_quantity").desc(), col("item_id").asc())

    weekly_reverse_window = Window.partitionBy(
        "report_year", "report_week", "city", "store_id"
    ).orderBy(col("ordered_quantity").asc(), col("item_id").asc())

    monthly_popularity_window = Window.partitionBy(
        "report_year", "report_month", "city", "store_id"
    ).orderBy(col("ordered_quantity").desc(), col("item_id").asc())

    monthly_reverse_window = Window.partitionBy(
        "report_year", "report_month", "city", "store_id"
    ).orderBy(col("ordered_quantity").asc(), col("item_id").asc())

    dm_items = (
        dm_items
        .withColumn("daily_popularity_rank", row_number().over(daily_popularity_window))
        .withColumn("daily_reverse_popularity_rank", row_number().over(daily_reverse_window))
        .withColumn("weekly_popularity_rank", row_number().over(weekly_popularity_window))
        .withColumn("weekly_reverse_popularity_rank", row_number().over(weekly_reverse_window))
        .withColumn("monthly_popularity_rank", row_number().over(monthly_popularity_window))
        .withColumn("monthly_reverse_popularity_rank", row_number().over(monthly_reverse_window))
        .select(
            "report_date",
            "report_year",
            "report_month",
            "report_day",
            "report_week",
            "city",
            "store_id",
            "store_address",
            "item_category",
            "item_id",
            "item_title",
            "item_gross_amount",
            "ordered_quantity",
            "canceled_quantity",
            "orders_with_item_cnt",
            "orders_with_item_cancellation_cnt",
            "daily_popularity_rank",
            "daily_reverse_popularity_rank",
            "weekly_popularity_rank",
            "weekly_reverse_popularity_rank",
            "monthly_popularity_rank",
            "monthly_reverse_popularity_rank",
        )
        .filter(col("store_id").isNotNull())
    )

    print("dm_orders rows:", dm_orders.count())
    print("dm_items rows:", dm_items.count())

    print("Truncating dm tables...")

    engine = create_engine(get_sqlalchemy_url())
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dm.dm_items"))
        conn.execute(text("TRUNCATE TABLE dm.dm_orders"))

    print("Converting dm_orders to pandas...")
    dm_orders_pd = dm_orders.toPandas()

    print("Writing dm_orders via psycopg2 execute_values...")
    dm_orders_columns = [
        "report_date",
        "report_year",
        "report_month",
        "report_day",
        "city",
        "store_id",
        "store_address",
        "gross_amount",
        "revenue_amount",
        "profit_amount",
        "orders_created_cnt",
        "orders_delivered_cnt",
        "orders_canceled_cnt",
        "orders_canceled_after_delivery_cnt",
        "orders_canceled_service_fault_cnt",
        "buyers_cnt",
        "avg_check_amount",
        "orders_per_buyer",
        "revenue_per_buyer",
        "orders_with_driver_change_cnt",
        "active_drivers_cnt",
    ]
    _bulk_insert_dataframe(
        engine=engine,
        schema="dm",
        table="dm_orders",
        pd_df=dm_orders_pd,
        columns=dm_orders_columns,
        page_size=1000,
    )

    print("Writing dm_items via Spark iterator...")
    dm_items_columns = [
        "report_date",
        "report_year",
        "report_month",
        "report_day",
        "report_week",
        "city",
        "store_id",
        "store_address",
        "item_category",
        "item_id",
        "item_title",
        "item_gross_amount",
        "ordered_quantity",
        "canceled_quantity",
        "orders_with_item_cnt",
        "orders_with_item_cancellation_cnt",
        "daily_popularity_rank",
        "daily_reverse_popularity_rank",
        "weekly_popularity_rank",
        "weekly_reverse_popularity_rank",
        "monthly_popularity_rank",
        "monthly_reverse_popularity_rank",
    ]
    _bulk_insert_spark_df(
        engine=engine,
        schema="dm",
        table="dm_items",
        spark_df=dm_items,
        columns=dm_items_columns,
        page_size=1000,
    )

    print("Datamarts build completed successfully.")
    spark.stop()