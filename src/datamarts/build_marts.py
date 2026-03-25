from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, countDistinct, avg, when, to_date, year, month, dayofmonth
from sqlalchemy import create_engine, text

def main():

    spark = (
    SparkSession.builder
    .appName("DatamartBuilder")
    .config("spark.jars", "/opt/spark/jars/postgresql.jar")
    .getOrCreate())

    DB_URL = "postgresql://postgres:postgres@postgres:5432/postgres"
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dm.dm_orders RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE dm.dm_items RESTART IDENTITY CASCADE"))
        

    jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
    jdbc_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    orders = spark.read.jdbc(url=jdbc_url, table="core.orders", properties=jdbc_properties)
    order_items = spark.read.jdbc(url=jdbc_url, table="core.order_items", properties=jdbc_properties)
    stores = spark.read.jdbc(url=jdbc_url, table="core.stores", properties=jdbc_properties)
    items = spark.read.jdbc(url=jdbc_url, table="core.items", properties=jdbc_properties)
    order_drivers = spark.read.jdbc(url=jdbc_url, table="core.order_drivers", properties=jdbc_properties)

    orders_with_items = orders.join(order_items, "order_id", "left")

    orders_with_items = orders_with_items.withColumn(
        "item_revenue",
        col("item_price") * col("item_quantity") * (1 - col("item_discount") / 100) * (1 - col("order_discount") / 100)
    ).withColumn(
        "item_revenue_actual",
        when(col("item_canceled_quantity") == 0, col("item_revenue"))
        .otherwise(
            col("item_price") * (col("item_quantity") - col("item_canceled_quantity")) * 
            (1 - col("item_discount") / 100) * (1 - col("order_discount") / 100)
        )
    )

    driver_changes = order_drivers.groupBy("order_id").agg(
        count("driver_id").alias("driver_count")
    )

    orders_with_items = orders_with_items.join(driver_changes, "order_id", "left")

    dm_orders = orders_with_items.groupBy(
        "order_id", "user_id", "store_id", "created_at", "delivery_cost",
        "canceled_at", "delivered_at", "order_cancellation_reason"
    ).agg(
        spark_sum("item_revenue").alias("turnover"),
        spark_sum("item_revenue_actual").alias("revenue")
    )

    dm_orders = dm_orders.withColumn(
        "profit",
        col("revenue") - col("delivery_cost")
    )

    dm_orders = dm_orders.withColumn(
        "is_canceled",
        when(col("canceled_at").isNotNull(), 1).otherwise(0)
    )

    dm_orders = dm_orders.withColumn(
        "is_delivered",
        when(col("delivered_at").isNotNull(), 1).otherwise(0)
    )

    dm_orders = dm_orders.withColumn(
        "canceled_after_delivery",
        when(col("canceled_at").isNotNull() & col("delivered_at").isNotNull(), 1).otherwise(0)
    )

    dm_orders = dm_orders.withColumn(
        "canceled_by_service",
        when(col("order_cancellation_reason").isin(["Ошибка приложения", "Проблемы с оплатой"]), 1).otherwise(0)
    )

    dm_orders = dm_orders.withColumn(
        "driver_changed",
        when(col("driver_count") > 1, 1).otherwise(0)
    )

    dm_orders = dm_orders.withColumn("date", to_date("created_at"))
    dm_orders = dm_orders.withColumn("year", year("created_at"))
    dm_orders = dm_orders.withColumn("month", month("created_at"))
    dm_orders = dm_orders.withColumn("day", dayofmonth("created_at"))

    dm_orders = dm_orders.join(stores.select("store_id", "store_address"), "store_id", "left")

    dm_orders.write.jdbc(url=jdbc_url, table="dm.dm_orders", mode="append", properties=jdbc_properties)

    order_aggregated = dm_orders.groupBy("year", "month", "day", "store_id", "store_address").agg(
        spark_sum("turnover").alias("turnover"),
        spark_sum("revenue").alias("revenue"),
        spark_sum("profit").alias("profit"),
        count("order_id").alias("total_orders"),
        spark_sum("is_delivered").alias("delivered_orders"),
        spark_sum("is_canceled").alias("canceled_orders"),
        spark_sum("canceled_after_delivery").alias("canceled_after_delivery"),
        spark_sum("canceled_by_service").alias("canceled_by_service"),
        countDistinct("user_id").alias("customer_count"),
        avg("revenue").alias("avg_check"),
        (count("order_id") / countDistinct("user_id")).alias("orders_per_customer"),
        (spark_sum("revenue") / countDistinct("user_id")).alias("revenue_per_customer"),
        spark_sum("driver_changed").alias("orders_with_driver_change")
    )

    order_aggregated.write.jdbc(url=jdbc_url, table="dm.dm_orders_aggregated", mode="append", properties=jdbc_properties)

    items_data = order_items.join(orders, "order_id", "left")
    items_data = items_data.join(items, "item_id", "left")
    items_data = items_data.join(stores, "store_id", "left")

    items_data = items_data.withColumn(
        "item_turnover",
        col("item_price") * col("item_quantity") * (1 - col("item_discount") / 100) * (1 - col("order_discount") / 100)
    )

    items_data = items_data.withColumn("date", to_date("created_at"))

    dm_items = items_data.groupBy("item_id", "item_title", "item_category", "date", "store_id", "store_address").agg(
        spark_sum("item_turnover").alias("turnover"),
        spark_sum("item_quantity").alias("total_quantity_ordered"),
        spark_sum("item_canceled_quantity").alias("total_quantity_canceled"),
        count("order_id").alias("orders_with_item")
    )

    dm_items = dm_items.withColumn(
        "orders_with_canceled_item",
        when(col("total_quantity_canceled") > 0, 1).otherwise(0)
    )

    dm_items = dm_items.withColumn(
        "total_quantity_delivered",
        col("total_quantity_ordered") - col("total_quantity_canceled")
    )

    dm_items = dm_items.withColumn("year", year("date"))
    dm_items = dm_items.withColumn("month", month("date"))
    dm_items = dm_items.withColumn("day", dayofmonth("date"))

    dm_items.write.jdbc(url=jdbc_url, table="dm.dm_items", mode="append", properties=jdbc_properties)

    active_drivers = order_drivers.groupBy("driver_id").agg(
        count("order_id").alias("order_count")
    )

    active_drivers.write.jdbc(url=jdbc_url, table="dm.active_drivers", mode="append", properties=jdbc_properties)

    spark.stop()
