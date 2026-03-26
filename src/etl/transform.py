from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def transform():
    spark = (
        SparkSession.builder
        .appName("Transform")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )

    src_path = "/opt/airflow/data/stage/extracted"
    dst_base = "/opt/airflow/data/stage/transformed"

    df = spark.read.parquet(src_path)

    df = df.fillna({
        "item_discount": 0,
        "order_discount": 0,
        "delivery_cost": 0,
        "item_canceled_quantity": 0
    })

    users_df = (
        df.select("user_id", "user_phone")
        .dropna(subset=["user_id"])
        .dropDuplicates(["user_id"])
    )

    users_df.write.mode("overwrite").parquet(f"{dst_base}/users")

    stores_df = (
        df.select("store_id", "store_address")
        .dropna(subset=["store_id"])
        .withColumn(
            "store_city",
            F.when(
                F.col("store_address").isNotNull(),
                F.trim(F.split(F.col("store_address"), ",").getItem(1))
            ).otherwise(None)
        )
        .dropDuplicates(["store_id"])
    )

    stores_df.write.mode("overwrite").parquet(f"{dst_base}/stores")

    drivers_df = (
        df.select("driver_id", "driver_phone")
        .dropna(subset=["driver_id"])
        .dropDuplicates(["driver_id"])
    )

    drivers_df.write.mode("overwrite").parquet(f"{dst_base}/drivers")

    items_df = (
        df.select("item_id", "item_title", "item_category")
        .dropna(subset=["item_id", "item_title"])
        .dropDuplicates(["item_id"])
    )

    items_df.write.mode("overwrite").parquet(f"{dst_base}/items")

    orders_df = (
        df.select(
            "order_id",
            "user_id",
            "store_id",
            "address_text",
            "created_at",
            "paid_at",
            "delivery_started_at",
            "delivered_at",
            "canceled_at",
            "payment_type",
            "order_discount",
            "order_cancellation_reason",
            "delivery_cost"
        )
        .withColumn(
            "delivery_city",
            F.when(
                F.col("address_text").isNotNull(),
                F.trim(F.split(F.col("address_text"), ",").getItem(0))
            ).otherwise(None)
        )
        .dropna(subset=["order_id", "user_id"])
        .dropDuplicates(["order_id"])
    )

    orders_df.write.mode("overwrite").parquet(f"{dst_base}/orders")

    order_items_df = (
        df.select(
            "order_id",
            "item_id",
            "item_quantity",
            "item_price",
            "item_canceled_quantity",
            "item_discount",
            "item_replaced_id"
        )
        .dropna(subset=["order_id", "item_id", "item_quantity", "item_price"])
    )

    order_items_df.write.mode("overwrite").parquet(f"{dst_base}/order_items")

    order_drivers_df = (
        df.select("order_id", "driver_id")
        .dropna(subset=["order_id", "driver_id"])
        .dropDuplicates(["order_id", "driver_id"])
    )

    order_drivers_df.write.mode("overwrite").parquet(f"{dst_base}/order_drivers")

    spark.stop()