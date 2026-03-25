from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text

def load():
    spark = SparkSession.builder \
        .appName("Load") \
        .config("spark.jars", "/opt/spark/jars/postgresql.jar") \
        .getOrCreate()

    df = spark.read.parquet("/opt/airflow/data/stage/transformed")

    DB_URL = "postgresql://postgres:postgres@postgres:5432/postgres"
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE core.users RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE core.stores RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE core.drivers RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE core.items RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE core.orders RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE core.order_items RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE core.order_drivers RESTART IDENTITY CASCADE"))


    jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
    jdbc_props = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    users_df = df.select("user_id", "user_phone").distinct().dropna(subset=["user_id"])
    users_df.write.jdbc(url=jdbc_url, table="core.users", mode="append", properties=jdbc_props)

    stores_df = df.select("store_id", "store_address").distinct().dropna(subset=["store_id"])
    stores_df.write.jdbc(url=jdbc_url, table="core.stores", mode="append", properties=jdbc_props)

    drivers_df = df.select("driver_id", "driver_phone").distinct().dropna(subset=["driver_id"])
    drivers_df.write.jdbc(url=jdbc_url, table="core.drivers", mode="append", properties=jdbc_props)

    items_df = df.select("item_id", "item_title", "item_category").distinct().dropna(subset=["item_id"])
    items_df.write.jdbc(url=jdbc_url, table="core.items", mode="append", properties=jdbc_props)

    orders_df = df.select(
        "order_id", "user_id", "store_id", "address_text",
        "created_at", "paid_at", "delivery_started_at", "delivered_at",
        "canceled_at", "payment_type", "order_discount",
        "order_cancellation_reason", "delivery_cost"
    ).distinct().dropna(subset=["order_id"])
    orders_df.write.jdbc(url=jdbc_url, table="core.orders", mode="append", properties=jdbc_props)

    order_items_df = df.select(
        "order_id", "item_id", "item_quantity", "item_price",
        "item_canceled_quantity", "item_discount", "item_replaced_id"
    ).dropna(subset=["order_id"])
    order_items_df.write.jdbc(url=jdbc_url, table="core.order_items", mode="append", properties=jdbc_props)

    order_drivers_df = df.select("order_id", "driver_id").distinct().dropna()
    order_drivers_df.write.jdbc(url=jdbc_url, table="core.order_drivers", mode="append", properties=jdbc_props)

    spark.stop()

if __name__ == "__main__":
    load()