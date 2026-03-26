from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text

from src.common.db_config import (
    get_jdbc_properties,
    get_jdbc_url,
    get_psycopg2_style_url,
)


def load():
    spark = (
        SparkSession.builder
        .appName("Load")
        .master("local[2]")
        .config("spark.jars", "/opt/spark/jars/postgresql.jar")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )

    base_path = "/opt/airflow/data/stage/transformed"

    users_df = spark.read.parquet(f"{base_path}/users")
    stores_df = spark.read.parquet(f"{base_path}/stores")
    drivers_df = spark.read.parquet(f"{base_path}/drivers")
    items_df = spark.read.parquet(f"{base_path}/items")
    orders_df = spark.read.parquet(f"{base_path}/orders")
    order_items_df = spark.read.parquet(f"{base_path}/order_items")
    order_drivers_df = spark.read.parquet(f"{base_path}/order_drivers")

    engine = create_engine(get_psycopg2_style_url())

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE core.order_drivers RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE core.order_items RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE core.orders RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE core.items RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE core.drivers RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE core.stores RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE core.users RESTART IDENTITY CASCADE"))

    jdbc_url = get_jdbc_url()
    jdbc_props = get_jdbc_properties()

    users_df.write.jdbc(
        url=jdbc_url,
        table="core.users",
        mode="append",
        properties=jdbc_props
    )

    stores_df.write.jdbc(
        url=jdbc_url,
        table="core.stores",
        mode="append",
        properties=jdbc_props
    )

    drivers_df.write.jdbc(
        url=jdbc_url,
        table="core.drivers",
        mode="append",
        properties=jdbc_props
    )

    items_df.write.jdbc(
        url=jdbc_url,
        table="core.items",
        mode="append",
        properties=jdbc_props
    )

    orders_df.write.jdbc(
        url=jdbc_url,
        table="core.orders",
        mode="append",
        properties=jdbc_props
    )

    order_items_df.write.jdbc(
        url=jdbc_url,
        table="core.order_items",
        mode="append",
        properties=jdbc_props
    )

    order_drivers_df.write.jdbc(
        url=jdbc_url,
        table="core.order_drivers",
        mode="append",
        properties=jdbc_props
    )

    spark.stop()