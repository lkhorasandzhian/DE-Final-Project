from pyspark.sql import SparkSession

def transform():
    spark = SparkSession.builder.appName("Transform").getOrCreate()

    df = spark.read.parquet("/opt/airflow/data/stage/extracted")

    df = df.dropDuplicates()

    df = df.dropna(subset=[
    "order_id", "user_id", "store_id",
    "item_id", "driver_id"])

    df = df.fillna({
        "item_discount": 0,
        "order_discount": 0,
        "delivery_cost": 0
    })

    df.write.mode("overwrite").parquet("/opt/airflow/data/stage/transformed")

    spark.stop()