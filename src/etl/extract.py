from pyspark.sql import SparkSession

def extract():
    spark = SparkSession.builder \
    .appName("Extract") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

    df = spark.read.parquet("/opt/airflow/data/raw/*.parquet")

    df.write.mode("overwrite").parquet("/opt/airflow/data/stage/extracted")

    spark.stop()