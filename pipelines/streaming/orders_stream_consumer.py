import sys
sys.path.insert(0, "/opt/airflow")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


def main():
    spark = SparkSession.builder \
        .appName("Orders Stream Consumer") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
        ) \
        .getOrCreate()

    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "orders_stream") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed = (
        df.selectExpr("CAST(value AS STRING) as json_str")
          .select(from_json(col("json_str"), schema).alias("data"))
          .select("data.*")
          .withColumn("total_price", col("price") * col("quantity"))
    )

    query = parsed.writeStream \
        .format("parquet") \
        .option("path", "/opt/airflow/output/streaming/orders_bronze") \
        .option("checkpointLocation", "/opt/airflow/output/streaming/checkpoints/orders_bronze") \
        .outputMode("append") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()