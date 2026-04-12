from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


def main():
    spark = SparkSession.builder \
        .appName("Orders Silver Pipeline") \
        .getOrCreate()

    bronze_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_price", DoubleType(), True),
    ])

    bronze_df = spark.readStream \
        .format("parquet") \
        .schema(bronze_schema) \
        .load("/opt/airflow/output/streaming/orders_bronze")

    silver_df = bronze_df.filter(
        col("order_id").isNotNull() &
        col("product").isNotNull() &
        col("category").isNotNull() &
        col("price").isNotNull() &
        col("quantity").isNotNull() &
        col("total_price").isNotNull()
    )

    query = silver_df.writeStream \
        .format("parquet") \
        .option("path", "/opt/airflow/output/streaming/orders_silver") \
        .option("checkpointLocation", "/opt/airflow/output/streaming/checkpoints/orders_silver") \
        .outputMode("append") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()