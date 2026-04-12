from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


def write_gold_batch(batch_df, batch_id):
    gold_df = batch_df.groupBy("category").agg(
        sum("total_price").alias("total_revenue")
    )

    gold_df.write.mode("overwrite").parquet("/opt/airflow/output/streaming/orders_gold")
    print(f"Processed batch {batch_id} into orders_gold")


def main():
    spark = SparkSession.builder \
        .appName("Orders Gold Pipeline") \
        .getOrCreate()

    silver_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_price", DoubleType(), True),
    ])

    silver_df = spark.readStream \
        .format("parquet") \
        .schema(silver_schema) \
        .load("/opt/airflow/output/streaming/orders_silver")

    query = silver_df.writeStream \
        .foreachBatch(write_gold_batch) \
        .option("checkpointLocation", "/opt/airflow/output/streaming/checkpoints/orders_gold") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()