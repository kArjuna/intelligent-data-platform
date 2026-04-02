from pyspark.sql import SparkSession


def ingest_orders():
    spark = SparkSession.builder.appName("Ingest Orders").getOrCreate()

    input_path = "/opt/airflow/data/orders.csv"
    output_path = "/opt/airflow/data/raw/orders"

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print("Orders raw data:")
    df.show()

    df.write.mode("overwrite").parquet(output_path)
    print(f"Orders ingested to {output_path}")

    spark.stop()


if __name__ == "__main__":
    ingest_orders()