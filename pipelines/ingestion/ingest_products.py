from pyspark.sql import SparkSession


def ingest_products():
    spark = SparkSession.builder.appName("Ingest Products").getOrCreate()

    input_path = "/opt/airflow/data/products.csv"
    output_path = "/opt/airflow/data/raw/products"

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print("Products raw data:")
    df.show()

    df.write.mode("overwrite").parquet(output_path)
    print(f"Products ingested to {output_path}")

    spark.stop()


if __name__ == "__main__":
    ingest_products()