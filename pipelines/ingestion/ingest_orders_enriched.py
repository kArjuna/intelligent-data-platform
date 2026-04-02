from pyspark.sql import SparkSession


def ingest_orders_enriched():
    spark = SparkSession.builder.appName("Ingest Orders Enriched").getOrCreate()

    input_path = "/opt/airflow/data/orders_enriched.csv"
    output_path = "/opt/airflow/data/raw/orders_enriched"

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print("Orders enriched raw data:")
    df.show()

    df.write.mode("overwrite").parquet(output_path)
    print(f"Orders enriched ingested to {output_path}")

    spark.stop()


if __name__ == "__main__":
    ingest_orders_enriched()