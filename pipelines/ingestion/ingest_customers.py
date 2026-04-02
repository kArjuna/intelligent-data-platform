from pyspark.sql import SparkSession


def ingest_customers():
    spark = SparkSession.builder.appName("Ingest Customers").getOrCreate()

    input_path = "/opt/airflow/data/customers.csv"
    output_path = "/opt/airflow/data/raw/customers"

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print("Customers raw data:")
    df.show()

    df.write.mode("overwrite").parquet(output_path)
    print(f"Customers ingested to {output_path}")

    spark.stop()


if __name__ == "__main__":
    ingest_customers()