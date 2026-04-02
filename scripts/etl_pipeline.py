import sys
import os
import json
import pandas as pd

sys.path.insert(0, "/opt/airflow")
print("PYTHONPATH check:", sys.path)
print("Folder check:", os.listdir("/opt/airflow"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

from pipelines.validation.schema_validator import validate_schema
from pipelines.validation.null_validator import check_nulls
from pipelines.validation.duplicate_validator import check_duplicates
from pipelines.validation.reconciliation import reconcile_counts
from lineage.lineage_tracker import store_lineage


def main():
    spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

    input_path = "/opt/airflow/data/orders.csv"
    output_path = "/opt/airflow/output/revenue_by_category"

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print("Raw data:")
    df.show()

    expected_columns = ["order_id", "product", "category", "price", "quantity"]
    validate_schema(df, expected_columns)

    null_results = check_nulls(df, ["order_id", "product", "category", "price", "quantity"])
    print("Null check results:", null_results)

    duplicate_count = check_duplicates(df, ["order_id"])
    print("Duplicate count:", duplicate_count)

    df_transformed = df.withColumn("total_price", col("price") * col("quantity"))

    df_agg = df_transformed.groupBy("category").agg(
        sum("total_price").alias("total_revenue")
    )

    print("Aggregated data:")
    df_agg.show()

    reconciliation = reconcile_counts(df, df_transformed)
    print("Reconciliation results:", reconciliation)

    df_agg.write.mode("overwrite").csv(output_path, header=True)
    
    validation_results = {
        "schema_validation": True,
        "null_check": null_results,
        "duplicate_count": duplicate_count,
        "reconciliation": reconciliation
    }

    with open("/opt/airflow/output/validation_results.json", "w") as f:
        json.dump(validation_results, f, indent=4)

    store_lineage(
        source="orders.csv",
        transformation="revenue_by_category_transformation",
        target="revenue_by_category"
    )

    spark.stop()


if __name__ == "__main__":
    main()