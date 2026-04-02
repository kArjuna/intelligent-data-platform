# import sys
# sys.path.insert(0, "/opt/airflow")

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col

# from pipelines.validation.schema_validator import validate_schema
# from pipelines.validation.null_validator import check_nulls
# from pipelines.validation.duplicate_validator import check_duplicates
# from pipelines.validation.reconciliation import reconcile_counts


# def run_sales_pipeline():
#     spark = SparkSession.builder.appName("Sales Pipeline").getOrCreate()

#     df = spark.read.parquet("/opt/airflow/data/raw/orders")

#     print("Raw Orders:")
#     df.show()

#     # Schema validation
#     validate_schema(df, ["order_id", "product", "category", "price", "quantity"])

#     # Data quality checks
#     print("Null check:", check_nulls(df, df.columns))
#     print("Duplicate count:", check_duplicates(df, ["order_id"]))

#     # Add total price column
#     df = df.withColumn("total_price", col("price") * col("quantity"))

#     print("Transformed Data:")
#     df.show()

#     # Aggregation
#     revenue = df.groupBy("category").sum("total_price")
#     revenue = revenue.withColumnRenamed("sum(total_price)", "total_revenue")

#     print("Revenue by Category:")
#     revenue.show()

#     # Reconciliation
#     reconciliation = reconcile_counts(df, df)
#     print("Reconciliation:", reconciliation)

#     # Write outputs
#     df.write.mode("overwrite").parquet("/opt/airflow/output/curated/orders_clean")
#     revenue.write.mode("overwrite").csv(
#         "/opt/airflow/output/revenue_by_category",
#         header=True
#     )

#     print("Pipeline completed successfully")

#     spark.stop()


# if __name__ == "__main__":
#     run_sales_pipeline()


import sys
sys.path.insert(0, "/opt/airflow")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipelines.validation.schema_validator import validate_schema
from pipelines.validation.null_validator import check_nulls
from pipelines.validation.duplicate_validator import check_duplicates
from pipelines.validation.reconciliation import reconcile_counts
from lineage.lineage_tracker import store_lineage

def run_sales_pipeline():
    spark = SparkSession.builder.appName("Sales Pipeline").getOrCreate()

    orders = spark.read.parquet("/opt/airflow/data/raw/orders_enriched")
    customers = spark.read.parquet("/opt/airflow/data/raw/customers")
    products = spark.read.parquet("/opt/airflow/data/raw/products")

    print("Orders:")
    orders.show()

    print("Customers:")
    customers.show()

    print("Products:")
    products.show()

    validate_schema(orders, ["order_id", "customer_id", "product_id", "quantity", "order_date"])
    validate_schema(customers, ["customer_id", "customer_name", "city", "signup_date"])
    validate_schema(products, ["product_id", "product_name", "category", "price"])

    print("Orders null check:", check_nulls(orders, ["order_id", "customer_id", "product_id", "quantity"]))
    print("Orders duplicate count:", check_duplicates(orders, ["order_id"]))

    enriched_sales = (
        orders.join(customers, on="customer_id", how="left")
              .join(products, on="product_id", how="left")
              .withColumn("total_price", col("price") * col("quantity"))
    )

    print("Enriched Sales:")
    enriched_sales.show()

    reconciliation = reconcile_counts(orders, enriched_sales)
    print("Reconciliation results:", reconciliation)

    enriched_sales.write.mode("overwrite").parquet("/opt/airflow/output/curated/enriched_sales")
    
    store_lineage(
    source="orders_enriched",
    transformation="sales_pipeline_transformation",
    target="enriched_sales"
    )
    
    print("Enriched sales written successfully")

    spark.stop()


if __name__ == "__main__":
    run_sales_pipeline()