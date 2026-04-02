import sys
sys.path.insert(0, "/opt/airflow")

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg
from lineage.lineage_tracker import store_lineage

def build_customer_metrics():
    spark = SparkSession.builder.appName("Customer Metrics").getOrCreate()

    sales = spark.read.parquet("/opt/airflow/output/curated/enriched_sales")

    customer_metrics = sales.groupBy("customer_id", "customer_name").agg(
        count("order_id").alias("total_orders"),
        sum("total_price").alias("total_spend"),
        avg("total_price").alias("avg_order_value")
    )

    customer_metrics.show()

    customer_metrics.write.mode("overwrite").parquet("/opt/airflow/output/curated/customer_metrics")
    
    store_lineage(
    source="enriched_sales",
    transformation="customer_metrics_transformation",
    target="customer_metrics"
    )
    
    print("Customer metrics written successfully")

    spark.stop()


if __name__ == "__main__":
    build_customer_metrics()