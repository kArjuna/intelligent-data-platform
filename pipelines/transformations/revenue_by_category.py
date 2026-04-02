import sys
sys.path.insert(0, "/opt/airflow")

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from lineage.lineage_tracker import store_lineage

def build_revenue_by_category():
    spark = SparkSession.builder.appName("Revenue By Category").getOrCreate()

    sales = spark.read.parquet("/opt/airflow/output/curated/enriched_sales")

    revenue = sales.groupBy("category").agg(
        sum("total_price").alias("total_revenue")
    )

    revenue.show()

    revenue.write.mode("overwrite").csv(
        "/opt/airflow/output/revenue_by_category",
        header=True
    )

    
    store_lineage(
    source="enriched_sales",
    transformation="revenue_by_category_transformation",
    target="revenue_by_category"
    )
    
    print("Revenue by category written successfully")

    spark.stop()


if __name__ == "__main__":
    build_revenue_by_category()