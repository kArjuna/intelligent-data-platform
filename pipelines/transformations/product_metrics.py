# from pyspark.sql import SparkSession
# from pyspark.sql.functions import sum


# def build_product_metrics():
#     spark = SparkSession.builder.appName("Product Metrics").getOrCreate()

#     df = spark.read.parquet("/opt/airflow/output/curated/orders_clean")

#     metrics = df.groupBy("product", "category").agg(
#         sum("quantity").alias("units_sold"),
#         sum("total_price").alias("total_revenue")
#     )

#     metrics.show()

#     metrics.write.mode("overwrite").parquet("/opt/airflow/output/curated/product_metrics")

#     print("Product metrics written")

#     spark.stop()


# if __name__ == "__main__":
#     build_product_metrics()

import sys
sys.path.insert(0, "/opt/airflow")

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from lineage.lineage_tracker import store_lineage

def build_product_metrics():
    spark = SparkSession.builder.appName("Product Metrics").getOrCreate()

    sales = spark.read.parquet("/opt/airflow/output/curated/enriched_sales")

    product_metrics = sales.groupBy("product_name", "category").agg(
        sum("quantity").alias("units_sold"),
        sum("total_price").alias("total_revenue")
    )

    product_metrics.show()

    product_metrics.write.mode("overwrite").parquet("/opt/airflow/output/curated/product_metrics")
    
    store_lineage(
    source="enriched_sales",
    transformation="product_metrics_transformation",
    target="product_metrics"
    )
    
    print("Product metrics written successfully")

    spark.stop()


if __name__ == "__main__":
    build_product_metrics()