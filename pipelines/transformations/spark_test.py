from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test_spark_job") \
    .getOrCreate()

data = [
    ("Alice", 34),
    ("Bob", 45),
    ("Charlie", 29)
]

columns = ["name", "age"]

df = spark.createDataFrame(data, columns)

df.show()

spark.stop()
