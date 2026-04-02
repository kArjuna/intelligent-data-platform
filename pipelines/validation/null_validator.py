from pyspark.sql.functions import col, sum, when

def check_nulls(df, columns):
    null_counts = df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns
    ])
    return null_counts.collect()[0].asDict()