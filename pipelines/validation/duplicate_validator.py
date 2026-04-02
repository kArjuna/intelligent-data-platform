def check_duplicates(df, key_columns):
    duplicate_count = (
        df.groupBy(key_columns)
        .count()
        .filter("count > 1")
        .count()
    )
    return duplicate_count