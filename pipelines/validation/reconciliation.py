def reconcile_counts(source_df, target_df):
    source_count = source_df.count()
    target_count = target_df.count()

    return {
        "source_count": source_count,
        "target_count": target_count,
        "difference": source_count - target_count
    }