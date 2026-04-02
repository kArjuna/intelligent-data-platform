def validate_schema(df, expected_columns):
    actual_columns = df.columns
    missing_columns = [col for col in expected_columns if col not in actual_columns]

    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")

    return True