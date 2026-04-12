from pipelines.validation.schema_validator import validate_schema


class MockDF:
    def __init__(self, columns):
        self.columns = columns


def test_validate_schema_success():
    df = MockDF(["order_id", "product", "category", "price", "quantity"])
    assert validate_schema(df, ["order_id", "product", "category", "price", "quantity"]) is True
