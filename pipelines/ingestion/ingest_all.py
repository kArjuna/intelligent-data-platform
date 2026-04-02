from pipelines.ingestion.ingest_orders import ingest_orders
from pipelines.ingestion.ingest_products import ingest_products
from pipelines.ingestion.ingest_customers import ingest_customers
from pipelines.ingestion.ingest_orders_enriched import ingest_orders_enriched


def run_all_ingestion():
    ingest_orders()
    ingest_products()
    ingest_customers()
    ingest_orders_enriched()


if __name__ == "__main__":
    run_all_ingestion()