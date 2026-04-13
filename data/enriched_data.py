import csv
import random
from datetime import datetime, timedelta

CUSTOMERS_FILE = "data/customers.csv"
PRODUCTS_FILE = "data/products.csv"
OUTPUT_FILE = "data/orders_enriched.csv"
NUM_ORDERS = 3000

random.seed(42)

def rand_date():
    start = datetime(2025, 1, 1)
    end = datetime(2026, 4, 1)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d")

def main():
    customers = []
    products = []

    with open(CUSTOMERS_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            customers.append(int(row["customer_id"]))

    with open(PRODUCTS_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            products.append(int(row["product_id"]))

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "product_id", "quantity", "order_date"])

        for order_id in range(1, NUM_ORDERS + 1):
            writer.writerow([
                order_id,
                random.choice(customers),
                random.choice(products),
                random.randint(1, 5),
                rand_date()
            ])

    print(f"Created {OUTPUT_FILE} with {NUM_ORDERS} rows")

if __name__ == "__main__":
    main()
