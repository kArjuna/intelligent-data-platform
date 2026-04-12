import os
import json
import pandas as pd
import streamlit as st
from datetime import datetime

st.set_page_config(page_title="Intelligent Data Platform", layout="wide")

st.title("Intelligent Data Quality & Lineage Platform")
st.subheader("Capstone Monitoring Dashboard")
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

st.markdown("---")

# =========================
# Paths
# =========================
validation_file = "output/validation_results.json"
revenue_dir = "output/revenue_by_category"
customer_metrics_dir = "output/curated/customer_metrics"
product_metrics_dir = "output/curated/product_metrics"
enriched_sales_dir = "output/curated/enriched_sales"

streaming_bronze_dir = "output/streaming/orders_bronze"
streaming_silver_dir = "output/streaming/orders_silver"
streaming_gold_dir = "output/streaming/orders_gold"


def folder_exists_with_files(folder_path: str) -> bool:
    return os.path.exists(folder_path) and len(os.listdir(folder_path)) > 0


def read_first_csv_from_folder(folder_path: str) -> pd.DataFrame | None:
    if not folder_exists_with_files(folder_path):
        return None

    files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    if not files:
        return None

    return pd.read_csv(os.path.join(folder_path, files[0]))


def read_parquet_folder(folder_path: str) -> pd.DataFrame | None:
    if not folder_exists_with_files(folder_path):
        return None
    try:
        return pd.read_parquet(folder_path)
    except Exception:
        return None


# =========================
# 1. Pipeline Overview
# =========================
st.header("1. Pipeline Overview")

pipeline_status = "Success" if folder_exists_with_files(revenue_dir) else "Not Available"

records_processed = "N/A"
duplicate_count_metric = "N/A"
validation_results = None

if os.path.exists(validation_file):
    with open(validation_file, "r") as f:
        validation_results = json.load(f)

    duplicate_count_metric = validation_results.get("duplicate_count", "N/A")
    reconciliation = validation_results.get("reconciliation", {})
    records_processed = reconciliation.get("source_count", "N/A")

col1, col2, col3 = st.columns(3)
col1.metric("Pipeline Status", pipeline_status)
col2.metric("Records Processed", records_processed)
col3.metric("Duplicate Count", duplicate_count_metric)

st.markdown("---")

# =========================
# 2. Data Quality Summary
# =========================
st.header("2. Data Quality Summary")

if validation_results:
    null_check = validation_results.get("null_check", {})
    duplicate_count = validation_results.get("duplicate_count", "N/A")
    reconciliation = validation_results.get("reconciliation", {})

    quality_df = pd.DataFrame({
        "Check": [
            "Schema Validation",
            "Null Check",
            "Duplicate Check",
            "Reconciliation Check"
        ],
        "Result": [
            "Passed" if validation_results.get("schema_validation", False) else "Failed",
            str(null_check),
            str(duplicate_count),
            str(reconciliation)
        ]
    })

    st.table(quality_df)
else:
    st.warning("validation_results.json not found yet.")

st.markdown("---")

# =========================
# 3. Revenue by Category
# =========================
st.header("3. Revenue by Category")

revenue_df = read_first_csv_from_folder(revenue_dir)

if revenue_df is not None:
    st.dataframe(revenue_df, use_container_width=True)

    if "category" in revenue_df.columns and "total_revenue" in revenue_df.columns:
        st.bar_chart(revenue_df.set_index("category"))
else:
    st.info("Revenue by category output not found yet.")

st.markdown("---")

# =========================
# 4. Customer Metrics
# =========================
st.header("4. Customer Metrics")

customer_df = read_parquet_folder(customer_metrics_dir)
if customer_df is not None:
    st.dataframe(customer_df, use_container_width=True)
else:
    st.info("Customer metrics output not found yet.")

st.markdown("---")

# =========================
# 5. Product Metrics
# =========================
st.header("5. Product Metrics")

product_df = read_parquet_folder(product_metrics_dir)
if product_df is not None:
    st.dataframe(product_df, use_container_width=True)

    if "product_name" in product_df.columns and "total_revenue" in product_df.columns:
        chart_df = product_df[["product_name", "total_revenue"]].set_index("product_name")
        st.bar_chart(chart_df)
else:
    st.info("Product metrics output not found yet.")

st.markdown("---")

# =========================
# 6. Curated Output Summary
# =========================
st.header("6. Curated Output Summary")

summary_df = pd.DataFrame({
    "Dataset": [
        "enriched_sales",
        "customer_metrics",
        "product_metrics",
        "revenue_by_category"
    ],
    "Available": [
        "Yes" if folder_exists_with_files(enriched_sales_dir) else "No",
        "Yes" if folder_exists_with_files(customer_metrics_dir) else "No",
        "Yes" if folder_exists_with_files(product_metrics_dir) else "No",
        "Yes" if folder_exists_with_files(revenue_dir) else "No",
    ]
})

st.table(summary_df)

st.markdown("---")

# =========================
# 7. Lineage Summary
# =========================
st.header("7. Lineage Summary")

st.markdown("""
**Batch Source Datasets**
- `orders.csv`
- `orders_enriched.csv`
- `customers.csv`
- `products.csv`

**Transformations**
- `sales_pipeline`
- `customer_metrics_transformation`
- `product_metrics_transformation`
- `revenue_by_category_transformation`

**Targets**
- `enriched_sales`
- `customer_metrics`
- `product_metrics`
- `revenue_by_category`
""")

st.markdown("---")

# =========================
# 8. Streaming Layer Status
# =========================
st.header("8. Streaming Layer Status")

stream_status_df = pd.DataFrame({
    "Layer": ["Bronze", "Silver", "Gold"],
    "Available": [
        "Yes" if folder_exists_with_files(streaming_bronze_dir) else "No",
        "Yes" if folder_exists_with_files(streaming_silver_dir) else "No",
        "Yes" if folder_exists_with_files(streaming_gold_dir) else "No",
    ]
})

st.table(stream_status_df)

st.markdown("---")

# =========================
# 9. Project Summary
# =========================
st.header("9. Project Summary")

st.markdown("""
This platform supports both **batch ETL** and **real-time streaming ingestion**.

**Batch Path**
- CSV -> Ingestion -> Validation -> Transformation -> Curated Outputs

**Streaming Path**
- Kafka Producer -> Kafka Topic -> Bronze -> Silver -> Gold

The project integrates **PySpark, Airflow, Neo4j, Streamlit, Docker, and Kafka** into a single production-style data engineering platform.
""")