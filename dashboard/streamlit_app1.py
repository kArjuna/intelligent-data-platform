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

# Paths
validation_file = "output/validation_results.json"
revenue_dir = "output/revenue_by_category"
customer_metrics_dir = "output/curated/customer_metrics"
product_metrics_dir = "output/curated/product_metrics"
enriched_sales_dir = "output/curated/enriched_sales"


def read_first_csv_from_folder(folder_path: str) -> pd.DataFrame | None:
    if not os.path.exists(folder_path):
        return None

    files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    if not files:
        return None

    file_path = os.path.join(folder_path, files[0])
    return pd.read_csv(file_path)


def folder_exists_with_files(folder_path: str) -> bool:
    return os.path.exists(folder_path) and len(os.listdir(folder_path)) > 0


# =========================
# 1. Pipeline Overview
# =========================
st.header("1. Pipeline Overview")

pipeline_status = "Success" if folder_exists_with_files(revenue_dir) else "Not Available"

records_processed = "N/A"
duplicate_count_metric = "N/A"

if os.path.exists(validation_file):
    with open(validation_file, "r") as f:
        validation_results = json.load(f)

    duplicate_count_metric = validation_results.get("duplicate_count", "N/A")

    reconciliation = validation_results.get("reconciliation", {})
    records_processed = reconciliation.get("source_count", "N/A")
else:
    validation_results = None

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

    quality_data = {
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
    }

    quality_df = pd.DataFrame(quality_data)
    st.table(quality_df)
else:
    st.warning("validation_results.json not found yet. Run the ETL pipeline first.")

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

if folder_exists_with_files(customer_metrics_dir):
    try:
        customer_df = pd.read_parquet(customer_metrics_dir)
        st.dataframe(customer_df, use_container_width=True)
    except Exception as e:
        st.warning(f"Could not read customer metrics: {e}")
else:
    st.info("Customer metrics output not found yet.")

st.markdown("---")

# =========================
# 5. Product Metrics
# =========================
st.header("5. Product Metrics")

if folder_exists_with_files(product_metrics_dir):
    try:
        product_df = pd.read_parquet(product_metrics_dir)
        st.dataframe(product_df, use_container_width=True)

        if "product_name" in product_df.columns and "total_revenue" in product_df.columns:
            chart_df = product_df[["product_name", "total_revenue"]].set_index("product_name")
            st.bar_chart(chart_df)
    except Exception as e:
        st.warning(f"Could not read product metrics: {e}")
else:
    st.info("Product metrics output not found yet.")

st.markdown("---")

# =========================
# 6. Curated Output Summary
# =========================
st.header("6. Curated Output Summary")

summary_data = {
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
}

summary_df = pd.DataFrame(summary_data)
st.table(summary_df)

st.markdown("---")

# =========================
# 7. Lineage Summary
# =========================
st.header("7. Lineage Summary")

st.markdown("""
**Source Datasets**
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

# =========================
# 8. Streaming Layer Status
# =========================

st.markdown("---")
st.header("8. Streaming Layer Status")

streaming_paths = {
    "Bronze": "output/streaming/orders_bronze",
    "Silver": "output/streaming/orders_silver",
    "Gold": "output/streaming/orders_gold"
}

stream_status = {
    "Layer": [],
    "Available": []
}

for layer, path in streaming_paths.items():
    stream_status["Layer"].append(layer)
    stream_status["Available"].append(
        "Working" if os.path.exists(path) and len(os.listdir(path)) > 0 else "Not Working"
    )

st.table(pd.DataFrame(stream_status))