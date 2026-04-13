import os
import json
from datetime import datetime

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Intelligent Data Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================
# Custom CSS
# =========================
st.markdown("""
<style>
    .main {
        background-color: #0e1117;
    }

    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }

    .hero-card {
        padding: 1.2rem 1.4rem;
        border-radius: 18px;
        background: linear-gradient(135deg, #111827, #1f2937);
        border: 1px solid rgba(255,255,255,0.08);
        box-shadow: 0 10px 30px rgba(0,0,0,0.25);
        margin-bottom: 1rem;
    }

    .section-card {
        padding: 1rem 1.2rem;
        border-radius: 16px;
        background: #111827;
        border: 1px solid rgba(255,255,255,0.06);
        margin-bottom: 1rem;
    }

    .small-label {
        font-size: 0.85rem;
        color: #9ca3af;
        margin-bottom: 0.15rem;
    }

    .big-value {
        font-size: 1.6rem;
        font-weight: 700;
        color: white;
    }

    .status-good {
        color: #22c55e;
        font-weight: 700;
    }

    .status-bad {
        color: #ef4444;
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

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

# =========================
# Helpers
# =========================
def has_real_files(folder_path: str) -> bool:
    if not os.path.exists(folder_path):
        return False

    for root, _, files in os.walk(folder_path):
        for f in files:
            if f.endswith(".parquet") or f.endswith(".csv") or f == "_SUCCESS":
                return True
    return False


def read_first_csv_from_folder(folder_path: str) -> pd.DataFrame | None:
    if not has_real_files(folder_path):
        return None

    try:
        files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
        if not files:
            return None
        return pd.read_csv(os.path.join(folder_path, files[0]))
    except Exception:
        return None


def read_parquet_folder(folder_path: str) -> pd.DataFrame | None:
    if not has_real_files(folder_path):
        return None
    try:
        return pd.read_parquet(folder_path)
    except Exception:
        return None


def read_validation_results() -> dict | None:
    if not os.path.exists(validation_file):
        return None
    try:
        with open(validation_file, "r") as f:
            return json.load(f)
    except Exception:
        return None


def status_text(flag: bool) -> str:
    return "Working Fine" if flag else "Not Available"


def styled_status(flag: bool) -> str:
    css_class = "status-good" if flag else "status-bad"
    text = "Working Fine" if flag else "Not Available"
    return f'<span class="{css_class}">{text}</span>'


# =========================
# Load Data
# =========================
validation_results = read_validation_results()
revenue_df = read_first_csv_from_folder(revenue_dir)
customer_df = read_parquet_folder(customer_metrics_dir)
product_df = read_parquet_folder(product_metrics_dir)
enriched_sales_df = read_parquet_folder(enriched_sales_dir)

streaming_bronze_ok = has_real_files(streaming_bronze_dir)
streaming_silver_ok = has_real_files(streaming_silver_dir)
streaming_gold_ok = has_real_files(streaming_gold_dir)

pipeline_ok = revenue_df is not None
records_processed = "N/A"
duplicate_count_metric = "N/A"

if validation_results:
    reconciliation = validation_results.get("reconciliation", {})
    records_processed = reconciliation.get("source_count", "N/A")
    duplicate_count_metric = validation_results.get("duplicate_count", "N/A")

# =========================
# Header
# =========================
st.markdown(f"""
<div class="hero-card">
    <div style="font-size:2rem; font-weight:800; color:white;">Intelligent Data Quality & Lineage Platform</div>
    <div style="font-size:1rem; color:#cbd5e1; margin-top:0.35rem;">
        Capstone Monitoring Dashboard
    </div>
    <div style="font-size:0.9rem; color:#94a3b8; margin-top:0.5rem;">
        Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    </div>
</div>
""", unsafe_allow_html=True)

# =========================
# Sidebar Filters
# =========================
st.sidebar.title("Controls")

dataset_option = st.sidebar.selectbox(
    "Choose dataset for exploration",
    ["enriched_sales", "customer_metrics", "product_metrics", "revenue_by_category"]
)

search_text = st.sidebar.text_input("Search text")
max_rows = st.sidebar.slider("Rows to display", 5, 100, 20)

chart_type = st.sidebar.selectbox(
    "Preferred chart type",
    ["Bar", "Line"]
)

# =========================
# KPI Row
# =========================
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.markdown(f"""
    <div class="section-card">
        <div class="small-label">Pipeline Status</div>
        <div class="big-value">{'Success' if pipeline_ok else 'Not Ready'}</div>
    </div>
    """, unsafe_allow_html=True)
with k2:
    st.markdown(f"""
    <div class="section-card">
        <div class="small-label">Records Processed</div>
        <div class="big-value">{records_processed}</div>
    </div>
    """, unsafe_allow_html=True)
with k3:
    st.markdown(f"""
    <div class="section-card">
        <div class="small-label">Duplicate Count</div>
        <div class="big-value">{duplicate_count_metric}</div>
    </div>
    """, unsafe_allow_html=True)
with k4:
    stream_ok = streaming_bronze_ok and streaming_silver_ok and streaming_gold_ok
    st.markdown(f"""
    <div class="section-card">
        <div class="small-label">Streaming Status</div>
        <div class="big-value">{'Active' if stream_ok else 'Partial / Not Ready'}</div>
    </div>
    """, unsafe_allow_html=True)

# =========================
# Tabs
# =========================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Overview",
    "Validation",
    "Batch Outputs",
    "Streaming",
    "Self-Service Query"
])

# =========================
# Tab 1 Overview
# =========================
with tab1:
    st.subheader("Platform Overview")

    summary_df = pd.DataFrame({
        "Dataset": [
            "enriched_sales",
            "customer_metrics",
            "product_metrics",
            "revenue_by_category",
            "streaming_bronze",
            "streaming_silver",
            "streaming_gold"
        ],
        "Status": [
            status_text(enriched_sales_df is not None),
            status_text(customer_df is not None),
            status_text(product_df is not None),
            status_text(revenue_df is not None),
            status_text(streaming_bronze_ok),
            status_text(streaming_silver_ok),
            status_text(streaming_gold_ok),
        ]
    })
    st.dataframe(summary_df, use_container_width=True)

    st.markdown("""
    **Batch Path**
    - CSV → Ingestion → Validation → Transformation → Curated Outputs

    **Streaming Path**
    - Kafka Producer → Kafka Topic → Bronze → Silver → Gold

    **Integrated Stack**
    - PySpark, Airflow, Neo4j, Streamlit, Docker, Kafka
    """)

# =========================
# Tab 2 Validation
# =========================
with tab2:
    st.subheader("Data Quality Summary")

    if validation_results:
        quality_df = pd.DataFrame({
            "Check": [
                "Schema Validation",
                "Null Check",
                "Duplicate Check",
                "Reconciliation Check"
            ],
            "Result": [
                "Passed" if validation_results.get("schema_validation", False) else "Failed",
                str(validation_results.get("null_check", {})),
                str(validation_results.get("duplicate_count", "N/A")),
                str(validation_results.get("reconciliation", {}))
            ]
        })
        st.dataframe(quality_df, use_container_width=True)
        st.json(validation_results)
    else:
        st.warning("validation_results.json not found yet.")

# =========================
# Tab 3 Batch Outputs
# =========================
with tab3:
    st.subheader("Batch Outputs")

    if revenue_df is not None:
        st.markdown("### Revenue by Category")
        st.dataframe(revenue_df.head(max_rows), use_container_width=True)

        if "category" in revenue_df.columns and "total_revenue" in revenue_df.columns:
            chart_df = revenue_df.set_index("category")[["total_revenue"]]
            if chart_type == "Bar":
                st.bar_chart(chart_df)
            else:
                st.line_chart(chart_df)

    if customer_df is not None:
        st.markdown("### Customer Metrics")
        display_df = customer_df.copy()
        if search_text:
            mask = display_df.astype(str).apply(
                lambda col: col.str.contains(search_text, case=False, na=False)
            ).any(axis=1)
            display_df = display_df[mask]
        st.dataframe(display_df.head(max_rows), use_container_width=True)

    if product_df is not None:
        st.markdown("### Product Metrics")
        display_df = product_df.copy()
        if search_text:
            mask = display_df.astype(str).apply(
                lambda col: col.str.contains(search_text, case=False, na=False)
            ).any(axis=1)
            display_df = display_df[mask]
        st.dataframe(display_df.head(max_rows), use_container_width=True)

# =========================
# Tab 4 Streaming
# =========================
with tab4:
    st.subheader("Streaming Layer Status")

    stream_df = pd.DataFrame({
        "Layer": ["Bronze", "Silver", "Gold"],
        "Available": [
            "Working Fine" if streaming_bronze_ok else "Not Working",
            "Working Fine" if streaming_silver_ok else "Not Working",
            "Working Fine" if streaming_gold_ok else "Not Working",
        ]
    })
    st.dataframe(stream_df, use_container_width=True)

    gold_stream_df = read_parquet_folder(streaming_gold_dir)
    if gold_stream_df is not None:
        st.markdown("### Gold Layer Preview")
        st.dataframe(gold_stream_df.head(max_rows), use_container_width=True)

# =========================
# Tab 5 Self-Service Query
# =========================
with tab5:
    st.subheader("Self-Service Query")

    dataset_map = {
        "enriched_sales": enriched_sales_df,
        "customer_metrics": customer_df,
        "product_metrics": product_df,
        "revenue_by_category": revenue_df
    }

    selected_df = dataset_map.get(dataset_option)

    if selected_df is None:
        st.info(f"{dataset_option} is not available yet.")
    else:
        st.markdown(f"### Querying: `{dataset_option}`")

        available_columns = list(selected_df.columns)
        selected_columns = st.multiselect(
            "Columns to display",
            options=available_columns,
            default=available_columns[:min(6, len(available_columns))]
        )

        filter_column = st.selectbox(
            "Filter column",
            options=["None"] + available_columns
        )

        filtered_df = selected_df.copy()

        if filter_column != "None":
            unique_vals = filtered_df[filter_column].dropna().astype(str).unique().tolist()
            unique_vals = sorted(unique_vals)[:500]
            chosen_vals = st.multiselect(
                f"Values for {filter_column}",
                options=unique_vals
            )
            if chosen_vals:
                filtered_df = filtered_df[filtered_df[filter_column].astype(str).isin(chosen_vals)]

        if search_text:
            mask = filtered_df.astype(str).apply(
                lambda col: col.str.contains(search_text, case=False, na=False)
            ).any(axis=1)
            filtered_df = filtered_df[mask]

        sort_column = st.selectbox("Sort by", options=["None"] + available_columns)
        sort_order_desc = st.checkbox("Sort descending", value=True)

        if sort_column != "None":
            filtered_df = filtered_df.sort_values(by=sort_column, ascending=not sort_order_desc)

        if selected_columns:
            filtered_df = filtered_df[selected_columns]

        st.write(f"Rows returned: {len(filtered_df)}")
        st.dataframe(filtered_df.head(max_rows), use_container_width=True)

        csv_data = filtered_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download Query Result as CSV",
            data=csv_data,
            file_name=f"{dataset_option}_query_result.csv",
            mime="text/csv"
        )
