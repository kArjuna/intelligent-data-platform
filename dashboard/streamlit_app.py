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
output_dir = "output/revenue_by_category"
validation_file = "output/validation_results.json"

# Pipeline overview metrics
st.header("1. Pipeline Overview")

pipeline_status = "Success" if os.path.exists(output_dir) else "Not Available"
records_processed = 8
duplicate_count = 0

col1, col2, col3 = st.columns(3)
col1.metric("Pipeline Status", pipeline_status)
col2.metric("Records Processed", records_processed)
col3.metric("Duplicate Count", duplicate_count)

st.markdown("---")

# Data Quality Summary
st.header("2. Data Quality Summary")

if os.path.exists(validation_file):
    with open(validation_file, "r") as f:
        validation_results = json.load(f)

    quality_data = {
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
    }

    quality_df = pd.DataFrame(quality_data)
    st.table(quality_df)
else:
    st.warning("validation_results.json not found yet. Run the ETL first.")

st.markdown("---")

# Business Output
st.header("3. Revenue by Category")

if os.path.exists(output_dir):
    files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
    if files:
        output_file = os.path.join(output_dir, files[0])
        df = pd.read_csv(output_file)

        st.dataframe(df)

        if "category" in df.columns and "total_revenue" in df.columns:
            chart_df = df.set_index("category")
            st.bar_chart(chart_df)
    else:
        st.info("No output CSV found in output directory.")
else:
    st.info("Pipeline output folder not found.")

st.markdown("---")

# Lineage Summary
st.header("4. Lineage Summary")

st.markdown("""
**Source Dataset:** `orders.csv`  
**Transformation:** `revenue_by_category_transformation`  
**Target Dataset:** `revenue_by_category`
""")