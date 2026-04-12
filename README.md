<!-- # Intelligent Data Quality & Lineage Platform

## Overview
This project implements an end-to-end data engineering platform using PySpark, Airflow, Neo4j, and Streamlit.

## Architecture
- Data Ingestion: CSV input
- Processing: PySpark
- Orchestration: Airflow
- Data Quality: Custom validation framework
- Lineage Tracking: Neo4j
- Monitoring: Streamlit dashboard
- Containerization: Docker

## Features
- Automated ETL pipeline
- Data validation (schema, nulls, duplicates)
- Aggregation and transformation
- Data lineage tracking
- Interactive monitoring dashboard

## Tech Stack
- Python
- PySpark
- Apache Airflow
- Neo4j
- Streamlit
- Dockerss

## How to Run
```bash
docker compose up -d

streamlit run dashboard/streamlit_app.py
 -->


# Intelligent Data Quality & Lineage Platform 🚀

## 📌 Overview

This project presents an end-to-end data engineering platform designed to ensure **data quality, reliability, and traceability**. The system integrates data ingestion, validation, transformation, orchestration, lineage tracking, and visualization into a unified architecture.

The platform processes raw CSV data, applies validation checks, transforms it into curated datasets, tracks lineage using Neo4j, and visualizes insights through a Streamlit dashboard.

---

## 🎯 Problem Statement

Modern data systems often suffer from:

* Poor data quality (missing, inconsistent, duplicate data)
* Lack of visibility into data transformations
* Limited integration between ETL pipelines and lineage tracking
* Difficulty monitoring pipeline outputs

This project addresses these challenges by building a **modular, scalable, and observable data pipeline**.

---

## 🏗️ Architecture

```
Raw CSV Data
   ↓
Ingestion Layer (PySpark)
   ↓
Raw Zone (Parquet)
   ↓
Validation Layer (Schema, Null, Duplicate, Reconciliation)
   ↓
Transformation Layer (Sales Pipeline)
   ↓
Curated Outputs
   ├── enriched_sales
   ├── customer_metrics
   ├── product_metrics
   └── revenue_by_category
   ↓
Lineage Tracking (Neo4j)
   ↓
Dashboard (Streamlit)
```

---

## ⚙️ Tech Stack

* **PySpark** → Data processing and transformations
* **Apache Airflow** → Workflow orchestration
* **Neo4j** → Data lineage tracking
* **Streamlit** → Interactive dashboard
* **Docker** → Containerized environment

---

## 🔄 Pipeline Workflow

### 1. Ingestion Layer

* Reads CSV data (`orders`, `customers`, `products`)
* Converts to Parquet format for efficient processing

### 2. Validation Layer

* Schema validation
* Null value detection
* Duplicate detection
* Data reconciliation

### 3. Transformation Layer

* Joins datasets to create `enriched_sales`
* Generates business metrics:

  * Customer metrics
  * Product metrics
  * Revenue by category

### 4. Orchestration

* Multi-step Airflow DAG
* Task dependencies:

  * ingestion → transformation → metrics

### 5. Lineage Tracking

* Tracks relationships:

  * datasets → transformations → outputs
* Stored in Neo4j graph database

### 6. Visualization

* Streamlit dashboard showing:

  * pipeline status
  * validation results
  * revenue insights
  * customer & product analytics

---

## 📂 Project Structure

```
intelligent-data-platform/
│
├── data/
├── dags/
├── pipelines/
│   ├── ingestion/
│   ├── transformations/
│   └── validation/
├── lineage/
├── dashboard/
├── docker/
├── output/
└── README.md
```

---

## ▶️ How to Run

### 1. Start Services

```
docker compose up -d
```

### 2. Run Airflow

Open:

```
http://localhost:8080
```

Trigger:

```
multi_step_etl_dag
```

---

### 3. Run Dashboard

```
streamlit run dashboard/streamlit_app.py
```

Open:

```
http://localhost:8501
```

---

### 4. View Lineage

Open:

```
http://localhost:7474
```

Run:

```
MATCH (n) RETURN n
```

---

## 📊 Outputs

* `enriched_sales` → cleaned + joined dataset
* `customer_metrics` → customer insights
* `product_metrics` → product performance
* `revenue_by_category` → business KPI

---

## 🧠 Key Features

* End-to-end ETL pipeline
* Data quality validation framework
* Multi-step Airflow DAG orchestration
* Graph-based lineage tracking
* Interactive analytics dashboard

---

## ⚠️ Challenges Faced

* Managing module imports inside Docker containers
* Configuring Spark and Airflow integration
* Ensuring consistent file paths across services
* Debugging distributed pipeline execution

---

## 🚀 Future Improvements

* Add Kafka-based real-time streaming ingestion
* Implement CI/CD using GitHub Actions
* Deploy platform on AWS or Azure
* Add monitoring and alerting system

---

## 📈 Resume Highlights

* Built scalable ETL pipelines using PySpark
* Designed Airflow DAGs with task dependencies
* Implemented data validation and reconciliation
* Developed lineage tracking using Neo4j
* Created interactive dashboards using Streamlit




## Real-Time Streaming Extension

The platform was extended with Kafka-based real-time ingestion for order events.

### Streaming Flow
Kafka Producer -> Kafka Topic (`orders_stream`) -> Spark Structured Streaming Consumer -> Bronze -> Silver -> Gold

### Streaming Outputs
- `output/streaming/orders_bronze`
- `output/streaming/orders_silver`
- `output/streaming/orders_gold`


---

## 👨‍💻 Author

Mallikarjuna

---

## 🏁 Conclusion

This project demonstrates a production-style data platform integrating ingestion, validation, transformation, lineage, and visualization. It highlights strong data engineering fundamentals and system design capabilities.


docker compose -f docker/docker-compose.yml up -d
docker exec -it airflow airflow users reset-password \\n  --username admin \\n  --password admin\n
docker compose -f docker/docker-compose.yml restart airflow
ls pipelines/transformations\ndocker exec -it airflow ls /opt/airflow/pipelines/transformations
pip install pyarrow
streamlit run dashboard/streamlit_app1.py
source /Users/mallikarjuna/Downloads/projects/intelligent-data-platform/venv/bin/activate
docker compose -f docker/docker-compose.yml restart airflow
docker compose -f docker/docker-compose.yml restart airflow
docker exec -it airflow python /opt/airflow/pipelines/transformations/revenue_by_category.py
docker exec -it airflow python /opt/airflow/pipelines/transformations/revenue_by_category.py\ndocker exec -it airflow python /opt/airflow/pipelines/transformations/customer_metrics.py\ndocker exec -it airflow python /opt/airflow/pipelines/transformations/product_metrics.py
docker compose -f docker/docker-compose.yml restart airflow
docker compose -f docker/docker-compose.yml restart airflow
streamlit run dashboard/streamlit_app1.py