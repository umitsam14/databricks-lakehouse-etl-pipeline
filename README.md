# 🚀 Databricks Lakehouse ETL Pipeline  
### Bronze → Silver → Gold | AWS S3 | PySpark | Delta Lake

A production-style **Data Engineering Lakehouse pipeline** built using  
**Databricks, PySpark, Delta Lake, and AWS S3**, implementing the modern  
**Medallion Architecture (Bronze, Silver, Gold)**.

This project demonstrates real-world enterprise data engineering patterns:
streaming ingestion, transformations, aggregations, and analytics-ready datasets.

---

## 🧱 Architecture Overview

### 🥉 Bronze Layer (Raw Ingestion)
- Streaming ingestion from AWS S3 (CSV files)
- Spark Structured Streaming
- Raw Delta tables
- Checkpointing for fault tolerance

### 🥈 Silver Layer (Transformation)
- Data cleaning
- Type casting
- Deduplication
- Business transformations
- Clean Delta tables

### 🥇 Gold Layer (Analytics)
- Aggregated datasets
- Customer analytics
- Business KPIs
- Reporting-ready tables

---

## 🔄 Data Flow

AWS S3 (Raw CSV)  
→ Bronze Delta Table  
→ Silver Delta Table  
→ Gold Delta Table  
→ Analytics / BI / SQL

---

## 🛠️ Tech Stack

- **Compute**: Databricks
- **Processing**: PySpark
- **Storage**: Delta Lake
- **Cloud**: AWS S3
- **Streaming**: Spark Structured Streaming
- **Architecture**: Lakehouse + Medallion Architecture

---

## 📂 Repository Structure

```txt
databricks-lakehouse-etl-pipeline/
│
├── README.md
│
├── notebooks/
│   ├── 1_bronze_ingestion.py
│   ├── 2_silver_transformation.py
│   └── 3_gold_aggregation.py
│
├── architecture/
│   └── architecture.txt
│
├── screenshots/
│    ├── bronze_notebook.png
│    ├── silver_notebook.png
│    ├── gold_notebook.png
│    └── etl_pipeline.png
│
├── utils/
│   └── common_paths.py
│
├── .gitignore
└── requirements.txt
