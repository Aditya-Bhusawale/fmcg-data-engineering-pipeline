# FMCG Data Engineering Pipeline with Databricks

An end-to-end Data Engineering project built using **PySpark**, **Databricks**, **Delta Lake**, and **Amazon S3**. The project implements the **Medallion Architecture (Bronze → Silver → Gold)** to process historical and incremental data for analytics.

---

## Business Problem

Atlon, a leading sports equipment manufacturer, acquired Sports Bar, a rapidly growing sports nutrition company. While Atlon's data was well-structured and maintained in ERP systems, Sports Bar's data was scattered across multiple sources with inconsistent formats and missing records.

The goal was to build a scalable data pipeline that could integrate data from both companies, standardize it, and provide a reliable analytics layer for reporting and business intelligence.

---

## Architecture

```
                 Amazon S3
                     │
                     ▼
            Bronze Layer (Raw)
                     │
                     ▼
        Silver Layer (Clean & Transform)
                     │
                     ▼
        Gold Layer (Analytics Ready)
```

The project follows the **Medallion Architecture**:

- **Bronze Layer** – Stores raw data ingested from Amazon S3.
- **Silver Layer** – Cleans, validates, transforms, and integrates data.
- **Gold Layer** – Stores curated business-ready Delta tables for reporting and analytics.

---

## Tech Stack

- PySpark
- Databricks
- Delta Lake
- Amazon S3
- Spark SQL
- Python

---

## Features

- End-to-End ETL Pipeline
- Medallion Architecture
- Historical Data Loading
- Incremental Data Loading
- Delta MERGE (Upsert)
- Data Cleaning & Transformation
- Schema Validation
- Delta Tables
- Scalable Data Processing

---

## Project Workflow

### 1. Data Ingestion (Bronze Layer)

- Read raw datasets from Amazon S3.
- Stored data as Delta tables.
- Preserved raw data without business transformations.

---

### 2. Data Processing (Silver Layer)

Performed multiple transformations including:

- Removing duplicate records
- Handling null values
- Standardizing column names
- Data type conversion
- Business rule implementation
- Joining transactional data with master data
- Data validation

---

### 3. Gold Layer

Created analytics-ready datasets by:

- Aggregating business metrics
- Creating curated Delta tables
- Preparing data for dashboards and reporting

---

## Historical & Incremental Loading

### Historical Load

- Executed during the first pipeline run.
- Loaded the complete dataset into Delta tables.

### Incremental Load

- Executed during subsequent runs.
- Processed only new or updated records.
- Implemented using **Delta MERGE** for efficient upsert operations.

---

## Folder Structure

```
consolidated_pipelines/
│
├── setup/
│   ├── catalog_setup
│   ├── schema_creation
│   └── utility_notebooks
│
├── dimension_processing/
│   ├── customer_pipeline
│   ├── product_pipeline
│   └── pricing_pipeline
│
├── fact_processing/
    ├── historical_load
    └── incremental_load

---

## Data Engineering Concepts Used

- Medallion Architecture
- Delta Lake
- Delta Tables
- Delta MERGE
- Historical Loading
- Incremental Loading
- ETL Pipeline
- Data Cleaning
- Schema Evolution
- Data Validation
- Spark DataFrames
- PySpark Transformations

---

## My Responsibilities

- Built ETL pipelines using PySpark.
- Ingested raw data from Amazon S3.
- Implemented Bronze, Silver, and Gold layers.
- Performed data cleansing and transformations.
- Developed historical and incremental loading logic.
- Used Delta MERGE for upsert operations.
- Created analytics-ready Delta tables.
- Optimized data processing workflows in Databricks.

---

## Learning Outcomes

This project helped me gain hands-on experience in:

- Apache Spark
- PySpark
- Delta Lake
- Databricks
- Amazon S3 Integration
- ETL Pipeline Development
- Incremental Data Processing
- Data Warehouse Design
- Medallion Architecture
- Data Engineering Best Practices

---

## Future Improvements

- Unity Catalog Integration
- Change Data Feed (CDF)
- Databricks Workflows
- Data Quality Framework
- CI/CD Pipeline
- Monitoring & Logging
- Performance Optimization
- Automated Testing

---


---

⭐ If you found this project useful, feel free to star the repository.
