# E-commerce Data Pipeline (Airflow + PySpark)

## Overview
This project implements an end-to-end data pipeline for processing e-commerce transaction data using **Apache Airflow** and **PySpark**.

The pipeline automates data ingestion, cleaning, transformation, and storage in an optimized format for analytics.

---

## Tech Stack
- Python  
- Apache Airflow  
- PySpark  
- Docker  
- Pandas (optional for local testing)  
- Parquet (data storage format)  

---

## Architecture

1. Airflow orchestrates the pipeline using DAGs  
2. PySpark processes large-scale data  
3. Cleaned & transformed data is stored as partitioned Parquet files  

---

## Pipeline Workflow

### 1️. Data Ingestion
- Reads raw CSV data (`ecommerce.csv`)

### 2. Data Cleaning
- Handles missing values  
- Removes invalid records  
- Standardizes categorical fields  

### 3️. Feature Engineering
- Extracts date features (year, month)  
- Creates revenue column (`Price × Quantity`)  

### 4️. Data Transformation
- Selects relevant columns  
- Prepares dataset for downstream analytics  

### 5️. Data Storage
- Stores output in **Parquet format**
- Partitioned by **year** for optimized querying  

