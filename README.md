# Financial ETL Pipeline with Risk and Compliance Monitoring

This project is an end-to-end data engineering pipeline built using Databricks, PySpark, and Delta Lake. It simulates how financial data is ingested, processed, analyzed, and monitored in real-world systems.

---

## Overview

The pipeline ingests stock market data, processes it through multiple stages, and generates risk and compliance insights. It also includes audit logging and error handling to make the system reliable and traceable.

---

## Key Features

* Incremental data ingestion (no full reloads)
* Layered architecture (Bronze, Silver, Gold)
* Time-series feature engineering
* Risk scoring based on price movement and volatility
* Basic compliance flag based on abnormal volume
* Audit logging for pipeline monitoring
* Error handling and data validation

---

## Architecture

```
Data Source (YFinance API)
        ↓
Bronze Layer (Raw Data)
        ↓
Incremental Processing
        ↓
Silver Layer (Cleaned Data)
        ↓
Feature Engineering
        ↓
Gold Layer (Risk & Compliance)
        ↓
Audit Logging
```

---

## Tech Stack

* Python
* PySpark
* Databricks
* Delta Lake

---

## Pipeline Workflow

### 1. Data Ingestion (Bronze Layer)

Stock data is fetched using the YFinance API and stored in a raw Delta table with ingestion metadata.

---

### 2. Incremental Processing

Only new records are loaded based on the latest available date in the existing dataset.

---

### 3. Data Cleaning (Silver Layer)

* Duplicate records are removed
* Invalid or null values are filtered
* Incorrect records are stored separately for tracking

---

### 4. Feature Engineering

The following features are created using PySpark window functions:

* Daily returns
* 7-day and 30-day moving averages
* Rolling average volume
* Volatility

---

### 5. Risk and Compliance (Gold Layer)

* A compliance flag is raised when volume exceeds 3x the rolling average
* A risk score is calculated using returns and volatility

---

### 6. Audit Logging

Each pipeline run is tracked with:

* Unique job ID
* Execution time
* Rows processed
* Rows rejected
* Status (success or failure)
* Error message (if any)

---

### 7. Error Handling and Data Quality

The pipeline is wrapped in a try-except block to:

* Capture failures
* Log errors
* Prevent silent crashes

Basic validation checks ensure that invalid data does not move forward.

---

## Data Model

### Bronze Table

Raw data with ingestion metadata.

### Silver Table

Cleaned and validated data.

### Gold Table

Final dataset with risk and compliance indicators.

### Audit Table

Logs of pipeline execution and performance.

---

## How to Run

1. Open the project in Databricks
2. Run notebooks in the following order:

```
01_ingestion_bronze  
02_silver_layer  
03_gold_layer  
04_audit_logging  
05_data_quality  
```

3. Verify outputs using:

```sql
SELECT * FROM gold_stock_risk;
SELECT * FROM ETL_Audit_Log;
```

---

## Use Case

This pipeline reflects how financial systems handle market data for:

* Risk monitoring
* Detecting unusual activity
* Data validation and reporting
* Audit and compliance tracking

---

## What I Learned

* Designing and building layered ETL pipelines
* Handling schema constraints in Delta Lake
* Writing efficient transformations using PySpark
* Implementing logging and error handling in pipelines

---

## Future Improvements

* Add support for multiple stocks
* Introduce real-time streaming
* Build dashboards for visualization
* Integrate with cloud storage systems

---

## Author

Shaik Raheem
