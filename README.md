# financial-etl-pipeline
End-to-end financial data engineering pipeline using Python pandas PySpark AWS S3 Airflow and Power BI

# Financial ETL Pipeline: End-to-End Financial Analytics Engineering Project

## Project Overview

This project is a multi-stage Financial Data Engineering and Analytics Pipeline designed to simulate how financial data moves through a modern analytics ecosystem.

The project starts with raw financial statement data and progressively transforms it into an analytics-ready data warehouse capable of supporting business intelligence, financial analysis, and dashboarding.

The overall objective is to combine:

* Financial Analysis
* Data Engineering
* SQL Analytics
* Cloud Technologies
* Business Intelligence

into a single end-to-end project.

---

# Project Roadmap

The project is divided into two major phases.

## Part 1: Financial ETL & Analytics Foundation (Completed)

Focus Areas:

* Data Extraction
* Data Cleaning
* Financial KPI Engineering
* PostgreSQL Data Warehouse
* SQL Analytics
* Git & GitHub

Architecture:

```text
Raw CSV Files
      │
      ▼
Pandas Data Cleaning
      │
      ▼
Financial KPI Engineering
      │
      ▼
Processed Dataset
      │
      ▼
PostgreSQL Warehouse
      │
      ▼
SQL Analytics
```

---

## Part 2: Production Data Engineering Pipeline (Planned)

Focus Areas:

* AWS S3
* PySpark
* Parquet Storage
* Apache Airflow
* Power BI
* Cloud Data Engineering

Architecture:

```text
Local CSV
      │
      ▼
AWS S3
      │
      ▼
PySpark Processing
      │
      ▼
Parquet Files
      │
      ▼
PostgreSQL Warehouse
      │
      ▼
Power BI Dashboard
```

---

# Business Problem

Financial statement data often exists across multiple years and companies.

Without transformation and standardization, it is difficult to:

* Compare company performance
* Analyze profitability
* Measure operational efficiency
* Evaluate growth trends
* Assess financial risk
* Build business intelligence dashboards

This project transforms raw financial statement data into a centralized analytics-ready warehouse containing financial KPIs and business insights.

---

# Part 1: Financial ETL & Analytics Foundation

## Dataset Description

The dataset contains multi-year company financial information including:

* Company Name
* Financial Year
* Sales Revenue
* Expenses
* Operating Profit
* Interest Expense
* Depreciation
* Profit Before Tax
* Net Profit
* Earnings Per Share (EPS)

The dataset contains financial information across multiple companies and years, making it suitable for profitability, growth, and risk analysis.

---

## Technology Stack Used

| Category                | Tools                |
| ----------------------- | -------------------- |
| Programming             | Python               |
| Data Processing         | Pandas               |
| Database                | PostgreSQL           |
| Database Connectivity   | SQLAlchemy, psycopg2 |
| Analytics               | SQL                  |
| Development Environment | Jupyter Notebook     |
| Version Control         | Git                  |
| Repository Hosting      | GitHub               |

---

## ETL Workflow

### Extract

Raw financial CSV files were loaded into Pandas DataFrames.

Activities:

* Dataset inspection
* Schema understanding
* Data profiling

---

### Transform

Data cleaning and feature engineering were performed.

Cleaning Tasks:

* Missing value handling
* Duplicate removal
* Data type validation
* Infinite value handling
* Standardization

Example:

```python
df = df.replace([np.inf, -np.inf], np.nan)
```

Financial KPI engineering was performed after cleaning.

---

### Load

The transformed dataset was loaded into PostgreSQL using:

* SQLAlchemy
* psycopg2

The final warehouse table:

```sql
company_financials
```

acts as the central analytics layer for downstream analysis.

---

# Financial KPI Engineering

The following KPIs were engineered.

## Profitability Metrics

### Profit Margin

Net Profit / Sales × 100

Measures profitability generated from revenue.

---

### Operating Profit Margin (OPM)

Operating Profit / Sales × 100

Measures operational efficiency before financing and taxes.

---

## Efficiency Metrics

### Expense Ratio

Expenses / Sales × 100

Measures cost intensity relative to revenue.

---

## Leverage Metrics

### Interest Coverage Ratio

Operating Profit / Interest

Measures ability to meet interest obligations.

---

### Interest Burden Ratio

Profit Before Tax / Operating Profit

Measures profitability lost due to financing costs.

---

## Tax Metrics

### Tax Burden Ratio

Net Profit / Profit Before Tax

Measures earnings retained after taxation.

---

## Growth Metrics

### Revenue Growth

Year-over-Year Sales Growth

---

### Profit Growth

Year-over-Year Profit Growth

---

### CAGR

Compound Annual Growth Rate

Measures long-term business growth.

---

# PostgreSQL Data Warehouse

A PostgreSQL warehouse was built to store:

* Financial metrics
* Growth metrics
* Profitability metrics
* Risk metrics
* Company-level KPI data

Warehouse Table:

```sql
company_financials
```

The warehouse serves as the single source of truth for analytics.

---

# SQL Analytics Layer

Business-focused SQL queries were created across four domains.

## Sales & Scale Analysis

* Top companies by sales
* Fastest growing companies by CAGR
* Industry sales trends
* Declining sales companies

---

## Profitability Analysis

* Highest profit margin companies
* Most efficient companies
* Lowest expense ratio companies

---

## Growth Analysis

* Revenue growth leaders
* Profit growth leaders
* Consistent growth companies
* Growth volatility analysis

---

## Risk & Leverage Analysis

* Weakest interest coverage companies
* Interest burden analysis
* Tax burden analysis
* Financial deterioration detection

---

# Key Skills Demonstrated

This project demonstrates:

* ETL Pipeline Development
* Data Cleaning
* Financial KPI Engineering
* SQL Analytics
* PostgreSQL Warehousing
* Business Analysis
* Financial Statement Analysis
* Git & GitHub Workflow

---

# Part 2 Roadmap

The next phase will convert the project into a production-style cloud pipeline using:

* AWS S3 Data Lake
* PySpark Transformations
* Apache Airflow Orchestration
* Automated ETL Pipelines
* Parquet Storage
* Power BI Dashboards

The final system will support cloud-based data processing and business intelligence reporting.

---

# Author

Lakshay Sopra

| Data Engineering Enthusiast | CA Inter Group 1 Qualified 
