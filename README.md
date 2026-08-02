# 📊 Financial ETL Pipeline
### End-to-End Financial Data Engineering & Analytics Pipeline using Python, Pandas, PySpark, AWS, PostgreSQL, Apache Airflow & Power BI

---

## Project Overview

Financial organizations generate massive amounts of financial statement data every year. However, raw financial data is often inconsistent, spread across multiple files, and unsuitable for direct business analysis.

This project demonstrates how raw financial data can be transformed into an analytics-ready data warehouse using modern Data Engineering principles.

The project follows an end-to-end ETL approach beginning with raw financial statements and progressively building a scalable analytics pipeline using both traditional and cloud-based data engineering technologies.

The project has been intentionally divided into two stages to demonstrate the evolution from foundational ETL development to production-style cloud data engineering.

---

# Project Objectives

The primary objectives of this project are to:

- Build a complete Financial ETL Pipeline
- Standardize raw financial statement data
- Engineer meaningful financial KPIs
- Design an analytics-ready warehouse
- Perform business-focused SQL analysis
- Build a production-style PySpark ETL pipeline
- Implement a comprehensive data validation framework
- Integrate AWS cloud services
- Simulate real-world Data Engineering workflows

---

# Business Problem

Financial statement data is typically collected across multiple companies and financial years.

Without proper transformation and validation, it becomes difficult to:

- Compare company performance
- Analyze profitability
- Measure operational efficiency
- Evaluate business growth
- Assess financial risk
- Build business intelligence dashboards
- Perform large-scale financial analytics

This project transforms raw financial statements into a centralized analytics-ready dataset suitable for business reporting and financial analysis.

---

# Project Roadmap

The project has been divided into two major phases.

---

# Part 1 — Financial ETL & Analytics Foundation (Completed)

The first phase focuses on building a complete Financial ETL pipeline using Python, Pandas, PostgreSQL, and SQL.

Major components include:

- Financial Data Extraction
- Data Cleaning
- Feature Engineering
- Financial KPI Engineering
- PostgreSQL Data Warehouse
- SQL Analytics
- Git Version Control

### Architecture

```text
                 Raw Financial CSV Files
                           │
                           ▼
                  Pandas Data Processing
                           │
                           ▼
              Data Cleaning & Standardization
                           │
                           ▼
             Financial KPI Engineering
                           │
                           ▼
              Processed Financial Dataset
                           │
                           ▼
              PostgreSQL Data Warehouse
                           │
                           ▼
                  Business SQL Analytics
```

---

# Part 2 — Production Data Engineering Pipeline (In Progress)

The second phase transforms the project into a production-style cloud data engineering solution using PySpark and AWS technologies.

Completed Components

- Spark Session Management
- AWS S3 Integration
- Modular ETL Architecture
- Data Extraction
- Data Exploration
- Data Transformation
- Financial KPI Engineering
- Production Data Validation Framework
- Logging
- Testing Utilities

Upcoming Components

- Parquet Data Lake
- Amazon Redshift
- Apache Airflow Orchestration
- Power BI Dashboard

### Architecture

```text
                 Raw Financial CSV Files
                           │
                           ▼
                  Amazon S3 (Raw Layer)
                           │
                           ▼
                 PySpark Data Extraction
                           │
                           ▼
                  Data Exploration
                           │
                           ▼
                 Data Transformation
                           │
                           ▼
             Financial KPI Engineering
                           │
                           ▼
           Production Data Validation
                           │
                           ▼
                 Parquet Data Lake
                           │
                           ▼
                 Amazon Redshift
                           │
                           ▼
             Apache Airflow Pipeline
                           │
                           ▼
                Power BI Dashboard
```

---

# Technology Stack

| Category | Technologies |
|-----------|--------------|
| Programming Language | Python |
| Data Processing | Pandas, PySpark |
| Big Data Framework | Apache Spark |
| Database | PostgreSQL |
| Cloud Storage | Amazon S3 |
| Data Warehouse | PostgreSQL, Amazon Redshift *(In Progress)* |
| File Format | CSV, Parquet |
| SQL | PostgreSQL SQL |
| Workflow Orchestration | Apache Airflow *(In Progress)* |
| Data Visualization | Power BI *(In Progress)* |
| Development Environment | Jupyter Notebook, VS Code |
| Version Control | Git |
| Repository Hosting | GitHub |

---

# Project Repository Structure

```text
financial-etl-pipeline/

│
├── README.md
│
├── part1_pandas/
│   │
│   ├── data/
│   ├── logs/
│   ├── notebooks/
│   ├── sql/
│   └── src/
│
├── part2_cloud_spark/
│   │
│   ├── docs/
│   ├── dags/
│   ├── logs/
│   ├── notebooks/
│   ├── tests/
│   │
│   ├── src/
│   │   ├── config/
│   │   ├── extract/
│   │   ├── transform/
│   │   ├── validation/
│   │   ├── ingestion/
│   │   ├── load/
│   │   ├── utils/
│   │   ├── spark_session.py
│   │   └── main.py
│   │
│   └── requirements.txt
```

---

# Skills Demonstrated

This project demonstrates practical experience with:

### Data Engineering

- ETL Pipeline Development
- Data Cleaning
- Data Transformation
- Feature Engineering
- Data Validation
- Production Data Processing

### Financial Analytics

- Financial Statement Analysis
- Financial KPI Engineering
- Profitability Analysis
- Growth Analysis
- Risk Analysis
- Business Performance Measurement

### Big Data

- Apache Spark
- PySpark
- Distributed Data Processing

### Cloud

- AWS S3
- Cloud-based ETL
- Data Lake Architecture

### Database

- PostgreSQL
- SQL Analytics
- Data Warehousing

### Software Engineering

- Modular Project Architecture
- Logging
- Testing
- Git Workflow
- GitHub Version Control

---


# Part 1 — Financial ETL & Analytics Foundation

The first phase of this project focuses on building a robust Financial ETL pipeline using Python and Pandas. The objective was to transform raw financial statement data into an analytics-ready warehouse capable of supporting business intelligence, financial reporting, and SQL-based analysis.

This phase establishes the foundation for the cloud-based PySpark pipeline implemented in Part 2.

---

# Dataset Description

The dataset contains multi-year financial statement data collected from thousands of publicly listed companies.

Each company contains multiple years of financial information, including:

| Financial Metric |
|------------------|
| Company Name |
| Financial Year |
| Sales Revenue |
| Expenses |
| Operating Profit |
| Interest Expense |
| Depreciation |
| Profit Before Tax |
| Net Profit |
| Earnings Per Share (EPS) |

The dataset enables profitability analysis, operational efficiency measurement, financial risk assessment, and long-term growth evaluation across companies.

---

# ETL Workflow

The Part 1 pipeline follows the traditional Extract → Transform → Load (ETL) architecture.

```text
               Raw Financial CSV Files
                          │
                          ▼
                    Data Extraction
                          │
                          ▼
                Data Profiling & Exploration
                          │
                          ▼
              Data Cleaning & Standardization
                          │
                          ▼
             Financial KPI Engineering
                          │
                          ▼
             Processed Financial Dataset
                          │
                          ▼
             PostgreSQL Data Warehouse
                          │
                          ▼
                  Business SQL Analytics
```

---

# Extract Phase

Raw financial statement CSV files were imported into Pandas DataFrames for exploratory analysis.

The extraction phase focused on understanding the dataset before applying transformations.

Activities performed:

- Loading raw CSV files
- Dataset exploration
- Schema inspection
- Data type analysis
- Initial profiling
- Duplicate detection
- Missing value identification

This stage helped identify inconsistencies that required cleaning before downstream processing.

---

# Transform Phase

The transformation phase converts raw financial statements into a clean and standardized analytical dataset.

Major cleaning activities include:

### Data Cleaning

- Duplicate record removal
- Missing value handling
- Numeric data standardization
- Infinite value replacement
- Column standardization
- Data type conversion
- Invalid value correction
- Consistent naming conventions

Example:

```python
df = df.replace([np.inf, -np.inf], np.nan)
```

The resulting dataset becomes suitable for financial calculations and SQL warehousing.

---

# Financial KPI Engineering

After cleaning, multiple financial KPIs were engineered to support profitability analysis, growth measurement, efficiency evaluation, and financial risk assessment.

## Profitability Metrics

### Profit Margin

Measures the percentage of revenue retained as profit after all expenses.

Formula

```text
(Net Profit / Sales) × 100
```

---

### Operating Profit Margin (OPM)

Measures operating profitability before financing costs and taxes.

Formula

```text
(Operating Profit / Sales) × 100
```

---

## Efficiency Metrics

### Expense Ratio

Measures operating expenses relative to revenue.

Formula

```text
(Expenses / Sales) × 100
```

---

## Financial Risk Metrics

### Interest Coverage Ratio

Measures a company's ability to meet interest obligations.

Formula

```text
Operating Profit / Interest
```

---

### Interest Burden Ratio

Measures the impact of financing costs on operating profitability.

Formula

```text
(Profit Before Tax / Operating Profit) × 100
```

---

### Tax Burden Ratio

Measures the proportion of profit retained after taxation.

Formula

```text
(Net Profit / Profit Before Tax) × 100
```

---

## Growth Metrics

### Revenue Growth

Year-over-Year sales growth.

---

### Profit Growth

Year-over-Year profit growth.

---

### EPS Growth

Year-over-Year earnings per share growth.

---

### CAGR (Compound Annual Growth Rate)

Measures long-term business growth across multiple financial years.

---

# Load Phase

The cleaned and enriched dataset was loaded into PostgreSQL using SQLAlchemy and psycopg2.

The warehouse serves as the centralized storage layer for downstream SQL analytics.

Target warehouse table:

```sql
company_financials
```

The warehouse contains:

- Clean financial statements
- Engineered KPIs
- Growth metrics
- Profitability metrics
- Risk metrics
- Company-level analytical data

---

# SQL Analytics Layer

After loading the data warehouse, business-focused SQL analysis was performed across multiple financial domains.

## Sales Analysis

Examples include:

- Top companies by revenue
- Highest sales growth
- Declining sales trends
- Revenue distribution

---

## Profitability Analysis

Examples include:

- Highest Profit Margin
- Highest Operating Margin
- Most efficient companies
- Lowest Expense Ratio

---

## Growth Analysis

Examples include:

- Revenue Growth Leaders
- Profit Growth Leaders
- Highest CAGR
- Consistent growth companies

---

## Financial Risk Analysis

Examples include:

- Weak Interest Coverage
- High Interest Burden
- Tax Burden Analysis
- Financial deterioration detection

---

# Key Learning Outcomes

The first phase demonstrates practical experience in:

- Data Cleaning
- Data Transformation
- Feature Engineering
- Financial KPI Development
- ETL Pipeline Design
- PostgreSQL Data Warehousing
- SQL Analytics
- Business Data Analysis
- Git & GitHub Version Control


# Part 2 — Production Data Engineering Pipeline

The second phase of this project extends the traditional ETL pipeline into a production-oriented cloud architecture using PySpark and AWS services.

Unlike the Pandas implementation, this phase focuses on scalability, modular software design, data quality validation, logging, and production-ready engineering practices.

The objective is to simulate how financial data would be processed in a real-world cloud data engineering environment.

---

# Production Pipeline Overview

The pipeline follows a modular Extract → Transform → Validate → Load architecture.

```text
                Local Financial CSV Files
                          │
                          ▼
                  Amazon S3 (Raw Layer)
                          │
                          ▼
                 PySpark Data Extraction
                          │
                          ▼
                  Data Exploration
                          │
                          ▼
                 Data Transformation
                          │
                          ▼
             Financial KPI Engineering
                          │
                          ▼
         Production Data Validation Framework
                          │
                          ▼
               Parquet Data Lake (Next Phase)
                          │
                          ▼
              Amazon Redshift Warehouse
                          │
                          ▼
          Apache Airflow Orchestration
                          │
                          ▼
               Power BI Dashboard
```

---

# Project Architecture

The project has been designed using a modular architecture where each component is responsible for a specific stage of the ETL pipeline.

```text
part2_cloud_spark/

│

├── notebooks/
│
├── tests/
│
├── src/
│   │
│   ├── config/
│   │      └── config.py
│   │
│   ├── extract/
│   │      └── extract_s3.py
│   │
│   ├── transform/
│   │      ├── explore_data.py
│   │      ├── transform_data.py
│   │      └── kpi_transformation.py
│   │
│   ├── validation/
│   │      └── validate_data.py
│   │
│   ├── ingestion/
│   │      └── upload_to_s3.py
│   │
│   ├── load/
│   │      ├── write_parquet.py
│   │      └── load_redshift.py
│   │
│   ├── utils/
│   │      └── logger.py
│   │
│   ├── spark_session.py
│   └── main.py
│
└── requirements.txt
```

The modular design improves maintainability, scalability, and code readability while making the pipeline easier to extend with additional processing stages.

---

# Spark ETL Workflow

The PySpark implementation follows the same ETL philosophy as Part 1 while introducing distributed data processing and production engineering practices.

## Step 1 — Spark Session

A reusable Spark Session is configured with the required dependencies and AWS connectivity.

Responsibilities:

- Spark configuration
- Memory configuration
- AWS package loading
- Session creation

---

## Step 2 — Data Extraction

The raw financial dataset is extracted from Amazon S3.

Responsibilities:

- Read CSV files from S3
- Infer schema
- Create Spark DataFrame

---

## Step 3 — Data Exploration

Before applying transformations, exploratory analysis is performed to understand the dataset.

Activities include:

- Schema inspection
- Duplicate analysis
- Company analysis
- Metric analysis
- Data grain verification
- Metrics-per-company analysis

This stage helps verify that the extracted data matches the expected business structure.

---

## Step 4 — Data Transformation

The transformation stage standardizes the dataset into an analytics-ready format.

Major activities include:

- Column renaming
- Data type conversion
- Pivot transformation
- Null handling
- Numeric standardization
- Dataset restructuring

The output is a clean and consistent DataFrame suitable for financial calculations.

---

## Step 5 — Financial KPI Engineering

After transformation, multiple business KPIs are calculated.

Implemented KPIs include:

- Profit Margin
- Operating Profit Margin
- Expense Ratio
- Interest Coverage Ratio
- Interest Burden Ratio
- Tax Burden Ratio
- Effective Tax Rate
- Revenue Growth
- Profit Growth
- EPS Growth
- CAGR
- Depreciation Ratio
- Operating Leverage

These KPIs support profitability analysis, operational efficiency measurement, growth tracking, and financial risk assessment.

---

# Production Logging

A centralized logging module has been implemented to improve pipeline monitoring.

Logging captures major pipeline stages including:

- Spark Session initialization
- Data extraction
- Transformation
- KPI calculations
- Validation
- Pipeline completion

This provides better observability and simplifies debugging in production environments.

---

# Testing

The project also includes dedicated testing scripts for validating the cloud environment.

Current testing modules include:

- Spark session testing
- AWS S3 connectivity testing
- S3 file listing
- Experimental notebooks for feature validation

These tests help verify infrastructure before executing the complete pipeline.

---

# Production Data Validation Framework

One of the primary objectives of this project was to build a production-style data validation framework before loading financial data into the analytical warehouse.

Rather than validating only missing values or duplicates, the pipeline performs a comprehensive multi-stage validation covering structural integrity, business rules, statistical analysis, and financial formula verification.

The validation framework consists of **5 major validation categories** and **20 sequential validation steps**, ensuring that only high-quality and business-consistent data progresses through the pipeline.

---

# Validation Workflow

```text
                    KPI Engineered Dataset
                              │
                              ▼
                 Structural Validation
                              │
                              ▼
                  Business Validation
                              │
                              ▼
                  Technical Validation
                              │
                              ▼
                 Statistical Profiling
                              │
                              ▼
                  Formula Validation
                              │
                              ▼
                Validation Report Generated
                              │
                              ▼
               Ready for Data Warehouse Load
```

---

# Validation Categories

## 1. Structural Validation

The first stage verifies the overall integrity and structure of the dataset before performing any business-level validation.

Validation Steps:

- Dataset Structure Validation
- Duplicate Company-Year Validation
- Column-wise NULL Profiling

Purpose:

- Verify schema consistency
- Detect duplicate records
- Identify incomplete columns
- Measure data completeness

---

## 2. Business Validation

Business validation ensures that financial metrics follow expected accounting and business rules.

Validation Steps:

- Sales Validation
- Interest Validation
- Revenue Growth Validation
- Profit Growth Validation
- EPS Growth Validation
- CAGR Validation
- Operating Leverage Validation

Purpose:

- Detect impossible financial values
- Validate growth calculations
- Identify missing KPI values
- Verify business consistency across financial years

---

## 3. Technical Validation

Technical validation focuses on identifying numerical anomalies that can affect downstream analytics.

Validation Steps:

- NaN Detection
- Infinity Detection
- Financial Year Validation

Purpose:

- Detect NaN values
- Detect positive and negative infinity
- Verify valid financial year ranges
- Ensure numerical stability

---

## 4. Statistical Profiling

Statistical profiling provides a high-level overview of numerical distributions and helps identify potential outliers or suspicious records.

Validation Steps:

- Minimum Value Analysis
- Maximum Value Analysis
- Negative Value Investigation

Purpose:

- Detect unusual value ranges
- Investigate negative financial values
- Identify unexpected statistical distributions

---

## 5. Formula Validation

The final validation stage verifies that every engineered KPI exactly matches its underlying financial formula.

Validated KPIs include:

- Operating Profit
- Profit Margin
- Operating Profit Margin (OPM)
- Expense Ratio
- Interest Coverage Ratio
- Interest Burden Ratio
- Tax Burden Ratio
- Effective Tax Rate
- Depreciation Ratio

Purpose:

- Verify KPI calculations
- Detect transformation errors
- Ensure mathematical consistency
- Validate analytical correctness

---

# Validation Summary

The validation framework verifies:

- Dataset schema integrity
- Duplicate records
- Missing values
- Financial data completeness
- Growth metric consistency
- Numerical stability
- Statistical distributions
- Business rule compliance
- Financial formula accuracy

This approach closely resembles production data quality checks performed before loading data into enterprise data warehouses.

---

# Why Data Validation Matters

Incorrect financial data can lead to inaccurate KPIs, misleading dashboards, and poor business decisions.

By validating the dataset before loading it into the warehouse, the pipeline ensures that downstream analytics are built on reliable, consistent, and trustworthy data.

The validation framework acts as a quality gate between transformation and data loading, helping maintain the integrity of the analytics pipeline.


# Getting Started

## Prerequisites

Before running the project, ensure the following software is installed:

- Python 3.11+
- Apache Spark
- Java 17+
- PostgreSQL
- Git
- AWS Account
- AWS CLI (configured)
- Visual Studio Code or Jupyter Notebook

---

# Installation

## Clone the Repository

```bash
git clone https://github.com/lakshaysopra/financial-etl-pipeline.git

cd financial-etl-pipeline
```

---

## Create a Virtual Environment

```bash
python -m venv venv
```

Activate the environment.

Windows

```bash
venv\Scripts\activate
```

Linux / macOS

```bash
source venv/bin/activate
```

---

## Install Dependencies

### Part 1

```bash
pip install -r requirements.txt
```

### Part 2

```bash
pip install -r part2_cloud_spark/requirements.txt
```

---

# Configuration

Before executing the pipeline, update the configuration file with your environment settings.

Example:

- AWS Credentials
- S3 Bucket Name
- PostgreSQL Credentials
- Spark Configuration
- Redshift Configuration *(when implemented)*

---

# Running the Project

## Part 1 — Pandas ETL

Run the notebooks sequentially:

1. Data Exploration
2. Data Cleaning
3. KPI Engineering
4. PostgreSQL Loading
5. SQL Analytics

---

## Part 2 — PySpark Pipeline

Execute the main application:

```bash
python part2_cloud_spark/src/main.py
```

The pipeline automatically performs:

- Spark Session Creation
- Data Extraction from Amazon S3
- Data Exploration
- Data Transformation
- Financial KPI Engineering
- Production Data Validation
- Logging
- Pipeline Completion

Future versions will additionally:

- Write Parquet files
- Load into Amazon Redshift
- Trigger Airflow DAGs

---

# Project Highlights

This project demonstrates practical implementation of:

## Data Engineering

- End-to-End ETL Pipeline
- Cloud Data Engineering
- Production ETL Design
- Data Validation Framework
- Data Quality Engineering
- Distributed Data Processing

---

## Big Data

- Apache Spark
- PySpark
- Distributed Transformations
- Window Functions
- Performance-Oriented Processing

---

## Financial Analytics

- Financial Statement Analysis
- Financial KPI Engineering
- Growth Analysis
- Profitability Analysis
- Operational Efficiency Analysis
- Financial Risk Assessment

---

## Cloud Technologies

- Amazon S3
- Cloud Storage
- Data Lake Architecture
- Amazon Redshift *(In Progress)*
- Apache Airflow *(In Progress)*

---

## Database

- PostgreSQL
- SQL Analytics
- Data Warehousing
- Business Query Development

---

## Software Engineering

- Modular Project Structure
- Production Logging
- Testing Utilities
- Configuration Management
- Git Workflow
- GitHub Version Control

---

# Future Enhancements

The next phase of the project will further enhance the production pipeline by introducing cloud-native data warehousing and orchestration.

Planned enhancements include:

### Data Storage

- Store validated datasets as Parquet files
- Partition data for efficient querying
- Build a cloud-based data lake

---

### Data Warehouse

- Load Parquet files into Amazon Redshift
- Implement optimized warehouse schema
- Perform warehouse-level validation

---

### Workflow Orchestration

- Build Apache Airflow DAGs
- Schedule automated ETL execution
- Configure retry and failure handling
- Implement monitoring and alerting

---

### Business Intelligence

- Build interactive Power BI dashboards
- Visualize financial KPIs
- Create executive business reports
- Enable self-service analytics

---

# Repository Statistics

Current project includes:

- Two complete ETL pipelines
- 20-step Production Validation Framework
- 13 Financial KPIs
- Modular PySpark Architecture
- PostgreSQL Data Warehouse
- SQL Analytics Layer
- AWS S3 Integration
- Production Logging
- Testing Utilities

The repository continues to evolve toward a complete production-grade cloud data engineering solution.

---

# Learning Outcomes

This project strengthened practical experience in:

- Python
- Pandas
- PySpark
- SQL
- PostgreSQL
- Apache Spark
- AWS S3
- Financial Analytics
- Data Validation
- ETL Pipeline Design
- Cloud Data Engineering
- Software Engineering Best Practices

---

# Author

## Lakshay Sopra

**Aspiring Data Engineer | Finance Background | ETL & Cloud Data Engineering Enthusiast**

This project reflects my journey of combining financial domain knowledge with modern data engineering practices.

My objective is to design scalable, reliable, and production-ready data pipelines while continuously expanding my expertise in cloud technologies, big data processing, and analytics engineering.

---

# Connect

If you have suggestions, feedback, or would like to discuss data engineering, analytics, or cloud technologies, feel free to connect through GitHub.

GitHub:
https://github.com/lakshaysopra

---



Feel free to use this repository for learning, educational purposes, and inspiration. Please provide appropriate attribution if you reuse significant portions of the project.