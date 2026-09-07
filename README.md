# 📊 Financial ETL Pipeline
### End-to-End Financial Data Engineering & Analytics Pipeline using Python, Pandas, PySpark, AWS S3 ,AWS redshift, PostgreSQL, Apache Airflow & Power BI

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

# Part 2 — Production Data Engineering Pipeline (Completed)

The second phase transforms the project into a production-style cloud data engineering solution using PySpark, AWS technologies and airflow.

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
| Data Warehouse | PostgreSQL, Amazon Redshift Serverless |
| File Format | CSV, Parquet |
| SQL | PostgreSQL SQL, Redshift SQL |
| Workflow Orchestration | Apache Airflow |
| Containerization | Docker, Docker Compose |
| Data Visualization | Power BI |
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


- The resulting dataset becomes suitable for financial calculations and SQL warehousing.



# Financial KPI Engineering

After cleaning, multiple financial KPIs were engineered to support profitability analysis, growth measurement, efficiency evaluation, and financial risk assessment.



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
                  Parquet Data Lake
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


# Processed Data Lake

After transformation and validation, the analytics-ready dataset is written in Parquet format to the processed layer in Amazon S3.

## Amazon S3

```text
raw/
    │
    ▼
PySpark ETL
    │
    ▼
Validation
    │
    ▼
processed/
    └── company_financials/
        └── Parquet files
```

Parquet provides a columnar storage format suitable for analytical workloads and downstream data warehouse ingestion.

# Amazon Redshift

The validated Parquet dataset stored in Amazon S3 is loaded into Amazon Redshift Serverless for analytical querying.

The Redshift warehouse provides a centralized analytical layer for performing business-focused financial analysis.

## Data Flow

```text
PySpark
   │
   ▼
Validated Dataset
   │
   ▼
Parquet
   │
   ▼
Amazon S3
   │
   ▼
Amazon Redshift
   │
   ▼
SQL Analytics
```

## Redshift Responsibilities

- Store processed financial data
- Provide an analytical warehouse layer
- Support large-scale SQL analysis
- Enable business-focused financial queries
- Serve as the analytical source for downstream reporting

**Target warehouse table:**

`company_financials`

# Redshift SQL Analytics

A comprehensive SQL analytics layer was implemented on top of the Amazon Redshift warehouse.

The analysis focuses on extracting business insights from financial statements and engineered KPIs.

## Sales Analysis

Examples include:

- Top companies by revenue
- Revenue growth analysis
- Sales trend analysis
- Declining revenue identification
- Revenue comparisons across financial years

## Profitability Analysis

Examples include:

- Highest Profit Margin
- Highest Operating Profit Margin
- Profitability comparison
- Companies with strong operating performance
- Profitability trend analysis

## Growth Analysis

Examples include:

- Revenue Growth Leaders
- Profit Growth Leaders
- Highest CAGR
- Continuous growth analysis
- Long-term financial growth comparison

## Cost & Efficiency Analysis

Examples include:

- Expense Ratio analysis
- Depreciation Ratio analysis
- Operating Leverage analysis
- Cost efficiency comparison
- Operating performance analysis

## Financial Risk Analysis

Examples include:

- Interest Coverage analysis
- Interest Burden analysis
- Tax Burden analysis
- Effective Tax Rate analysis
- Financial deterioration detection

The SQL layer transforms the warehouse from a simple storage system into an analytical platform capable of answering business-oriented financial questions.

# Docker Environment

Docker is used to provide a consistent execution environment for the production-oriented pipeline and Airflow orchestration layer.

The project uses Docker Compose to manage the Airflow environment and its supporting services.

## Docker Responsibilities

- Containerized Airflow environment
- Consistent runtime environment
- Spark pipeline execution from Airflow
- Service isolation
- Reproducible development environment

The Spark project is mounted into the Airflow environment so that Airflow can execute the production PySpark pipeline.

## Docker Compose

```text
Docker Compose
      │
      ├── Airflow
      │
      ├── PostgreSQL
      │
      └── Redis
             │
             ▼
       PySpark Pipeline
```

# Apache Airflow Orchestration

Apache Airflow is used to orchestrate the production PySpark pipeline.

Instead of manually executing the Spark application, Airflow manages the pipeline execution through a DAG.

## Airflow Workflow

```text
Airflow DAG
     │
     ▼
Spark Pipeline
     │
     ├── Extract
     ├── Explore
     ├── Transform
     ├── KPI Engineering
     ├── Validation
     └── Load
     │
     ▼
Processed S3 Data
```

## Airflow Features Implemented

- DAG-based orchestration
- Scheduled pipeline execution
- Manual pipeline triggering
- Retry configuration
- Failure handling
- Pipeline execution monitoring
- Task-level logging
- Integration with the PySpark application

The DAG is configured using the Asia/Kolkata timezone for scheduling while the underlying Airflow environment operates in UTC.

# Production Pipeline Execution

The complete production pipeline can be executed through Airflow.

The pipeline follows:

```text
Airflow
   │
   ▼
PySpark
   │
   ▼
Amazon S3 — Raw
   │
   ▼
Data Exploration
   │
   ▼
Data Transformation
   │
   ▼
KPI Engineering
   │
   ▼
Data Validation
   │
   ▼
Amazon S3 — Processed Parquet
   │
   ▼
Amazon Redshift
   │
   ▼
SQL Financial Analysis
   │
   ▼
Power BI
```

This provides an end-to-end workflow from raw financial data ingestion to business intelligence.

# Power BI Business Intelligence

Power BI is used as the final visualization and business intelligence layer of the pipeline.

The processed financial data and analytical results are used to create interactive financial dashboards.

## Power BI Analysis

The dashboard focuses on:

- Revenue and Sales Trends
- Net Profit Trends
- Profit Margin
- Operating Profit Margin
- Revenue Growth
- Profit Growth
- CAGR
- Expense Ratio
- Interest Coverage Ratio
- Interest Burden Ratio
- Tax Burden Ratio
- Effective Tax Rate
- Depreciation Ratio
- Operating Leverage

## Dashboard Sections

### Executive Overview

Provides a high-level view of financial performance using key financial KPIs, revenue trends, profit trends, and company comparisons.

### Profitability Analysis

Focuses on:

- Profit Margin
- Operating Profit Margin
- Company profitability comparison
- Profitability trends

### Growth Analysis

Focuses on:

- Revenue Growth
- Profit Growth
- CAGR
- Company growth comparison

### Cost & Efficiency Analysis

Focuses on:

- Expense Ratio
- Depreciation Ratio
- Operating Leverage
- Interest Coverage

### Financial Risk Analysis

Focuses on:

- Interest Burden
- Tax Burden
- Effective Tax Rate
- Financial performance indicators

## Interactive Dashboard Features

The Power BI report provides:

- Interactive KPI cards
- Company-level analysis
- Year-wise analysis
- Company comparison
- Financial trend analysis
- Interactive slicers and filters
- Business-focused financial reporting

---

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
- Docker Desktop
- Visual Studio Code or Jupyter Notebook
- Power BI Desktop

---

# Installation

## Clone the Repository

```bash
git clone https://github.com/lakshaysopra/financial-etl-pipeline.git
cd financial-etl-pipeline
```

## Create a Virtual Environment

```bash
python -m venv venv
```

Activate the environment.

### Windows

```bash
venv\Scripts\activate
```

### Linux / macOS

```bash
source venv/bin/activate
```

## Install Dependencies

### Part 1

```bash
pip install -r requirements.txt
```

### Part 2

```bash
pip install -r part2_cloud_spark/requirements.txt
```

# Configuration

Before executing the pipeline, configure the required environment settings.

Configuration includes:

- AWS credentials
- S3 bucket name
- S3 raw and processed paths
- PostgreSQL credentials
- Spark configuration
- Amazon Redshift configuration
- Airflow environment configuration

Sensitive credentials are managed through environment variables and configuration files rather than being hard-coded into the pipeline.

# Project Highlights

This project demonstrates practical implementation of:

## Data Engineering

- Data Engineering
- End-to-End ETL Pipeline
- Cloud Data Engineering
- Production ETL Design
- Data Quality Engineering
- Production Data Validation
- Distributed Data Processing
- Data Lake Architecture
- Big Data
- Apache Spark
- PySpark
- Distributed Transformations
- Window Functions
- Spark DataFrame Operations
- Performance-Oriented Processing

## Financial Analytics

- Financial Statement Analysis
- Financial KPI Engineering
- Growth Analysis
- Profitability Analysis
- Operational Efficiency Analysis
- Financial Risk Assessment
- Business SQL Analytics

## Cloud Technologies

- Amazon S3
- Cloud Data Lake
- Amazon Redshift Serverless
- AWS-based ETL
- Cloud Data Warehousing

## Workflow Orchestration

- Apache Airflow
- DAG Development
- Scheduled ETL Execution
- Retry Handling
- Pipeline Monitoring

## Containerization

- Docker
- Docker Compose
- Containerized Airflow Environment
- Reproducible Execution Environment

## Business Intelligence

- Power BI
- Financial KPI Dashboards
- Interactive Data Visualization
- Business Reporting
- Financial Trend Analysis

## Database

- PostgreSQL
- Amazon Redshift
- SQL Analytics
- Data Warehousing
- Business Query Development

## Software Engineering

- Modular Project Structure
- Production Logging
- Testing Utilities
- Configuration Management
- Git Workflow
- GitHub Version Control

# The project currently includes:

- Two complete ETL pipelines
- Production PySpark ETL pipeline
- 20-step Production Validation Framework
- 13 Financial KPIs
- Modular PySpark Architecture
- AWS S3 Data Lake
- Amazon Redshift Serverless Warehouse
- Comprehensive Redshift SQL Analytics
- Apache Airflow Orchestration
- Docker-based execution environment
- Production Logging
- Testing Utilities
- Power BI Business Intelligence Layer
- PostgreSQL Data Warehouse
- Git & GitHub Version Control

The project demonstrates an end-to-end progression from traditional Python-based ETL to a cloud-oriented, distributed, orchestrated, and analytics-driven Data Engineering architecture.

# Learning Outcomes

This project strengthened practical experience in:

- Python
- Pandas
- PySpark
- SQL
- PostgreSQL
- Amazon Redshift
- Apache Spark
- Amazon S3
- Apache Airflow
- Docker
- Power BI
- Financial Analytics
- Data Validation
- ETL Pipeline Design
- Cloud Data Engineering
- Data Warehousing
- Workflow Orchestration
- Business Intelligence
- Software Engineering Best Practices

# Author

**Lakshay Sopra**

*Aspiring Data Engineer | Finance Background | ETL & Cloud Data Engineering Enthusiast*

This project reflects my journey of combining financial domain knowledge with modern data engineering practices.

My objective is to design scalable, reliable, and production-ready data pipelines while continuously expanding my expertise in cloud technologies, big data processing, data warehousing, orchestration, and analytics engineering.

# Connect

If you have suggestions, feedback, or would like to discuss data engineering, analytics, or cloud technologies, feel free to connect through GitHub.

**GitHub:**

https://github.com/lakshaysopra

