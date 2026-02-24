[Streamlit Dashboard Front-End](https://311servicerequests-jc4uo3tearplnfpfg4c59s.streamlit.app/)

Dataset Used: [311 Service Requests Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/about_data)

[Map from .ipynb file](https://xyjiang970.github.io/311ServiceRequests/my_map.html)

## Project Overview

This project implements a production-style serverless data platform that ingests, processes, stores, and serves New York City 311 service request data for analytics and visualization.

The system continuously collects public civic data from the NYC Open Data platform and transforms it into a query-optimized analytical dataset using a modern lakehouse-style architecture built entirely on managed cloud services.

Rather than performing one-off analysis, the goal of this project is to demonstrate how a data engineer would design a fully automated ingestion → storage → query → analytics workflow capable of scaling to tens of millions of records.

Tech: AWS Lambda, EventBridge, S3, Athena, Python, Pandas, Parquet, Streamlit, SQL, REST APIs

- Designed and implemented an end-to-end serverless data pipeline ingesting NYC public civic data via REST API and transforming it into an analytics-ready lakehouse dataset
- Built incremental ingestion logic with pagination and retry handling to reliably process tens of millions of records from a rate-limited API source
- Converted raw JSON into partitioned columnar Parquet data lake (S3) reducing query scan cost and improving query performance in Athena
- Modeled query-optimized tables using schema-on-read architecture and external table definitions for serverless SQL analytics
- Automated daily ingestion using event-driven orchestration (EventBridge → Lambda) enabling fully hands-off pipeline operation
- Developed an interactive Streamlit analytics dashboard for geospatial and trend analysis of city service complaints
- Implemented cost-efficient analytics workflow leveraging per-query pricing in Athena instead of persistent compute infrastructure
- Demonstrated production data engineering patterns including idempotent runs, partition pruning, and scalable storage design

## Architecture

```bash
NYC Open Data (Socrata API)
        ↓
AWS EventBridge (daily schedule)
        ↓
AWS Lambda (data ingestion & transformation)
        ↓
Amazon S3 Data Lake (partitioned Parquet)
        ↓
AWS Athena (serverless SQL analytics)
        ↓
Streamlit Dashboard (data application)
```

## Key Design Principles

### Data Engineering Workflow

#### 1. Extraction — API Ingestion

The pipeline retrieves data from the NYC Open Data Socrata API using a custom ingestion client designed for large datasets:

Features:

- Offset pagination for >35M records
- Configurable batch size
- Rate limit handling & retry logic
- SoQL server-side filtering
- Incremental date-based ingestion

This simulates real-world ingestion from a public REST source that does not support bulk exports.

#### 2. Transformation — Data Standardization

Inside AWS Lambda the raw JSON records are transformed into analytics-ready tables:

- Column pruning
- Type casting
- Datetime normalization
- Null handling
- Schema enforcement

The data is then converted into columnar Parquet format to enable efficient analytical queries.

#### 3. Storage — Data Lake (S3)

Data is stored in a partitioned structure (store first model later):

```bash
s3://nyc-311-data/
    year=YYYY/
        month=MM/
            data.parquet
```

Benefits:

- Query pruning
- Reduced scan costs
- Faster Athena queries
- Efficient long-term storage

#### 4. Query Layer — Athena

AWS Athena provides a serverless SQL engine over the S3 data lake.
No database servers or warehouses are provisioned — queries operate directly on Parquet files.

This demonstrates:

- Schema-on-read architecture
- External table definitions
- Cost-based analytics ($ per TB scanned)

#### 5. Consumption — Streamlit Analytics App

A Streamlit application serves as the presentation layer, enabling users to interactively explore civic issue trends.

Example use cases:

- Complaint volume by borough
- Seasonal service trends
- Geographic clustering of complaints
- Category-level incident analysis

## Tech Stack

### Cloud & Infrastructure:

- AWS Lambda — serverless compute
- AWS EventBridge — scheduled orchestration
- Amazon S3 — data lake storage
- AWS Athena — serverless query engine

### Data Processing:

- Python
- Pandas
- Parquet (columnar storage format)
- Socrata Open Data API (SODA / SoQL)

### Analytics & Visualization:

- Streamlit
- Folium (geospatial mapping)
- SQL analytics via Athena

### Development & Reproducibility

- Jupyter Notebooks (EDA)
- Environment configuration via .env and secrets
- Modular ETL components

## Data Engineering Skills Demonstrated:

- Designing serverless ETL pipelines
- Handling large API-based datasets
- Incremental ingestion strategies
- Data lake partitioning strategies
- Columnar storage optimization
- Cost-aware analytics architecture
- Building data applications on top of a lakehouse
- End-to-end data platform ownership

Raw API → Data Lake → Serverless SQL → Data App

This pattern is widely used in production systems because it:

- Scales automatically
- Minimizes infrastructure management
- Reduces operational cost
- Supports both BI and ML workflows
