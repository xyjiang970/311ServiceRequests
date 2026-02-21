# Overview

Dataset Used: [311 Service Requests Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/about_data)

This project implements a Python-based ETL pipeline for extracting NYC 311 Service Request data from the NYC Open Data platform via the Socrata Open Data API (SODA). The pipeline handles pagination, rate limiting, and query optimization to efficiently retrieve large datasets from the city's 35M+ record database.

The core functionality includes:

- API Integration: RESTful API client with offset-based pagination and configurable batch sizing
- Query Optimization: SoQL (Socrata Query Language) support for server-side filtering to minimize data transfer
- Data Extraction: Parameterized WHERE clause construction for flexible date range, status, and attribute filtering
- Data Transformation: Column selection and type conversion using pandas for downstream analytics
- Error Handling: Request retry logic and timeout management for reliable data retrieval

Built for scalability and reusability, the module can be integrated into larger data pipelines for automated 311 data ingestion, real-time monitoring dashboards, or batch processing workflows.


[Map from .ipynb file](https://xyjiang970.github.io/311ServiceRequests/my_map.html)

## ETL Architecture:

NYC Open Data API -> AWS Lambda (scheduled) -> S3 (parquet files) -> AWS Athena -> Streamlit.io

Pipeline will:

- Pull data daily from NYC API (automated with EventBridge)
- Store as Parquet with year/month partitioning (10x compression vs JSON)
- Query via SQL in Athena (no data warehouse setup)
- Display in Streamlit.io
- Cost: ~$12-15/month for 1M records

---
