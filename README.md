# 311ServiceRequests

Dataset Used: [311 Service Requests Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/about_data)

Main goal of this project is to set up a end to end, automatic ETL pipeline collecting data (from the past year) from data.cityofnewyork.us domain API (note data is updated daily - but there will still be lag in date complaint was first filed) to Streamlit.io front end.

EDA Qs:

1. Top Complaints by complaint type
2. Top Complaint Agency (city Dept. reported to)
3. Most # of Complaints by Borough
4. Most # of Complaints by Zip-Code
5. Top Complaints in a certain zip code (zip code search function)
6. Repeated complaints from same address
7. Heatmap of complaints of NYC (dist. of complaints across the city)

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
