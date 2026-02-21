-- Athena Table Setup for NYC 311 Service Requests
-- Run these queries in AWS Athena Query Editor

-- 1. Create database (if not exists)
CREATE DATABASE IF NOT EXISTS nyc_311;

-- 2. Create external table pointing to S3 Parquet files
CREATE EXTERNAL TABLE IF NOT EXISTS nyc_311.service_requests_311 (
    unique_key STRING,
    created_date TIMESTAMP,
    closed_date TIMESTAMP,
    agency STRING,
    agency_name STRING,
    complaint_type STRING,
    descriptor STRING,
    location_type STRING,
    incident_zip STRING,
    incident_address STRING,
    street_name STRING,
    cross_street_1 STRING,
    cross_street_2 STRING,
    intersection_street_1 STRING,
    intersection_street_2 STRING,
    address_type STRING,
    city STRING,
    landmark STRING,
    facility_type STRING,
    status STRING,
    due_date TIMESTAMP,
    resolution_description STRING,
    resolution_action_updated_date TIMESTAMP,
    community_board STRING,
    bbl STRING,
    borough STRING,
    open_data_channel_type STRING,
    park_facility_name STRING,
    park_borough STRING,
    vehicle_type STRING,
    taxi_company_borough STRING,
    taxi_pick_up_location STRING,
    bridge_highway_name STRING,
    bridge_highway_direction STRING,
    road_ramp STRING,
    bridge_highway_segment STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    location STRING,
    council_district STRING,
    police_precinct STRING
)
PARTITIONED BY (
    year INT,
    month INT
)
STORED AS PARQUET
LOCATION 's3://311-processed-data/processed/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'projection.enabled'='true',
    'projection.year.type'='integer',
    'projection.year.range'='2020,2030',
    'projection.month.type'='integer',
    'projection.month.range'='1,12',
    'projection.month.digits'='2',
    'storage.location.template'='s3://311-processed-data/processed/year=${year}/month=${month}'
);

-- 3. Repair table to add partitions (run after first data load)
-- This scans S3 and automatically adds partitions
MSCK REPAIR TABLE nyc_311.service_requests_311;

-- Alternative: Manual partition addition
-- ADD PARTITION (year=2025, month=1) LOCATION 's3://311-processed-data/processed/year=2025/month=01/';
-- ADD PARTITION (year=2025, month=2) LOCATION 's3://311-processed-data/processed/year=2025/month=02/';

-- 4. Test queries
-- Count total records
SELECT COUNT(*) as total_records
FROM nyc_311.service_requests_311;

-- Count by partition
SELECT year, month, COUNT(*) as count
FROM nyc_311.service_requests_311
GROUP BY year, month
ORDER BY year, month;

-- Sample data
SELECT *
FROM nyc_311.service_requests_311
WHERE year = 2025 AND month = 2
LIMIT 10;

-- 5. Create view for easier querying (optional)
CREATE OR REPLACE VIEW nyc_311.recent_requests AS
SELECT 
    unique_key,
    created_date,
    agency_name,
    complaint_type,
    descriptor,
    incident_zip,
    incident_address,
    borough,
    status,
    resolution_description,
    latitude,
    longitude
FROM nyc_311.service_requests_311
WHERE year >= YEAR(CURRENT_DATE) - 1  -- Last year
ORDER BY created_date DESC;

-- 6. Test aggregation queries (your EDA questions)

-- Q1: Top complaint types
SELECT 
    complaint_type,
    COUNT(*) as total_complaints
FROM nyc_311.service_requests_311
WHERE year = 2025
GROUP BY complaint_type
ORDER BY total_complaints DESC
LIMIT 10;

-- Q3: Complaints by borough
SELECT 
    borough,
    COUNT(*) as total_complaints
FROM nyc_311.service_requests_311
WHERE year = 2025 AND borough IS NOT NULL
GROUP BY borough
ORDER BY total_complaints DESC;

-- Q4: Top zip codes
SELECT 
    incident_zip,
    COUNT(*) as total_complaints
FROM nyc_311.service_requests_311
WHERE year = 2025 AND incident_zip IS NOT NULL
GROUP BY incident_zip
ORDER BY total_complaints DESC
LIMIT 15;

-- Q6: Repeated complaints from same address
SELECT 
    incident_address,
    complaint_type,
    COUNT(*) as complaint_count,
    COUNT(DISTINCT DATE(created_date)) as days_with_complaints
FROM nyc_311.service_requests_311
WHERE year = 2025
AND incident_address IS NOT NULL
GROUP BY incident_address, complaint_type
HAVING COUNT(*) > 5
ORDER BY complaint_count DESC
LIMIT 20;

-- Query optimization tip: Check partition scan
-- This should only scan month=2 partition
SELECT COUNT(*) 
FROM nyc_311.service_requests_311
WHERE year = 2025 AND month = 2;