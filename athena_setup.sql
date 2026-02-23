-- Athena Table Setup for NYC 311 Service Requests
-- Run these queries in AWS Athena Query Editor

-- 1. Create database (if not exists)
CREATE DATABASE IF NOT EXISTS nyc_311
COMMENT '311 Service Requests Database'
LOCATION 's3://311-processed-data-jason/processed/';

-- 2. Create external table pointing to S3 Parquet files
CREATE EXTERNAL TABLE IF NOT EXISTS nyc_311.service_requests_311 (
    unique_key STRING,
    created_date TIMESTAMP,
    agency STRING,
    agency_name STRING,
    complaint_type STRING,
    descriptor STRING,
    incident_zip STRING,
    incident_address STRING,
    street_name STRING,
    address_type STRING,
    city STRING,
    facility_type STRING,
    status STRING,
    resolution_description STRING,
    resolution_action_updated_date TIMESTAMP,
    community_board STRING,
    council_district STRING,
    bbl STRING,
    police_precinct STRING,
    borough STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    location STRING
)
PARTITIONED BY (
    year STRING,
    month STRING
)
STORED AS PARQUET
LOCATION 's3://311-processed-data-jason/processed/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
);

-- 3. Repair table to add partitions (run after first data load)
-- This scans S3 and automatically adds partitions
MSCK REPAIR TABLE nyc_311.service_requests_311;

-- Alternative: Manual partition addition (if MSCK doesn't work)
-- ALTER TABLE nyc_311.service_requests_311 
-- ADD PARTITION (year='2025', month='02') 
-- LOCATION 's3://311-processed-data-jason/processed/year=2025/month=02/';

-- 4. Test queries
-- Count total records
SELECT COUNT(*) as total_records
FROM nyc_311.service_requests_311;

-- Count by partition
SELECT year, month, COUNT(*) as count
FROM nyc_311.service_requests_311
GROUP BY year, month
ORDER BY year, month

-- Sample data
SELECT *
FROM nyc_311.service_requests_311
WHERE year = '2025' AND month = '02'
LIMIT 10;

-- Show partitions
SHOW PARTITIONS nyc_311.service_requests_311;

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
WHERE CAST(year AS INT) >= YEAR(CURRENT_DATE) - 1
ORDER BY created_date DESC;

-- 6. Test aggregation queries

-- Q1: Top complaint types
SELECT 
    complaint_type,
    COUNT(*) as total_complaints
FROM nyc_311.service_requests_311
WHERE year = '2025'
GROUP BY complaint_type
ORDER BY total_complaints DESC
LIMIT 10;

-- Q2: Complaints by borough
SELECT 
    borough,
    COUNT(*) as total_complaints
FROM nyc_311.service_requests_311
WHERE year = '2025' AND borough IS NOT NULL
GROUP BY borough
ORDER BY total_complaints DESC;

-- Q3: Top zip codes
SELECT 
    incident_zip,
    COUNT(*) as total_complaints
FROM nyc_311.service_requests_311
WHERE year = '2025' AND incident_zip IS NOT NULL
GROUP BY incident_zip
ORDER BY total_complaints DESC
LIMIT 15;

-- Q4: Status breakdown
SELECT 
    status,
    COUNT(*) as count
FROM nyc_311.service_requests_311
WHERE year = '2025'
GROUP BY status
ORDER BY count DESC;

-- Q5: Repeated complaints from same address
SELECT 
    incident_address,
    complaint_type,
    COUNT(*) as complaint_count,
    COUNT(DISTINCT created_date) as days_with_complaints
FROM nyc_311.service_requests_311
WHERE year = '2025'
AND incident_address IS NOT NULL
GROUP BY incident_address, complaint_type
HAVING COUNT(*) > 5
ORDER BY complaint_count DESC
LIMIT 20;

-- Query optimization tip: Always filter by partition columns
-- This should only scan month=02 partitions
SELECT COUNT(*) 
FROM nyc_311.service_requests_311
WHERE year = '2025' AND month = '02';