"""
AWS Lambda Function: 311 Service Requests Data Ingestion
Pulls data from NYC Open Data API, saves to S3 as Parquet

STRATEGY:
- Initial load: Fetch past year of Open/In Progress complaints
- Daily incremental: Fetch new records created since last run (max 10K/day)
- Track last run timestamp in S3 to avoid overlaps

Deploy as: 311-data-collector
Runtime: Python 3.12
Memory: 1024 MB
Timeout: 15 minutes
"""

import json
import os
import boto3
from datetime import datetime, timedelta
import pandas as pd
import requests
from io import BytesIO
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
SOCRATA_APP_TOKEN = os.environ.get('SOCRATA_APP_TOKEN')
S3_RAW_BUCKET = os.environ.get('S3_RAW_BUCKET', '311-raw-data')
S3_PROCESSED_BUCKET = os.environ.get('S3_PROCESSED_BUCKET', '311-processed-data')
S3_STATE_BUCKET = os.environ.get('S3_STATE_BUCKET', S3_PROCESSED_BUCKET)  # Store state in processed bucket
DATASET_ID = 'erm2-nwe9'
BASE_URL = f'https://data.cityofnewyork.us/resource/{DATASET_ID}.json'
STATE_FILE_KEY = 'pipeline_state/last_run_timestamp.json'

s3_client = boto3.client('s3')


def get_last_run_timestamp():
    """
    Retrieve the last successful run timestamp from S3
    Returns None if this is the first run
    """
    try:
        response = s3_client.get_object(
            Bucket=S3_STATE_BUCKET,
            Key=STATE_FILE_KEY
        )
        state_data = json.loads(response['Body'].read())
        last_run = state_data.get('last_run_timestamp')
        logger.info(f"Last run timestamp: {last_run}")
        return last_run
    except s3_client.exceptions.NoSuchKey:
        logger.info("No previous run found - this is the initial load")
        return None
    except Exception as e:
        logger.error(f"Error reading state file: {e}")
        return None


def save_last_run_timestamp(timestamp):
    """
    Save the current run timestamp to S3 for next incremental load
    """
    try:
        state_data = {
            'last_run_timestamp': timestamp,
            'updated_at': datetime.now().isoformat()
        }
        
        s3_client.put_object(
            Bucket=S3_STATE_BUCKET,
            Key=STATE_FILE_KEY,
            Body=json.dumps(state_data),
            ContentType='application/json'
        )
        logger.info(f"Saved last run timestamp: {timestamp}")
        return True
    except Exception as e:
        logger.error(f"Error saving state file: {e}")
        return False


def fetch_nyc_data(where_clause, max_records=10000, batch_size=2000):
    """
    Fetch data from NYC Open Data API with daily limit
    
    Args:
        where_clause: SQL WHERE clause for filtering
        max_records: Maximum records to fetch per run (default: 10K)
        batch_size: Records per API request (default: 2K, max: 50K)
    
    Returns:
        List of records
    """
    all_data = []
    current_offset = 0
    
    while len(all_data) < max_records:
        # Calculate how many more records we need
        remaining = max_records - len(all_data)
        current_limit = min(batch_size, remaining, 50000)  # Socrata max is 50K
        
        params = {
            '$limit': current_limit,
            '$offset': current_offset,
            '$where': where_clause,
            '$order': 'created_date ASC',  # Oldest first for consistent pagination
            '$$app_token': SOCRATA_APP_TOKEN
        }
        
        logger.info(f"Fetching records {current_offset} to {current_offset + current_limit} (fetched so far: {len(all_data)})")
        
        try:
            response = requests.get(BASE_URL, params=params, timeout=300)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                logger.info("No more data available")
                break
            
            all_data.extend(data)
            logger.info(f"Retrieved {len(data)} records (total: {len(all_data)})")
            
            # If we got fewer records than requested, we've hit the end
            if len(data) < current_limit:
                logger.info("Reached end of available data")
                break
            
            # Check if we've hit our daily limit
            if len(all_data) >= max_records:
                logger.info(f"Reached daily limit of {max_records} records")
                break
            
            current_offset += current_limit
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data: {e}")
            break
    
    return all_data


def save_to_s3_raw(data, date_str):
    """Save raw JSON data to S3"""
    year, month, day = date_str.split('-')
    key = f'raw/{year}/{month}/{day}/data.json'
    
    try:
        s3_client.put_object(
            Bucket=S3_RAW_BUCKET,
            Key=key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        logger.info(f"Saved raw data to s3://{S3_RAW_BUCKET}/{key}")
        return True
    except Exception as e:
        logger.error(f"Error saving raw data: {e}")
        return False


def convert_and_save_parquet(data, date_str):
    """
    Convert JSON data to Parquet and save to S3 with partitioning
    """
    if not data:
        logger.warning("No data to convert")
        return False
    
    try:
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Convert date columns to proper datetime
        date_columns = ['created_date', 'resolution_action_updated_date', 'closed_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert numeric columns
        numeric_columns = ['latitude', 'longitude']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Add partition columns
        df['year'] = df['created_date'].dt.year
        df['month'] = df['created_date'].dt.month
        
        # Group by year/month partitions
        for (year, month), group_df in df.groupby(['year', 'month']):
            # Drop partition columns from data (will be in path)
            group_df = group_df.drop(columns=['year', 'month'])
            
            # Convert to Parquet in memory
            buffer = BytesIO()
            group_df.to_parquet(buffer, index=False, compression='snappy')
            buffer.seek(0)
            
            # Upload to S3 with partition path
            key = f'processed/year={int(year)}/month={int(month):02d}/data_{date_str}.parquet'
            
            s3_client.put_object(
                Bucket=S3_PROCESSED_BUCKET,
                Key=key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            logger.info(f"Saved {len(group_df)} records to s3://{S3_PROCESSED_BUCKET}/{key}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error converting to Parquet: {e}")
        return False


def lambda_handler(event, context):
    """
    Main Lambda handler
    
    Two modes:
    1. Initial load: Fetch past year of Open/In Progress complaints
    2. Incremental: Fetch new records since last run (max 10K/day)
    
    Event structure (optional):
    {
        "force_initial_load": true,  # Force initial load even if state exists
        "max_records": 10000,        # Max records per run (default: 10K)
        "initial_lookback_days": 365 # For initial load (default: 365)
    }
    """
    try:
        # Parse event parameters
        force_initial = event.get('force_initial_load', False)
        max_records = event.get('max_records', 10000)
        initial_lookback_days = event.get('initial_lookback_days', 365)
        
        current_run_time = datetime.now()
        current_run_str = current_run_time.isoformat()
        today_str = current_run_time.strftime('%Y-%m-%d')
        
        # Get last run timestamp to determine mode
        last_run_timestamp = None if force_initial else get_last_run_timestamp()
        
        if last_run_timestamp is None:
            # INITIAL LOAD: Fetch past year of Open/In Progress complaints
            logger.info("=== INITIAL LOAD MODE ===")
            
            start_date = current_run_time - timedelta(days=initial_lookback_days)
            start_date_str = start_date.strftime('%Y-%m-%dT00:00:00')
            end_date_str = current_run_str
            
            # Your original filter from .ipynb file
            where_clause = f"((status='Open' OR status='In Progress') AND created_date >= '{start_date_str}')"
            
            logger.info(f"Fetching data from {start_date_str} to {end_date_str}")
            logger.info(f"Filter: Open/In Progress status only")
            logger.info(f"Max records: {max_records}")
            
        else:
            # INCREMENTAL LOAD: Fetch only new records since last run
            logger.info("=== INCREMENTAL LOAD MODE ===")
            
            # Add 1 second to last run to avoid duplicates
            last_run_dt = datetime.fromisoformat(last_run_timestamp)
            start_date_str = (last_run_dt + timedelta(seconds=1)).isoformat()
            end_date_str = current_run_str
            
            # Fetch new Open/In Progress complaints created since last run
            where_clause = f"((status='Open' OR status='In Progress') AND created_date > '{start_date_str}' AND created_date <= '{end_date_str}')"
            
            logger.info(f"Fetching new data from {start_date_str} to {end_date_str}")
            logger.info(f"Last run was: {last_run_timestamp}")
            logger.info(f"Max records: {max_records}")
        
        logger.info(f"Where clause: {where_clause}")
        
        # Fetch data
        raw_data = fetch_nyc_data(
            where_clause=where_clause,
            max_records=max_records,
            batch_size=2000
        )
        
        if not raw_data:
            logger.warning("No new data fetched")
            
            # Still update timestamp to mark successful run
            save_last_run_timestamp(current_run_str)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No new data available',
                    'records': 0,
                    'mode': 'initial' if last_run_timestamp is None else 'incremental',
                    'timestamp': current_run_str
                })
            }
        
        logger.info(f"Total records fetched: {len(raw_data)}")
        
        # Save raw JSON
        save_to_s3_raw(raw_data, today_str)
        
        # Convert and save as Parquet
        success = convert_and_save_parquet(raw_data, today_str)
        
        if success:
            # Update last run timestamp
            save_last_run_timestamp(current_run_str)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Data ingestion successful',
                    'records': len(raw_data),
                    'mode': 'initial' if last_run_timestamp is None else 'incremental',
                    'date_range': f'{start_date_str} to {end_date_str}',
                    'timestamp': current_run_str
                })
            }
        else:
            # Don't update timestamp if Parquet conversion failed
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error during Parquet conversion',
                    'records': len(raw_data),
                    'timestamp': current_run_str
                })
            }
    
    except Exception as e:
        logger.error(f"Lambda execution error: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error: {str(e)}',
                'timestamp': datetime.now().isoformat()
            })
        }


# For local testing
if __name__ == '__main__':
    # Test event - initial load
    test_event_initial = {
        'force_initial_load': True,
        'max_records': 100,  # Small test
        'initial_lookback_days': 7
    }
    
    # Test event - incremental
    test_event_incremental = {
        'max_records': 100
    }
    
    result = lambda_handler(test_event_initial, None)
    print(result)