# How It Works

### State File Location

```
s3://311-processed-data/pipeline_state/last_run_timestamp.json
```

### State File Contents

```json
{
  "last_run_timestamp": "2025-02-21T02:00:00.123456",
  "updated_at": "2025-02-21T02:05:23.789012"
}
```

### Logic Flow

```
1. Lambda runs
2. Check: Does state file exist?

   NO → INITIAL LOAD
        - Fetch past year (Open/In Progress)
        - Max 10K records
        - Save state file

   YES → INCREMENTAL LOAD
        - Read last_run_timestamp
        - Fetch records created after that timestamp
        - Max 10K records
        - Update state file
```

## Deployment Steps

1. Deploy updated `lambda_function.py` (same process as before)
2. Configure EventBridge with simplified payload:
   ```json
   { "max_records": 10000 }
   ```
3. Trigger first run - it automatically does initial load
4. Daily runs automatically switch to incremental

## Testing

### Quick Test (Recommended First)

```bash
# Test with 100 records from past 7 days
aws lambda invoke \
  --function-name 311-data-collector \
  --payload '{
    "force_initial_load": true,
    "max_records": 100,
    "initial_lookback_days": 7
  }' \
  response.json
```

### Real Initial Load

```bash
# Fetch past year, max 10K records
aws lambda invoke \
  --function-name 311-data-collector \
  --payload '{"max_records": 10000, "initial_lookback_days": 365}' \
  response.json
```

## Key Differences from Original Design

| Aspect         | Original               | Updated (Your Requirements)               |
| -------------- | ---------------------- | ----------------------------------------- |
| Initial Load   | Full year, all records | Past year, Open/In Progress only, max 10K |
| Incremental    | Fetch day's data       | Fetch new records since last run, max 10K |
| State Tracking | None                   | S3 state file with timestamp              |
| Duplicates     | Possible               | Prevented by state tracking               |
| Data Growth    | All at once            | Gradual (10K/day max)                     |

## `lambda_function.py`

### 1. State Management Functions

```python
def get_last_run_timestamp():
    """Retrieve last successful run timestamp from S3"""
    # Reads from: s3://311-processed-data/pipeline_state/last_run_timestamp.json
    # Returns None if first run (triggers initial load)

def save_last_run_timestamp(timestamp):
    """Save current run timestamp to S3 for next incremental load"""
    # Saves to: s3://311-processed-data/pipeline_state/last_run_timestamp.json
```

**State file format:**

```json
{
  "last_run_timestamp": "2025-02-21T10:30:45.123456",
  "updated_at": "2025-02-21T10:35:12.789012"
}
```

### 2. `fetch_nyc_data()` Function

**Behavior:**

- Accept `max_records` parameter (default: 10,000)
- Fetch in batches of 2,000 until hitting 10K limit
- Order by `created_date ASC` for consistent pagination
- Stop when limit reached or no more data

```python
def fetch_nyc_data(where_clause, max_records=10000, batch_size=2000):
    # Fetches max 10K records in 2K batches
    # Example: 10K limit reached after 5 API calls (2K each)
```

### 3. `lambda_handler()` - Core Logic

**Two distinct modes:**

#### Mode 1: Initial Load (First Run)

Triggered when: No state file exists in S3

```python
# Filter: Your .ipynb WHERE clause
where_clause = "((status='Open' OR status='In Progress') AND created_date >= '2025-01-01T00:00:00')"

# Fetches up to 10K records matching:
# - Created in past year
# - Status is Open OR In Progress
```

#### Mode 2: Incremental Load (Daily Runs)

Triggered when: State file exists

```python
# Filter: New records since last run
last_run = "2025-02-20T10:30:45"  # From state file
where_clause = "((status='Open' OR status='In Progress') AND created_date > '2025-02-20T10:30:46')"

# Fetches up to 10K records created since last run
# Adds 1 second to avoid duplicate of last record
```

---

## Data Flow Example

### Day 1 (Initial Load)

```
Lambda runs at 2025-02-21 02:00:00

1. Check S3 state file: NOT FOUND
2. Mode: INITIAL LOAD
3. WHERE clause: created_date >= '2024-02-21T00:00:00' (1 year back)
                 AND (status='Open' OR status='In Progress')
4. Fetch: Up to 10,000 records
5. Results: 10,000 records fetched
6. Save to S3:
   - Raw: s3://311-raw-data/raw/2025/02/21/data.json
   - Processed: s3://311-processed-data/processed/year=2025/month=02/data_2025-02-21.parquet
7. Update state: last_run_timestamp = "2025-02-21T02:00:00.123456"
```

### Day 2 (Incremental)

```
Lambda runs at 2025-02-22 02:00:00

1. Check S3 state file: FOUND
   - last_run_timestamp: "2025-02-21T02:00:00.123456"
2. Mode: INCREMENTAL LOAD
3. WHERE clause: created_date > '2025-02-21T02:00:01.123456' (last run + 1 sec)
                 AND created_date <= '2025-02-22T02:00:00.000000'
                 AND (status='Open' OR status='In Progress')
4. Fetch: Up to 10,000 new records
5. Results: 3,247 new records fetched
6. Save to S3 (appends to existing data)
7. Update state: last_run_timestamp = "2025-02-22T02:00:00.654321"
```

### Day 3 (Incremental - No New Data)

```
Lambda runs at 2025-02-23 02:00:00

1. Check S3 state file: FOUND
2. Mode: INCREMENTAL LOAD
3. WHERE clause: created_date > '2025-02-22T02:00:01.654321'
4. Fetch: 0 records (quiet day, no new complaints)
5. Results: "No new data available"
6. Update state: last_run_timestamp = "2025-02-23T02:00:00.111111" (still update!)
```

---

## S3 Storage Structure

```
s3://311-processed-data/
├── processed/
│   ├── year=2024/
│   │   ├── month=02/
│   │   │   └── data_2025-02-21.parquet  (10K records from initial load)
│   │   ├── month=03/
│   │   │   └── data_2025-02-21.parquet  (if initial load spans multiple months)
│   │   └── ...
│   └── year=2025/
│       ├── month=01/
│       │   └── data_2025-02-21.parquet
│       └── month=02/
│           ├── data_2025-02-21.parquet  (initial load)
│           ├── data_2025-02-22.parquet  (day 2: 3,247 new records)
│           ├── data_2025-02-23.parquet  (not created - no data)
│           └── data_2025-02-24.parquet  (day 4: 5,123 new records)
└── pipeline_state/
    └── last_run_timestamp.json  (state file)
```

---

## How the 10K Daily Limit Works

### Scenario 1: New Complaints < 10K (Normal Day)

```
New complaints created: 3,500
Lambda fetches: 3,500 records
All new data captured ✓
```

### Scenario 2: New Complaints > 10K (Busy Day)

```
New complaints created: 15,000
Lambda fetches: 10,000 records (hits limit)
Missed records: 5,000

Next day's run will pick up the missed 5,000:
- Last run timestamp: 2025-02-22T02:00:00
- Some complaints from 2025-02-22 01:00:00 - 02:00:00 were missed
- But they're still > last run timestamp
- Next day fetches them: created_date > '2025-02-22T02:00:00'
```

**Important:** The 10K limit is per run, but you won't lose data. Older complaints within the time window get fetched first (ORDER BY created_date ASC), and next run picks up what was missed.

### Preventing Data Loss

If you consistently hit the 10K limit, you have options:

**Option A: Increase daily limit**

```python
# EventBridge event
{
  "max_records": 20000  # Increase to 20K
}
```

**Option B: Run twice daily**

```
EventBridge schedule:
- First run: 2 AM (fetch morning data)
- Second run: 2 PM (fetch afternoon data)
```

**Option C: Monitor and alert**

```python
# In Lambda, add check:
if len(raw_data) >= max_records:
    logger.warning("Hit daily limit - may have missed records")
    # Send SNS notification
```

---

## Testing Your Setup

### Test 1: Initial Load (First Run)

```bash
# Trigger Lambda with test event
aws lambda invoke \
  --function-name 311-data-collector \
  --payload '{
    "force_initial_load": true,
    "max_records": 10000,
    "initial_lookback_days": 365
  }' \
  response.json

# Check result
cat response.json
# Should show:
# {
#   "statusCode": 200,
#   "body": {
#     "message": "Data ingestion successful",
#     "records": 10000,
#     "mode": "initial",
#     "date_range": "2024-02-21T... to 2025-02-21T..."
#   }
# }

# Verify state file created
aws s3 ls s3://311-processed-data/pipeline_state/
# Should show: last_run_timestamp.json
```

### Test 2: Incremental Load (Second Run)

```bash
# Wait a few minutes, then trigger again
aws lambda invoke \
  --function-name 311-data-collector \
  --payload '{"max_records": 10000}' \
  response.json

# Check result
cat response.json
# Should show:
# {
#   "statusCode": 200,
#   "body": {
#     "message": "No new data available",  (or "Data ingestion successful")
#     "records": 0,  (or small number)
#     "mode": "incremental"
#   }
# }
```

### Test 3: Small Sample Test

```bash
# Test with just 100 records
aws lambda invoke \
  --function-name 311-data-collector \
  --payload '{
    "force_initial_load": true,
    "max_records": 100,
    "initial_lookback_days": 7
  }' \
  response.json

# Faster to test, verify Parquet files created correctly
```

---

# More Granular:

### 1. Lambda Environment Variables

**Add one new variable:**

```
S3_STATE_BUCKET = 311-processed-data  (same as S3_PROCESSED_BUCKET)
```

**Complete list:**

- `SOCRATA_APP_TOKEN`: [your token]
- `S3_RAW_BUCKET`: 311-raw-data
- `S3_PROCESSED_BUCKET`: 311-processed-data
- `S3_STATE_BUCKET`: 311-processed-data ← NEW

### 2. EventBridge Schedule

**For daily 10K fetch:**

```json
{
  "max_records": 10000
}
```

**Schedule:** `cron(0 6 * * ? *)` - Daily at 2 AM ET (6 AM UTC)

No other changes needed to EventBridge configuration.

### 3. IAM Permissions

Lambda role needs permission to read/write the state file:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::311-processed-data/pipeline_state/*"
    }
  ]
}
```

This is already covered by `AmazonS3FullAccess` policy.

---

## Monitoring & Validation

### Check Lambda Logs (CloudWatch)

```bash
# Look for these log messages:

# Initial load:
"=== INITIAL LOAD MODE ==="
"Fetching data from 2024-02-21T00:00:00 to 2025-02-21T..."
"Filter: Open/In Progress status only"
"Total records fetched: 10000"

# Incremental load:
"=== INCREMENTAL LOAD MODE ==="
"Last run was: 2025-02-21T02:00:00.123456"
"Fetching new data from 2025-02-21T02:00:01.123456 to 2025-02-22T..."
"Total records fetched: 3247"
```

### Verify State File

```bash
# Download and check state
aws s3 cp s3://311-processed-data/pipeline_state/last_run_timestamp.json .
cat last_run_timestamp.json

# Should contain:
{
  "last_run_timestamp": "2025-02-21T02:00:00.123456",
  "updated_at": "2025-02-21T02:05:23.789012"
}
```

### Validate No Duplicates

```sql
-- In Athena, check for duplicate unique_keys
SELECT
    unique_key,
    COUNT(*) as count
FROM nyc_311.service_requests_311
GROUP BY unique_key
HAVING COUNT(*) > 1;

-- Should return 0 rows (no duplicates)
```

---

## FAQ

### Q: What happens if Lambda fails mid-run?

**A:** State file is only updated AFTER successful Parquet conversion. If Lambda fails, state file is unchanged, so next run will retry from the same timestamp (no data loss, but possible duplicates).

**Solution:** The duplicate check query above will identify them. You can deduplicate in Athena:

```sql
-- Create deduplicated view
CREATE VIEW service_requests_deduped AS
SELECT DISTINCT unique_key, *
FROM service_requests_311;
```

### Q: Can I manually trigger a specific date range?

**A:** Yes, use the test event:

```json
{
  "force_initial_load": true,
  "max_records": 10000,
  "initial_lookback_days": 30 // Last 30 days
}
```

### Q: How do I reset and start over?

**A:** Delete the state file:

```bash
aws s3 rm s3://311-processed-data/pipeline_state/last_run_timestamp.json

# Next run will do initial load again
```

### Q: Backfilling historical data?

**A:** After initial load, manually trigger for older data:

```json
{
  "force_initial_load": true,
  "max_records": 10000,
  "initial_lookback_days": 730 // 2 years
}
```

---
