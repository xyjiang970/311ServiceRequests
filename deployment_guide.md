# Deployment Guide: NYC 311 ETL Pipeline

Complete step-by-step guide to deploy your end-to-end pipeline from NYC Open Data → AWS → Streamlit.

---

## Phase 1: AWS Infrastructure Setup

### Step 1.1: Create S3 Buckets

```bash
# Login to AWS Console → S3 → Create Bucket

# Bucket 1: Raw data
Name: 311-raw-data-[your-name]
Region: us-east-1
Block all public access: Yes

# Bucket 2: Processed data
Name: 311-processed-data-[your-name]
Region: us-east-1
Block all public access: Yes

# Bucket 3: Athena query results
Name: 311-athena-results-[your-name]
Region: us-east-1
Block all public access: Yes
```

### Step 1.2: Create IAM Role for Lambda

```bash
# AWS Console → IAM → Roles → Create Role

# Trust entity: Lambda
# Permissions: Attach policies
# - AmazonS3FullAccess (or create custom policy with just your buckets)
# - AWSLambdaBasicExecutionRole

Role name: Lambda-311-DataCollector-Role
```

### Step 1.3: Deploy Lambda Function

**Option A: AWS Console (Easier for first time)**

1. Go to Lambda → Create Function
2. Function name: `311-data-collector`
3. Runtime: Python 3.12
4. Architecture: x86_64
5. Execution role: Use existing → `Lambda-311-DataCollector-Role`
6. Create function

**Upload Code:**

```bash
# Package dependencies locally
mkdir lambda_package
cd lambda_package
pip install -t . -r lambda_requirements.txt
cp ../lambda_function.py .

# Create ZIP
zip -r ../lambda_deployment.zip .
cd ..

# Upload via AWS Console:
# Lambda → 311-data-collector → Upload from → .zip file → lambda_deployment.zip
```

**Configure Lambda:**

- Memory: 1024 MB
- Timeout: 15 minutes (900 seconds)
- Environment variables:
  - `SOCRATA_APP_TOKEN`: [your token]
  - `S3_RAW_BUCKET`: 311-raw-data-[your-name]
  - `S3_PROCESSED_BUCKET`: 311-processed-data-[your-name]

**Option B: AWS CLI (Advanced)**

```bash
# Create deployment package
./deploy_lambda.sh

# Deploy
aws lambda create-function \
  --function-name 311-data-collector \
  --runtime python3.12 \
  --role arn:aws:iam::[YOUR_ACCOUNT_ID]:role/Lambda-311-DataCollector-Role \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://lambda_deployment.zip \
  --timeout 900 \
  --memory-size 1024 \
  --environment Variables="{SOCRATA_APP_TOKEN=[YOUR_TOKEN],S3_RAW_BUCKET=311-raw-data-[your-name],S3_PROCESSED_BUCKET=311-processed-data-[your-name]}"
```

### Step 1.4: Test Lambda Function

```bash
# AWS Console → Lambda → 311-data-collector → Test

# Create test event:
Event name: test-incremental
Event JSON:
{
  "mode": "incremental",
  "days_back": 7
}

# Click Test
# Check CloudWatch Logs for execution details
# Verify files appear in S3 buckets
```

### Step 1.5: Schedule Lambda with EventBridge

```bash
# AWS Console → EventBridge → Rules → Create Rule

Name: 311-daily-ingestion
Rule type: Schedule
Schedule pattern: Cron expression
Cron: 0 6 * * ? *   # Daily at 6 AM UTC (2 AM ET)

Target: Lambda function
Function: 311-data-collector
Configure input: Constant (JSON text)

# Event payload for daily incremental load (10K max)
{
  "max_records": 10000
}

# Create rule
```

**How it works:**

- **First run**: No state file exists → triggers initial load (past year of Open/In Progress)
- **Daily runs**: State file exists → fetches only new records since last run (max 10K)
- See `INCREMENTAL_LOADING_CHANGES.md` for detailed logic

---

## Phase 2: Athena Setup

### Step 2.1: Create Athena Database and Table

1. AWS Console → Athena → Query Editor
2. Set up query result location:
   - Settings → Manage → Query result location
   - `s3://311-athena-results-[your-name]/`
3. Run queries from `athena_setup.sql`:

```sql
-- Copy and paste from athena_setup.sql
-- Run each section sequentially
```

### Step 2.2: Verify Table Setup

```sql
-- Check partitions
SHOW PARTITIONS nyc_311.service_requests_311;

-- Count records
SELECT COUNT(*) FROM nyc_311.service_requests_311;

-- Sample query
SELECT * FROM nyc_311.service_requests_311 LIMIT 10;
```

---

## Phase 3: Streamlit Dashboard

### Step 3.1: Prepare Streamlit Files

Create project structure:

```
311-streamlit-app/
├── streamlit_app.py
├── requirements.txt (use streamlit_requirements.txt)
├── .streamlit/
│   └── secrets.toml
└── README.md
```

### Step 3.2: Configure AWS Credentials

Create `.streamlit/secrets.toml`:

```toml
[aws]
access_key_id = "YOUR_AWS_ACCESS_KEY"
secret_access_key = "YOUR_AWS_SECRET_KEY"
region = "us-east-1"
s3_staging_dir = "s3://311-athena-results-[your-name]/"
```

**How to get AWS credentials:**

1. AWS Console → IAM → Users → Your user → Security credentials
2. Create access key → Application running outside AWS
3. Copy access key ID and secret access key

### Step 3.3: Test Locally

```bash
cd 311-streamlit-app
pip install -r requirements.txt
streamlit run streamlit_app.py

# Open browser: http://localhost:8501
# Verify dashboard loads and queries work
```

### Step 3.4: Deploy to Streamlit Cloud

1. Push code to GitHub:

```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/[your-username]/311-streamlit-app.git
git push -u origin main
```

2. Deploy on Streamlit Cloud:
   - Go to https://share.streamlit.io
   - Sign in with GitHub
   - New app → Select your repository
   - Main file path: `streamlit_app.py`
   - Advanced settings → Secrets
     - Paste your secrets.toml content
   - Deploy!

3. Your dashboard will be live at:
   `https://[your-app].streamlit.app`

---

## Phase 4: First Data Load

### Option A: Automatic Initial Load (Recommended)

Simply trigger Lambda - it will detect no state file and automatically do initial load:

```bash
# Trigger Lambda for initial load
aws lambda invoke \
  --function-name 311-data-collector \
  --payload '{"max_records": 10000, "initial_lookback_days": 365}' \
  response.json

# Monitor progress in CloudWatch Logs
# Fetches up to 10K records from past year (Open/In Progress only)
# Takes ~2-3 minutes
```

**What happens:**

1. Lambda checks for state file: NOT FOUND
2. Triggers INITIAL LOAD mode
3. Fetches: `WHERE (status='Open' OR 'In Progress') AND created_date >= '2024-02-21'`
4. Gets up to 10K records
5. Saves state file for future incremental loads

### Option B: Multiple Batches (If you want >10K for testing)

If you want to test with more than 10K records:

```bash
# Batch 1: Initial load (10K)
aws lambda invoke \
  --function-name 311-data-collector \
  --payload '{"force_initial_load": true, "max_records": 10000}' \
  response.json

# Wait 30 seconds

# Batch 2: Next 10K (manually reset state)
aws s3 rm s3://311-processed-data/pipeline_state/last_run_timestamp.json
aws lambda invoke \
  --function-name 311-data-collector \
  --payload '{"max_records": 10000}' \
  response.json

# Repeat as needed
```

### Option C: Test with Small Sample First

Recommended for testing:

```bash
# Test with just 7 days of data, 100 records max
aws lambda invoke \
  --function-name 311-data-collector \
  --payload '{
    "force_initial_load": true,
    "max_records": 100,
    "initial_lookback_days": 7
  }' \
  response.json

# Verify everything works, then do full initial load:
aws s3 rm s3://311-processed-data/pipeline_state/last_run_timestamp.json
aws lambda invoke \
  --function-name 311-data-collector \
  --payload '{"max_records": 10000, "initial_lookback_days": 365}' \
  response.json
```

---

## Phase 5: Monitoring & Maintenance

### CloudWatch Alarms

Create alarms for:

1. **Lambda Failures**
   - Metric: Errors
   - Threshold: > 0 in 5 minutes
   - Action: SNS email notification

2. **Lambda Duration**
   - Metric: Duration
   - Threshold: > 13 minutes (approaching 15 min timeout)
   - Action: SNS email

### Cost Monitoring

Set up AWS Budgets:

- Budget: $20/month
- Alert at 80% threshold
- Services to watch: S3, Lambda, Athena

### Data Quality Checks

Add to Lambda:

```python
def validate_data(df):
    """Data quality checks"""
    checks = {
        'null_unique_key': df['unique_key'].isnull().sum(),
        'null_created_date': df['created_date'].isnull().sum(),
        'invalid_lat_lng': ((df['latitude'] < 40) | (df['latitude'] > 41)).sum(),
        'future_dates': (df['created_date'] > datetime.now()).sum()
    }

    if any(checks.values()):
        logger.warning(f"Data quality issues: {checks}")

    return checks
```

---

## Troubleshooting

### Lambda timeouts

**Problem**: Lambda times out before completing data fetch
**Solution**:

- Reduce `days_back` parameter
- Increase memory (more memory = more CPU)
- Split into multiple smaller runs

### Athena query fails

**Problem**: "Table not found" or "No partitions"
**Solution**:

```sql
-- Repair partitions
MSCK REPAIR TABLE nyc_311.service_requests_311;

-- Or add manually
ALTER TABLE nyc_311.service_requests_311
ADD PARTITION (year=2025, month=2);
```

### Streamlit "Connection timeout"

**Problem**: Athena queries timeout in Streamlit
**Solution**:

- Add query result caching: `@st.cache_data(ttl=3600)`
- Reduce data volume: Add `LIMIT 10000` to geo queries
- Use query result location closer to compute

### High AWS costs

**Problem**: Unexpected charges
**Solution**:

- Check Athena query history - are you scanning too much data?
- Verify S3 lifecycle policies to delete old raw data
- Review CloudWatch Logs retention (default 30 days)

---

## Production Enhancements

Once MVP is working, consider:

1. **Incremental Loading**: Only fetch new/updated records

   ```python
   # Track last run timestamp in S3 or DynamoDB
   last_run = get_last_run_timestamp()
   where_clause = f"created_date > '{last_run}'"
   ```

2. **Data Validation**: Add schema validation, data quality checks

3. **Error Handling**: Dead letter queue (DLQ) for failed Lambda runs

4. **Data Retention**: Lifecycle policy to delete old raw JSON

   ```bash
   # S3 Lifecycle Rule
   - Delete objects in /raw/ after 7 days
   - Keep /processed/ permanently
   ```

5. **Monitoring Dashboard**: CloudWatch Dashboard for pipeline health

6. **CI/CD**: GitHub Actions to auto-deploy Lambda on code changes

---

## Cost Estimate (Monthly)

Based on 365 days × ~3,000 records/day = ~1M records total:

- **Lambda**: ~$2 (daily runs, 5 min each)
- **S3**: ~$5 (3 GB processed data + query results)
- **Athena**: ~$5 (100 queries/day, avg 200 MB scanned each)
- **Streamlit**: Free (public app)

**Total**: ~$12-15/month

---
