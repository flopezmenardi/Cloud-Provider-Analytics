# AstraDB Setup Guide - Step by Step

This guide walks you through setting up AstraDB for the Cloud Provider Analytics serving layer.

## Prerequisites
- AstraDB account (free tier is sufficient)
- Internet connection
- Project setup completed (Bronze, Silver, Gold layers)

---

## Step 1: Create AstraDB Account

1. Go to https://astra.datastax.com/
2. Click "Get Started Free" or "Sign Up"
3. Create account with email (no credit card required)
4. Verify your email address

**✅ Checkpoint**: You should be logged into the AstraDB dashboard

---

## Step 2: Create Database

1. In AstraDB dashboard, click **"Create Database"**
2. Fill in the form:
   - **Database name**: `cloud_analytics`
   - **Keyspace name**: `cloud_analytics`
   - **Provider**: AWS (or your preferred cloud)
   - **Region**: Choose closest to you (e.g., `us-east-2`)
3. Click **"Create Database"**
4. Wait 2-3 minutes for provisioning (status will change to "Active")

**✅ Checkpoint**: Your database status shows "Active" with a green dot

---

## Step 3: Get Connection Details

1. Click on your `cloud_analytics` database
2. Click the **"Connect"** tab
3. You should see a section labeled **"Data API"**
4. Note the following information:
   - **API Endpoint**: A URL like `https://xxxxx-xxxxx.apps.astra.datastax.com`
   - This is your database endpoint ✅

**✅ Checkpoint**: You have copied your API Endpoint URL

---

## Step 4: Generate Application Token

1. Still on the "Connect" page, look for **"Generate Token"** button
2. Click **"Generate Token"**
3. Select role: **"Database Administrator"**
4. Click **"Generate Token"**
5. **IMPORTANT**: Copy the token immediately - it will only be shown once!
   - It looks like: `AstraCS:xxxxxxxxxxxxxxxx:xxxxxxxxxxxxxxxx`

**⚠️ WARNING**: Save this token securely - you won't see it again!

**✅ Checkpoint**: You have copied your token starting with `AstraCS:`

---

## Step 5: Configure Project

1. In your project root, create a file called `.env`:
   ```bash
   touch .env
   ```

2. Open `.env` and add your credentials:
   ```bash
   # AstraDB Configuration
   ASTRADB_TOKEN=AstraCS:your_token_here
   ASTRADB_API_ENDPOINT=https://xxxxx-xxxxx.apps.astra.datastax.com
   ASTRADB_NAMESPACE=cloud_analytics
   ```

3. Replace:
   - `your_token_here` with your actual token from Step 4
   - `https://xxxxx-xxxxx...` with your actual API endpoint from Step 3

**✅ Checkpoint**: Your `.env` file exists with 3 variables set

---

## Step 6: Install Dependencies

```bash
source setup.sh
pip install astrapy python-dotenv
```

**✅ Checkpoint**: Libraries installed without errors

---

## Step 7: Verify Connection

```bash
python -m src.config.astradb_config
```

**Expected output**:
```
AstraDB Configuration Check
============================================================
API Endpoint: https://xxxxx...
Namespace: cloud_analytics
Token: ✓ Configured

✓ Configuration is valid!
```

**✅ Checkpoint**: Configuration check passes

---

## Step 8: Create Collections (Tables)

```bash
./scripts/setup_astradb.sh
```

**Expected output**:
```
Creating collection: org_daily_usage
Creating collection: org_service_costs
Creating collection: tickets_critical_daily
Creating collection: revenue_monthly
Creating collection: genai_tokens_daily

✓ All 5 collections created successfully!
```

**✅ Checkpoint**: All collections created

---

## Step 9: Load Data from Gold Layer

```bash
./scripts/load_to_astradb.sh
```

This will take 30-60 seconds to load ~45,000 documents.

**Expected output**:
```
Loading org_daily_usage: 11,050 documents
Loading org_service_costs: ~33,150 documents
Loading tickets_critical_daily: xxx documents
Loading revenue_monthly: 227 documents
Loading genai_tokens_daily: 848 documents

✓ All data loaded successfully!
```

**✅ Checkpoint**: Data loaded without errors

---

## Step 10: Run Demo Queries

```bash
./scripts/run_demo_queries.sh
```

**Expected output**: 5 formatted tables showing query results

**✅ Checkpoint**: All 5 queries return data

---

## Troubleshooting

### Error: "ASTRADB_TOKEN not configured"
- Check that `.env` file exists in project root
- Check that token starts with `AstraCS:`
- Try: `cat .env` to verify contents

### Error: "Invalid token"
- Token may have been copied incorrectly (check for extra spaces)
- Token may have expired - generate a new one in AstraDB dashboard

### Error: "Collection already exists"
- This is OK - collections were created previously
- Script will skip creation and continue

### Error: "Connection timeout"
- Check internet connection
- Verify API endpoint URL is correct
- Try pinging: `curl <your-api-endpoint>`

### Error: "Module astrapy not found"
- Run: `pip install astrapy python-dotenv`
- Make sure virtual environment is activated

---

## Security Notes

- ✅ `.env` file is in `.gitignore` - won't be committed
- ✅ Never share your token publicly
- ✅ Use environment variables, not hardcoded values
- ✅ Rotate tokens periodically via AstraDB dashboard

---

## Next Steps

After successful setup:
1. Explore data in AstraDB dashboard → Data Explorer
2. Modify demo queries for your use cases
3. Create presentation/demo using the queries
4. Export query results for analysis

---

## Quick Reference

**Files created**:
- `.env` - Your credentials (NOT committed to git)
- `.env.example` - Template (committed to git)

**Scripts**:
- `./scripts/setup_astradb.sh` - Create collections
- `./scripts/load_to_astradb.sh` - Load data
- `./scripts/run_demo_queries.sh` - Run queries

**Verify setup**:
```bash
python -m src.config.astradb_config  # Check configuration
```

**AstraDB Dashboard**: https://astra.datastax.com/
