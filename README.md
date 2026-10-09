# Sarvagram Demo — Customer Deployment Guide

## What you are getting

Three files that create a complete, self-contained demo in your Snowflake account:

| File | Purpose |
|------|---------|
| `sarvagram_deploy.sql` | Single SQL script — creates everything: database, schemas, roles, warehouse, tables, synthetic data, Dynamic Tables, ML model, stored procedures, Streamlit apps, validation queries |
| `sarvagram_drought_app.py` | Streamlit app: drought / crop-stress early warning dashboard |
| `sarvagram_route_app.py` | Streamlit app: collection agent route optimization dashboard |

---

## Prerequisites

| Requirement | How to check |
|-------------|-------------|
| **Role**: ACCOUNTADMIN or SYSADMIN + SECURITYADMIN | `SELECT CURRENT_ROLE();` |
| **Region**: Any commercial AWS/Azure/GCP region | `SELECT CURRENT_REGION();` |
| **Snowflake ML Functions**: Anomaly Detection | Available in all commercial regions by default |
| **Streamlit in Snowflake**: Enabled | `SHOW STREAMLITS;` should not error |
| **Snowpark Python 3.11**: Available | Default on all accounts |

No external data sources, API keys, Marketplace subscriptions, or container images are needed. Everything uses synthetic data and built-in Snowflake capabilities.

---

## Deployment steps

### Step 1: Run the SQL script (sections 1–8)

Open `sarvagram_deploy.sql` in a Snowflake worksheet (Snowsight) and execute sections 1 through 8 **sequentially**. Each section is marked with a header comment.

**Important**: After section 4 (synthetic data) and before section 6 (ML training), wait for the Dynamic Tables in section 5 to complete their first refresh. Check with:

```sql
SELECT COUNT(*) FROM SARVAGRAM_DEMO.CURATED.PLOT_DAILY_FEATURES;
-- Expected: ~73,000 rows. If 0, wait 1-2 minutes and re-check.
```

### Step 2: Upload the Streamlit app files

From a Snowflake worksheet or SnowSQL, upload the two Python files to the stage:

```sql
USE DATABASE SARVAGRAM_DEMO;
USE WAREHOUSE SARVAGRAM_WH;

PUT file:///path/to/sarvagram_drought_app.py @APP.STREAMLIT_STAGE/drought/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/sarvagram_route_app.py @APP.STREAMLIT_STAGE/route/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

Replace `/path/to/` with the actual location of the files on your machine.

### Step 3: Run sections 9–10 of the SQL script

This creates the Streamlit app objects, grants viewer access, and generates the initial route run.

### Step 4: Verify

Run the validation queries in section 11, or open the apps:
- **Snowsight** → Projects → Streamlit → "Sarvagram Drought Early Warning"
- **Snowsight** → Projects → Streamlit → "Sarvagram Route Optimization"

---

## What gets created

### Objects

| Schema | Object | Type | Description |
|--------|--------|------|-------------|
| RAW | PLOT_REGISTRY | Table | 200 synthetic plots across 5 Rajasthan districts |
| RAW | WEATHER_DAILY | Table | 365 days × 10 tehsils, monsoon pattern + drought injection |
| RAW | SATELLITE_INDICATORS_DAILY | Table | NDVI/EVI/NDWI derived from rainfall with ~14-day lag |
| RAW | AGENTS | Table | 8 collection agents with territories, shifts, capacity |
| RAW | COLLECTION_TASKS | Table | ~145 tasks for demo date 2024-09-15 |
| RAW | COLLECTION_HISTORY | Table | 500 historical visit records |
| CURATED | PLOT_CLEAN | Dynamic Table | Validated plot coordinates |
| CURATED | WEATHER_CLEAN | Dynamic Table | Data quality flags on weather readings |
| CURATED | PLOT_DAILY_FEATURES | Dynamic Table | 73K rows — rolling rainfall, NDVI change, dry days |
| ML | DROUGHT_ANOMALY_MODEL | ML Model | Snowflake Anomaly Detection trained on Jan–Aug rainfall |
| ML | ANOMALY_RESULTS | Table | Detection output for Sep–Dec |
| ML | ANOMALY_TRAINING_V / DETECTION_V | Views | Model input views |
| ML | RUN_DROUGHT_SCORING() | Procedure | Re-runs anomaly detection on demand |
| ANALYTICS | PLOT_EARLY_WARNING | Table | 24K scored rows with priority tiers and signals |
| ANALYTICS | ROUTE_RUNS | Table | Route summary per agent per run |
| ANALYTICS | ROUTE_STOPS | Table | Ordered stops with leg distances |
| ANALYTICS | UNASSIGNED_TASKS | Table | Tasks not assigned, with reasons |
| ANALYTICS | ROUTE_BASELINE_COMPARISON | View | Optimized vs naive distance comparison |
| ANALYTICS | GENERATE_ROUTES(DATE) | Procedure | Snowpark Python route solver |
| APP | DROUGHT_EARLY_WARNING | Streamlit | Drought dashboard |
| APP | ROUTE_OPTIMIZATION | Streamlit | Route dashboard |

### Roles

| Role | Access |
|------|--------|
| SARVAGRAM_ADMIN | Full ownership of all SARVAGRAM_DEMO objects |
| SARVAGRAM_DEV | Read all schemas + create in ML/APP |
| SARVAGRAM_VIEWER | Read-only on ANALYTICS and APP (no access to RAW farmer tokens or ML internals) |

### Warehouse

`SARVAGRAM_WH`: X-Small, auto-suspend 60s, auto-resume. Estimated ~2–4 credits for full setup.

---

## Cost notes

| Activity | Estimated credits |
|----------|------------------|
| Initial setup (data gen + DT refresh + ML training + route solve) | ~2–4 credits |
| Dynamic Table hourly refresh (3 DTs, XS warehouse) | ~0.5 credits/day |
| Each Streamlit session | Negligible (XS warehouse queries) |
| Each `GENERATE_ROUTES()` call | <0.1 credits |

**To reduce ongoing cost after the demo**: suspend or drop the Dynamic Tables, or change their target_lag to `DOWNSTREAM`.

```sql
ALTER DYNAMIC TABLE SARVAGRAM_DEMO.CURATED.PLOT_DAILY_FEATURES SUSPEND;
ALTER DYNAMIC TABLE SARVAGRAM_DEMO.CURATED.WEATHER_CLEAN SUSPEND;
ALTER DYNAMIC TABLE SARVAGRAM_DEMO.CURATED.PLOT_CLEAN SUSPEND;
```

---

## Customization for real data

| Replace this | With this | What changes |
|-------------|-----------|-------------|
| Section 4 (synthetic data INSERTs) | COPY INTO from S3/stage with customer data | RAW tables get real rows; everything downstream refreshes |
| Hardcoded Rajasthan coordinates | Customer's actual plot/agent coordinates | Geospatial joins and maps update |
| `'2024-09-15'` demo date | Current date or customer-specified date range | Route solver and early warning cover live data |
| Drought injection logic | Actual weather anomalies from real data | ML model detects real patterns |
| Straight-line distance (ST_DISTANCE) | Road-network distance matrix | Route optimization uses real travel times |
| Synthetic farmer/account tokens | Tokenized real identifiers with masking policies | Add column masking per RBI/privacy requirements |

---

## Live demo commands

```sql
-- Re-run drought scoring (updates PLOT_EARLY_WARNING)
CALL SARVAGRAM_DEMO.ML.RUN_DROUGHT_SCORING();

-- Generate new route run
CALL SARVAGRAM_DEMO.ANALYTICS.GENERATE_ROUTES('2024-09-15');
```

---

## Cleanup

```sql
DROP DATABASE IF EXISTS SARVAGRAM_DEMO;
DROP WAREHOUSE IF EXISTS SARVAGRAM_WH;
DROP ROLE IF EXISTS SARVAGRAM_VIEWER;
DROP ROLE IF EXISTS SARVAGRAM_DEV;
DROP ROLE IF EXISTS SARVAGRAM_ADMIN;
```

---

## Important disclaimers (per the architecture brief)

- **Priority tiers are rule-based**, not predicted crop loss or credit default probabilities. Supervised classification requires historical outcome labels.
- **Distances are straight-line (great-circle)**, not road distance or travel time. A road-network matrix is needed for production routing.
- **The route solver is custom Snowpark Python code**, not a built-in Snowflake vehicle-routing product.
- **Data is synthetic**. Replace with approved customer data only after confirming data-use agreements, Marketplace terms, and compliance requirements.
- **Role-based access is demonstrated** but should not be represented as a complete RBI compliance determination.
