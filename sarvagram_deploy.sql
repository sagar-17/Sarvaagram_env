-- =============================================================================
-- SARVAGRAM DEMO: DEPLOYMENT SCRIPT
-- Drought Early Warning + Collection Route Optimization
-- =============================================================================
-- 
-- PREREQUISITES (verify before running):
--   1. Role with CREATE DATABASE, CREATE WAREHOUSE, CREATE ROLE privileges
--      (ACCOUNTADMIN or SYSADMIN + SECURITYADMIN recommended for initial setup)
--   2. Snowflake ML Functions available in the account region
--      (Anomaly Detection: all commercial regions; check docs for gov regions)
--   3. Streamlit in Snowflake enabled (default on most accounts)
--   4. Snowpark Python 3.11 runtime available (default on all accounts)
--
-- ESTIMATED CREDIT COST (one-time setup + synthetic data):
--   ~2-4 credits total on XS warehouse (data gen, DT refresh, ML training,
--   route solving). Ongoing: ~0.5 credits/day if Dynamic Tables refresh hourly.
--   Reduce by pausing DTs or extending target_lag after the demo.
--
-- WHAT THIS SCRIPT DOES NOT DO:
--   - Connect to external data sources (S3, Postgres, Marketplace)
--   - Schedule recurring Tasks or ML Jobs (manual invocation only)
--   - Create masking policies (add per customer compliance requirements)
--   - Use road-network distance (straight-line proxy only)
--   - Predict crop loss or credit default (rule-based prioritization only)
--
-- RUN ORDER: Execute sections 1-9 sequentially. Each section is idempotent.
-- =============================================================================


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 1: DATABASE, SCHEMAS, WAREHOUSE, ROLES                         ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

CREATE DATABASE IF NOT EXISTS SARVAGRAM_DEMO
    COMMENT = 'Sarvagram demo: drought early warning + collection route optimization';

CREATE SCHEMA IF NOT EXISTS SARVAGRAM_DEMO.RAW
    COMMENT = 'Source-shaped or synthetic inputs';
CREATE SCHEMA IF NOT EXISTS SARVAGRAM_DEMO.CURATED
    COMMENT = 'Cleaned entities and joined daily facts';
CREATE SCHEMA IF NOT EXISTS SARVAGRAM_DEMO.ML
    COMMENT = 'Model objects, training views, evaluation outputs';
CREATE SCHEMA IF NOT EXISTS SARVAGRAM_DEMO.ANALYTICS
    COMMENT = 'Scores, alerts, route runs, reporting views';
CREATE SCHEMA IF NOT EXISTS SARVAGRAM_DEMO.APP
    COMMENT = 'Streamlit apps and semantic layer';

CREATE WAREHOUSE IF NOT EXISTS SARVAGRAM_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Sarvagram demo workload';

-- Roles: ADMIN (owns objects) > DEV (create in ML/APP) > VIEWER (read ANALYTICS)
CREATE ROLE IF NOT EXISTS SARVAGRAM_ADMIN;
CREATE ROLE IF NOT EXISTS SARVAGRAM_DEV;
CREATE ROLE IF NOT EXISTS SARVAGRAM_VIEWER;

GRANT ROLE SARVAGRAM_ADMIN TO ROLE SYSADMIN;         -- adjust to your hierarchy
GRANT ROLE SARVAGRAM_DEV TO ROLE SARVAGRAM_ADMIN;
GRANT ROLE SARVAGRAM_VIEWER TO ROLE SARVAGRAM_DEV;

GRANT USAGE ON WAREHOUSE SARVAGRAM_WH TO ROLE SARVAGRAM_ADMIN;
GRANT USAGE ON WAREHOUSE SARVAGRAM_WH TO ROLE SARVAGRAM_DEV;
GRANT USAGE ON WAREHOUSE SARVAGRAM_WH TO ROLE SARVAGRAM_VIEWER;

GRANT ALL PRIVILEGES ON DATABASE SARVAGRAM_DEMO TO ROLE SARVAGRAM_ADMIN;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE SARVAGRAM_DEMO TO ROLE SARVAGRAM_ADMIN;

GRANT USAGE ON DATABASE SARVAGRAM_DEMO TO ROLE SARVAGRAM_DEV;
GRANT USAGE ON ALL SCHEMAS IN DATABASE SARVAGRAM_DEMO TO ROLE SARVAGRAM_DEV;
GRANT SELECT ON FUTURE TABLES IN DATABASE SARVAGRAM_DEMO TO ROLE SARVAGRAM_DEV;
GRANT SELECT ON FUTURE VIEWS IN DATABASE SARVAGRAM_DEMO TO ROLE SARVAGRAM_DEV;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN DATABASE SARVAGRAM_DEMO TO ROLE SARVAGRAM_DEV;

GRANT USAGE ON DATABASE SARVAGRAM_DEMO TO ROLE SARVAGRAM_VIEWER;
GRANT USAGE ON SCHEMA SARVAGRAM_DEMO.ANALYTICS TO ROLE SARVAGRAM_VIEWER;
GRANT USAGE ON SCHEMA SARVAGRAM_DEMO.APP TO ROLE SARVAGRAM_VIEWER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA SARVAGRAM_DEMO.ANALYTICS TO ROLE SARVAGRAM_VIEWER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA SARVAGRAM_DEMO.ANALYTICS TO ROLE SARVAGRAM_VIEWER;

-- Grant roles to the user running the demo (CHANGE THIS to the customer's user)
-- GRANT ROLE SARVAGRAM_ADMIN TO USER <DEMO_USER>;
-- GRANT ROLE SARVAGRAM_VIEWER TO USER <VIEWER_USER>;

USE DATABASE SARVAGRAM_DEMO;
USE WAREHOUSE SARVAGRAM_WH;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 2: RAW TABLES — DROUGHT USE CASE                               ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

CREATE OR REPLACE TABLE RAW.PLOT_REGISTRY (
    PLOT_ID              VARCHAR(20),
    FARMER_TOKEN         VARCHAR(20),
    DISTRICT             VARCHAR(50),
    TEHSIL               VARCHAR(50),
    PLOT_GEOGRAPHY       GEOGRAPHY,
    AREA_HECTARES        FLOAT,
    CROP_TYPE            VARCHAR(30),
    PORTFOLIO_EXPOSURE_INR FLOAT,
    CREATED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.WEATHER_DAILY (
    AREA_ID              VARCHAR(50),
    OBS_DATE             DATE,
    RAINFALL_MM          FLOAT,
    MAX_TEMP_C           FLOAT,
    MIN_TEMP_C           FLOAT,
    HUMIDITY_PCT         FLOAT,
    SOIL_MOISTURE_INDEX  FLOAT,
    SOURCE               VARCHAR(20) DEFAULT 'SYNTHETIC',
    LOADED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.SATELLITE_INDICATORS_DAILY (
    AREA_ID              VARCHAR(50),
    OBS_DATE             DATE,
    NDVI                 FLOAT,
    EVI                  FLOAT,
    NDWI                 FLOAT,
    SOURCE               VARCHAR(20) DEFAULT 'SYNTHETIC',
    LOADED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 3: RAW TABLES — ROUTE USE CASE                                 ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

CREATE OR REPLACE TABLE RAW.AGENTS (
    AGENT_TOKEN          VARCHAR(20),
    AGENT_NAME           VARCHAR(50),
    BASE_LONGITUDE       FLOAT,
    BASE_LATITUDE        FLOAT,
    BASE_LOCATION        GEOGRAPHY,
    TERRITORY            VARCHAR(50),
    SHIFT_START_TIME     TIME,
    SHIFT_END_TIME       TIME,
    MAX_STOPS_PER_DAY    NUMBER(38,0),
    VEHICLE_TYPE         VARCHAR(20),
    IS_ACTIVE            BOOLEAN DEFAULT TRUE,
    CREATED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.COLLECTION_TASKS (
    TASK_ID              VARCHAR(20),
    ACCOUNT_TOKEN        VARCHAR(20),
    CUSTOMER_LONGITUDE   FLOAT,
    CUSTOMER_LATITUDE    FLOAT,
    CUSTOMER_LOCATION    GEOGRAPHY,
    TERRITORY            VARCHAR(50),
    DUE_DATE             DATE,
    DUE_WINDOW_START     TIME,
    DUE_WINDOW_END       TIME,
    SERVICE_DURATION_MIN NUMBER(38,0),
    PRIORITY             NUMBER(38,0),  -- 1=highest, 5=lowest
    ESTIMATED_AMOUNT_INR FLOAT,
    TASK_STATUS          VARCHAR(20) DEFAULT 'PENDING',
    CREATED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.COLLECTION_HISTORY (
    VISIT_ID             VARCHAR(20),
    ACCOUNT_TOKEN        VARCHAR(20),
    AGENT_TOKEN          VARCHAR(20),
    VISIT_DATE           DATE,
    ACTUAL_DURATION_MIN  NUMBER(38,0),
    AMOUNT_COLLECTED_INR FLOAT,
    OUTCOME              VARCHAR(20),
    CREATED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 4: SYNTHETIC DATA GENERATION                                    ║
-- ║                                                                          ║
-- ║  Replace this section with COPY INTO from your actual data source        ║
-- ║  if approved customer data or Marketplace shares are available.           ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- 4a. Plot registry: 200 plots across 5 Rajasthan districts / 10 tehsils
INSERT INTO RAW.PLOT_REGISTRY
  (PLOT_ID, FARMER_TOKEN, DISTRICT, TEHSIL, PLOT_GEOGRAPHY, AREA_HECTARES, CROP_TYPE, PORTFOLIO_EXPOSURE_INR)
WITH districts AS (
    SELECT * FROM VALUES
        ('Jodhpur',   'Osian',     26.28, 73.02),
        ('Jodhpur',   'Bilara',    26.18, 73.70),
        ('Barmer',    'Barmer',    25.75, 71.38),
        ('Barmer',    'Balotra',   25.83, 72.24),
        ('Jaisalmer', 'Jaisalmer', 26.91, 70.91),
        ('Jaisalmer', 'Pokaran',   26.92, 71.92),
        ('Nagaur',    'Nagaur',    27.20, 73.73),
        ('Nagaur',    'Degana',    26.90, 74.32),
        ('Pali',      'Pali',      25.77, 73.32),
        ('Pali',      'Sojat',     25.92, 73.67)
    AS t(district, tehsil, base_lat, base_lon)
),
plot_gen AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY d.district, d.tehsil, s.seq) AS rn,
        d.district, d.tehsil,
        d.base_lat + (UNIFORM(-500, 500, RANDOM()) / 10000.0) AS lat,
        d.base_lon + (UNIFORM(-500, 500, RANDOM()) / 10000.0) AS lon,
        ROUND(UNIFORM(5, 250, RANDOM()) / 10.0, 1) AS area,
        CASE MOD(s.seq, 5) WHEN 0 THEN 'Bajra' WHEN 1 THEN 'Jowar'
            WHEN 2 THEN 'Groundnut' WHEN 3 THEN 'Moong' WHEN 4 THEN 'Guar' END AS crop,
        ROUND(UNIFORM(20000, 500000, RANDOM()), -2) AS exposure
    FROM districts d CROSS JOIN (SELECT SEQ4() AS seq FROM TABLE(GENERATOR(ROWCOUNT => 20))) s
)
SELECT 'PLT-' || LPAD(rn, 5, '0'), 'FRM-' || LPAD(MOD(rn*7+13, 180)+1, 5, '0'),
       district, tehsil, ST_MAKEPOINT(lon, lat), area, crop, exposure
FROM plot_gen;


-- 4b. Weather: 365 days x 10 areas with monsoon pattern + drought injection
--     Barmer/Balotra/Jaisalmer/Pokaran get 70% rain reduction in Aug-Sep
INSERT INTO RAW.WEATHER_DAILY (AREA_ID, OBS_DATE, RAINFALL_MM, MAX_TEMP_C, MIN_TEMP_C, HUMIDITY_PCT, SOIL_MOISTURE_INDEX)
WITH areas AS (
    SELECT * FROM VALUES ('Osian'),('Bilara'),('Barmer'),('Balotra'),('Jaisalmer'),
        ('Pokaran'),('Nagaur'),('Degana'),('Pali'),('Sojat') AS t(area_id)
),
dates AS (
    SELECT DATEADD('day', SEQ4(), '2024-01-01')::DATE AS obs_date
    FROM TABLE(GENERATOR(ROWCOUNT => 365))
),
base AS (
    SELECT a.area_id, d.obs_date, DAYOFYEAR(d.obs_date) AS doy,
        GREATEST(0,
            CASE WHEN doy BETWEEN 150 AND 270 THEN
                12.0 * EXP(-0.5 * POW((doy - 210.0) / 30.0, 2)) + UNIFORM(0, 800, RANDOM()) / 100.0
            ELSE UNIFORM(0, 100, RANDOM()) / 100.0 END
        ) AS base_rain,
        CASE WHEN a.area_id IN ('Barmer','Balotra','Jaisalmer','Pokaran')
                  AND doy BETWEEN 210 AND 270 THEN 0.3 ELSE 1.0 END AS drought_factor,
        28 + 15 * SIN((doy - 100) * 3.14159 / 180.0) + UNIFORM(-200, 200, RANDOM()) / 100.0 AS max_t
    FROM areas a CROSS JOIN dates d
)
SELECT area_id, obs_date,
    ROUND(base_rain * drought_factor, 1),
    ROUND(max_t, 1),
    ROUND(max_t - 8 - UNIFORM(0, 500, RANDOM()) / 100.0, 1),
    ROUND(LEAST(100, GREATEST(10, 30 + base_rain*4 + UNIFORM(-500,500,RANDOM())/100.0)), 1),
    ROUND(LEAST(1.0, GREATEST(0.0, 0.3 + base_rain*drought_factor/30.0 + UNIFORM(-100,100,RANDOM())/1000.0)), 3)
FROM base;


-- 4c. Satellite indicators (NDVI, EVI, NDWI): derived from weather with ~14-day lag
INSERT INTO RAW.SATELLITE_INDICATORS_DAILY (AREA_ID, OBS_DATE, NDVI, EVI, NDWI)
WITH areas AS (
    SELECT * FROM VALUES ('Osian'),('Bilara'),('Barmer'),('Balotra'),('Jaisalmer'),
        ('Pokaran'),('Nagaur'),('Degana'),('Pali'),('Sojat') AS t(area_id)
),
dates AS (
    SELECT DATEADD('day', SEQ4(), '2024-01-01')::DATE AS obs_date
    FROM TABLE(GENERATOR(ROWCOUNT => 365))
),
weather_ref AS (
    SELECT area_id, obs_date,
        AVG(rainfall_mm) OVER (PARTITION BY area_id ORDER BY obs_date
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS rain_14d
    FROM RAW.WEATHER_DAILY
)
SELECT w.area_id, w.obs_date,
    ROUND(LEAST(0.75, GREATEST(0.08, 0.15 + (w.rain_14d/15.0)*0.4 + UNIFORM(-30,30,RANDOM())/1000.0)), 3),
    ROUND(LEAST(0.55, GREATEST(0.05, 0.10 + (w.rain_14d/15.0)*0.3 + UNIFORM(-20,20,RANDOM())/1000.0)), 3),
    ROUND(LEAST(0.40, GREATEST(-0.30, -0.15 + (w.rain_14d/12.0)*0.3 + UNIFORM(-30,30,RANDOM())/1000.0)), 3)
FROM weather_ref w;


-- 4d. Agents: 8 collection agents across territories
INSERT INTO RAW.AGENTS
  (AGENT_TOKEN, AGENT_NAME, BASE_LONGITUDE, BASE_LATITUDE, TERRITORY,
   SHIFT_START_TIME, SHIFT_END_TIME, MAX_STOPS_PER_DAY, VEHICLE_TYPE)
VALUES
    ('AGT-001', 'Agent Rajesh',  73.02, 26.28, 'Jodhpur-North',  '08:00', '17:00', 12, 'Motorcycle'),
    ('AGT-002', 'Agent Sunil',   73.70, 26.18, 'Jodhpur-South',  '08:00', '17:00', 12, 'Motorcycle'),
    ('AGT-003', 'Agent Vikram',  71.38, 25.75, 'Barmer',         '08:30', '16:30', 10, 'Motorcycle'),
    ('AGT-004', 'Agent Deepak',  70.91, 26.91, 'Jaisalmer',      '08:00', '16:00', 8,  'Motorcycle'),
    ('AGT-005', 'Agent Mahesh',  73.73, 27.20, 'Nagaur',         '08:00', '17:00', 12, 'Motorcycle'),
    ('AGT-006', 'Agent Prakash', 73.32, 25.77, 'Pali',           '09:00', '18:00', 10, 'Motorcycle'),
    ('AGT-007', 'Agent Karan',   72.24, 25.83, 'Barmer-East',    '08:00', '16:30', 10, 'Motorcycle'),
    ('AGT-008', 'Agent Arjun',   71.92, 26.92, 'Jaisalmer-East', '08:30', '16:00', 8,  'Car');

UPDATE RAW.AGENTS SET BASE_LOCATION = ST_MAKEPOINT(BASE_LONGITUDE, BASE_LATITUDE);


-- 4e. Collection tasks: ~145 tasks for demo date 2024-09-15
INSERT INTO RAW.COLLECTION_TASKS
  (TASK_ID, ACCOUNT_TOKEN, CUSTOMER_LONGITUDE, CUSTOMER_LATITUDE, TERRITORY,
   DUE_DATE, DUE_WINDOW_START, DUE_WINDOW_END, SERVICE_DURATION_MIN, PRIORITY, ESTIMATED_AMOUNT_INR)
WITH territories AS (
    SELECT * FROM VALUES
        ('Jodhpur-North',73.02,26.28,25), ('Jodhpur-South',73.70,26.18,20),
        ('Barmer',71.38,25.75,18), ('Barmer-East',72.24,25.83,15),
        ('Jaisalmer',70.91,26.91,12), ('Jaisalmer-East',71.92,26.92,10),
        ('Nagaur',73.73,27.20,25), ('Pali',73.32,25.77,20)
    AS t(territory, ctr_lon, ctr_lat, task_count)
),
task_gen AS (
    SELECT ROW_NUMBER() OVER (ORDER BY t.territory, s.seq) AS rn,
        t.territory,
        t.ctr_lon + (UNIFORM(-400, 400, RANDOM()) / 10000.0) AS lon,
        t.ctr_lat + (UNIFORM(-400, 400, RANDOM()) / 10000.0) AS lat,
        CASE WHEN UNIFORM(1,10,RANDOM()) <= 3 THEN 1 WHEN UNIFORM(1,10,RANDOM()) <= 6 THEN 2
             WHEN UNIFORM(1,10,RANDOM()) <= 8 THEN 3 ELSE 4 END AS priority,
        10 + UNIFORM(0, 20, RANDOM()) AS svc_min,
        ROUND(UNIFORM(500, 50000, RANDOM()), -1) AS amt
    FROM territories t
    CROSS JOIN (SELECT SEQ4() AS seq FROM TABLE(GENERATOR(ROWCOUNT => 25))) s
    WHERE s.seq < t.task_count
)
SELECT 'TSK-' || LPAD(rn, 5, '0'), 'ACC-' || LPAD(MOD(rn*11+7, 200)+1, 5, '0'),
       lon, lat, territory, '2024-09-15', '08:00', '17:00', svc_min, priority, amt
FROM task_gen;

UPDATE RAW.COLLECTION_TASKS
SET CUSTOMER_LOCATION = ST_MAKEPOINT(CUSTOMER_LONGITUDE, CUSTOMER_LATITUDE);


-- 4f. Collection history: 500 past visits for optional ML
INSERT INTO RAW.COLLECTION_HISTORY
  (VISIT_ID, ACCOUNT_TOKEN, AGENT_TOKEN, VISIT_DATE, ACTUAL_DURATION_MIN, AMOUNT_COLLECTED_INR, OUTCOME)
WITH gen AS (
    SELECT SEQ4()+1 AS rn,
        'ACC-' || LPAD(UNIFORM(1, 200, RANDOM()), 5, '0') AS acc,
        'AGT-' || LPAD(UNIFORM(1, 8, RANDOM()), 3, '0') AS agt,
        DATEADD('day', -UNIFORM(1, 90, RANDOM()), '2024-09-15')::DATE AS vdate,
        10 + UNIFORM(0, 25, RANDOM()) AS dur,
        UNIFORM(1, 10, RANDOM()) AS outcome_roll
    FROM TABLE(GENERATOR(ROWCOUNT => 500))
)
SELECT 'VIS-' || LPAD(rn, 6, '0'), acc, agt, vdate, dur,
    CASE WHEN outcome_roll <= 5 THEN ROUND(UNIFORM(500, 50000, RANDOM()), -1)
         WHEN outcome_roll <= 7 THEN ROUND(UNIFORM(100, 10000, RANDOM()), -1)
         ELSE 0 END,
    CASE WHEN outcome_roll <= 5 THEN 'COLLECTED' WHEN outcome_roll <= 7 THEN 'PARTIAL'
         WHEN outcome_roll <= 8 THEN 'NOT_HOME' WHEN outcome_roll = 9 THEN 'REFUSED'
         ELSE 'RESCHEDULED' END
FROM gen;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 5: DYNAMIC TABLES — CURATED LAYER                              ║
-- ║                                                                          ║
-- ║  These refresh automatically to meet target_lag. Adjust lag to '1 day'   ║
-- ║  or DOWNSTREAM to reduce cost when not actively demoing.                 ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

CREATE OR REPLACE DYNAMIC TABLE CURATED.PLOT_CLEAN
    TARGET_LAG = '1 hour'
    WAREHOUSE = SARVAGRAM_WH
AS
SELECT
    PLOT_ID, FARMER_TOKEN, DISTRICT, TEHSIL, PLOT_GEOGRAPHY,
    ST_Y(PLOT_GEOGRAPHY) AS LATITUDE,
    ST_X(PLOT_GEOGRAPHY) AS LONGITUDE,
    AREA_HECTARES, CROP_TYPE, PORTFOLIO_EXPOSURE_INR,
    CASE WHEN ST_Y(PLOT_GEOGRAPHY) BETWEEN 23.0 AND 30.0
          AND ST_X(PLOT_GEOGRAPHY) BETWEEN 69.0 AND 77.0
         THEN TRUE ELSE FALSE END AS COORDS_VALID,
    CREATED_AT
FROM RAW.PLOT_REGISTRY
WHERE PLOT_GEOGRAPHY IS NOT NULL;


CREATE OR REPLACE DYNAMIC TABLE CURATED.WEATHER_CLEAN
    TARGET_LAG = '1 hour'
    WAREHOUSE = SARVAGRAM_WH
AS
SELECT
    AREA_ID, OBS_DATE, RAINFALL_MM, MAX_TEMP_C, MIN_TEMP_C,
    HUMIDITY_PCT, SOIL_MOISTURE_INDEX,
    CASE WHEN RAINFALL_MM < 0 OR RAINFALL_MM > 500 THEN TRUE ELSE FALSE END AS RAINFALL_SUSPECT,
    CASE WHEN MAX_TEMP_C < MIN_TEMP_C THEN TRUE ELSE FALSE END AS TEMP_INVERTED,
    CASE WHEN RAINFALL_MM IS NULL OR MAX_TEMP_C IS NULL THEN TRUE ELSE FALSE END AS HAS_MISSING,
    SOURCE, LOADED_AT
FROM RAW.WEATHER_DAILY
WHERE OBS_DATE IS NOT NULL;


CREATE OR REPLACE DYNAMIC TABLE CURATED.PLOT_DAILY_FEATURES
    TARGET_LAG = '1 hour'
    WAREHOUSE = SARVAGRAM_WH
AS
WITH weather_features AS (
    SELECT w.AREA_ID, w.OBS_DATE, w.RAINFALL_MM, w.MAX_TEMP_C,
        w.HUMIDITY_PCT, w.SOIL_MOISTURE_INDEX, w.RAINFALL_SUSPECT, w.HAS_MISSING,
        SUM(w.RAINFALL_MM) OVER (PARTITION BY w.AREA_ID ORDER BY w.OBS_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RAIN_7D_MM,
        SUM(w.RAINFALL_MM) OVER (PARTITION BY w.AREA_ID ORDER BY w.OBS_DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS RAIN_30D_MM,
        AVG(w.RAINFALL_MM) OVER (PARTITION BY w.AREA_ID ORDER BY w.OBS_DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS RAIN_30D_AVG,
        AVG(w.RAINFALL_MM) OVER (PARTITION BY w.AREA_ID) AS RAIN_ANNUAL_AVG,
        SUM(CASE WHEN w.RAINFALL_MM < 1.0 THEN 1 ELSE 0 END) OVER (PARTITION BY w.AREA_ID ORDER BY w.OBS_DATE ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS DRY_DAYS_15D,
        AVG(w.MAX_TEMP_C) OVER (PARTITION BY w.AREA_ID ORDER BY w.OBS_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS TEMP_7D_AVG
    FROM CURATED.WEATHER_CLEAN w
),
satellite AS (
    SELECT AREA_ID, OBS_DATE, NDVI, EVI, NDWI,
        AVG(NDVI) OVER (PARTITION BY AREA_ID ORDER BY OBS_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS NDVI_7D_AVG,
        AVG(NDVI) OVER (PARTITION BY AREA_ID ORDER BY OBS_DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS NDVI_30D_AVG,
        LAG(NDVI, 7) OVER (PARTITION BY AREA_ID ORDER BY OBS_DATE) AS NDVI_7D_PRIOR,
        LAG(NDVI, 30) OVER (PARTITION BY AREA_ID ORDER BY OBS_DATE) AS NDVI_30D_PRIOR
    FROM RAW.SATELLITE_INDICATORS_DAILY
)
SELECT
    p.PLOT_ID, p.FARMER_TOKEN, p.DISTRICT, p.TEHSIL, p.CROP_TYPE,
    p.AREA_HECTARES, p.PORTFOLIO_EXPOSURE_INR, wf.OBS_DATE,
    wf.RAINFALL_MM, wf.RAIN_7D_MM, wf.RAIN_30D_MM, wf.RAIN_30D_AVG,
    CASE WHEN wf.RAIN_ANNUAL_AVG > 0
         THEN ROUND((wf.RAIN_30D_AVG - wf.RAIN_ANNUAL_AVG) / NULLIF(wf.RAIN_ANNUAL_AVG, 0), 3)
         ELSE NULL END AS RAIN_DEVIATION_FROM_BASELINE,
    wf.DRY_DAYS_15D, wf.MAX_TEMP_C, wf.TEMP_7D_AVG, wf.HUMIDITY_PCT, wf.SOIL_MOISTURE_INDEX,
    s.NDVI, s.EVI, s.NDWI, s.NDVI_7D_AVG, s.NDVI_30D_AVG,
    CASE WHEN s.NDVI_7D_PRIOR > 0
         THEN ROUND((s.NDVI - s.NDVI_7D_PRIOR) / s.NDVI_7D_PRIOR, 3)
         ELSE NULL END AS NDVI_7D_CHANGE_PCT,
    CASE WHEN s.NDVI_30D_PRIOR > 0
         THEN ROUND((s.NDVI - s.NDVI_30D_PRIOR) / s.NDVI_30D_PRIOR, 3)
         ELSE NULL END AS NDVI_30D_CHANGE_PCT,
    wf.RAINFALL_SUSPECT,
    wf.HAS_MISSING AS WEATHER_HAS_MISSING
FROM CURATED.PLOT_CLEAN p
JOIN weather_features wf ON p.TEHSIL = wf.AREA_ID
LEFT JOIN satellite s ON wf.AREA_ID = s.AREA_ID AND wf.OBS_DATE = s.OBS_DATE
WHERE p.COORDS_VALID = TRUE;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 6: ML — ANOMALY DETECTION                                       ║
-- ║                                                                          ║
-- ║  Wait for Dynamic Tables to finish their first refresh before running.    ║
-- ║  Check: SELECT COUNT(*) FROM CURATED.PLOT_DAILY_FEATURES;                ║
-- ║  Expected: ~73,000 rows (200 plots × 365 days).                          ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- Training view: Jan–Aug data for learning seasonal patterns
CREATE OR REPLACE VIEW ML.ANOMALY_TRAINING_V AS
SELECT DISTINCT
    TEHSIL AS SERIES_ID,
    OBS_DATE AS TIMESTAMP,
    RAINFALL_MM AS RAINFALL,
    NDVI,
    SOIL_MOISTURE_INDEX AS SMI
FROM CURATED.PLOT_DAILY_FEATURES
WHERE OBS_DATE <= '2024-08-31'
ORDER BY TEHSIL, OBS_DATE;

-- Detection view: Sep–Dec data where drought signals should appear
CREATE OR REPLACE VIEW ML.ANOMALY_DETECTION_V AS
SELECT DISTINCT
    TEHSIL AS SERIES_ID,
    OBS_DATE AS TIMESTAMP,
    RAINFALL_MM AS RAINFALL,
    NDVI,
    SOIL_MOISTURE_INDEX AS SMI
FROM CURATED.PLOT_DAILY_FEATURES
WHERE OBS_DATE > '2024-08-31'
ORDER BY TEHSIL, OBS_DATE;

-- Train anomaly detection model
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ML.DROUGHT_ANOMALY_MODEL(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'SARVAGRAM_DEMO.ML.ANOMALY_TRAINING_V'),
    SERIES_COLNAME => 'SERIES_ID',
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'RAINFALL',
    LABEL_COLNAME => ''
);

-- Run detection and persist results
CALL ML.DROUGHT_ANOMALY_MODEL!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'SARVAGRAM_DEMO.ML.ANOMALY_DETECTION_V'),
    SERIES_COLNAME => 'SERIES_ID',
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'RAINFALL',
    CONFIG_OBJECT => {'prediction_interval': 0.95}
);

CREATE OR REPLACE TABLE ML.ANOMALY_RESULTS AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 7: ANALYTICS — EARLY WARNING SCORES + ROUTE TABLES              ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- 7a. Plot early warning: combines ML anomaly output with rule-based tiers
CREATE OR REPLACE TABLE ANALYTICS.PLOT_EARLY_WARNING AS
WITH anomaly_scores AS (
    SELECT SERIES AS TEHSIL, TS::DATE AS OBS_DATE, IS_ANOMALY,
           PERCENTILE AS ANOMALY_PERCENTILE, DISTANCE AS ANOMALY_DISTANCE,
           FORECAST AS EXPECTED_RAINFALL, Y AS ACTUAL_RAINFALL,
           LOWER_BOUND, UPPER_BOUND
    FROM ML.ANOMALY_RESULTS
),
plot_features AS (
    SELECT DISTINCT
        PLOT_ID, FARMER_TOKEN, DISTRICT, TEHSIL, CROP_TYPE, AREA_HECTARES,
        PORTFOLIO_EXPOSURE_INR, OBS_DATE, RAINFALL_MM, RAIN_7D_MM, RAIN_30D_MM,
        RAIN_DEVIATION_FROM_BASELINE, DRY_DAYS_15D, MAX_TEMP_C, TEMP_7D_AVG,
        HUMIDITY_PCT, SOIL_MOISTURE_INDEX, NDVI, NDVI_7D_AVG, NDVI_30D_AVG,
        NDVI_7D_CHANGE_PCT, NDVI_30D_CHANGE_PCT, WEATHER_HAS_MISSING
    FROM CURATED.PLOT_DAILY_FEATURES
    WHERE OBS_DATE > '2024-08-31'
)
SELECT
    pf.PLOT_ID, pf.FARMER_TOKEN, pf.DISTRICT, pf.TEHSIL, pf.CROP_TYPE,
    pf.AREA_HECTARES, pf.PORTFOLIO_EXPOSURE_INR,
    pf.OBS_DATE AS AS_OF_DATE,
    a.IS_ANOMALY AS RAINFALL_ANOMALY_FLAG,
    ROUND(a.ANOMALY_DISTANCE, 3) AS ANOMALY_DISTANCE,
    ROUND(a.EXPECTED_RAINFALL, 2) AS EXPECTED_RAINFALL_MM,
    ROUND(a.ACTUAL_RAINFALL, 2) AS ACTUAL_RAINFALL_MM,
    pf.RAIN_7D_MM, pf.RAIN_30D_MM,
    ROUND(pf.RAIN_DEVIATION_FROM_BASELINE, 3) AS RAIN_DEVIATION_PCT,
    pf.DRY_DAYS_15D, pf.NDVI,
    ROUND(pf.NDVI_7D_CHANGE_PCT, 3) AS NDVI_7D_CHANGE_PCT,
    ROUND(pf.NDVI_30D_CHANGE_PCT, 3) AS NDVI_30D_CHANGE_PCT,
    pf.SOIL_MOISTURE_INDEX, pf.MAX_TEMP_C,
    -- Rule-based tier: NOT a predicted loss probability
    CASE
        WHEN a.IS_ANOMALY AND pf.NDVI < 0.2 AND pf.DRY_DAYS_15D >= 12 THEN 'CRITICAL'
        WHEN a.IS_ANOMALY AND (pf.NDVI < 0.3 OR pf.DRY_DAYS_15D >= 10) THEN 'HIGH'
        WHEN a.IS_ANOMALY OR pf.NDVI < 0.25 OR pf.DRY_DAYS_15D >= 10 THEN 'MODERATE'
        WHEN pf.RAIN_DEVIATION_FROM_BASELINE < -0.3 OR pf.NDVI_7D_CHANGE_PCT < -0.15 THEN 'WATCH'
        ELSE 'NORMAL'
    END AS PRIORITY_TIER,
    ARRAY_CONSTRUCT_COMPACT(
        IFF(a.IS_ANOMALY, 'RAINFALL_ANOMALY', NULL),
        IFF(pf.NDVI < 0.2, 'NDVI_VERY_LOW', IFF(pf.NDVI < 0.3, 'NDVI_LOW', NULL)),
        IFF(pf.DRY_DAYS_15D >= 12, 'EXTENDED_DRY_SPELL', IFF(pf.DRY_DAYS_15D >= 8, 'DRY_SPELL', NULL)),
        IFF(pf.RAIN_DEVIATION_FROM_BASELINE < -0.3, 'BELOW_BASELINE_RAIN', NULL),
        IFF(pf.NDVI_7D_CHANGE_PCT < -0.15, 'RAPID_NDVI_DECLINE', NULL),
        IFF(pf.SOIL_MOISTURE_INDEX < 0.15, 'LOW_SOIL_MOISTURE', NULL),
        IFF(pf.MAX_TEMP_C > 42, 'EXTREME_HEAT', NULL)
    ) AS CONTRIBUTING_SIGNALS,
    pf.WEATHER_HAS_MISSING,
    'DROUGHT_ANOMALY_MODEL' AS MODEL_ID,
    'v1-synthetic-2024' AS RUN_ID,
    CURRENT_TIMESTAMP() AS SCORED_AT
FROM plot_features pf
LEFT JOIN anomaly_scores a ON pf.TEHSIL = a.TEHSIL AND pf.OBS_DATE = a.OBS_DATE;


-- 7b. Route result tables
CREATE OR REPLACE TABLE ANALYTICS.ROUTE_RUNS (
    RUN_ID VARCHAR(50), RUN_DATE DATE, TERRITORY VARCHAR(50), AGENT_TOKEN VARCHAR(20),
    TOTAL_STOPS NUMBER(38,0),
    TOTAL_DISTANCE_M FLOAT COMMENT 'Straight-line distance, NOT road distance',
    TOTAL_SERVICE_MIN NUMBER(38,0), TOTAL_ESTIMATED_AMOUNT FLOAT,
    PRIORITY_COVERAGE_PCT FLOAT, OBJECTIVE_VALUE FLOAT,
    SOLVER_METHOD VARCHAR(50), CONSTRAINT_VIOLATIONS NUMBER(38,0) DEFAULT 0,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE ANALYTICS.ROUTE_STOPS (
    RUN_ID VARCHAR(50), AGENT_TOKEN VARCHAR(20), STOP_NUMBER NUMBER(38,0),
    TASK_ID VARCHAR(20), ACCOUNT_TOKEN VARCHAR(20),
    CUSTOMER_LONGITUDE FLOAT, CUSTOMER_LATITUDE FLOAT,
    LEG_DISTANCE_M FLOAT COMMENT 'Straight-line distance from previous stop, NOT road distance',
    CUMULATIVE_DISTANCE_M FLOAT, EXPECTED_ARRIVAL_MIN NUMBER(38,0),
    SERVICE_DURATION_MIN NUMBER(38,0), TASK_PRIORITY NUMBER(38,0),
    ESTIMATED_AMOUNT_INR FLOAT
);

CREATE OR REPLACE TABLE ANALYTICS.UNASSIGNED_TASKS (
    RUN_ID VARCHAR(50), TASK_ID VARCHAR(20), ACCOUNT_TOKEN VARCHAR(20),
    TERRITORY VARCHAR(50), REASON VARCHAR(200),
    TASK_PRIORITY NUMBER(38,0), ESTIMATED_AMOUNT_INR FLOAT
);


-- 7c. Baseline comparison view
CREATE OR REPLACE VIEW ANALYTICS.ROUTE_BASELINE_COMPARISON AS
WITH stop_with_prev AS (
    SELECT rs.RUN_ID, rs.AGENT_TOKEN, rs.STOP_NUMBER,
        rs.CUSTOMER_LONGITUDE, rs.CUSTOMER_LATITUDE,
        COALESCE(LAG(rs.CUSTOMER_LONGITUDE) OVER (PARTITION BY rs.RUN_ID, rs.AGENT_TOKEN ORDER BY rs.TASK_ID), a.BASE_LONGITUDE) AS PREV_LON_NAIVE,
        COALESCE(LAG(rs.CUSTOMER_LATITUDE) OVER (PARTITION BY rs.RUN_ID, rs.AGENT_TOKEN ORDER BY rs.TASK_ID), a.BASE_LATITUDE) AS PREV_LAT_NAIVE
    FROM ANALYTICS.ROUTE_STOPS rs
    JOIN RAW.AGENTS a ON rs.AGENT_TOKEN = a.AGENT_TOKEN
),
naive AS (
    SELECT RUN_ID, AGENT_TOKEN,
        SUM(ST_DISTANCE(ST_MAKEPOINT(PREV_LON_NAIVE, PREV_LAT_NAIVE),
                        ST_MAKEPOINT(CUSTOMER_LONGITUDE, CUSTOMER_LATITUDE))) AS NAIVE_DISTANCE_M
    FROM stop_with_prev GROUP BY RUN_ID, AGENT_TOKEN
),
optimized AS (
    SELECT RUN_ID, TERRITORY, AGENT_TOKEN, TOTAL_STOPS, TOTAL_DISTANCE_M,
           TOTAL_SERVICE_MIN, PRIORITY_COVERAGE_PCT, SOLVER_METHOD
    FROM ANALYTICS.ROUTE_RUNS
)
SELECT o.TERRITORY, o.AGENT_TOKEN, o.TOTAL_STOPS,
    ROUND(o.TOTAL_DISTANCE_M / 1000, 1) AS OPTIMIZED_DIST_KM,
    ROUND(n.NAIVE_DISTANCE_M / 1000, 1) AS NAIVE_DIST_KM,
    ROUND((1 - o.TOTAL_DISTANCE_M / NULLIF(n.NAIVE_DISTANCE_M, 0)) * 100, 1) AS DISTANCE_SAVINGS_PCT,
    o.PRIORITY_COVERAGE_PCT, o.SOLVER_METHOD,
    'Straight-line distance proxy, NOT road distance' AS DISTANCE_NOTE
FROM optimized o JOIN naive n ON o.RUN_ID = n.RUN_ID AND o.AGENT_TOKEN = n.AGENT_TOKEN;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 8: STORED PROCEDURES                                            ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- 8a. Drought scoring: re-runs anomaly detection and rebuilds early warning
CREATE OR REPLACE PROCEDURE ML.RUN_DROUGHT_SCORING()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    CALL SARVAGRAM_DEMO.ML.DROUGHT_ANOMALY_MODEL!DETECT_ANOMALIES(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'SARVAGRAM_DEMO.ML.ANOMALY_DETECTION_V'),
        SERIES_COLNAME => 'SERIES_ID',
        TIMESTAMP_COLNAME => 'TIMESTAMP',
        TARGET_COLNAME => 'RAINFALL',
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    CREATE OR REPLACE TABLE SARVAGRAM_DEMO.ML.ANOMALY_RESULTS AS
    SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    LET row_count INT := (SELECT COUNT(*) FROM SARVAGRAM_DEMO.ML.ANOMALY_RESULTS);
    RETURN 'Anomaly scoring complete. ' || :row_count || ' detection rows written.';
END;
$$;


-- 8b. Route solver: nearest-neighbor + 2-opt local improvement heuristic
--     This is CUSTOM CODE, not a built-in Snowflake routing product.
CREATE OR REPLACE PROCEDURE ANALYTICS.GENERATE_ROUTES(RUN_DATE DATE)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import math
from datetime import datetime

def haversine_m(lon1, lat1, lon2, lat2):
    """Great-circle distance in meters. Straight-line, NOT road distance."""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def nearest_neighbor_route(agent, tasks):
    if not tasks:
        return [], []
    route = []
    remaining = list(tasks)
    cur_lon, cur_lat = agent['lon'], agent['lat']
    cum_dist = 0.0
    cum_time = 0
    while remaining and len(route) < agent['max_stops']:
        best_idx, best_score = -1, float('inf')
        for i, t in enumerate(remaining):
            dist = haversine_m(cur_lon, cur_lat, t['lon'], t['lat'])
            score = dist - (5 - t['priority']) * 2000
            if score < best_score:
                best_score = score
                best_idx = i
        chosen = remaining.pop(best_idx)
        leg = haversine_m(cur_lon, cur_lat, chosen['lon'], chosen['lat'])
        cum_dist += leg
        travel_min = (leg / 1000) / 30 * 60
        cum_time += travel_min + chosen['svc_min']
        route.append({
            'task_id': chosen['task_id'], 'account': chosen['account'],
            'lon': chosen['lon'], 'lat': chosen['lat'],
            'leg_dist': round(leg, 1), 'cum_dist': round(cum_dist, 1),
            'arrival_min': round(cum_time - chosen['svc_min']),
            'svc_min': chosen['svc_min'], 'priority': chosen['priority'],
            'amount': chosen['amount']
        })
        cur_lon, cur_lat = chosen['lon'], chosen['lat']
    return route, remaining

def two_opt_improve(route, agent):
    if len(route) < 3:
        return route
    improved = True
    while improved:
        improved = False
        for i in range(len(route) - 1):
            for j in range(i + 2, len(route)):
                p_lon = agent['lon'] if i == 0 else route[i-1]['lon']
                p_lat = agent['lat'] if i == 0 else route[i-1]['lat']
                d_before = haversine_m(p_lon, p_lat, route[i]['lon'], route[i]['lat'])
                d_after = haversine_m(p_lon, p_lat, route[j]['lon'], route[j]['lat'])
                if j < len(route) - 1:
                    d_before += haversine_m(route[j]['lon'], route[j]['lat'], route[j+1]['lon'], route[j+1]['lat'])
                    d_after += haversine_m(route[i]['lon'], route[i]['lat'], route[j+1]['lon'], route[j+1]['lat'])
                if d_after < d_before - 10:
                    route[i:j+1] = route[i:j+1][::-1]
                    improved = True
    cur_lon, cur_lat = agent['lon'], agent['lat']
    cd, ct = 0.0, 0
    for s in route:
        leg = haversine_m(cur_lon, cur_lat, s['lon'], s['lat'])
        cd += leg
        ct += (leg/1000)/30*60 + s['svc_min']
        s['leg_dist'] = round(leg, 1)
        s['cum_dist'] = round(cd, 1)
        s['arrival_min'] = round(ct - s['svc_min'])
        cur_lon, cur_lat = s['lon'], s['lat']
    return route

def main(session, run_date):
    run_id = f"RUN-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    agents_df = session.sql("""
        SELECT AGENT_TOKEN, AGENT_NAME, BASE_LONGITUDE, BASE_LATITUDE,
               TERRITORY, MAX_STOPS_PER_DAY
        FROM SARVAGRAM_DEMO.RAW.AGENTS WHERE IS_ACTIVE = TRUE
    """).collect()
    agents = {}
    for r in agents_df:
        agents[r['TERRITORY']] = {
            'token': r['AGENT_TOKEN'], 'name': r['AGENT_NAME'],
            'lon': float(r['BASE_LONGITUDE']), 'lat': float(r['BASE_LATITUDE']),
            'territory': r['TERRITORY'], 'max_stops': int(r['MAX_STOPS_PER_DAY'])
        }
    tasks_df = session.sql(f"""
        SELECT TASK_ID, ACCOUNT_TOKEN, CUSTOMER_LONGITUDE, CUSTOMER_LATITUDE,
               TERRITORY, SERVICE_DURATION_MIN, PRIORITY, ESTIMATED_AMOUNT_INR
        FROM SARVAGRAM_DEMO.RAW.COLLECTION_TASKS
        WHERE DUE_DATE = '{run_date}' AND TASK_STATUS = 'PENDING'
    """).collect()
    tasks_by_territory = {}
    for r in tasks_df:
        t = r['TERRITORY']
        if t not in tasks_by_territory:
            tasks_by_territory[t] = []
        tasks_by_territory[t].append({
            'task_id': r['TASK_ID'], 'account': r['ACCOUNT_TOKEN'],
            'lon': float(r['CUSTOMER_LONGITUDE']), 'lat': float(r['CUSTOMER_LATITUDE']),
            'svc_min': int(r['SERVICE_DURATION_MIN']),
            'priority': int(r['PRIORITY']), 'amount': float(r['ESTIMATED_AMOUNT_INR'])
        })
    all_stops, all_unassigned = [], []
    run_summaries = []
    for territory, agent in agents.items():
        territory_tasks = tasks_by_territory.get(territory, [])
        if not territory_tasks:
            continue
        route, unassigned = nearest_neighbor_route(agent, territory_tasks)
        route = two_opt_improve(route, agent)
        total_dist = route[-1]['cum_dist'] if route else 0
        total_svc = sum(s['svc_min'] for s in route)
        total_amt = sum(s['amount'] for s in route)
        p1_in_route = sum(1 for s in route if s['priority'] == 1)
        p1_total = sum(1 for t in territory_tasks if t['priority'] == 1)
        p1_cov = (p1_in_route / p1_total * 100) if p1_total > 0 else 100.0
        obj = total_dist + (100 - p1_cov) * 1000
        session.sql(f"""
            INSERT INTO SARVAGRAM_DEMO.ANALYTICS.ROUTE_RUNS
            (RUN_ID, RUN_DATE, TERRITORY, AGENT_TOKEN, TOTAL_STOPS, TOTAL_DISTANCE_M,
             TOTAL_SERVICE_MIN, TOTAL_ESTIMATED_AMOUNT, PRIORITY_COVERAGE_PCT,
             OBJECTIVE_VALUE, SOLVER_METHOD, CONSTRAINT_VIOLATIONS)
            VALUES ('{run_id}', '{run_date}', '{territory}', '{agent["token"]}',
                    {len(route)}, {total_dist}, {total_svc}, {total_amt},
                    {round(p1_cov, 1)}, {round(obj, 1)}, 'nearest_neighbor_2opt', 0)
        """).collect()
        for i, s in enumerate(route):
            session.sql(f"""
                INSERT INTO SARVAGRAM_DEMO.ANALYTICS.ROUTE_STOPS
                (RUN_ID, AGENT_TOKEN, STOP_NUMBER, TASK_ID, ACCOUNT_TOKEN,
                 CUSTOMER_LONGITUDE, CUSTOMER_LATITUDE, LEG_DISTANCE_M,
                 CUMULATIVE_DISTANCE_M, EXPECTED_ARRIVAL_MIN, SERVICE_DURATION_MIN,
                 TASK_PRIORITY, ESTIMATED_AMOUNT_INR)
                VALUES ('{run_id}', '{agent["token"]}', {i+1}, '{s["task_id"]}',
                        '{s["account"]}', {s["lon"]}, {s["lat"]}, {s["leg_dist"]},
                        {s["cum_dist"]}, {s["arrival_min"]}, {s["svc_min"]},
                        {s["priority"]}, {s["amount"]})
            """).collect()
            all_stops.append(s)
        for u in unassigned:
            reason = f"Exceeded max stops ({agent['max_stops']}) for agent {agent['token']}"
            session.sql(f"""
                INSERT INTO SARVAGRAM_DEMO.ANALYTICS.UNASSIGNED_TASKS
                (RUN_ID, TASK_ID, ACCOUNT_TOKEN, TERRITORY, REASON, TASK_PRIORITY, ESTIMATED_AMOUNT_INR)
                VALUES ('{run_id}', '{u["task_id"]}', '{u["account"]}', '{territory}',
                        '{reason}', {u["priority"]}, {u["amount"]})
            """).collect()
            all_unassigned.append(u)
        run_summaries.append(territory)
    return f"Run {run_id}: {len(all_stops)} stops across {len(run_summaries)} routes ({', '.join(run_summaries)}), {len(all_unassigned)} unassigned"
$$;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 9: STREAMLIT APPS + STAGE                                       ║
-- ║                                                                          ║
-- ║  Upload the Python files to the stage, then create the Streamlit objects. ║
-- ║  The two app files (sarvagram_drought_app.py, sarvagram_route_app.py)    ║
-- ║  must be uploaded separately — see the companion files.                   ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

CREATE OR REPLACE STAGE APP.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Streamlit app code for Sarvagram demo';

-- After uploading sarvagram_drought_app.py to @APP.STREAMLIT_STAGE/drought/:
--   PUT file:///path/to/sarvagram_drought_app.py @SARVAGRAM_DEMO.APP.STREAMLIT_STAGE/drought/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
CREATE OR REPLACE STREAMLIT APP.DROUGHT_EARLY_WARNING
    ROOT_LOCATION = '@SARVAGRAM_DEMO.APP.STREAMLIT_STAGE/drought'
    MAIN_FILE = 'sarvagram_drought_app.py'
    QUERY_WAREHOUSE = SARVAGRAM_WH
    TITLE = 'Sarvagram Drought Early Warning'
    COMMENT = 'Drought and crop-stress early warning dashboard with ML anomaly detection';

-- After uploading sarvagram_route_app.py to @APP.STREAMLIT_STAGE/route/:
--   PUT file:///path/to/sarvagram_route_app.py @SARVAGRAM_DEMO.APP.STREAMLIT_STAGE/route/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
CREATE OR REPLACE STREAMLIT APP.ROUTE_OPTIMIZATION
    ROOT_LOCATION = '@SARVAGRAM_DEMO.APP.STREAMLIT_STAGE/route'
    MAIN_FILE = 'sarvagram_route_app.py'
    QUERY_WAREHOUSE = SARVAGRAM_WH
    TITLE = 'Sarvagram Route Optimization'
    COMMENT = 'Collection agent route optimization with constrained nearest-neighbor solver';


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 10: GRANT VIEWER ACCESS + RUN INITIAL ROUTE GENERATION          ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE SARVAGRAM_VIEWER;
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS TO ROLE SARVAGRAM_VIEWER;

-- Generate initial route run
CALL ANALYTICS.GENERATE_ROUTES('2024-09-15');


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 11: VALIDATION QUERIES                                          ║
-- ║  Run these to confirm the demo is working correctly.                     ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- V1: Drought-injected areas should have anomalies; normal areas should not
SELECT TEHSIL,
    CASE WHEN TEHSIL IN ('Barmer','Balotra','Jaisalmer','Pokaran') THEN 'DROUGHT_INJECTED' ELSE 'NORMAL' END AS CATEGORY,
    COUNT(DISTINCT CASE WHEN RAINFALL_ANOMALY_FLAG = TRUE THEN AS_OF_DATE END) AS ANOMALY_DAYS,
    COUNT(DISTINCT CASE WHEN PRIORITY_TIER IN ('CRITICAL','HIGH') THEN PLOT_ID || AS_OF_DATE END) AS CRITICAL_HIGH_ALERTS
FROM ANALYTICS.PLOT_EARLY_WARNING
GROUP BY TEHSIL ORDER BY CATEGORY, TEHSIL;

-- V2: Every alert has contributing signals, model metadata, and freshness
SELECT PRIORITY_TIER, COUNT(*) AS total,
    SUM(CASE WHEN ARRAY_SIZE(CONTRIBUTING_SIGNALS) > 0 THEN 1 ELSE 0 END) AS has_signals,
    SUM(CASE WHEN MODEL_ID IS NOT NULL THEN 1 ELSE 0 END) AS has_model_id
FROM ANALYTICS.PLOT_EARLY_WARNING
WHERE PRIORITY_TIER IN ('CRITICAL','HIGH','MODERATE')
GROUP BY PRIORITY_TIER;

-- V3: All tasks assigned or have unassigned reason
WITH latest AS (SELECT MAX(RUN_ID) AS rid FROM ANALYTICS.ROUTE_RUNS)
SELECT
    (SELECT COUNT(*) FROM RAW.COLLECTION_TASKS WHERE DUE_DATE='2024-09-15' AND TASK_STATUS='PENDING') AS eligible,
    (SELECT COUNT(*) FROM ANALYTICS.ROUTE_STOPS WHERE RUN_ID=(SELECT rid FROM latest)) AS assigned,
    (SELECT COUNT(*) FROM ANALYTICS.UNASSIGNED_TASKS WHERE RUN_ID=(SELECT rid FROM latest)) AS unassigned;

-- V4: No constraint violations
SELECT TERRITORY, AGENT_TOKEN, TOTAL_STOPS, CONSTRAINT_VIOLATIONS
FROM ANALYTICS.ROUTE_RUNS
WHERE RUN_ID = (SELECT MAX(RUN_ID) FROM ANALYTICS.ROUTE_RUNS);

-- V5: Baseline comparison
SELECT * FROM ANALYTICS.ROUTE_BASELINE_COMPARISON ORDER BY TERRITORY;
