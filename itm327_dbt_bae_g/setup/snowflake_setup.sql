-- =============================================================
-- ITM327 dbt Project — Snowflake Setup
-- Run this once in Snowflake before your first dbt run.
-- Replace LASTN_FI with your actual name prefix (e.g., SMITHJ).
-- =============================================================

-- -------------------------------------------------------
-- 1. Use your role and warehouse
-- -------------------------------------------------------
USE ROLE TRAINING_ROLE;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE SNOWBEARAIR_DB;

-- -------------------------------------------------------
-- 2. Create DEV and PROD output schemas for dbt
--    (RAW schema already exists — your Airflow DAGs write there)
-- -------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS SNOWBEARAIR_DB.DEV;
CREATE SCHEMA IF NOT EXISTS SNOWBEARAIR_DB.PROD;

-- -------------------------------------------------------
-- 3. Verify your raw tables are present
--    (These are loaded by your Airflow pipelines)
-- -------------------------------------------------------
SHOW TABLES IN SCHEMA SNOWBEARAIR_DB.RAW;

-- Expected: you should see tables named like
--   LASTN_FI_STOCKS, LASTN_FI_NEWS, LASTN_FI_WEATHER

-- -------------------------------------------------------
-- 4. Grant permissions for dbt to write to DEV and PROD
-- -------------------------------------------------------
GRANT USAGE  ON SCHEMA SNOWBEARAIR_DB.DEV  TO ROLE TRAINING_ROLE;
GRANT CREATE TABLE ON SCHEMA SNOWBEARAIR_DB.DEV  TO ROLE TRAINING_ROLE;
GRANT CREATE VIEW  ON SCHEMA SNOWBEARAIR_DB.DEV  TO ROLE TRAINING_ROLE;

GRANT USAGE  ON SCHEMA SNOWBEARAIR_DB.PROD TO ROLE TRAINING_ROLE;
GRANT CREATE TABLE ON SCHEMA SNOWBEARAIR_DB.PROD TO ROLE TRAINING_ROLE;
GRANT CREATE VIEW  ON SCHEMA SNOWBEARAIR_DB.PROD TO ROLE TRAINING_ROLE;

-- -------------------------------------------------------
-- 5. (Optional) Enable query logging for observability
-- -------------------------------------------------------
ALTER SESSION SET QUERY_TAG = 'dbt_itm327';
