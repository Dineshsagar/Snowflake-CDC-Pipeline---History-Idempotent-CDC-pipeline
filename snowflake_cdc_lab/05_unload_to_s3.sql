/* =========================================================
   05_unload_to_s3.sql
   GOLD -> S3 (outbound feed for downstream consumers)
   ========================================================= */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_CDC;
USE SCHEMA AIRLINE_DL.UTIL;

-- Outbound stage. In prod this is an external stage on the outbound S3 bucket.
CREATE OR REPLACE STAGE UTIL.STG_OUTBOUND
  FILE_FORMAT = UTIL.FF_CSV
  ENCRYPTION  = (TYPE = 'SNOWFLAKE_SSE')
  DIRECTORY   = (ENABLE = TRUE);

CREATE OR REPLACE FILE FORMAT UTIL.FF_CSV_OUT
  TYPE = CSV
  COMPRESSION = GZIP
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ();          -- write real empty strings, not the literal \N


/* ---------- THE UNLOAD ---------- */
COPY INTO @UTIL.STG_OUTBOUND/booking_fact/
FROM (
    SELECT BOOKING_ID, PASSENGER_ID, FLIGHT_NUMBER, ROUTE, BOOKING_DATE,
           BOOKING_STATUS, CHANNEL, CABIN_CLASS, CHECKED_IN, FARE_AMOUNT,
           CLAIM_COUNT, CLAIM_AMOUNT, NET_REVENUE, REVENUE_BAND, IS_AT_RISK
    FROM GOLD.BOOKING_FACT
)
FILE_FORMAT = (FORMAT_NAME = UTIL.FF_CSV_OUT)
HEADER = TRUE
OVERWRITE = TRUE
SINGLE = FALSE                 -- let Snowflake parallelise into multiple files
MAX_FILE_SIZE = 16000000       -- 16MB chunks
INCLUDE_QUERY_ID = TRUE;       -- unique filenames, no accidental overwrite

LIST @UTIL.STG_OUTBOUND/booking_fact/;

-- read it back to prove it wrote correctly
SELECT $1, $2, $4, $13
FROM @UTIL.STG_OUTBOUND/booking_fact/ (FILE_FORMAT => UTIL.FF_CSV)
LIMIT 5;


/* ---------- Wrap it in a proc so a task can call it ---------- */
CREATE OR REPLACE PROCEDURE UTIL.SP_UNLOAD_GOLD()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  COPY INTO @UTIL.STG_OUTBOUND/booking_fact/
  FROM (
      SELECT BOOKING_ID, PASSENGER_ID, FLIGHT_NUMBER, ROUTE, BOOKING_DATE,
             BOOKING_STATUS, CHANNEL, CABIN_CLASS, CHECKED_IN, FARE_AMOUNT,
             CLAIM_COUNT, CLAIM_AMOUNT, NET_REVENUE, REVENUE_BAND, IS_AT_RISK
      FROM GOLD.BOOKING_FACT
  )
  FILE_FORMAT = (FORMAT_NAME = UTIL.FF_CSV_OUT)
  HEADER = TRUE OVERWRITE = TRUE INCLUDE_QUERY_ID = TRUE;

  INSERT INTO UTIL.PIPELINE_LOG (STEP, TARGET, ROWS_AFFECTED, NOTE)
  SELECT 'GOLD->S3', '@STG_OUTBOUND/booking_fact/', COUNT(*), 'unload ok'
  FROM GOLD.BOOKING_FACT;

  RETURN 'unloaded';
END;
$$;

CALL UTIL.SP_UNLOAD_GOLD();


/* ---------- Final link in the DAG ---------- */
CREATE OR REPLACE TASK UTIL.TSK_UNLOAD
  WAREHOUSE = WH_CDC
  AFTER GOLD.TSK_LOAD_GOLD
AS
  CALL UTIL.SP_UNLOAD_GOLD();

ALTER TASK UTIL.TSK_UNLOAD RESUME;

-- Now the whole DAG is armed. Root task last:
-- ALTER TASK SILVER.TSK_LOAD_SILVER RESUME;

SELECT SYSTEM$TASK_DEPENDENTS_ENABLED('AIRLINE_DL.SILVER.TSK_LOAD_SILVER');


/* ---------- REAL PROJECT: unload to actual S3 ----------
COPY INTO 's3://airline-outbound-prod/booking_fact/dt=2026-08-30/'
FROM (SELECT * FROM GOLD.BOOKING_FACT)
STORAGE_INTEGRATION = S3_INT
FILE_FORMAT = (TYPE = PARQUET)
HEADER = TRUE;

Partition the output by date so downstream can pick up just today's slice:
COPY INTO 's3://airline-outbound-prod/booking_fact/'
FROM (SELECT *, BOOKING_DATE AS dt FROM GOLD.BOOKING_FACT)
PARTITION BY ('dt=' || TO_VARCHAR(dt))
STORAGE_INTEGRATION = S3_INT FILE_FORMAT = (TYPE = PARQUET);
-------------------------------------------------------- */
