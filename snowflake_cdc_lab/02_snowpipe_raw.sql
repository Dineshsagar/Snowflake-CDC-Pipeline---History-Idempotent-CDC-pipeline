/* =========================================================
   02_snowpipe_raw.sql
   S3 -> Snowpipe -> RAW
   ========================================================= */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_CDC;
USE SCHEMA AIRLINE_DL.UTIL;

CREATE OR REPLACE FILE FORMAT UTIL.FF_CSV
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE;

/* Internal stage = our stand-in for the S3 bucket.
   SNOWFLAKE_SSE is required or Snowsight hides the "+ Files" upload button. */
CREATE OR REPLACE STAGE UTIL.STG_LANDING
  FILE_FORMAT = UTIL.FF_CSV
  ENCRYPTION  = (TYPE = 'SNOWFLAKE_SSE')
  DIRECTORY   = (ENABLE = TRUE);

/* ---- REAL PROJECT: the S3 version. Explain, don't run. ----
CREATE STORAGE INTEGRATION S3_INT
  TYPE = EXTERNAL_STAGE  STORAGE_PROVIDER = 'S3'  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://airline-cdc-landing-prod/qlik/');

DESC INTEGRATION S3_INT;   -- gives IAM user ARN + external id for the trust policy

CREATE STAGE UTIL.STG_LANDING
  STORAGE_INTEGRATION = S3_INT
  URL = 's3://airline-cdc-landing-prod/qlik/'
  FILE_FORMAT = UTIL.FF_PARQUET;

The PIPE never names the bucket - it points at the STAGE, and the stage holds the URL.
The pipe learns a file arrived because S3 pushes an event to an SQS queue that
Snowflake owns:  SHOW PIPES;  ->  copy notification_channel  ->  S3 bucket >
Properties > Event notifications > s3:ObjectCreated:* > SQS > paste ARN.
------------------------------------------------------------ */



LIST @UTIL.STG_LANDING;     -- expect 6 files


/* =========================================================
   PIPES.  One per table. They read BOTH fullload/ and cdc/
   because the transformation is identical - a full-load row is just
   a change record with oper = 'L'.

   AUTO_INGEST = FALSE only because an internal stage has no S3 events.
   In prod this is TRUE and nobody ever calls REFRESH.
   ========================================================= */
USE SCHEMA AIRLINE_DL.RAW;

CREATE OR REPLACE PIPE RAW.PIPE_BOOKING AUTO_INGEST = FALSE AS
COPY INTO RAW.BOOKING
  (BOOKING_ID, PASSENGER_ID, FLIGHT_NUMBER, ORIGIN, DESTINATION, BOOKING_TS,
   BOOKING_STATUS, FARE_AMOUNT, CURRENCY, CHANNEL,
   CHANGE_OPER, CHANGE_SEQ, CHANGE_TS, SRC_FILE)
FROM (
  SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, METADATA$FILENAME
  FROM @UTIL.STG_LANDING
)
PATTERN = '.*booking.*[.]csv'
FILE_FORMAT = (FORMAT_NAME = UTIL.FF_CSV);

CREATE OR REPLACE PIPE RAW.PIPE_RESERVATION AUTO_INGEST = FALSE AS
COPY INTO RAW.RESERVATION
  (RESERVATION_ID, BOOKING_ID, SEAT_NUMBER, CABIN_CLASS, RESERVATION_STATUS,
   CHECKED_IN_FLAG, UPDATED_TS, CHANGE_OPER, CHANGE_SEQ, CHANGE_TS, SRC_FILE)
FROM (
  SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10, METADATA$FILENAME
  FROM @UTIL.STG_LANDING
)
PATTERN = '.*reservation.*[.]csv'
FILE_FORMAT = (FORMAT_NAME = UTIL.FF_CSV);

CREATE OR REPLACE PIPE RAW.PIPE_CLAIM AUTO_INGEST = FALSE AS
COPY INTO RAW.CLAIM
  (CLAIM_ID, BOOKING_ID, CLAIM_TYPE, CLAIM_STATUS, CLAIM_AMOUNT, FILED_TS, SETTLED_TS,
   CHANGE_OPER, CHANGE_SEQ, CHANGE_TS, SRC_FILE)
FROM (
  SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10, METADATA$FILENAME
  FROM @UTIL.STG_LANDING
)
PATTERN = '.*claim.*[.]csv'
FILE_FORMAT = (FORMAT_NAME = UTIL.FF_CSV);


/* ---------- LOAD 1: the one-time FULL LOAD ---------- */
ALTER PIPE RAW.PIPE_BOOKING     REFRESH PREFIX = 'fullload/';
ALTER PIPE RAW.PIPE_RESERVATION REFRESH PREFIX = 'fullload/';
ALTER PIPE RAW.PIPE_CLAIM       REFRESH PREFIX = 'fullload/';

-- wait ~30-60s
SELECT SYSTEM$PIPE_STATUS('RAW.PIPE_BOOKING');

SELECT 'BOOKING' tbl, COUNT(*) FROM RAW.BOOKING
UNION ALL SELECT 'RESERVATION', COUNT(*) FROM RAW.RESERVATION
UNION ALL SELECT 'CLAIM', COUNT(*) FROM RAW.CLAIM;
-- expect 10 / 10 / 5, all with CHANGE_OPER = 'L'

SELECT CHANGE_OPER, COUNT(*) FROM RAW.BOOKING GROUP BY 1;

/* STOP HERE and go run 03_raw_to_silver.sql.
   The hourly CDC batch is loaded there, after the stream exists -
   otherwise the stream is created after the rows land and sees nothing. */
