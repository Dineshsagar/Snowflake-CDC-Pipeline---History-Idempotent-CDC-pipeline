/* =========================================================
   01_setup.sql
   Warehouse, database, RAW / SILVER / GOLD / UTIL schemas, tables.
   Run everything in this file top to bottom. Snowsight worksheet.
   ========================================================= */

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS WH_CDC
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS AIRLINE_DL;
USE DATABASE AIRLINE_DL;

CREATE SCHEMA IF NOT EXISTS RAW     COMMENT = 'Landing. Current batch only. Truncate + load.';
CREATE SCHEMA IF NOT EXISTS SILVER  COMMENT = 'Current state, one row per PK, typed.';
CREATE SCHEMA IF NOT EXISTS GOLD    COMMENT = 'Business layer for BI + outbound.';
CREATE SCHEMA IF NOT EXISTS UTIL    COMMENT = 'Stage, file format, audit.';

USE WAREHOUSE WH_CDC;


/* =========================================================
   RAW  -  shape = exactly what Qlik writes.
   Everything VARCHAR. No casting, no rules. Just land it.
   ========================================================= */
USE SCHEMA AIRLINE_DL.RAW;

CREATE OR REPLACE TABLE RAW.BOOKING (
    BOOKING_ID          VARCHAR,
    PASSENGER_ID        VARCHAR,
    FLIGHT_NUMBER       VARCHAR,
    ORIGIN              VARCHAR,
    DESTINATION         VARCHAR,
    BOOKING_TS          VARCHAR,
    BOOKING_STATUS      VARCHAR,
    FARE_AMOUNT         VARCHAR,
    CURRENCY            VARCHAR,
    CHANNEL             VARCHAR,
    CHANGE_OPER         VARCHAR,   -- L=full load, I=insert, U=update, D=delete
    CHANGE_SEQ          VARCHAR,   -- ordering key from the source txn log
    CHANGE_TS           VARCHAR,
    SRC_FILE            VARCHAR,
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.RESERVATION (
    RESERVATION_ID      VARCHAR,
    BOOKING_ID          VARCHAR,
    SEAT_NUMBER         VARCHAR,
    CABIN_CLASS         VARCHAR,
    RESERVATION_STATUS  VARCHAR,
    CHECKED_IN_FLAG     VARCHAR,
    UPDATED_TS          VARCHAR,
    CHANGE_OPER         VARCHAR,
    CHANGE_SEQ          VARCHAR,
    CHANGE_TS           VARCHAR,
    SRC_FILE            VARCHAR,
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.CLAIM (
    CLAIM_ID            VARCHAR,
    BOOKING_ID          VARCHAR,
    CLAIM_TYPE          VARCHAR,
    CLAIM_STATUS        VARCHAR,
    CLAIM_AMOUNT        VARCHAR,
    FILED_TS            VARCHAR,
    SETTLED_TS          VARCHAR,
    CHANGE_OPER         VARCHAR,
    CHANGE_SEQ          VARCHAR,
    CHANGE_TS           VARCHAR,
    SRC_FILE            VARCHAR,
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


/* =========================================================
   SILVER  -  typed, one row per PK, soft deletes.
   ========================================================= */
USE SCHEMA AIRLINE_DL.SILVER;

CREATE OR REPLACE TABLE SILVER.BOOKING (
    BOOKING_ID       VARCHAR       NOT NULL,
    PASSENGER_ID     VARCHAR,
    FLIGHT_NUMBER    VARCHAR,
    ORIGIN           VARCHAR(3),
    DESTINATION      VARCHAR(3),
    BOOKING_TS       TIMESTAMP_NTZ,
    BOOKING_STATUS   VARCHAR,
    FARE_AMOUNT      NUMBER(12,2),
    CURRENCY         VARCHAR(3),
    CHANNEL          VARCHAR,
    IS_DELETED       BOOLEAN       DEFAULT FALSE,
    CHANGE_SEQ       NUMBER,
    UPDATED_AT       TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SILVER.RESERVATION (
    RESERVATION_ID     VARCHAR NOT NULL,
    BOOKING_ID         VARCHAR,
    SEAT_NUMBER        VARCHAR,
    CABIN_CLASS        VARCHAR,
    RESERVATION_STATUS VARCHAR,
    CHECKED_IN         BOOLEAN,
    IS_DELETED         BOOLEAN DEFAULT FALSE,
    CHANGE_SEQ         NUMBER,
    UPDATED_AT         TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SILVER.CLAIM (
    CLAIM_ID       VARCHAR NOT NULL,
    BOOKING_ID     VARCHAR,
    CLAIM_TYPE     VARCHAR,
    CLAIM_STATUS   VARCHAR,
    CLAIM_AMOUNT   NUMBER(12,2),
    FILED_TS       TIMESTAMP_NTZ,
    SETTLED_TS     TIMESTAMP_NTZ,
    IS_DELETED     BOOLEAN DEFAULT FALSE,
    CHANGE_SEQ     NUMBER,
    UPDATED_AT     TIMESTAMP_NTZ
);


/* =========================================================
   GOLD  -  business layer. Built in 04 (SQL) or by dbt.
   ========================================================= */
USE SCHEMA AIRLINE_DL.GOLD;

CREATE OR REPLACE TABLE GOLD.BOOKING_FACT (
    BOOKING_ID        VARCHAR,
    PASSENGER_ID      VARCHAR,
    FLIGHT_NUMBER     VARCHAR,
    ROUTE             VARCHAR,
    BOOKING_DATE      DATE,
    BOOKING_STATUS    VARCHAR,
    CHANNEL           VARCHAR,
    CABIN_CLASS       VARCHAR,
    CHECKED_IN        BOOLEAN,
    FARE_AMOUNT       NUMBER(12,2),
    CLAIM_COUNT       NUMBER,
    CLAIM_AMOUNT      NUMBER(12,2),
    NET_REVENUE       NUMBER(12,2),
    REVENUE_BAND      VARCHAR,
    IS_AT_RISK        BOOLEAN,
    LOAD_TS           TIMESTAMP_NTZ
);


/* =========================================================
   UTIL  -  run audit so every task writes a row we can query.
   ========================================================= */
USE SCHEMA AIRLINE_DL.UTIL;

CREATE OR REPLACE TABLE UTIL.PIPELINE_LOG (
    LOG_TS     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    STEP       VARCHAR,
    TARGET     VARCHAR,
    ROWS_AFFECTED NUMBER,
    NOTE       VARCHAR
);

SHOW TABLES IN DATABASE AIRLINE_DL;
