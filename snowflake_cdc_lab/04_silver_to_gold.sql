/* =========================================================
   04_silver_to_gold.sql
   SILVER --stream--> TASK --> SP (business logic) --> GOLD
   ========================================================= */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_CDC;
USE SCHEMA AIRLINE_DL.GOLD;

/* ---------- STREAM ----------
   NOT append-only this time. SILVER rows get UPDATED by the MERGE, and we
   care about those updates. A standard stream tracks insert + update + delete. */
CREATE OR REPLACE STREAM GOLD.STR_SILVER_BOOKING ON TABLE SILVER.BOOKING;

SELECT COUNT(*) FROM GOLD.STR_SILVER_BOOKING;


/* =========================================================
   THE BUSINESS LOGIC PROCEDURE

   Transformations applied here (this is what makes it GOLD, not SILVER):
     1. Drop soft-deleted and cancelled bookings  - reporting shows live revenue only
     2. Join reservation + claim onto booking     - one row per booking
     3. ROUTE          = ORIGIN-DESTINATION       - derived dimension
     4. NET_REVENUE    = fare - settled claims    - the actual business measure
     5. REVENUE_BAND   = HIGH / MEDIUM / LOW      - bucketing for the dashboard
     6. IS_AT_RISK     = open claim or not checked in - operational flag
   ========================================================= */

CREATE OR REPLACE PROCEDURE GOLD.SP_LOAD_GOLD()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  n NUMBER DEFAULT 0;
BEGIN

  -- Consume the stream so its offset advances, even though we rebuild from
  -- SILVER. The stream is the TRIGGER ("something changed"), not the source.
  LET changed NUMBER := (SELECT COUNT(*) FROM GOLD.STR_SILVER_BOOKING);

  CREATE OR REPLACE TEMPORARY TABLE _changed_keys AS
  SELECT DISTINCT BOOKING_ID FROM GOLD.STR_SILVER_BOOKING;

  MERGE INTO GOLD.BOOKING_FACT tgt
  USING (
      SELECT
          b.BOOKING_ID,
          b.PASSENGER_ID,
          b.FLIGHT_NUMBER,
          b.ORIGIN || '-' || b.DESTINATION            AS ROUTE,
          DATE(b.BOOKING_TS)                          AS BOOKING_DATE,
          b.BOOKING_STATUS,
          b.CHANNEL,
          r.CABIN_CLASS,
          NVL(r.CHECKED_IN, FALSE)                    AS CHECKED_IN,
          b.FARE_AMOUNT,
          NVL(c.CLAIM_CNT, 0)                         AS CLAIM_COUNT,
          NVL(c.CLAIM_AMT, 0)                         AS CLAIM_AMOUNT,
          b.FARE_AMOUNT - NVL(c.SETTLED_AMT, 0)       AS NET_REVENUE,
          CASE
              WHEN b.FARE_AMOUNT >= 1200 THEN 'HIGH'
              WHEN b.FARE_AMOUNT >=  900 THEN 'MEDIUM'
              ELSE 'LOW'
          END                                         AS REVENUE_BAND,
          (NVL(c.OPEN_CNT,0) > 0 OR NOT NVL(r.CHECKED_IN,FALSE)) AS IS_AT_RISK,
          CURRENT_TIMESTAMP()                         AS LOAD_TS
      FROM SILVER.BOOKING b
      LEFT JOIN SILVER.RESERVATION r
             ON r.BOOKING_ID = b.BOOKING_ID AND NOT r.IS_DELETED
      LEFT JOIN (
          SELECT BOOKING_ID,
                 COUNT(*)                                          AS CLAIM_CNT,
                 SUM(CLAIM_AMOUNT)                                 AS CLAIM_AMT,
                 SUM(IFF(CLAIM_STATUS='SETTLED', CLAIM_AMOUNT, 0)) AS SETTLED_AMT,
                 COUNT_IF(CLAIM_STATUS='OPEN')                     AS OPEN_CNT
          FROM SILVER.CLAIM WHERE NOT IS_DELETED
          GROUP BY BOOKING_ID
      ) c ON c.BOOKING_ID = b.BOOKING_ID
      WHERE NOT b.IS_DELETED
        AND b.BOOKING_STATUS <> 'CANCELLED'
        AND b.BOOKING_ID IN (SELECT BOOKING_ID FROM _changed_keys)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY b.BOOKING_ID ORDER BY b.CHANGE_SEQ DESC) = 1
  ) src
  ON tgt.BOOKING_ID = src.BOOKING_ID
  WHEN MATCHED THEN UPDATE SET
        tgt.PASSENGER_ID = src.PASSENGER_ID, tgt.FLIGHT_NUMBER = src.FLIGHT_NUMBER,
        tgt.ROUTE = src.ROUTE, tgt.BOOKING_DATE = src.BOOKING_DATE,
        tgt.BOOKING_STATUS = src.BOOKING_STATUS, tgt.CHANNEL = src.CHANNEL,
        tgt.CABIN_CLASS = src.CABIN_CLASS, tgt.CHECKED_IN = src.CHECKED_IN,
        tgt.FARE_AMOUNT = src.FARE_AMOUNT, tgt.CLAIM_COUNT = src.CLAIM_COUNT,
        tgt.CLAIM_AMOUNT = src.CLAIM_AMOUNT, tgt.NET_REVENUE = src.NET_REVENUE,
        tgt.REVENUE_BAND = src.REVENUE_BAND, tgt.IS_AT_RISK = src.IS_AT_RISK,
        tgt.LOAD_TS = src.LOAD_TS
  WHEN NOT MATCHED THEN INSERT VALUES
       (src.BOOKING_ID, src.PASSENGER_ID, src.FLIGHT_NUMBER, src.ROUTE, src.BOOKING_DATE,
        src.BOOKING_STATUS, src.CHANNEL, src.CABIN_CLASS, src.CHECKED_IN, src.FARE_AMOUNT,
        src.CLAIM_COUNT, src.CLAIM_AMOUNT, src.NET_REVENUE, src.REVENUE_BAND,
        src.IS_AT_RISK, src.LOAD_TS);

  n := SQLROWCOUNT;

  -- a booking that became deleted/cancelled must leave the fact table
  DELETE FROM GOLD.BOOKING_FACT
  WHERE BOOKING_ID IN (
      SELECT BOOKING_ID FROM SILVER.BOOKING
      WHERE IS_DELETED OR BOOKING_STATUS = 'CANCELLED'
  );

  INSERT INTO UTIL.PIPELINE_LOG (STEP, TARGET, ROWS_AFFECTED, NOTE)
  VALUES ('SILVER->GOLD', 'GOLD.BOOKING_FACT', :n, 'changed keys: ' || :changed);

  RETURN 'GOLD loaded. rows merged = ' || :n;
END;
$$;

CALL GOLD.SP_LOAD_GOLD();

SELECT * FROM GOLD.BOOKING_FACT ORDER BY BOOKING_ID;

-- BK1002 and BK1007 are CANCELLED, BK1005 deleted -> none should appear
SELECT BOOKING_ID FROM GOLD.BOOKING_FACT WHERE BOOKING_ID IN ('BK1002','BK1005','BK1007');


/* ---------- A small aggregate for the dashboard ---------- */
CREATE OR REPLACE VIEW GOLD.V_DAILY_ROUTE AS
SELECT
    BOOKING_DATE,
    ROUTE,
    CHANNEL,
    COUNT(*)                AS BOOKINGS,
    SUM(FARE_AMOUNT)        AS GROSS_REVENUE,
    SUM(NET_REVENUE)        AS NET_REVENUE,
    SUM(CLAIM_AMOUNT)       AS CLAIM_EXPOSURE,
    COUNT_IF(IS_AT_RISK)    AS AT_RISK_BOOKINGS
FROM GOLD.BOOKING_FACT
GROUP BY 1,2,3;

SELECT * FROM GOLD.V_DAILY_ROUTE ORDER BY BOOKING_DATE, ROUTE;


/* =========================================================
   TASK - chained AFTER the silver task, so it's one DAG:
        TSK_LOAD_SILVER  ->  TSK_LOAD_GOLD  ->  TSK_UNLOAD (in 05)
   ========================================================= */
CREATE OR REPLACE TASK GOLD.TSK_LOAD_GOLD
  WAREHOUSE = WH_CDC
  AFTER SILVER.TSK_LOAD_SILVER
AS
  CALL GOLD.SP_LOAD_GOLD();

-- children resume first, root last. Root suspended for now.
ALTER TASK GOLD.TSK_LOAD_GOLD RESUME;

SHOW TASKS IN DATABASE AIRLINE_DL;
