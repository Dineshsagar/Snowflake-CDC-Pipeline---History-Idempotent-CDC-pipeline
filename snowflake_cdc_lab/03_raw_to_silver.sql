/* =========================================================
   03_raw_to_silver.sql
   RAW --stream--> TASK --> SP (MERGE on PK) --> SILVER, then TRUNCATE RAW
   This is the heart of the pipeline.
   ========================================================= */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_CDC;
USE SCHEMA AIRLINE_DL.SILVER;





Test fpr streams:  SELECT * FROM SILVER.STR_BOOKING;
/* ---------- STREAMS ----------
   APPEND_ONLY = TRUE is MANDATORY here, not a preference.
   RAW gets truncated after every batch. A standard stream would report those
   truncated rows as DELETEs and feed them straight into the MERGE.
   Append-only tracks inserts only, so the truncate is invisible to it. */

CREATE OR REPLACE STREAM SILVER.STR_BOOKING     ON TABLE RAW.BOOKING     APPEND_ONLY = TRUE;
CREATE OR REPLACE STREAM SILVER.STR_RESERVATION ON TABLE RAW.RESERVATION APPEND_ONLY = TRUE;
CREATE OR REPLACE STREAM SILVER.STR_CLAIM       ON TABLE RAW.CLAIM       APPEND_ONLY = TRUE;

/* A stream starts from the moment it is created. The full load rows landed in RAW
   in step 02, BEFORE these streams existed, so the streams are empty right now: */
SELECT COUNT(*) AS should_be_zero FROM SILVER.STR_BOOKING;

/* Re-land the full load so the streams can see it.
   ALTER PIPE ... REFRESH would do nothing here - Snowpipe remembers file names for
   14 days and skips them. So we truncate and COPY with FORCE = TRUE instead.
   TALK: this is exactly the "why did my re-uploaded file not load" incident. */
TRUNCATE TABLE RAW.BOOKING;
TRUNCATE TABLE RAW.RESERVATION;
TRUNCATE TABLE RAW.CLAIM;

COPY INTO RAW.BOOKING
  (BOOKING_ID, PASSENGER_ID, FLIGHT_NUMBER, ORIGIN, DESTINATION, BOOKING_TS,
   BOOKING_STATUS, FARE_AMOUNT, CURRENCY, CHANNEL, CHANGE_OPER, CHANGE_SEQ, CHANGE_TS, SRC_FILE)
FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, METADATA$FILENAME
      FROM @UTIL.STG_LANDING/fullload/)
PATTERN = '.*booking.*[.]csv' FILE_FORMAT = (FORMAT_NAME = UTIL.FF_CSV) FORCE = TRUE;

COPY INTO RAW.RESERVATION
  (RESERVATION_ID, BOOKING_ID, SEAT_NUMBER, CABIN_CLASS, RESERVATION_STATUS,
   CHECKED_IN_FLAG, UPDATED_TS, CHANGE_OPER, CHANGE_SEQ, CHANGE_TS, SRC_FILE)
FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10, METADATA$FILENAME
      FROM @UTIL.STG_LANDING/fullload/)
PATTERN = '.*reservation.*[.]csv' FILE_FORMAT = (FORMAT_NAME = UTIL.FF_CSV) FORCE = TRUE;

COPY INTO RAW.CLAIM
  (CLAIM_ID, BOOKING_ID, CLAIM_TYPE, CLAIM_STATUS, CLAIM_AMOUNT, FILED_TS, SETTLED_TS,
   CHANGE_OPER, CHANGE_SEQ, CHANGE_TS, SRC_FILE)
FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10, METADATA$FILENAME
      FROM @UTIL.STG_LANDING/fullload/)
PATTERN = '.*claim.*[.]csv' FILE_FORMAT = (FORMAT_NAME = UTIL.FF_CSV) FORCE = TRUE;

SELECT COUNT(*) AS now_10 FROM SILVER.STR_BOOKING;   -- the stream can see them now


/* =========================================================
   THE STORED PROCEDURE
   MERGE ... ON PK: update when matched, insert when not.
   Then TRUNCATE RAW - in the SAME procedure, AFTER the stream is consumed.
   ========================================================= */

CREATE OR REPLACE PROCEDURE SILVER.SP_LOAD_SILVER()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  n_book NUMBER DEFAULT 0;
  n_resv NUMBER DEFAULT 0;
  n_clam NUMBER DEFAULT 0;
BEGIN

  -- ---------------- BOOKING ----------------
  MERGE INTO SILVER.BOOKING tgt
  USING (
      -- one row per PK. Latest CHANGE_SEQ wins.
      -- Without this the MERGE fails: "duplicate row detected during DML action"
      SELECT * FROM SILVER.STR_BOOKING
      QUALIFY ROW_NUMBER() OVER (PARTITION BY BOOKING_ID
                                 ORDER BY TRY_TO_NUMBER(CHANGE_SEQ) DESC) = 1
  ) src
  ON tgt.BOOKING_ID = src.BOOKING_ID
  WHEN MATCHED THEN UPDATE SET
        tgt.PASSENGER_ID   = src.PASSENGER_ID,
        tgt.FLIGHT_NUMBER  = src.FLIGHT_NUMBER,
        tgt.ORIGIN         = UPPER(TRIM(src.ORIGIN)),
        tgt.DESTINATION    = UPPER(TRIM(src.DESTINATION)),
        tgt.BOOKING_TS     = TRY_TO_TIMESTAMP_NTZ(src.BOOKING_TS),
        tgt.BOOKING_STATUS = src.BOOKING_STATUS,
        tgt.FARE_AMOUNT    = TRY_TO_NUMBER(src.FARE_AMOUNT, 12, 2),
        tgt.CURRENCY       = NVL(src.CURRENCY, 'USD'),
        tgt.CHANNEL        = src.CHANNEL,
        tgt.IS_DELETED     = (src.CHANGE_OPER = 'D'),
        tgt.CHANGE_SEQ     = TRY_TO_NUMBER(src.CHANGE_SEQ),
        tgt.UPDATED_AT     = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT
       (BOOKING_ID, PASSENGER_ID, FLIGHT_NUMBER, ORIGIN, DESTINATION, BOOKING_TS,
        BOOKING_STATUS, FARE_AMOUNT, CURRENCY, CHANNEL, IS_DELETED, CHANGE_SEQ, UPDATED_AT)
       VALUES
       (src.BOOKING_ID, src.PASSENGER_ID, src.FLIGHT_NUMBER,
        UPPER(TRIM(src.ORIGIN)), UPPER(TRIM(src.DESTINATION)),
        TRY_TO_TIMESTAMP_NTZ(src.BOOKING_TS), src.BOOKING_STATUS,
        TRY_TO_NUMBER(src.FARE_AMOUNT, 12, 2), NVL(src.CURRENCY,'USD'), src.CHANNEL,
        (src.CHANGE_OPER = 'D'), TRY_TO_NUMBER(src.CHANGE_SEQ), CURRENT_TIMESTAMP());

  n_book := SQLROWCOUNT;

  -- ---------------- RESERVATION ----------------
  MERGE INTO SILVER.RESERVATION tgt
  USING (
      SELECT * FROM SILVER.STR_RESERVATION
      QUALIFY ROW_NUMBER() OVER (PARTITION BY RESERVATION_ID
                                 ORDER BY TRY_TO_NUMBER(CHANGE_SEQ) DESC) = 1
  ) src
  ON tgt.RESERVATION_ID = src.RESERVATION_ID
  WHEN MATCHED THEN UPDATE SET
        tgt.BOOKING_ID         = src.BOOKING_ID,
        tgt.SEAT_NUMBER        = src.SEAT_NUMBER,
        tgt.CABIN_CLASS        = src.CABIN_CLASS,
        tgt.RESERVATION_STATUS = src.RESERVATION_STATUS,
        tgt.CHECKED_IN         = (UPPER(src.CHECKED_IN_FLAG) = 'Y'),
        tgt.IS_DELETED         = (src.CHANGE_OPER = 'D'),
        tgt.CHANGE_SEQ         = TRY_TO_NUMBER(src.CHANGE_SEQ),
        tgt.UPDATED_AT         = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT
       (RESERVATION_ID, BOOKING_ID, SEAT_NUMBER, CABIN_CLASS, RESERVATION_STATUS,
        CHECKED_IN, IS_DELETED, CHANGE_SEQ, UPDATED_AT)
       VALUES
       (src.RESERVATION_ID, src.BOOKING_ID, src.SEAT_NUMBER, src.CABIN_CLASS,
        src.RESERVATION_STATUS, (UPPER(src.CHECKED_IN_FLAG) = 'Y'),
        (src.CHANGE_OPER = 'D'), TRY_TO_NUMBER(src.CHANGE_SEQ), CURRENT_TIMESTAMP());

  n_resv := SQLROWCOUNT;

  -- ---------------- CLAIM ----------------
  MERGE INTO SILVER.CLAIM tgt
  USING (
      SELECT * FROM SILVER.STR_CLAIM
      QUALIFY ROW_NUMBER() OVER (PARTITION BY CLAIM_ID
                                 ORDER BY TRY_TO_NUMBER(CHANGE_SEQ) DESC) = 1
  ) src
  ON tgt.CLAIM_ID = src.CLAIM_ID
  WHEN MATCHED THEN UPDATE SET
        tgt.BOOKING_ID   = src.BOOKING_ID,
        tgt.CLAIM_TYPE   = src.CLAIM_TYPE,
        tgt.CLAIM_STATUS = src.CLAIM_STATUS,
        tgt.CLAIM_AMOUNT = TRY_TO_NUMBER(src.CLAIM_AMOUNT, 12, 2),
        tgt.FILED_TS     = TRY_TO_TIMESTAMP_NTZ(src.FILED_TS),
        tgt.SETTLED_TS   = TRY_TO_TIMESTAMP_NTZ(src.SETTLED_TS),
        tgt.IS_DELETED   = (src.CHANGE_OPER = 'D'),
        tgt.CHANGE_SEQ   = TRY_TO_NUMBER(src.CHANGE_SEQ),
        tgt.UPDATED_AT   = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT
       (CLAIM_ID, BOOKING_ID, CLAIM_TYPE, CLAIM_STATUS, CLAIM_AMOUNT,
        FILED_TS, SETTLED_TS, IS_DELETED, CHANGE_SEQ, UPDATED_AT)
       VALUES
       (src.CLAIM_ID, src.BOOKING_ID, src.CLAIM_TYPE, src.CLAIM_STATUS,
        TRY_TO_NUMBER(src.CLAIM_AMOUNT, 12, 2),
        TRY_TO_TIMESTAMP_NTZ(src.FILED_TS), TRY_TO_TIMESTAMP_NTZ(src.SETTLED_TS),
        (src.CHANGE_OPER = 'D'), TRY_TO_NUMBER(src.CHANGE_SEQ), CURRENT_TIMESTAMP());

  n_clam := SQLROWCOUNT;

  -- ---------------- RAW housekeeping ----------------
  -- Safe ONLY because the three MERGEs above already consumed the streams.
  TRUNCATE TABLE RAW.BOOKING;
  TRUNCATE TABLE RAW.RESERVATION;
  TRUNCATE TABLE RAW.CLAIM;

  INSERT INTO UTIL.PIPELINE_LOG (STEP, TARGET, ROWS_AFFECTED, NOTE)
  VALUES ('RAW->SILVER', 'SILVER.*', :n_book + :n_resv + :n_clam, 'raw truncated');

  RETURN 'SILVER loaded. booking=' || :n_book ||
         ' reservation=' || :n_resv || ' claim=' || :n_clam;
END;
$$;


/* ---------- Run it for the full load ---------- */
CALL SILVER.SP_LOAD_SILVER();

SELECT * FROM SILVER.BOOKING ORDER BY BOOKING_ID;
SELECT COUNT(*) AS raw_rows_left FROM RAW.BOOKING;        -- 0, we truncated
SELECT COUNT(*) AS stream_rows_left FROM SILVER.STR_BOOKING; -- 0, offset advanced


/* =========================================================
   NOW THE HOURLY CDC BATCH - this is the part to demo slowly
   ========================================================= */
ALTER PIPE RAW.PIPE_BOOKING     REFRESH PREFIX = 'cdc/';
ALTER PIPE RAW.PIPE_RESERVATION REFRESH PREFIX = 'cdc/';
ALTER PIPE RAW.PIPE_CLAIM       REFRESH PREFIX = 'cdc/';

-- wait ~30-60s
SELECT CHANGE_OPER, COUNT(*) FROM RAW.BOOKING GROUP BY 1;   -- I / U / D
SELECT COUNT(*) FROM SILVER.STR_BOOKING;                    -- stream has the batch

/* *** THE TEACHING MOMENT ***
   BK1011 appears TWICE in this batch (an insert then an update).
   Run this MERGE without the QUALIFY and it fails with
   "Duplicate row detected during DML action". Try it, then fix it.
   This is the single most asked CDC interview question. */

CALL SILVER.SP_LOAD_SILVER();

-- BK1005 was deleted at source -> soft deleted here
SELECT BOOKING_ID, BOOKING_STATUS, FARE_AMOUNT, IS_DELETED, CHANGE_SEQ
FROM SILVER.BOOKING ORDER BY BOOKING_ID;

-- BK1011 got 910.00 (the later event), not 880.00. Proves ordering by CHANGE_SEQ.
SELECT * FROM SILVER.BOOKING WHERE BOOKING_ID = 'BK1011';


/* =========================================================
   THE TASK  -  TRIGGERED, not scheduled.

   A triggered task has NO SCHEDULE clause. It only has WHEN.
   Snowflake polls the stream internally and fires the task as soon as
   data appears - so the moment Snowpipe lands a CDC batch in RAW, this runs.

   Scheduled task            vs   Triggered task
   ----------------------------   -------------------------------------
   wakes on a timer                wakes when the stream has data
   fixed latency (up to 1 hour)    latency ~ seconds to a minute
   evaluates WHEN, usually skips   never runs for nothing
   charged per evaluation          no idle evaluation cost

   Requirement: change tracking must be on for the underlying tables.
   Streams enable it automatically, so we are already covered - but be
   explicit about it, because this is the #1 reason a triggered task
   silently never fires.
   ========================================================= */

ALTER TABLE RAW.BOOKING     SET CHANGE_TRACKING = TRUE;
ALTER TABLE RAW.RESERVATION SET CHANGE_TRACKING = TRUE;
ALTER TABLE RAW.CLAIM       SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE TASK SILVER.TSK_LOAD_SILVER
  WAREHOUSE = WH_CDC
  WHEN SYSTEM$STREAM_HAS_DATA('SILVER.STR_BOOKING')
       OR SYSTEM$STREAM_HAS_DATA('SILVER.STR_RESERVATION')
       OR SYSTEM$STREAM_HAS_DATA('SILVER.STR_CLAIM')
AS
  CALL SILVER.SP_LOAD_SILVER();

/* How often will it check? Controlled by this parameter.
   Default 30 seconds, minimum 10. Set it on the schema or the task. */
ALTER TASK SILVER.TSK_LOAD_SILVER
  SET USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = 30;

ALTER TASK SILVER.TSK_LOAD_SILVER RESUME;

/* Now prove it. Land a CDC batch and DON'T call the proc - just wait. */
SELECT COUNT(*) AS stream_rows FROM SILVER.STR_BOOKING;

-- wait ~30-60 seconds, then:
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
      SCHEDULED_TIME_RANGE_START => DATEADD(hour,-1,CURRENT_TIMESTAMP())))
ORDER BY SCHEDULED_TIME DESC;

SELECT COUNT(*) AS stream_now_empty FROM SILVER.STR_BOOKING;   -- 0: the task ran
SELECT * FROM UTIL.PIPELINE_LOG ORDER BY LOG_TS DESC;          -- proof it fired

/* If it never fires, check in this order:
     1. Is the task RESUMED?             SHOW TASKS IN SCHEMA SILVER;
     2. Does the stream actually have data?  SELECT SYSTEM$STREAM_HAS_DATA('SILVER.STR_BOOKING');
     3. Is change tracking on?           SHOW TABLES LIKE 'BOOKING' IN SCHEMA RAW;
     4. Did it error?                    TASK_HISTORY above, ERROR_MESSAGE column.  */

EXECUTE TASK SILVER.TSK_LOAD_SILVER;     -- manual run, works either way

-- keep the trial safe
ALTER TASK SILVER.TSK_LOAD_SILVER SUSPEND;


/* ---------- THE SCHEDULED ALTERNATIVE (what most older projects use) ----------
   Same task, but timer-driven. Worth showing so he can compare, and because
   plenty of production pipelines still look like this.

CREATE OR REPLACE TASK SILVER.TSK_LOAD_SILVER
  WAREHOUSE = WH_CDC
  SCHEDULE  = 'USING CRON 0 * * * * UTC'      -- top of every hour
  WHEN SYSTEM$STREAM_HAS_DATA('SILVER.STR_BOOKING')
AS
  CALL SILVER.SP_LOAD_SILVER();

   With a schedule, the WHEN clause is a COST GUARD: the task wakes hourly,
   evaluates the condition for free, and skips the warehouse spin-up if the
   stream is empty. With a triggered task you don't need that guard at all.
------------------------------------------------------------------------------ */
