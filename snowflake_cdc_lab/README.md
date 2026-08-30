# Snowflake CDC Lab — Airline Booking / Reservation / Claim

Hands-on lab mirroring the client's project. Runs entirely on a **Snowflake trial, no AWS**.

```
S3 ─▶ Snowpipe ─▶ RAW ─stream+task▶ SILVER ─stream+task▶ GOLD ─▶ S3
                (truncate+load)    (MERGE on PK)      (business logic)
                                                        └── dbt owns this layer
```

## Files
| File | Purpose |
|---|---|
| [00_ARCHITECTURE.md](00_ARCHITECTURE.md) | The diagram, layer rules, design decisions |
| [RUN_ORDER.txt](RUN_ORDER.txt) | **Start here.** Step-by-step, what to run and what to say |
| [01_setup.sql](01_setup.sql) | Warehouse, database, 4 schemas, all tables |
| [02_snowpipe_raw.sql](02_snowpipe_raw.sql) | File format, stage, 3 pipes, full load |
| [03_raw_to_silver.sql](03_raw_to_silver.sql) | Streams + MERGE procedure + task — **the core** |
| [04_silver_to_gold.sql](04_silver_to_gold.sql) | Business logic, transformations, DAG |
| [05_unload_to_s3.sql](05_unload_to_s3.sql) | `COPY INTO @stage` outbound feed |
| [06_dq_and_ops.sql](06_dq_and_ops.sql) | DQ tests, monitoring, Time Travel, cleanup |
| [07_reset_and_rerun.sql](07_reset_and_rerun.sql) | Replay everything end-to-end |
| [dbt_gold/README.md](dbt_gold/README.md) | dbt in 20 min — GOLD layer only, nothing to install |
| [08_interview_qa.md](08_interview_qa.md) | Q&A drill |

## Sample data
- `data/fullload/` — 10 bookings, 10 reservations, 5 claims (`CHANGE_OPER = L`)
- `data/cdc/` — inserts, updates, a delete, and **two events for `BK1011` in one batch**

That last one is deliberate. It's what breaks a naive `MERGE` and it's the best teaching
moment in the lab.

## Uploading the data
Everything is Snowsight — SQL in worksheets, CSVs via the UI:

> Data > Databases > AIRLINE_DL > UTIL > Stages > STG_LANDING > **+ Files**

Upload `data/fullload/` with path `fullload`, then `data/cdc/` with path `cdc`.
The path box is not optional.

If **+ Files** is missing, the stage needs `ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')` —
Snowsight can't write to client-side-encrypted stages. It's already in
[02_snowpipe_raw.sql](02_snowpipe_raw.sql); just re-run the `CREATE STAGE`.

## Always run at the end
```sql
ALTER TASK UTIL.TSK_UNLOAD        SUSPEND;
ALTER TASK GOLD.TSK_LOAD_GOLD     SUSPEND;
ALTER TASK SILVER.TSK_LOAD_SILVER SUSPEND;
ALTER WAREHOUSE WH_CDC SUSPEND;
```
