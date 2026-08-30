# Snowflake-CDC-Pipeline---History-Idempotent-CDC-pipeline

# Architecture — what we are building

```
   S3 (Qlik Replicate output)
     |  fullload/   one time
     |  cdc/        every hour
     v
   SNOWPIPE            auto-ingest on S3 event, ~1 min latency
     v
   RAW                 landing. TRUNCATE + LOAD -> only the CURRENT batch lives here
     |  STREAM (append-only) + TRIGGERED TASK (fires when the stream has data)
     v
   SILVER              MERGE on PK: update when matched, insert when not.
     |                 current-state, typed, deduped, soft deletes
     |  STREAM + TASK (chained AFTER the silver task)
     v
   GOLD                business logic + transformations (dbt owns this layer)
     v
   UNLOAD -> S3        COPY INTO @stage from GOLD
```

## Why triggered tasks, not scheduled
The RAW→SILVER task has **no `SCHEDULE`** — only a `WHEN SYSTEM$STREAM_HAS_DATA(...)`
clause. Snowflake watches the stream and fires the task as soon as Snowpipe lands a
batch. Latency drops from "up to an hour" to seconds, and the task never runs for nothing.

The whole chain is therefore event-driven end to end: S3 event → Snowpipe → RAW insert →
stream has data → triggered task → SILVER → chained task → GOLD → chained task → unload.
Nothing in the pipeline is on a timer.

Requires change tracking on the source tables (streams turn it on automatically, but set
it explicitly — a missing change-tracking flag is the usual reason a triggered task
silently never fires).

## Layer rules
| Layer | Contains | Load pattern | Owned by |
|---|---|---|---|
| RAW | exactly one batch, as-delivered, all VARCHAR | truncate + load | Snowpipe |
| SILVER | full current state, one row per PK, typed | MERGE from stream | Snowflake SP + Task |
| GOLD | business metrics, joins, derived columns | rebuild / incremental | **dbt** |

## Why RAW is truncate-and-load
RAW is a landing pad, not history. Every hour Snowpipe drops the new CDC batch in,
the task merges it into SILVER, then RAW is emptied. Keeps RAW tiny and makes
"what did the last batch contain" trivially answerable. History lives in S3 (the files
are immutable) and current state lives in SILVER.

**Two things that make this safe** — say these out loud, they're the design's weak points:
1. The stream must be `APPEND_ONLY = TRUE`. A standard stream would treat the TRUNCATE
   as a pile of deletes and feed them into the MERGE.
2. The TRUNCATE must happen **inside the same stored procedure**, after the MERGE has
   consumed the stream. Truncating on a separate schedule risks wiping a batch that was
   never merged.

## Where dbt fits (deliberately small)
Snowflake native handles ingestion and CDC application — Snowpipe, streams, tasks, MERGE.
That's the hard, stateful part and it stays in SQL.

dbt owns **GOLD only**: business logic, transformations, and tests. That's 2 models and
one `schema.yml`. Minimal surface area, but a genuine and defensible boundary:
*"we used Snowflake native for CDC ingestion and dbt for the business/presentation layer."*

## Run order
`01` setup → `02` snowpipe→RAW → `03` RAW→SILVER → `04` SILVER→GOLD → `05` unload →
`06` DQ/ops → `07` full end-to-end rerun. dbt is optional and read-only.
