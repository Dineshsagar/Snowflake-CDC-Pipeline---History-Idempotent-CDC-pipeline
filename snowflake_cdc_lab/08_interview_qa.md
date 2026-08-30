# Interview Q&A drill

## Architecture

**Q: Walk me through your pipeline.**
Source OLTP → Qlik Replicate reads the transaction log → writes to S3 in two paths,
`fullload/` (one time) and `cdc/` (hourly) → S3 event → SQS → **Snowpipe** auto-ingests
into **RAW** → RAW is truncate-and-load so it only ever holds the current batch →
a **stream** on RAW + a **task** (`WHEN SYSTEM$STREAM_HAS_DATA`) fires a stored proc that
**MERGEs on the primary key** into **SILVER** → a stream on SILVER triggers the next task
which applies business logic into **GOLD** → GOLD is unloaded back to S3 for downstream
consumers and read by Tableau. dbt owns the GOLD layer and the tests.

**Q: Why is RAW truncate-and-load?**
RAW is a landing pad, not an archive. History already lives in S3 as immutable files, and
current state lives in SILVER. Keeping only the current batch makes RAW tiny, makes
"what was in the last batch" trivial to answer, and avoids a table that grows forever
for no reason.

**Q: Isn't truncating RAW dangerous with a stream on it?**
Yes, and there are two guards. First, the stream is `APPEND_ONLY = TRUE` — it tracks
inserts only, so the truncate is invisible to it. A standard stream would surface those
rows as deletes and feed them into the MERGE. Second, the TRUNCATE lives *inside* the
same stored procedure, after the MERGE has already consumed the stream. If truncation
ran on its own schedule you could wipe a batch that was never merged.

**Q: Full load vs CDC?**
Full load is a one-time snapshot of every existing row. CDC is the ongoing stream of
changes read from the source's transaction log. Qlik runs the full load first, caches
changes that happen during it, applies the cache, then switches to CDC. Full-load rows
carry no operation flag — we tag them `L`.

## Snowpipe

**Q: How does Snowpipe know a file arrived?**
It doesn't poll. With `AUTO_INGEST = TRUE` Snowflake provisions an SQS queue in its own
AWS account and gives you the ARN via `SHOW PIPES`. You configure the S3 bucket
(Properties → Event notifications → `s3:ObjectCreated:*` → SQS → that ARN) so S3 pushes
the event. The pipe itself never mentions the bucket — it points at a **stage**, and the
stage holds the URL and the storage integration.

**Q: Snowpipe vs COPY vs Task?**
COPY is manual batch and uses your warehouse. Snowpipe is serverless, event-driven,
~1 minute latency, billed per file plus compute. Tasks are the scheduler for
transformations *after* the data has landed. Different stages, not alternatives.

**Q: A file was re-uploaded and the rows never appeared. Why?**
Snowpipe dedups by file name for 14 days (COPY does it for 64). Re-writing a file with
the same name after a Qlik task reload gets silently skipped. Fix is `COPY ... FORCE = TRUE`
or writing with a new file name. This is the classic "where did my rows go" incident.

## CDC / MERGE

**Q: My MERGE failed with "duplicate row detected during DML action".**
The source side has more than one row for the same key in one batch — normal in CDC when
a record is inserted and then updated within the same hour. Fix:
`QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY change_seq DESC) = 1`.

**Q: How do you handle deletes?**
Soft delete. `CHANGE_OPER = 'D'` sets `IS_DELETED = TRUE` in SILVER, and the GOLD layer
filters them out. Keeps history, keeps downstream joins stable, and lets you audit what
disappeared and when.

**Q: Out-of-order events?**
Order by `CHANGE_SEQ` from the source transaction log, never by wall-clock timestamp.
Compare against the stored `CHANGE_SEQ` before applying.

**Q: What does a stream actually store?**
Nothing — it's an offset plus metadata columns over the source table's change tracking.
It costs no storage. Reading it inside a DML statement advances the offset. Different
consumers need their own streams.

**Q: Scheduled task or triggered task?**
Triggered. The RAW→SILVER task has no `SCHEDULE` clause at all — just
`WHEN SYSTEM$STREAM_HAS_DATA(...)`. Snowflake watches the stream and fires the task as
soon as Snowpipe lands a batch, so latency is seconds rather than waiting for the next
cron slot, and the task never wakes up to do nothing. With a *scheduled* task the `WHEN`
clause serves a different purpose — it's a cost guard that lets the task skip the
warehouse spin-up when the stream is empty. Triggered tasks need change tracking enabled
on the source tables; that's the usual reason one silently never fires.

**Q: How is the whole chain orchestrated?**
One DAG. The triggered task is the root; the GOLD task is `AFTER` it and the unload task
is `AFTER` that. Resume children first and the root last, otherwise the chain won't arm.
Nothing in the pipeline runs on a timer — it's event-driven from the S3 notification all
the way to the outbound file.

## GOLD / dbt

**Q: What's the difference between SILVER and GOLD?**
SILVER is the faithful current state of the source — same grain, typed, deduped, nothing
invented. GOLD is where business logic lives: joins across entities, derived columns like
route and net revenue, bucketing, filtering out cancelled bookings. SILVER is *what
happened*; GOLD is *what the business wants to see*.

**Q: Where does dbt fit?**
Ingestion and CDC stay native Snowflake — Snowpipe, streams, tasks, MERGE. That part is
stateful and event-driven. dbt owns SILVER → GOLD: the business models and the tests.
The Snowflake task chain ends after SILVER and hands off to a scheduled `dbt build`.

**Q: Why not use dbt for the whole thing?**
dbt is a transformation tool, not an ingestion tool. It has no concept of an S3 event or
a stream offset. Forcing the CDC application into dbt incremental models would mean
reimplementing the stream logic in Jinja for no benefit.

**Q: What dbt tests did you use?**
Generic ones in `schema.yml` — `not_null`, `unique`, `accepted_values`, `relationships` —
plus `source freshness` to catch a stalled Qlik task. Every test compiles to a SELECT
that must return zero rows.

## Ops

**Q: The dashboard is stale. Triage it.**
Top down. (1) `UTIL.V_FRESHNESS` — is SILVER updating? (2) If not, is RAW getting rows?
(3) If RAW is empty, `SYSTEM$PIPE_STATUS` — if `lastReceivedMessageTimestamp` is null the
problem is in AWS (event notification, bucket policy, IAM), not Snowflake. (4) If messages
are arriving but nothing loads, check `COPY_HISTORY` for file errors and check the pipe's
`PATTERN`. (5) If RAW is fine, check `TASK_HISTORY` for a failed task or a failed DQ test
blocking the chain. (6) Upstream: is the Qlik task actually running, and what's the
latency in QEM?

**Q: Common failures you saw?**
Schema drift when someone runs DDL on the source, long-running source transactions
inflating latency, disk pressure on the replication server, expired IAM credentials, and
duplicate-key MERGE errors after a Qlik task reload.

**Q: Someone corrupted SILVER at 6am.**
Time Travel — `SELECT ... AT(OFFSET => -3600)` or `BEFORE(STATEMENT => '<query_id>')`,
validate it, then `CREATE OR REPLACE TABLE ... AS SELECT` from that point. Zero-copy clone
the database first so I can investigate without touching prod. `UNDROP` if it was dropped.

**Q: Cost optimisation?**
Auto-suspend at 60s. `WHEN SYSTEM$STREAM_HAS_DATA()` on the task so quiet hours cost
nothing. Separate warehouses for ETL and BI so dashboards don't queue behind loads.
Materialised aggregates instead of Tableau scanning the fact table. Resource monitors that
notify at 75% and suspend at 100%. `STATEMENT_TIMEOUT_IN_SECONDS` to kill runaways.

**Q: How do you monitor in production?**
QEM for the Qlik side — task state, source/target latency, throughput, applied I/U/D
counts, memory and disk on the replication server. CloudWatch alarms on the S3 and
pipeline metrics. Inside Snowflake: `PIPE_USAGE_HISTORY`, `TASK_HISTORY`, the DQ results
table, and a freshness view with an SLA threshold.
