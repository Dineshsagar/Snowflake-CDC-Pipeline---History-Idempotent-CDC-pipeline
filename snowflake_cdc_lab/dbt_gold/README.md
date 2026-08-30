# dbt — the GOLD layer only

## Why only GOLD
Ingestion and CDC (Snowpipe → RAW → stream/task/MERGE → SILVER) stay native Snowflake.
That part is stateful and event-driven; dbt is a bad fit and you'd be fighting it.

dbt takes over at **SILVER → GOLD**: business logic, derived columns, and tests.
That is a clean, defensible boundary and it's how plenty of real teams run it.

> *"We used Snowflake native objects for CDC ingestion into Silver, and dbt for the
> Gold business layer and data quality tests."*

That one sentence is the whole story. It's honest, it's common, and it means you only
need to know 4 dbt concepts.

## The only 4 things to know
1. **`source()` / `ref()`** — dbt builds the run order (DAG) from these. Nothing else.
2. **Materialization** — `view`, `table`, or `incremental`. Set in `{{ config() }}`.
3. **Tests** — declared in `schema.yml`. Each one compiles to a SELECT that must return
   zero rows. Same thing we hand-wrote in `06_dq_and_ops.sql`.
4. **Commands** — `dbt run`, `dbt test`, `dbt build` (= run + test in DAG order).

## Files here
```
dbt_gold/
  dbt_project.yml               project config
  models/
    sources.yml                 points at SILVER (loaded by our task, not by dbt)
    booking_fact.sql            the business logic - replaces GOLD.SP_LOAD_GOLD
    daily_route_summary.sql     aggregate for the dashboard
    schema.yml                  tests
```

## Mapping — everything here already exists in the SQL you ran
| SQL we wrote | dbt equivalent |
|---|---|
| `SILVER.BOOKING` etc | `source('silver','booking')` |
| `GOLD.SP_LOAD_GOLD()` | `models/booking_fact.sql` |
| `GOLD.V_DAILY_ROUTE` | `models/daily_route_summary.sql` |
| `UTIL.SP_RUN_DQ()` inserts | `schema.yml` tests |
| `GOLD.TSK_LOAD_GOLD` task | a scheduled `dbt build` |

## How it would be orchestrated
The Snowflake task chain ends after SILVER. Then Airflow / dbt Cloud / a GitHub Action
runs `dbt build --select gold`. Two systems, one handoff point, no overlap.

## If you're asked something deeper
> *"We ran dbt Core. I owned the Gold models and the tests; the macro and CI framework
> was maintained by our platform team."*

Don't oversell it. Knowing where the boundary is and why is more convincing than
reciting macro syntax.
