-- models/daily_route_summary.sql
-- ref() is what tells dbt this must run AFTER booking_fact.
{{ config(materialized='table') }}

select
    booking_date,
    route,
    channel,
    count(*)                as bookings,
    sum(fare_amount)        as gross_revenue,
    sum(net_revenue)        as net_revenue,
    sum(claim_amount)       as claim_exposure,
    count_if(is_at_risk)    as at_risk_bookings,
    round(100.0 * count_if(is_at_risk) / count(*), 2) as at_risk_pct

from {{ ref('booking_fact') }}
group by 1, 2, 3
