-- models/booking_fact.sql
-- The dbt version of GOLD.SP_LOAD_GOLD(). Same business logic, no procedural code.
{{ config(materialized='table') }}

with booking as (

    select * from {{ source('silver', 'booking') }}
    where not is_deleted
      and booking_status <> 'CANCELLED'

),

## testing the join to reservation and claim tables. The join is done on booking_id, which is the primary key of booking table and foreign key in reservation and claim tables.

reservation as (

    select booking_id, cabin_class, checked_in
    from {{ source('silver', 'reservation') }}
    where not is_deleted

),

claims as (

    select
        booking_id,
        count(*)                                            as claim_count,
        sum(claim_amount)                                   as claim_amount,
        sum(iff(claim_status = 'SETTLED', claim_amount, 0)) as settled_amount,
        count_if(claim_status = 'OPEN')                     as open_claims
    from {{ source('silver', 'claim') }}
    where not is_deleted
    group by booking_id

)

select
    b.booking_id,
    b.passenger_id,
    b.flight_number,
    b.origin || '-' || b.destination            as route,
    date(b.booking_ts)                          as booking_date,
    b.booking_status,
    b.channel,
    r.cabin_class,
    coalesce(r.checked_in, false)               as checked_in,
    b.fare_amount,
    coalesce(c.claim_count, 0)                  as claim_count,
    coalesce(c.claim_amount, 0)                 as claim_amount,
    b.fare_amount - coalesce(c.settled_amount, 0) as net_revenue,

    case
        when b.fare_amount >= 1200 then 'HIGH'
        when b.fare_amount >=  900 then 'MEDIUM'
        else 'LOW'
    end                                         as revenue_band,

    (coalesce(c.open_claims, 0) > 0 or not coalesce(r.checked_in, false))
                                                as is_at_risk,
    current_timestamp()                         as load_ts

from booking b
left join reservation r on r.booking_id = b.booking_id
left join claims      c on c.booking_id = b.booking_id
