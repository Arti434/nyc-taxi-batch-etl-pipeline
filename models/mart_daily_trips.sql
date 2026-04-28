with trips as (
    select * from {{ ref('stg_taxi_trips') }}
)
select
    pickup_date,
    COUNT(*)                             as total_trips,
    ROUND(AVG(fare_amount), 2)           as avg_fare,
    ROUND(SUM(total_amount), 2)          as total_revenue,
    ROUND(AVG(trip_distance), 2)         as avg_distance,
    ROUND(AVG(tip_percentage), 2)        as avg_tip_pct,
    ROUND(AVG(trip_duration_minutes), 2) as avg_duration_mins,
    COUNTIF(pickup_hour between 7 and 9)   as morning_rush_trips,
    COUNTIF(pickup_hour between 17 and 19) as evening_rush_trips
from trips
group by pickup_date
order by pickup_date