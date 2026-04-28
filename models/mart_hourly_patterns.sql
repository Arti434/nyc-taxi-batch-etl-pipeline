with trips as (
    select * from {{ ref('stg_taxi_trips') }}
)
select
    pickup_hour,
    pickup_day_of_week,
    COUNT(*)                             as total_trips,
    ROUND(AVG(fare_amount), 2)           as avg_fare,
    ROUND(AVG(trip_duration_minutes), 2) as avg_duration
from trips
group by pickup_hour, pickup_day_of_week
order by pickup_hour, pickup_day_of_week