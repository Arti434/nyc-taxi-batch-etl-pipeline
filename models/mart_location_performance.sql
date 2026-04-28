with trips as (
    select * from {{ ref('stg_taxi_trips') }}
)
select
    pickup_location_id,
    COUNT(*)                      as total_trips,
    ROUND(AVG(fare_amount), 2)    as avg_fare,
    ROUND(AVG(trip_distance), 2)  as avg_distance,
    ROUND(SUM(total_amount), 2)   as total_revenue,
    ROUND(AVG(tip_percentage), 2) as avg_tip_pct
from trips
group by pickup_location_id
order by total_trips desc
limit 20