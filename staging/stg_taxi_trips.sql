-- models/staging/stg_taxi_trips.sql
-- Staging model: cleans and standardizes raw taxi trips

with source as (
    select * from {{ source('taxi_data', 'trips') }}
),

cleaned as (
    select
        -- identifiers
        vendor_id,
        pickup_location_id,
        dropoff_location_id,

        -- timestamps
        pickup_datetime,
        dropoff_datetime,

        -- trip details
        passenger_count,
        trip_distance,

        -- financials
        fare_amount,
        tip_amount,
        total_amount,

        -- calculated fields
        DATE(pickup_datetime)                    as pickup_date,
        EXTRACT(HOUR FROM pickup_datetime)       as pickup_hour,
        EXTRACT(DAYOFWEEK FROM pickup_datetime)  as pickup_day_of_week,
        TIMESTAMP_DIFF(
            dropoff_datetime,
            pickup_datetime,
            MINUTE
        )                                        as trip_duration_minutes,

        -- tip percentage
        ROUND(
            SAFE_DIVIDE(tip_amount, fare_amount) * 100,
        2)                                       as tip_percentage

    from source

    -- data quality filters
    where fare_amount > 0
      and trip_distance > 0
      and passenger_count between 1 and 6
      and pickup_datetime is not null
)

select * from cleaned