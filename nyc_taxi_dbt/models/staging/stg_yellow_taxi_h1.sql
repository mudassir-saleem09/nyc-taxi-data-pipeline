with source as (
 
    select * from {{ source('raw', 'yellow_taxi_2026_h1') }}
 
),
 
renamed as (

    select
        vendorid                   as vendor_id,
        tpep_pickup_datetime       as pickup_datetime,
        tpep_dropoff_datetime      as dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecode_clean             as rate_code_id,
        store_and_fwd_flag,
        pulocationid                as pickup_location_id,
        dolocationid                as dropoff_location_id,
        payment_type_clean          as payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,
        total_revenue,
        trip_duration_minutes,
        pickup_hour,
        pickup_date,
        pickup_day_of_week,
        LPAD(MONTH(pickup_date)::VARCHAR, 2, '0') AS pickup_month,
        is_incomplete_metadata
 
    from source
 
)

select * from renamed