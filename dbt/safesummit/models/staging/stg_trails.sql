-- just to show test
{{ config(materialized='table', unique_key='id')}}
-- delete all the above

with source as ( select * from {{ source('main_database', 'raw_trail_data') }} ),
del_dup as (
    select
        *,
        row_number() over (
            partition by lon, lat
            order by inserted_at
        ) as row_num
    from source
)

SELECT 
    id,
    name,
    distance,
    slope,
    lon,
    lat,
    inserted_at
FROM del_dup
WHERE row_num = 1