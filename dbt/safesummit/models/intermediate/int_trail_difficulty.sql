-- just to show test
{{ config(materialized='incremental', unique_key='id')}}
-- delete all the above
with trail_difficulty_table as (
    select
        *,
        (
            case when distance >= 30 then 4
                when distance >= 20 then 3
                when distance >= 10 then 2
                when distance >= 5 then 1
                else 0
            end
            +
            case when slope >= 1000 then 5
                when slope >= 600 then 4
                when slope >= 300 then 3
                when slope >= 150 then 2
                when slope >= 50 then 1
                else 0
            end
        ) as trail_difficulty_score
    from {{ ref('stg_trails') }}
)

SELECT
    *,
    (
        CASE WHEN trail_difficulty_score <= 1 THEN 'Easy'
            WHEN trail_difficulty_score <= 3 THEN 'Moderate'
            WHEN trail_difficulty_score <= 6 THEN 'Challenging'
            WHEN trail_difficulty_score <= 8 THEN 'Hard'
            ELSE 'Extreme'
        END
    ) AS trail_difficulty_label
FROM trail_difficulty_table