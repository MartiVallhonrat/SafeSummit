with weather_difficulty_table as (
    select
        *,
        (
            case when temperature <= -20 then 4
                when temperature <= -10 then 3
                when temperature <= 0 then 2
                when temperature <= 7 then 1
                when temperature >= 40 then 4
                when temperature >= 35 then 3
                when temperature >= 30 then 2
                when temperature >= 25 then 1
                else 0
            end 
            +
            case when humidity <= 20 then 1
                when humidity >= 90 then 1
                else 0
            end
            +
            case when visibility <= 100 then 3
                when visibility <= 500 then 2
                when visibility <= 1000 then 1
                else 0
            end
            +
            case when wind >= 20 then 3
                when wind >= 10 then 2
                when wind >= 5 then 1
                else 0
            end
            +
            case when rain >= 20 then 3
                when rain >= 10 then 2
                when rain > 0 then 1
                else 0
            end
            +
            case when snow >= 20 then 4
                when snow >= 10 then 3
                when snow > 0 then 2
                else 0
            end
            +
            case when datetime < sunrise or datetime > sunset then 2
                else 0
            end
        ) as weather_difficulty_score
    from {{ ref('stg_weather') }}
)

SELECT
    *,
    (
        CASE WHEN weather_difficulty_score < 1 THEN 'None'
            WHEN weather_difficulty_score <= 3 THEN 'Mild'
            WHEN weather_difficulty_score <= 7 THEN 'Noticeable'
            WHEN weather_difficulty_score <= 12 THEN 'Tough'
            WHEN weather_difficulty_score <= 18 THEN 'Harsh'
            else 'Severe'
        END
    ) AS weather_difficulty_label
FROM weather_difficulty_table
