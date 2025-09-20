{{ config(materialized='incremental', unique_key='id')}}


with trail_conditions as (
    select
        w.id,
        t.name,
        t.trail_difficulty_score,
        t.trail_difficulty_label,
        w.weather_difficulty_score,
        w.weather_difficulty_label,
        w.datetime as datetime_at,
        (t.trail_difficulty_score + w.weather_difficulty_score) as combined_difficulty
    from {{ ref('int_weather_difficulty') }} w
    left join {{ ref('int_trail_difficulty') }} t
        on w.lon = t.lon and w.lat = t.lat
    {% if is_incremental() %}
        where w.datetime > (select max(datetime_at) from {{ this }})
    {% endif %}
),

trail_conditions_labeled as (
    select
        *,
        (
            case when weather_difficulty_label = 'None' or weather_difficulty_label = 'Mild' then trail_difficulty_label
                when weather_difficulty_label = 'Noticeable' then
                    case when trail_difficulty_label = 'Easy' then 'Moderate'
                        when trail_difficulty_label = 'Moderate' then 'Challenging'
                        when trail_difficulty_label = 'Challenging' then 'Hard'
                        else 'Extreme'
                    end
                when weather_difficulty_label = 'Tough' then
                    case when trail_difficulty_label = 'Easy' then 'Challenging'
                        when trail_difficulty_label = 'Moderate' then 'Hard'
                        else 'Extreme'
                    end
                when weather_difficulty_label = 'Hard' then
                    case when trail_difficulty_label = 'Easy' then 'Hard'
                        else 'Extreme'
                    end
                else 'Extreme'
            end        
        ) as combined_difficulty_label
    from trail_conditions
)

SELECT * FROM trail_conditions_labeled