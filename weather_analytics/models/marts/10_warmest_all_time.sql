with ranked as (
    select
        capital,
        temp as max_temp,
        date(fetched_at) as weather_date,
        row_number() over(partition by capital order by temp desc) as rn
    from {{ source('weather_data', 'openweather_europe_etl') }}
)

select
    capital,
    max_temp,
    weather_date
from ranked
where rn = 1
order by max_temp desc
limit 10 