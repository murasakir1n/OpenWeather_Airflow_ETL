with ranked as (
    select
        capital,
        temp as min_temp,
        date(fetched_at) as weather_date,
        row_number() over(partition by capital order by temp) as rn
    from {{ source('weather_data', 'openweather_europe_etl') }}
)

select
    capital,
    min_temp,
    weather_date
from ranked
where rn = 1
order by min_temp
limit 10
