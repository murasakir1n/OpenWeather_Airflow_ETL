select
    capital,
    date(fetched_at) as weather_date,
    round(avg(temp)::numeric, 2) as avg_temp,
    min(temp) as min_temp,
    max(temp) as max_temp,
    avg(pressure) as avg_pressure,
    avg(humidity) as avg_humidity
from {{ source('weather_data', 'openweather_europe_etl') }}
where date(fetched_at) = (
    select max(date(fetched_at))
    from {{ source('weather_data', 'openweather_europe_etl') }}
    )
group by capital, date(fetched_at)
order by weather_date, capital