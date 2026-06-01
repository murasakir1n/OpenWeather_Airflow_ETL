select
    capital,
    date(fetched_at) as weather_date,
    round(avg(humidity)::numeric, 2) as avg_humidity,
    round(avg(pressure)::numeric, 2) as avg_pressure
from {{ source('weather_data', 'openweather_europe_etl') }}
group by capital, date(fetched_at)
order by capital, weather_date