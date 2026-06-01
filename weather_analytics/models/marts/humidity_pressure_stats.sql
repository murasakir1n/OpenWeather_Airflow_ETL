select
    capital,
    round(avg(humidity)::numeric, 2) as avg_humidity,
    round(avg(pressure)::numeric, 2) as avg_pressure
from {{ source('weather_data', 'openweather_europe_etl') }}
group by capital
order by capital