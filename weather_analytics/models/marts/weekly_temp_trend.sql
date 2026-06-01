select
    capital,
    date(fetched_at) as weather_date,
    round(avg(temp)::numeric, 2) as avg_temp
from {{ source('weather_data', 'openweather_europe_etl') }}
group by capital, date(fetched_at)
order by capital, weather_date