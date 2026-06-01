import json
from datetime import datetime, timezone

import boto3
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from sqlalchemy import create_engine
import pandas as pd

from european_capitals import EUROPEAN_CAPITALS

OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"


@dag(
    dag_id="openweather_europe_etl",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["openweather", "etl"],
)
def openweather_europe_etl():

    @task()
    def extract() -> list:
        api_key = Variable.get("API_KEY")
        results = []

        for city in EUROPEAN_CAPITALS:
            params = {
                "lat": city["lat"],
                "lon": city["lon"],
                "appid": api_key,
                "units": "metric",
            }
            response = requests.get(OPENWEATHER_URL, params=params, timeout=10)
            response.raise_for_status()
            results.append({
                "country": city["country"],
                "capital": city["capital"],
                "lat":     city["lat"],
                "lon":     city["lon"],
                "raw":     response.json(),
            })

        return results

    @task()
    def transform(data: list) -> list:
        results = []

        for city in data:
            raw = city["raw"]
            results.append({
                "country":    city["country"],
                "capital":    city["capital"],
                "lat":        city["lat"],
                "lon":        city["lon"],
                "temp":       raw["main"]["temp"],
                "feels_like": raw["main"]["feels_like"],
                "humidity":   raw["main"]["humidity"],
                "pressure":   raw["main"]["pressure"],
                "wind_speed": raw["wind"]["speed"],
                "weather":    raw["weather"][0]["description"],
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            })

        return results

    @task()
    def load_to_s3(results: list) -> None:
        access_key = Variable.get('ACCESS_KEY')
        secret_key = Variable.get('SEC_KEY')
        bucket = Variable.get('BUCKET')

        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        now = datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        current_time = now.strftime("%H-%M-%S")

        key = f"weather-data/date={current_date}/europe_{current_time}.json"

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(results, ensure_ascii=False),
            ContentType='application/json',
        )
        print(f'Loaded {len(results)} cities to S3: {key}')

    @task()
    def load_to_db(results):

            conn = Variable.get('DB_CONN')
            engine = create_engine(conn, pool_pre_ping=True)

            data = pd.DataFrame(results)

            data.to_sql(
                name='openweather_europe_etl',
                con=engine,
                if_exists='append',
                index=False,
            )

            print(f'Data loaded: {len(data)}')

    raw = extract()
    transformed = transform(raw)
    load_to_s3(transformed)
    load_to_db(transformed)


openweather_europe_etl()
