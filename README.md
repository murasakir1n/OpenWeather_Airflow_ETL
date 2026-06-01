# OpenWeather ETL — European Capitals

An end-to-end batch ETL pipeline that collects hourly weather data for **45 European capital cities** from the OpenWeather API, lands raw data in object storage, loads it into a cloud data warehouse, transforms it with dbt, and visualizes it in Metabase.

> 🇷🇺 Russian version below / Русская версия ниже

---

## Architecture

```
OpenWeather API
      │
      ▼
   Airflow  ──extract──▶ transform ──┬──▶ Yandex S3 (raw JSON, partitioned by date)
   (@hourly)                         │
                                     └──▶ Neon Postgres (raw table)
                                              │
                                              ▼
                                            dbt (marts / views)
                                              │
                                              ▼
                                           Metabase (dashboard)
```

The pipeline writes to **two destinations in parallel**: raw JSON to S3 (acting as a data lake / source of truth) and structured rows to Postgres (the analytics warehouse). If the warehouse is ever lost, data can be replayed from S3.

---

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 3.x (TaskFlow API) |
| Raw storage | Yandex Object Storage (S3-compatible) |
| Warehouse | Neon (serverless Postgres) |
| Transformations | dbt (postgres adapter) |
| Visualization | Metabase |
| Containerization | Docker Compose |

---

## Pipeline (Airflow DAG)

The DAG (`dags/openweather_dag.py`) runs hourly and is built with the TaskFlow `@task` API:

1. **`extract`** — iterates over the list of European capitals (`dags/european_capitals.py`), calls the OpenWeather API per city using lat/lon coordinates, and returns the raw responses.
2. **`transform`** — flattens each raw API response into a clean record (temp, feels_like, humidity, pressure, wind_speed, weather description) and stamps each row with `fetched_at` in UTC.
3. **`load_to_s3`** — writes the batch as JSON to Yandex S3, partitioned as `weather-data/date={date}/...`.
4. **`load_to_db`** — appends the batch to the `openweather_europe_etl` table in Neon Postgres.

`extract` and `transform` run sequentially; `load_to_s3` and `load_to_db` run in parallel as the final fan-out.

Secrets and config (API key, city list, S3 credentials, DB connection) are stored in **Airflow Variables**

---

## Data Models (dbt)

The raw table is declared as a **dbt source** (`models/staging/sources.yaml`), enabling lineage tracking and freshness checks. All marts reference it via `{{ source('weather_data', 'openweather_europe_etl') }}`.

| Model | Description |
|---|---|
| `latest_day_weather_average` | Per-city averages (temp, pressure, humidity) for the **most recent day**. Self-updating via a `max(date)` subquery — no hard-coded dates. |
| `weekly_temp_trend` | Per-city, per-day temperature aggregation for time-series trend charts. |
| `10_warmest_all_time` | Top 10 hottest readings across all cities, using `ROW_NUMBER()` window function to dedupe per city. |
| `10_coldest_all_time` | Top 10 coldest readings, same windowing approach. |
| `humidity_pressure_stats` | Humidity and pressure statistics per city. |

Data tests (`not_null`, `unique`) are defined in `schema.yml`.

---

## Dashboard (Metabase)

The Metabase dashboard visualizes:

- **Temperature trends** — line chart of daily average temperature per city over time.
- **Warmest / coldest capitals** — ranked bar charts.
- **Humidity by city** — bar chart (pressure shown contextually, as it varies in a narrow band and is less meaningful for cross-city comparison).

---

## Running the Project

```bash
# 1. Start all services
docker compose up -d

# 2. Open Airflow UI and set the required Variables
#    (API key, city list, S3 credentials, DB connection)

# 3. Enable the openweather_dag — it runs hourly and starts collecting

# 4. Run dbt transformations
dbt run

# 5. Run data tests
dbt test

# 6. Open Metabase, connect to the Neon Postgres warehouse, and build/view the dashboard
```

---

## Design Notes

- **Raw layer as a safety net.** Writing raw JSON to S3 alongside the warehouse load means the warehouse is a rebuildable view of the data, not the only copy.
- **Self-updating models.** Latest-day models use `max(date)` rather than hard-coded dates, so the pipeline requires no manual changes as new data arrives.
- **Long format over pivoting.** Time-series data is stored in long format (one row per city/day); reshaping into charts is handled by the BI layer, keeping models stable as new days are collected.
- **Serverless trade-offs.** Neon's scale-to-zero saves cost during idle periods at the price of a brief cold start on the first query — handled gracefully by Airflow's built-in task retries.

---
---

# 🇷🇺 OpenWeather ETL — Столицы Европы

Сквозной batch ETL-пайплайн: ежечасно собирает данные о погоде по **45 столицам европейских стран** из OpenWeather API, складывает сырые данные в объектное хранилище, загружает в облачное хранилище, трансформирует через dbt и визуализирует в Metabase.

## Архитектура

Пайплайн пишет данные **в два места параллельно**: сырой JSON в S3 (как data lake / источник истины) и структурированные строки в Postgres (аналитическое хранилище). Если хранилище потеряется — данные можно восстановить из S3.

## Стек

| Слой | Инструмент |
|---|---|
| Оркестрация | Apache Airflow 3.x (TaskFlow API) |
| Сырое хранилище | Yandex Object Storage (S3-совместимое) |
| Хранилище | Neon (serverless Postgres) |
| Трансформации | dbt (postgres adapter) |
| Визуализация | Metabase |
| Контейнеризация | Docker Compose |

## Пайплайн (Airflow DAG)

DAG (`dags/openweather_dag.py`) запускается ежечасно, построен на `@task` API:

1. **`extract`** — проходит по списку столиц (`dags/european_capitals.py`), для каждого города делает запрос к OpenWeather API по координатам, возвращает сырые ответы.
2. **`transform`** — разворачивает каждый сырой ответ в плоскую запись (температура, ощущается как, влажность, давление, скорость ветра, описание погоды), проставляет `fetched_at` в UTC.
3. **`load_to_s3`** — пишет батч в Yandex S3 в формате JSON, партиционирование `weather-data/date={date}/...`.
4. **`load_to_db`** — добавляет батч в таблицу `openweather_europe_etl` в Neon Postgres.

`extract` и `transform` выполняются последовательно; `load_to_s3` и `load_to_db` — параллельно на финальном этапе.

Секреты и конфиг (API-ключ, список городов, доступы к S3, строка подключения к БД) хранятся в **Airflow Variables**

## Модели данных (dbt)

Сырая таблица объявлена как **dbt source** (`models/staging/sources.yaml`) — это даёт lineage и проверки свежести данных. Все витрины обращаются к ней через `{{ source('weather_data', 'openweather_europe_etl') }}`.

| Модель | Описание |
|---|---|
| `latest_day_weather_average` | Средние по городам (температура, давление, влажность) за **последний день**. Самообновляется через подзапрос `max(date)` — без захардкоженных дат. |
| `weekly_temp_trend` | Агрегация температуры по городу и дню для графиков трендов. |
| `10_warmest_all_time` | Топ-10 самых жарких замеров по всем городам, дедупликация через оконную функцию `ROW_NUMBER()`. |
| `10_coldest_all_time` | Топ-10 самых холодных замеров, тот же подход с окном. |
| `humidity_pressure_stats` | Статистика влажности и давления по городам. |

Тесты данных (`not_null`, `unique`) описаны в `schema.yml`.

## Дашборд (Metabase)

- **Тренды температуры** — линейный график средней дневной температуры по городам во времени.
- **Самые жаркие / холодные столицы** — рейтинговые столбчатые диаграммы.
- **Влажность по городам** — bar chart (давление показано контекстно: оно колеблется в узком диапазоне и менее показательно для сравнения городов).

## Запуск

```bash
docker compose up -d        # поднять сервисы
# задать Airflow Variables (API-ключ, список городов, доступы S3, подключение к БД)
# включить openweather_dag — запускается ежечасно
dbt run                     # трансформации
dbt test                    # тесты данных
# подключить Metabase к Neon Postgres и открыть дашборд
```

## Инженерные решения

- **Сырой слой как страховка.** Запись сырого JSON в S3 параллельно с загрузкой в хранилище означает, что хранилище — пересобираемое представление данных, а не единственная копия.
- **Самообновляющиеся модели.** Модели «за последний день» используют `max(date)` вместо хардкора дат — пайплайн не требует ручных правок при поступлении новых данных.
- **Длинный формат вместо pivot.** Временные ряды хранятся в длинном формате (строка на город/день); преобразование в графики — задача BI-слоя, модели остаются стабильными при добавлении новых дней.
- **Компромиссы serverless.** Scale-to-zero у Neon экономит ресурсы в простое ценой короткого cold start на первом запросе — обрабатывается через встроенные retry задач Airflow.