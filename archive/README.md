# weather_pipeline

A small, container-friendly weather data ETL pipeline and dashboard focused on Nigerian cities.

This repository demonstrates a pragmatic data engineering workflow: ingesting city lists, fetching weather observations from OpenWeather, storing time-series readings in Postgres, and serving a Streamlit dashboard for exploration. The project is intentionally lightweight and designed for local development, testing, and Docker-based deployments.

## Contents

- `pipeline.py` — Main pipeline process. Loads cities, fetches weather (group or single-city), transforms the payloads, and writes to the DB.
- `load_database.py` — Helper script to populate/seed the `cities`, `states`, and `geo_political_zones` tables from CSV or other sources.
- `db.py` — Database helpers: connection pool, context manager, and an idempotent index-creation helper used to deduplicate readings.
- `dashboard.py` — Streamlit-based dashboard that reads from the Postgres DB (or falls back to `nigerian_cities.csv`) and displays latest readings.
- `nigerian_cities.csv` — CSV of Nigerian cities used by the pipeline and dashboard for lookup/fallback.
- `city.list.json` — Optional city list (commonly used for mapping/prefetching city metadata).
- `init.sql` — Database DDL used to create the schema (tables and indexes) for Postgres.
- `Dockerfile_pipeline` and `Dockerfile_dashboard` — Container images for the pipeline/loader and the dashboard respectively.
- `docker-compose.yml` — Orchestrates Postgres, pgAdmin, loader/pipeline, and dashboard services for local development.
- `tests/` — Unit tests (e.g., `tests/test_process_weather_data.py`) covering processing logic.

## High-level architecture

- The pipeline runs periodically (configured via environment variable). At each run it:
	1. Loads a list of cities (either from the seeded Postgres tables or from `nigerian_cities.csv`).
	2. Attempts a batched group request (OpenWeather `group` endpoint) for efficiency. If the API key is not allowed for group endpoints, the pipeline falls back to single-city `/weather?q=` calls.
	3. Processes the API JSON into a normalized reading (temperature in °C, humidity, pressure, wind speed, description, UTC timestamp) and stores it into Postgres using an upsert strategy.
	4. The dashboard queries the database and displays the latest readings per city.

This separation keeps ETL logic (pipeline + loader) independent from presentation (dashboard).

## Visual data architecture

Below is a simple ASCII diagram that shows the main components and the data flow through the system.

```text
										 +-----------------------------+
										 | nigerian_cities.csv        |
										 | (city.list.json optional)  |
										 +-------------+---------------+
															|
															v
						  +----------------+   seed/load   +--------------------+
						  | load_database.py|-------------->|   Postgres DB      |
						  |  (seeding)     |               | (cities, readings) |
						  +----------------+               +---------+----------+
																				  ^    ^
																				  |    |
												  read / write (upserts)  |    |  read
																				  |    |  (latest readings)
				 +----------------+     fetch & transform         |    |
				 |  pipeline.py   |--------------------------------+    |
				 |  (ETL job)     |   (OpenWeather API calls)         |
				 +----------------+                                     |
						|  ^                                                 |
						|  |                                                 |
						|  +--------------------------------------+          |
						|                                         |         |
						v                                         |         v
		  OpenWeather API   <--- group (batch) or single (q=)  |   +-------------+
	  (external service)                                  dashboard.py |
																			 (Streamlit)  |
																							 +-------------+
```

Explanation of components and flow
- `nigerian_cities.csv` / `city.list.json`: canonical city lists used to seed the DB or to fall back when the DB is unavailable. Contains city ids, names, coordinates, and metadata.
- `load_database.py`: one-time or on-demand seeding script that reads CSV/json and populates `geo_political_zones`, `states`, and `cities` tables in Postgres.
- Postgres DB: the central store. Two main areas: dimension tables (`geo_political_zones`, `states`, `cities`) and the `weather_readings` fact table that contains time-series observations.
- `pipeline.py`: scheduled ETL job that queries OpenWeather (batch group endpoint if available, otherwise single-city requests), transforms the API JSON into normalized readings, and writes/upserts into `weather_readings`.
- OpenWeather API: external data provider. The pipeline handles 429 (rate limiting) with backoff and falls back from group to single queries on 401 errors for group endpoint access.
- `dashboard.py`: Streamlit app that reads the latest values from Postgres (or falls back to `nigerian_cities.csv` if the DB is unreachable) and presents them to users; it auto-refreshes every minute if enabled.

Data contracts and shapes
- Seeding CSV / city list: expected columns include `id`, `name`, `country`, `coord` (a lon/lat mapping), `geopolitical_zone`, and `state`.
- Pipeline -> OpenWeather response: pipeline expects the standard OpenWeather JSON with keys like `main.temp`, `main.humidity`, `wind.speed`, `weather[0].main`, `weather[0].description`, and `dt` (epoch).
- Normalized DB row (`weather_readings`): { city_id (FK), temperature (°C float), humidity (int), pressure (int), wind_speed (float), weather_main (str), weather_desc (str), reading_timestamp (timestamptz) }

Error modes and mitigations
- API auth (401): pipeline detects 401 and falls back to single-city queries or logs and skips depending on the failure; do not retry auth failures indefinitely.
- Rate limiting (429): pipeline respects `Retry-After` and uses exponential backoff with jitter.
- DB down: dashboard falls back to CSV for UI filters; pipeline waits for DB readiness on startup and exits if DB remains unavailable after retries.

This visual and textual summary should help onboard new contributors and clarify where to extend or instrument the system (e.g., add metrics, alerts, or caching layers).

## Quick start — prerequisites

- Python 3.11+ (project uses modern idioms; Docker images are based on Python 3.12-slim).
- Docker & Docker Compose (for containerized run).
- A Postgres instance (local or via Docker Compose in this repo).

Install dependencies (recommended in a virtualenv):

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Environment variables

Create a `.env` file in the project root (it's loaded by python-dotenv). Example:

```ini
# Postgres
DB_HOST=database
DB_NAME=weather
DB_USER=weather_user
DB_PASSWORD=supersecret

# OpenWeather
OPENWEATHER_API_KEY=your_openweather_api_key_here

# Pipeline scheduling
PIPELINE_INTERVAL_MINUTES=10
OPENWEATHER_RETRIES=3
OPENWEATHER_TIMEOUT=10
```

Never commit real secrets to git. Use `.gitignore` for `.env`.

## Running locally (without Docker)

1. Ensure Postgres is running and `init.sql` has been executed to create tables (or run `load_database.py` which will seed data).
2. Start the pipeline locally:

```bash
python weather_pipeline/pipeline.py
```

3. Start the dashboard in a separate terminal:

```bash
streamlit run dashboard.py
```

Notes:
- The dashboard includes an Auto-refresh checkbox that will cause the page to re-run every minute (powered by `streamlit-autorefresh`); install it via `pip install streamlit-autorefresh`.
- Tests can be run with `pytest -q` after you install dev dependencies.

## Running with Docker Compose

This repo includes a `docker-compose.yml` that sets up Postgres, pgAdmin, loader, pipeline, and dashboard. The compose file builds two images using `weather_pipeline/Dockerfile_pipeline` and `weather_pipeline/Dockerfile_dashboard`.

Start the stack:

```bash
docker compose up --build -d
docker compose logs -f
```

Key notes:
- The compose file expects environment variables to come from a `.env` file in the repo root or the shell environment.
- The loader service runs `load_database.py` to seed the DB, the pipeline service runs `pipeline.py`, and the dashboard runs Streamlit on port 8501.

## Database schema

`init.sql` contains the schema. Main tables:
- `geo_political_zones` (id, zone_name)
- `states` (id, state_name, zone_id)
- `cities` (id, city_name, city_id, state_id, latitude, longitude)
- `weather_readings` (id, city_id, temperature, humidity, pressure, wind_speed, weather_main, weather_desc, reading_timestamp, created_at)

An idempotent unique index is created on `(city_id, reading_timestamp)` to help deduplicate inserts. `db.py` provides a helper `ensure_unique_index()` called at pipeline startup.

## How the pipeline handles API quirks

- Group requests: the pipeline attempts batching (up to 20 city ids per group request). This is much faster but some OpenWeather API keys or subscription plans may not allow group calls. When the group endpoint returns `401 Unauthorized`, the pipeline now falls back to single-city `/weather?q=` requests.
- Retries: Network and rate-limit (429) handling uses exponential backoff with jitter; authorization errors (401) short-circuit retries.
- Idempotency: writes use `ON CONFLICT` upsert on `(city_id, reading_timestamp)` so repeated runs don't create duplicates.

## Dashboard behavior and refresh

- The dashboard (`dashboard.py`) uses Streamlit cached helper functions (e.g., `@st.cache_data`) to avoid excessive DB queries. For near-real-time behavior the dashboard includes an Auto-refresh feature that re-runs the app every minute and refreshes cached data.
- If the DB is unreachable the dashboard falls back to listing cities from `nigerian_cities.csv` for filtering and navigation; readings won't be available in that mode.

## Testing

Run unit tests with pytest:

```bash
pytest -q
```

There are tests focused on processing logic (convert temperature, timestamp handling, missing-key behavior). Add more tests to cover DB integration (mock the pool) and edge cases.

## Development notes & common troubleshooting

- 401 Unauthorized from OpenWeather:
	- Confirm `OPENWEATHER_API_KEY` is set in your environment or `.env`.
	- Verify the key's permissions on OpenWeather (group endpoint vs single-city endpoint).
	- Avoid logging full URLs with `appid` included to prevent secret leakage.

- DB connection issues:
	- Ensure Postgres service is running and credentials in `.env` match.
	- The pipeline waits for the DB to become ready and will fail if it cannot connect after several retries.

- Docker build COPY errors:
	- Dockerfile COPY paths are relative to the build context set in `docker-compose.yml`. If you change the compose build context, update `COPY` instructions accordingly or move files into the build context.

## Next improvements (ideas)

- Add a CI workflow (GitHub Actions) to run linters and tests on push.
- Add a `test` docker-compose service to run pytest against a test Postgres DB.
- Add monitoring/metrics (Prometheus) for pipeline success/failure counts and API error rates.
- Improve matching between CSV city names and DB records (fuzzy matching or canonicalization) to reduce skipped stores.

## Contributing

Contributions are welcome. Open an issue or a pull request describing the change, include tests and update `requirements.txt` if you add dependencies.

## License

This project does not include an explicit license file. Add a `LICENSE` file (e.g., MIT) if you want to make this project open source.

## Contact

For questions about this repository, reach out to the owner/maintainer in the project metadata or open an issue in this repository.
