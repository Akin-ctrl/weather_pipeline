# Weather Pipeline

A production-ready, object-oriented weather data ETL pipeline and dashboard for Nigerian cities.

## Project Overview

This project demonstrates professional data engineering practices with a clean OOP architecture, featuring:
- Real-time weather data ingestion from OpenWeatherMap API
- PostgreSQL database with star schema design
- Interactive Streamlit dashboard 
- Docker containerization for easy deployment
- Full type hints and comprehensive documentation

## Project Structure

```
weather_pipeline/
├── app/                        # Main application package
│   ├── core/                   # Core business logic
│   │   ├── api_client.py      # WeatherAPIClient class
│   │   ├── database.py        # DatabaseManager with connection pooling
│   │   ├── pipeline.py        # WeatherPipeline orchestrator
│   │   ├── processor.py       # WeatherDataProcessor
│   │   └── seeder.py          # DatabaseSeeder
│   ├── models/                 # Data models
│   │   └── weather.py         # WeatherReading, City dataclasses
│   └── utils/                  # Utility modules
│       ├── config.py          # Configuration management
│       └── dashboard_service.py # Dashboard data service
├── data/                       # Data files
│   ├── nigerian_cities.csv    # 523 Nigerian cities with coordinates
│   └── city.list.json         # OpenWeather city list
├── sql/                        # SQL scripts
│   └── init.sql               # Database schema initialization
├── run_pipeline.py            # Pipeline entry point
├── run_seeder.py              # Seeder entry point
├── run_dashboard.py           # Dashboard entry point
├── docker-compose.yml         # Multi-service orchestration
├── Dockerfile_pipeline        # Container image definition
└── requirements.txt           # Python dependencies
```

## Architecture

### Core Components

**1. DatabaseManager** (`app/core/database.py`)
- Connection pooling (configurable min/max connections)
- Context manager protocol for safe connection handling
- Automatic transaction management (commit/rollback)
- Type-hinted methods with comprehensive docstrings
- Custom exception: `DatabaseConnectionError`

**2. WeatherAPIClient** (`app/core/api_client.py`)
- Retry logic with exponential backoff
- HTTP session management with connection pooling
- Timeout handling and error recovery
- HTTP status code error handling (401, 404, 429, 5xx)
- Custom exception: `WeatherAPIError`

**3. WeatherDataProcessor** (`app/core/processor.py`)
- Data validation and transformation
- Temperature unit conversion (Kelvin → Celsius)
- Quality checks for sensor readings (temperature, humidity, pressure, wind speed)
- Timestamp handling (UTC timezone)
- Custom exception: `DataProcessingError`

**4. WeatherPipeline** (`app/core/pipeline.py`)
- Orchestrates the entire ETL process
- Scheduled execution with configurable intervals
- Success/failure tracking and comprehensive logging
- Resource cleanup on exit
- Context manager support

**5. DatabaseSeeder** (`app/core/seeder.py`)
- Idempotent CSV → database loading (TRUNCATE CASCADE)
- Coordinate parsing with validation
- Duplicate detection
- Transaction safety

**6. DashboardDataService** (`app/utils/dashboard_service.py`)
- Separation of data fetching from UI rendering
- Fallback to CSV when database unavailable
- Caching support for Streamlit

**7. Data Models** (`app/models/weather.py`)
- `WeatherReading` dataclass with type hints
- `City` dataclass for city metadata
- `to_dict()` methods for database insertion

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.12+ (for local development)
- OpenWeatherMap API key ([Get one here](https://openweathermap.org/api))

### Environment Setup

Create a `.env` file:

```bash
# Database
DB_HOST=database
DB_NAME=weather
DB_USER=weather_user
DB_PASSWORD=supersecret

# OpenWeatherMap API
OPENWEATHER_API_KEY=your_api_key_here
OPENWEATHER_TIMEOUT=10
OPENWEATHER_RETRIES=3

# Pipeline Configuration
PIPELINE_INTERVAL_MINUTES=10
CSV_PATH=data/nigerian_cities.csv

# pgAdmin (Optional)
PGADMIN_DEFAULT_EMAIL=admin@example.com
PGADMIN_DEFAULT_PASSWORD=admin
```

### Running with Docker (Recommended)

```bash
# Start all services
docker compose up --build -d

# View logs
docker compose logs -f pipeline
docker compose logs -f dashboard

# Stop services
docker compose down
```

**Services:**
- Dashboard: http://localhost:8501
- pgAdmin: http://localhost:5050
- PostgreSQL: localhost:5432

### Running Locally

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Ensure PostgreSQL is running and execute sql/init.sql

# Seed the database
python run_seeder.py

# Start the pipeline
python run_pipeline.py &

# Start the dashboard
streamlit run run_dashboard.py
```

## Database Schema

**Star Schema Design:**

```sql
-- Dimension Tables
geo_political_zones (id, zone_name)
    ↓
states (id, state_name, zone_id)
    ↓
cities (id, city_name, city_id, state_id, latitude, longitude)
    ↓
-- Fact Table
weather_readings (
    id, city_id, temperature, humidity, pressure,
    wind_speed, weather_main, weather_desc,
    reading_timestamp, created_at
)
```

**Performance Features:**
- Composite index on `(city_id, reading_timestamp DESC)` for fast queries
- Foreign key constraints for referential integrity
- BIGSERIAL for weather_readings to handle millions of rows

## Dashboard Features

- **Real-time Data**: Auto-refresh every 60 seconds
- **Hierarchical Navigation**: Zone → State → City selection
- **Interactive Maps**: Lat/lon visualization for cities
- **Fallback Mode**: CSV-based filtering when DB is unavailable
- **Responsive Layout**: 3-column grid for major cities
- **Latest Readings**: Current weather conditions with timestamps

## Key Design Principles

### OOP Best Practices
- **Single Responsibility**: Each class has one well-defined purpose
- **Dependency Injection**: Dependencies passed to constructors
- **Context Managers**: Automatic resource cleanup (`__enter__`, `__exit__`)
- **Type Safety**: Full type hints throughout codebase
- **Custom Exceptions**: Specific error classes for different failure modes
- **Comprehensive Docstrings**: Google-style documentation

### Error Handling
- Custom exception classes for specific error scenarios
- Graceful degradation (CSV fallback when DB unavailable)
- Comprehensive logging at all levels (DEBUG, INFO, WARNING, ERROR)
- Automatic retry logic with exponential backoff

### Resource Management
- Connection pooling to prevent leaks
- Context managers for automatic cleanup
- Session management for HTTP requests
- Proper transaction handling (commit/rollback)

### Configuration
- Environment-based config (12-factor app)
- Centralized config classes
- Validation before startup
- Sensible defaults with override options

## Data Flow

```
CSV File → DatabaseSeeder → PostgreSQL
                                ↓
OpenWeatherMap API → WeatherAPIClient → WeatherDataProcessor → DatabaseManager → PostgreSQL
                                                                                        ↓
                                                                                  DashboardDataService → Streamlit UI
```


## Troubleshooting

**Database connection fails:**
```bash
# Check if PostgreSQL is running
docker compose ps database

# View database logs
docker compose logs database

# Restart database service
docker compose restart database
```

**API errors (401 Unauthorized):**
- Verify `OPENWEATHER_API_KEY` in `.env`
- Check API key permissions on OpenWeatherMap dashboard
- Ensure API key is active

**Dashboard shows no readings:**
- Ensure pipeline has run at least once
- Check pipeline logs: `docker compose logs pipeline`
- Verify database has data: Connect via pgAdmin

**Pipeline not updating:**
- Check API rate limits
- Verify network connectivity
- Review pipeline logs for errors


## Testing

### Verification Tests
```bash
# Test imports
python -c "from app.core.database import DatabaseManager; print('✓ DatabaseManager')"
python -c "from app.core.api_client import WeatherAPIClient; print('✓ WeatherAPIClient')"
python -c "from app.core.pipeline import WeatherPipeline; print('✓ WeatherPipeline')"
python -c "from app.core.seeder import DatabaseSeeder; print('✓ DatabaseSeeder')"
python -c "from app.utils.dashboard_service import DashboardDataService; print('✓ DashboardDataService')"

# Test database connection
python -c "from app.core.database import DatabaseManager; import os; db = DatabaseManager(os.getenv('DB_HOST', 'localhost'), os.getenv('DB_NAME', 'weather'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD')); print('✓ DB connected')"
```


## Contributing

Contributions are welcome! Please follow these guidelines:
1. Fork the repository
2. Create a feature branch
3. Follow the code style guidelines
4. Add appropriate tests
5. Submit a pull request



## Author

**Akin-ctrl**


## Acknowledgments

- [OpenWeatherMap](https://openweathermap.org/) for weather data API
- [Streamlit](https://streamlit.io/) for dashboard framework
- [PostgreSQL](https://www.postgresql.org/) for robust data storage
- Nigerian cities dataset for comprehensive coverage

---

**Built using Python, PostgreSQL, Docker, and Streamlit**
