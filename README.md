# Weather Pipeline - OOP Refactored Version

A production-ready, object-oriented weather data ETL pipeline and dashboard for Nigerian cities.

## Project Overview

This project demonstrates professional data engineering practices with a clean OOP architecture, featuring:
- Real-time weather data ingestion from OpenWeatherMap API
- PostgreSQL database with star schema design
- Interactive Streamlit dashboard with modern UI
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
│   └── nigerian_cities.csv    # 523 Nigerian cities with coordinates
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

### OOP Design Principles

**1. DatabaseManager** (`app/core/database.py`)
- Connection pooling for efficient resource usage
- Context manager protocol for safe connection handling
- Type-hinted methods with comprehensive docstrings
- Automatic transaction management (commit/rollback)

**2. WeatherAPIClient** (`app/core/api_client.py`)
- Retry logic with exponential backoff
- HTTP session management
- Custom exception handling
- Rate limiting support

**3. WeatherDataProcessor** (`app/core/processor.py`)
- Data validation and transformation
- Temperature unit conversion (Kelvin → Celsius)
- Quality checks for sensor readings
- Structured error handling

**4. WeatherPipeline** (`app/core/pipeline.py`)
- Orchestrates the entire ETL process
- Scheduled execution with configurable intervals
- Comprehensive logging and metrics
- Resource cleanup on exit

**5. DatabaseSeeder** (`app/core/seeder.py`)
- Idempotent CSV → database loading
- Coordinate parsing with validation
- Duplicate detection
- Transaction safety

**6. DashboardDataService** (`app/utils/dashboard_service.py`)
- Separation of data fetching from UI rendering
- Fallback to CSV when database unavailable
- Caching support for Streamlit

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.12+ (for local development)
- OpenWeatherMap API key

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

# pgAdmin
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
- Composite index on `(city_id, reading_timestamp DESC)`
- Foreign key constraints for referential integrity
- BIGSERIAL for weather_readings to handle millions of rows

## Dashboard Features

- **Real-time Data**: Auto-refresh every 60 seconds
- **Hierarchical Navigation**: Zone → State → City
- **Interactive Maps**: Lat/lon visualization for cities
- **Fallback Mode**: CSV-based filtering when DB is unavailable
- **Responsive Layout**: 3-column grid for major cities

## Key Features

### Error Handling
- Custom exception classes (`DatabaseConnectionError`, `WeatherAPIError`, `DataProcessingError`)
- Graceful degradation (CSV fallback)
- Comprehensive logging at all levels

### Type Safety
- Full type hints throughout codebase
- Dataclasses for structured data (`WeatherReading`, `City`)
- Optional types for nullable fields

### Resource Management
- Context managers for automatic cleanup
- Connection pooling to prevent leaks
- Session management for HTTP requests

### Configuration
- Environment-based config (12-factor app)
- Centralized config classes (`DatabaseConfig`, `APIConfig`, `PipelineConfig`)
- Validation before startup

## 📈 Data Flow

```
CSV File → DatabaseSeeder → PostgreSQL
                                ↓
OpenWeatherMap API → WeatherAPIClient → WeatherDataProcessor → DatabaseManager → PostgreSQL
                                                                                        ↓
                                                                                  DashboardDataService → Streamlit UI
```

## 🔧 Development

### Adding New Features

**1. New Data Model:**
```python
# app/models/your_model.py
from dataclasses import dataclass

@dataclass
class YourModel:
    field1: str
    field2: int
```

**2. New Service:**
```python
# app/core/your_service.py
class YourService:
    def __init__(self, db: DatabaseManager):
        self._db = db
    
    def your_method(self) -> None:
        """Docstring with type hints."""
        pass
```

### Code Style
- Type hints on all functions
- Docstrings (Google style)
- Context managers for resource cleanup
- Custom exceptions for specific error cases

## Troubleshooting

**Database connection fails:**
```bash
# Check if PostgreSQL is running
docker compose ps database

# View database logs
docker compose logs database
```

**API errors (401):**
- Verify `OPENWEATHER_API_KEY` in `.env`
- Check API key permissions on OpenWeatherMap

**Dashboard shows no readings:**
- Ensure pipeline has run at least once
- Check pipeline logs: `docker compose logs pipeline`

## Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | Database host | `localhost` |
| `DB_NAME` | Database name | `weather` |
| `DB_USER` | Database user | `weather_user` |
| `DB_PASSWORD` | Database password | Required |
| `OPENWEATHER_API_KEY` | API key | Required |
| `OPENWEATHER_TIMEOUT` | Request timeout (s) | `10` |
| `OPENWEATHER_RETRIES` | Max retries | `3` |
| `PIPELINE_INTERVAL_MINUTES` | Schedule interval | `10` |
| `CSV_PATH` | Cities CSV path | `data/nigerian_cities.csv` |

## Future Enhancements

- [ ] Add pytest test suite
- [ ] Implement GitHub Actions CI/CD
- [ ] Add Prometheus metrics
- [ ] Implement data retention policies
- [ ] Add async API calls for parallel processing
- [ ] Create Grafana dashboards
- [ ] Add alerting system
- [ ] Implement caching layer (Redis)


## Author

**Akin-ctrl**
- GitHub: [@Akin-ctrl](https://github.com/Akin-ctrl)
- Repository: [weather_pipeline](https://github.com/Akin-ctrl/weather_pipeline)

## Acknowledgments

- OpenWeatherMap for weather data API
- Streamlit for dashboard framework
- PostgreSQL for robust data storage
