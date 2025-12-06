"""
Weather ETL Pipeline orchestrator.
==================================
Main pipeline class that coordinates data extraction, transformation, and loading.
"""

import logging
import time
from typing import Dict, List, Optional
import schedule
from app.core.database import DatabaseManager, DatabaseConnectionError
from app.core.api_client import WeatherAPIClient, WeatherAPIError
from app.core.processor import WeatherDataProcessor, DataProcessingError
from app.models.weather import WeatherReading


logger = logging.getLogger(__name__)


class WeatherPipeline:
    """
    Orchestrates the weather data ETL process.
    
    Coordinates fetching weather data from API, processing it,
    and storing in the database. Runs on a configurable schedule.
    
    Attributes:
        _db: DatabaseManager instance.
        _api_client: WeatherAPIClient instance.
        _processor: WeatherDataProcessor instance.
        _interval_minutes: Schedule interval in minutes.
    
    Example:
        >>> pipeline = WeatherPipeline(
        ...     db_host="localhost",
        ...     db_name="weather",
        ...     db_user="user",
        ...     db_password="pass",
        ...     api_key="your_key",
        ...     interval_minutes=10
        ... )
        >>> pipeline.run_once()  # Single execution
        >>> pipeline.run_scheduled()  # Continuous execution
    """
    
    def __init__(
        self,
        db_host: str,
        db_name: str,
        db_user: str,
        db_password: str,
        api_key: str,
        interval_minutes: int = 10
    ) -> None:
        """
        Initialize the weather pipeline.
        
        Args:
            db_host: Database host address.
            db_name: Database name.
            db_user: Database username.
            db_password: Database password.
            api_key: OpenWeatherMap API key.
            interval_minutes: Schedule interval in minutes.
        
        Raises:
            DatabaseConnectionError: If database connection fails.
            ValueError: If API key is invalid.
        """
        self._db = DatabaseManager(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
        
        self._api_client = WeatherAPIClient(api_key=api_key)
        self._processor = WeatherDataProcessor()
        self._interval_minutes = interval_minutes
        
        logger.info(f"WeatherPipeline initialized (interval={interval_minutes} min)")
    
    def load_cities_from_db(self) -> Dict[str, int]:
        """
        Load city names and IDs from the database.
        
        Returns:
            Dictionary mapping city names to database IDs.
        
        Raises:
            DatabaseConnectionError: If query fails.
        
        Example:
            >>> cities = pipeline.load_cities_from_db()
            >>> print(cities)
            {'Lagos': 1, 'Abuja': 2, ...}
        """
        query = "SELECT id, city_name FROM cities;"
        
        try:
            results = self._db.execute_query(query, fetch_all=True)
            
            if not results:
                logger.warning("No cities found in database")
                return {}
            
            cities_map = {row['city_name']: row['id'] for row in results}
            logger.info(f"Loaded {len(cities_map)} cities from database")
            return cities_map
            
        except DatabaseConnectionError as e:
            logger.error(f"Failed to load cities: {e}")
            raise
    
    def store_weather_reading(self, reading: WeatherReading) -> bool:
        """
        Store a weather reading in the database.
        
        Args:
            reading: WeatherReading object to store.
        
        Returns:
            True if storage successful, False otherwise.
        """
        query = """
            INSERT INTO weather_readings 
            (city_id, temperature, humidity, pressure, wind_speed, 
             weather_main, weather_desc, reading_timestamp)
            VALUES (%(city_id)s, %(temperature)s, %(humidity)s, %(pressure)s, 
                    %(wind_speed)s, %(weather_main)s, %(weather_desc)s, %(reading_timestamp)s)
            ON CONFLICT DO NOTHING;
        """
        
        try:
            self._db.execute_query(query, params=reading.to_dict(), fetch_all=False)
            logger.info(f"Stored reading for city_id={reading.city_id}")
            return True
            
        except DatabaseConnectionError as e:
            logger.error(f"Failed to store reading for city_id={reading.city_id}: {e}")
            return False
    
    def process_city(self, city_name: str, city_id: int) -> bool:
        """
        Fetch, process, and store weather data for a single city.
        
        Args:
            city_name: Name of the city.
            city_id: Database ID of the city.
        
        Returns:
            True if successful, False otherwise.
        """
        try:
            # Fetch raw data from API
            raw_data = self._api_client.fetch_weather_by_city(city_name)
            
            if not raw_data:
                logger.warning(f"No data returned for {city_name}")
                return False
            
            # Process raw data
            reading = self._processor.process(raw_data, city_id)
            
            if not reading:
                logger.warning(f"Failed to process data for {city_name}")
                return False
            
            # Validate reading
            if not self._processor.validate_reading(reading):
                logger.warning(f"Invalid reading for {city_name}")
                return False
            
            # Store in database
            return self.store_weather_reading(reading)
            
        except Exception as e:
            logger.error(f"Error processing {city_name}: {e}")
            return False
    
    def run_once(self) -> None:
        """
        Execute the pipeline once for all cities.
        
        Fetches weather data for all cities in the database,
        processes it, and stores the results.
        """
        logger.info("Starting weather pipeline job...")
        start_time = time.time()
        
        try:
            # Load cities from database
            cities_map = self.load_cities_from_db()
            
            if not cities_map:
                logger.error("No cities to process. Exiting.")
                return
            
            # Process each city
            success_count = 0
            failure_count = 0
            
            for city_name, city_id in cities_map.items():
                if self.process_city(city_name, city_id):
                    success_count += 1
                else:
                    failure_count += 1
            
            elapsed = time.time() - start_time
            logger.info(
                f"Pipeline job completed in {elapsed:.2f}s: "
                f"{success_count} succeeded, {failure_count} failed"
            )
            
        except Exception as e:
            logger.error(f"Pipeline job failed: {e}")
    
    def run_scheduled(self) -> None:
        """
        Run the pipeline on a continuous schedule.
        
        Executes immediately, then repeats at the configured interval.
        Runs indefinitely until interrupted.
        """
        # Schedule the job
        schedule.every(self._interval_minutes).minutes.do(self.run_once)
        
        logger.info(f"Scheduler started (interval={self._interval_minutes} minutes)")
        
        # Run immediately
        logger.info("Running first job immediately...")
        self.run_once()
        
        # Continuous execution
        logger.info("Waiting for next scheduled run...")
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    def close(self) -> None:
        """Clean up resources."""
        self._db.close()
        self._api_client.close()
        logger.info("Pipeline resources cleaned up")
    
    def __enter__(self):
        """Support context manager protocol."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up on context manager exit."""
        self.close()
    
    def __repr__(self) -> str:
        return f"WeatherPipeline(interval={self._interval_minutes}min)"
