"""
Weather data processor for transforming API responses.
======================================================
Handles data validation, transformation, and normalization.
"""

import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from app.models.weather import WeatherReading


logger = logging.getLogger(__name__)


class DataProcessingError(Exception):
    """Raised when data processing fails."""
    pass


class WeatherDataProcessor:
    """
    Processes raw weather API data into normalized format.
    
    Transforms OpenWeatherMap JSON responses into structured
    WeatherReading objects with proper unit conversions and
    validation.
    
    Example:
        >>> processor = WeatherDataProcessor()
        >>> raw_data = {'main': {'temp': 300, 'humidity': 70}, ...}
        >>> reading = processor.process(raw_data, city_id=123)
        >>> print(reading.temperature)  # In Celsius
        26.85
    """
    
    def __init__(self) -> None:
        """Initialize the data processor."""
        logger.info("WeatherDataProcessor initialized")
    
    def process(
        self,
        raw_data: Dict[str, Any],
        city_id: int
    ) -> Optional[WeatherReading]:
        """
        Process raw API response into WeatherReading object.
        
        Args:
            raw_data: Raw JSON response from OpenWeatherMap API.
            city_id: Database city ID to associate with reading.
        
        Returns:
            WeatherReading object or None if processing fails.
        
        Raises:
            DataProcessingError: If required fields are missing.
        
        Example:
            >>> raw = {
            ...     'main': {'temp': 300, 'humidity': 70, 'pressure': 1010},
            ...     'weather': [{'main': 'Clear', 'description': 'clear sky'}],
            ...     'wind': {'speed': 5.5},
            ...     'dt': 1701878400
            ... }
            >>> reading = processor.process(raw, city_id=1)
        """
        if not raw_data:
            logger.warning("Empty raw data provided")
            return None
        
        try:
            # Extract and validate required fields
            main = raw_data.get('main', {})
            weather_list = raw_data.get('weather', [])
            wind = raw_data.get('wind', {})
            timestamp = raw_data.get('dt')
            
            if not all([main, weather_list, timestamp]):
                raise DataProcessingError("Missing required fields in raw data")
            
            # Convert temperature from Kelvin to Celsius
            temp_kelvin = main.get('temp')
            if temp_kelvin is None:
                raise DataProcessingError("Temperature field missing")
            temperature = round(temp_kelvin - 273.15, 2)
            
            # Extract weather description
            weather_info = weather_list[0] if weather_list else {}
            
            # Create WeatherReading object
            reading = WeatherReading(
                city_id=city_id,
                temperature=temperature,
                humidity=main.get('humidity', 0),
                pressure=main.get('pressure', 0),
                wind_speed=wind.get('speed', 0.0),
                weather_main=weather_info.get('main', 'Unknown'),
                weather_desc=weather_info.get('description', 'No description'),
                reading_timestamp=datetime.fromtimestamp(timestamp, tz=timezone.utc)
            )
            
            logger.debug(f"Processed reading for city_id={city_id}: {temperature}°C")
            return reading
            
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"Data processing error for city_id={city_id}: {e}")
            raise DataProcessingError(f"Failed to process data: {e}")
        except Exception as e:
            logger.error(f"Unexpected error processing data: {e}")
            return None
    
    def validate_reading(self, reading: WeatherReading) -> bool:
        """
        Validate a weather reading for data quality.
        
        Args:
            reading: WeatherReading object to validate.
        
        Returns:
            True if reading passes validation, False otherwise.
        
        Example:
            >>> reading = WeatherReading(...)
            >>> is_valid = processor.validate_reading(reading)
        """
        try:
            # Temperature range check (-50 to 60 Celsius)
            if not -50 <= reading.temperature <= 60:
                logger.warning(f"Temperature out of range: {reading.temperature}°C")
                return False
            
            # Humidity range check (0 to 100%)
            if not 0 <= reading.humidity <= 100:
                logger.warning(f"Humidity out of range: {reading.humidity}%")
                return False
            
            # Pressure range check (900 to 1100 hPa)
            if not 900 <= reading.pressure <= 1100:
                logger.warning(f"Pressure out of range: {reading.pressure} hPa")
                return False
            
            # Wind speed check (0 to 100 m/s)
            if not 0 <= reading.wind_speed <= 100:
                logger.warning(f"Wind speed out of range: {reading.wind_speed} m/s")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False
    
    def __repr__(self) -> str:
        return "WeatherDataProcessor()"
