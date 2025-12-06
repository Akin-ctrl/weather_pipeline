"""
Weather data models and schemas.
================================
Contains data classes for weather readings and city information.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class WeatherReading:
    """
    Represents a single weather observation.
    
    Attributes:
        city_id: Database ID of the city.
        temperature: Temperature in Celsius.
        humidity: Relative humidity percentage.
        pressure: Atmospheric pressure in hPa.
        wind_speed: Wind speed in m/s.
        weather_main: Main weather condition (e.g., 'Rain', 'Clear').
        weather_desc: Detailed weather description.
        reading_timestamp: UTC timestamp of the reading.
    """
    city_id: int
    temperature: float
    humidity: int
    pressure: int
    wind_speed: float
    weather_main: str
    weather_desc: str
    reading_timestamp: datetime
    
    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion."""
        return {
            'city_id': self.city_id,
            'temperature': self.temperature,
            'humidity': self.humidity,
            'pressure': self.pressure,
            'wind_speed': self.wind_speed,
            'weather_main': self.weather_main,
            'weather_desc': self.weather_desc,
            'reading_timestamp': self.reading_timestamp
        }


@dataclass
class City:
    """
    Represents a city with geographic information.
    
    Attributes:
        id: Database primary key.
        name: City name.
        city_id: OpenWeatherMap city ID.
        state_id: Foreign key to states table.
        latitude: Geographic latitude.
        longitude: Geographic longitude.
    """
    id: int
    name: str
    city_id: int
    state_id: int
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    def __repr__(self) -> str:
        return f"City(id={self.id}, name='{self.name}')"
