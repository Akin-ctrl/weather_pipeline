"""
Configuration management utilities.
===================================
Centralizes configuration loading from environment variables.
"""

import os
from typing import Optional
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    host: str
    database: str
    user: str
    password: str
    
    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        """Load database config from environment variables."""
        return cls(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "weather"),
            user=os.getenv("DB_USER", "weather_user"),
            password=os.getenv("DB_PASSWORD", "")
        )


@dataclass
class APIConfig:
    """API configuration settings."""
    api_key: str
    timeout: int = 10
    max_retries: int = 3
    
    @classmethod
    def from_env(cls) -> 'APIConfig':
        """Load API config from environment variables."""
        return cls(
            api_key=os.getenv("OPENWEATHER_API_KEY", ""),
            timeout=int(os.getenv("OPENWEATHER_TIMEOUT", "10")),
            max_retries=int(os.getenv("OPENWEATHER_RETRIES", "3"))
        )


@dataclass
class PipelineConfig:
    """Pipeline configuration settings."""
    interval_minutes: int = 10
    csv_path: str = "data/nigerian_cities.csv"
    
    @classmethod
    def from_env(cls) -> 'PipelineConfig':
        """Load pipeline config from environment variables."""
        return cls(
            interval_minutes=int(os.getenv("PIPELINE_INTERVAL_MINUTES", "10")),
            csv_path=os.getenv("CSV_PATH", "data/nigerian_cities.csv")
        )


def load_config() -> None:
    """Load environment variables from .env file."""
    load_dotenv()


def validate_config() -> bool:
    """
    Validate that all required configuration is present.
    
    Returns:
        True if configuration is valid, False otherwise.
    """
    required_vars = [
        "OPENWEATHER_API_KEY",
        "DB_HOST",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD"
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        return False
    
    return True
