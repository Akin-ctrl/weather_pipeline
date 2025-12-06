"""
Weather API client with retry logic and error handling.
=======================================================
Handles all interactions with the OpenWeatherMap API.
"""

import logging
import time
from typing import Optional, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = logging.getLogger(__name__)


class WeatherAPIError(Exception):
    """Raised when API request fails."""
    pass


class WeatherAPIClient:
    """
    Client for OpenWeatherMap API with retry logic.
    
    Handles API authentication, request retries, rate limiting,
    and error recovery for weather data fetching.
    
    Attributes:
        _api_key: OpenWeatherMap API key.
        _base_url: API base URL.
        _timeout: Request timeout in seconds.
        _session: Requests session with retry configuration.
    
    Example:
        >>> client = WeatherAPIClient(api_key="your_key")
        >>> data = client.fetch_weather_by_city("Lagos")
        >>> print(data['main']['temp'])
    """
    
    def __init__(
        self,
        api_key: str,
        timeout: int = 10,
        max_retries: int = 3
    ) -> None:
        """
        Initialize the weather API client.
        
        Args:
            api_key: OpenWeatherMap API key.
            timeout: Request timeout in seconds.
            max_retries: Maximum number of retry attempts.
        
        Raises:
            ValueError: If api_key is empty.
        """
        if not api_key:
            raise ValueError("API key cannot be empty")
        
        self._api_key = api_key
        self._base_url = "http://api.openweathermap.org/data/2.5/weather"
        self._timeout = timeout
        
        # Configure session with retry strategy
        self._session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        
        logger.info(f"WeatherAPIClient initialized with {max_retries} max retries")
    
    def fetch_weather_by_city(self, city_name: str) -> Optional[Dict[str, Any]]:
        """
        Fetch weather data for a specific city.
        
        Args:
            city_name: Name of the city.
        
        Returns:
            JSON response dict or None if request fails.
        
        Raises:
            WeatherAPIError: If API request fails after retries.
        
        Example:
            >>> data = client.fetch_weather_by_city("Lagos")
            >>> temp = data['main']['temp']
        """
        if not city_name:
            raise ValueError("City name cannot be empty")
        
        params = {
            'q': city_name,
            'appid': self._api_key
        }
        
        try:
            logger.debug(f"Fetching weather for {city_name}")
            response = self._session.get(
                self._base_url,
                params=params,
                timeout=self._timeout
            )
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched weather for {city_name}")
            return data
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"City not found: {city_name}")
                return None
            elif e.response.status_code == 401:
                logger.error("Invalid API key")
                raise WeatherAPIError(f"Authentication failed: {e}")
            else:
                logger.error(f"HTTP error for {city_name}: {e}")
                raise WeatherAPIError(f"API request failed: {e}")
                
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {city_name}")
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {city_name}: {e}")
            return None
    
    def fetch_weather_by_id(self, city_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch weather data by OpenWeatherMap city ID.
        
        Args:
            city_id: OpenWeatherMap city ID.
        
        Returns:
            JSON response dict or None if request fails.
        """
        params = {
            'id': city_id,
            'appid': self._api_key
        }
        
        try:
            response = self._session.get(
                self._base_url,
                params=params,
                timeout=self._timeout
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for city_id {city_id}: {e}")
            return None
    
    def close(self) -> None:
        """Close the session and clean up resources."""
        if hasattr(self, '_session'):
            self._session.close()
            logger.info("API client session closed")
    
    def __enter__(self):
        """Support context manager protocol."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up on context manager exit."""
        self.close()
    
    def __repr__(self) -> str:
        return f"WeatherAPIClient(timeout={self._timeout}s)"
