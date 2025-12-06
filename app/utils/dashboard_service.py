"""
Dashboard data service for fetching weather data.
=================================================
Handles all database queries for the dashboard.
"""

import logging
from typing import List, Dict, Optional, Any
import pandas as pd
import ast
from app.core.database import DatabaseManager


logger = logging.getLogger(__name__)


class DashboardDataService:
    """
    Provides data fetching methods for the dashboard.
    
    Handles all database queries with fallback to CSV data
    when database is unavailable.
    
    Attributes:
        _db: DatabaseManager instance or None.
        _csv_path: Path to fallback CSV file.
    """
    
    def __init__(
        self,
        db_host: Optional[str] = None,
        db_name: Optional[str] = None,
        db_user: Optional[str] = None,
        db_password: Optional[str] = None,
        csv_path: str = "data/nigerian_cities.csv"
    ) -> None:
        """
        Initialize the data service.
        
        Args:
            db_host: Database host (optional).
            db_name: Database name (optional).
            db_user: Database username (optional).
            db_password: Database password (optional).
            csv_path: Path to CSV fallback file.
        """
        self._csv_path = csv_path
        self._db = None
        
        if all([db_host, db_name, db_user, db_password]):
            try:
                self._db = DatabaseManager(
                    host=db_host,
                    database=db_name,
                    user=db_user,
                    password=db_password
                )
                logger.info("Database connection established")
            except Exception as e:
                logger.warning(f"Database connection failed: {e}. Using CSV fallback.")
                self._db = None
    
    def is_db_available(self) -> bool:
        """Check if database connection is available."""
        return self._db is not None
    
    def fetch_zones(self) -> List[Dict[str, Any]]:
        """Fetch geopolitical zones."""
        if self._db:
            try:
                query = "SELECT id, zone_name FROM geo_political_zones ORDER BY zone_name;"
                results = self._db.execute_query(query, fetch_all=True)
                return list(results) if results else []
            except Exception as e:
                logger.error(f"Failed to fetch zones: {e}")
        
        # Fallback to CSV
        return self._fetch_zones_from_csv()
    
    def fetch_states(self, zone_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch states, optionally filtered by zone."""
        if self._db:
            try:
                if zone_id:
                    query = "SELECT id, state_name FROM states WHERE zone_id = %s ORDER BY state_name;"
                    results = self._db.execute_query(query, params=(zone_id,), fetch_all=True)
                else:
                    query = "SELECT id, state_name FROM states ORDER BY state_name;"
                    results = self._db.execute_query(query, fetch_all=True)
                return list(results) if results else []
            except Exception as e:
                logger.error(f"Failed to fetch states: {e}")
        
        # Fallback to CSV
        return self._fetch_states_from_csv(zone_id)
    
    def fetch_cities(self, state_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch cities, optionally filtered by state."""
        if self._db:
            try:
                if state_id:
                    query = "SELECT id, city_name, latitude, longitude FROM cities WHERE state_id = %s ORDER BY city_name;"
                    results = self._db.execute_query(query, params=(state_id,), fetch_all=True)
                else:
                    query = "SELECT id, city_name, latitude, longitude FROM cities ORDER BY city_name;"
                    results = self._db.execute_query(query, fetch_all=True)
                return list(results) if results else []
            except Exception as e:
                logger.error(f"Failed to fetch cities: {e}")
        
        # Fallback to CSV
        return self._fetch_cities_from_csv(state_id)
    
    def fetch_cities_by_zone(self, zone_id: int) -> List[Dict[str, Any]]:
        """Fetch cities in a geopolitical zone."""
        if self._db:
            try:
                query = """
                    SELECT c.id, c.city_name, c.latitude, c.longitude
                    FROM cities c
                    JOIN states s ON c.state_id = s.id
                    WHERE s.zone_id = %s
                    ORDER BY c.city_name;
                """
                results = self._db.execute_query(query, params=(zone_id,), fetch_all=True)
                return list(results) if results else []
            except Exception as e:
                logger.error(f"Failed to fetch cities by zone: {e}")
        
        # Fallback to CSV
        return self._fetch_cities_from_csv(zone_id)
    
    def fetch_latest_reading(self, city_id: int) -> Optional[Dict[str, Any]]:
        """Fetch the latest weather reading for a city."""
        if not self._db:
            return None
        
        try:
            query = """
                SELECT temperature, humidity, pressure, wind_speed, 
                       weather_main, weather_desc, reading_timestamp
                FROM weather_readings
                WHERE city_id = %s
                ORDER BY reading_timestamp DESC
                LIMIT 1;
            """
            result = self._db.execute_query(query, params=(city_id,), fetch_one=True)
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Failed to fetch reading for city_id={city_id}: {e}")
            return None
    
    def _load_csv(self) -> pd.DataFrame:
        """Load the fallback CSV file."""
        try:
            return pd.read_csv(self._csv_path)
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            return pd.DataFrame()
    
    def _fetch_zones_from_csv(self) -> List[Dict[str, Any]]:
        """Fetch zones from CSV fallback."""
        df = self._load_csv()
        if df.empty or 'geopolitical_zone' not in df.columns:
            return []
        zones = sorted(df['geopolitical_zone'].dropna().unique())
        return [{'id': z, 'zone_name': z} for z in zones]
    
    def _fetch_states_from_csv(self, zone_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch states from CSV fallback."""
        df = self._load_csv()
        if df.empty or 'state' not in df.columns:
            return []
        
        if zone_id:
            states = df[df['geopolitical_zone'] == zone_id]['state'].dropna().unique()
        else:
            states = df['state'].dropna().unique()
        
        states = sorted(states)
        return [{'id': s, 'state_name': s} for s in states]
    
    def _fetch_cities_from_csv(self, filter_value: Optional[Any] = None) -> List[Dict[str, Any]]:
        """Fetch cities from CSV fallback."""
        df = self._load_csv()
        if df.empty:
            return []
        
        if filter_value:
            # Try filtering by state or zone
            if 'state' in df.columns:
                sub = df[df['state'] == filter_value]
            elif 'geopolitical_zone' in df.columns:
                sub = df[df['geopolitical_zone'] == filter_value]
            else:
                sub = df
        else:
            sub = df
        
        cities = []
        for _, row in sub.sort_values('name').iterrows():
            coord = row.get('coord', '')
            lat = lon = None
            try:
                coord_d = ast.literal_eval(coord) if isinstance(coord, str) else {}
                lon = coord_d.get('lon')
                lat = coord_d.get('lat')
            except Exception:
                pass
            
            cities.append({
                'id': row['name'],
                'city_name': row['name'],
                'latitude': lat,
                'longitude': lon
            })
        
        return cities
    
    def close(self) -> None:
        """Clean up resources."""
        if self._db:
            self._db.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
