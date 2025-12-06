"""
Database seeder for populating dimension tables.
================================================
Reads CSV data and populates cities, states, and geopolitical zones.
"""

import logging
import csv
import ast
from typing import Dict, Set, Optional
from app.core.database import DatabaseManager, DatabaseConnectionError


logger = logging.getLogger(__name__)


class DatabaseSeeder:
    """
    Seeds the database with dimension data from CSV files.
    
    Populates geo_political_zones, states, and cities tables
    from Nigerian cities CSV data with proper validation and
    error handling.
    
    Attributes:
        _db: DatabaseManager instance.
        _csv_path: Path to CSV file.
    
    Example:
        >>> seeder = DatabaseSeeder(
        ...     db_host="localhost",
        ...     db_name="weather",
        ...     db_user="user",
        ...     db_password="pass",
        ...     csv_path="data/nigerian_cities.csv"
        ... )
        >>> seeder.seed()
    """
    
    def __init__(
        self,
        db_host: str,
        db_name: str,
        db_user: str,
        db_password: str,
        csv_path: str = "data/nigerian_cities.csv"
    ) -> None:
        """
        Initialize the database seeder.
        
        Args:
            db_host: Database host address.
            db_name: Database name.
            db_user: Database username.
            db_password: Database password.
            csv_path: Path to Nigerian cities CSV file.
        
        Raises:
            DatabaseConnectionError: If database connection fails.
        """
        self._db = DatabaseManager(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
        self._csv_path = csv_path
        
        logger.info(f"DatabaseSeeder initialized with CSV: {csv_path}")
    
    def clear_tables(self) -> None:
        """
        Clear existing data from dimension tables.
        
        Uses TRUNCATE CASCADE to remove all data while
        resetting identity sequences.
        
        Raises:
            DatabaseConnectionError: If truncate fails.
        """
        logger.info("Clearing existing dimension data...")
        
        queries = [
            "TRUNCATE TABLE cities RESTART IDENTITY CASCADE;",
            "TRUNCATE TABLE states RESTART IDENTITY CASCADE;",
            "TRUNCATE TABLE geo_political_zones RESTART IDENTITY CASCADE;"
        ]
        
        for query in queries:
            self._db.execute_query(query, fetch_all=False)
        
        logger.info("Tables cleared successfully")
    
    def insert_zone(self, zone_name: str) -> int:
        """
        Insert a geopolitical zone and return its ID.
        
        Args:
            zone_name: Name of the geopolitical zone.
        
        Returns:
            Database ID of the zone.
        """
        query = """
            INSERT INTO geo_political_zones (zone_name) 
            VALUES (%s) 
            ON CONFLICT (zone_name) DO NOTHING 
            RETURNING id;
        """
        
        result = self._db.execute_query(query, params=(zone_name,), fetch_one=True)
        
        if result:
            return result['id']
        
        # If conflict occurred, fetch existing ID
        query = "SELECT id FROM geo_political_zones WHERE zone_name = %s;"
        result = self._db.execute_query(query, params=(zone_name,), fetch_one=True)
        return result['id']
    
    def insert_state(self, state_name: str, zone_id: int) -> int:
        """
        Insert a state and return its ID.
        
        Args:
            state_name: Name of the state.
            zone_id: Foreign key to geopolitical zone.
        
        Returns:
            Database ID of the state.
        """
        query = """
            INSERT INTO states (state_name, zone_id) 
            VALUES (%s, %s) 
            ON CONFLICT (state_name) DO NOTHING 
            RETURNING id;
        """
        
        result = self._db.execute_query(query, params=(state_name, zone_id), fetch_one=True)
        
        if result:
            return result['id']
        
        # If conflict occurred, fetch existing ID
        query = "SELECT id FROM states WHERE state_name = %s;"
        result = self._db.execute_query(query, params=(state_name,), fetch_one=True)
        return result['id']
    
    def insert_city(
        self,
        city_name: str,
        city_id: int,
        state_id: int,
        latitude: float,
        longitude: float
    ) -> bool:
        """
        Insert a city into the database.
        
        Args:
            city_name: Name of the city.
            city_id: OpenWeatherMap city ID.
            state_id: Foreign key to states table.
            latitude: Geographic latitude.
            longitude: Geographic longitude.
        
        Returns:
            True if successful, False otherwise.
        """
        query = """
            INSERT INTO cities (city_name, city_id, state_id, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (city_id) DO NOTHING;
        """
        
        try:
            self._db.execute_query(
                query,
                params=(city_name, city_id, state_id, latitude, longitude),
                fetch_all=False
            )
            return True
        except DatabaseConnectionError as e:
            logger.error(f"Failed to insert city {city_name}: {e}")
            return False
    
    def parse_coordinates(self, coord_str: str) -> Optional[tuple[float, float]]:
        """
        Parse coordinate string from CSV.
        
        Args:
            coord_str: String representation of coordinates dict.
        
        Returns:
            Tuple of (longitude, latitude) or None if parsing fails.
        
        Example:
            >>> coords = seeder.parse_coordinates("{'lon': 3.39, 'lat': 6.45}")
            >>> print(coords)
            (3.39, 6.45)
        """
        try:
            coord_dict = ast.literal_eval(coord_str)
            lon = coord_dict['lon']
            lat = coord_dict['lat']
            return (lon, lat)
        except (SyntaxError, ValueError, KeyError, TypeError) as e:
            logger.debug(f"Failed to parse coordinates: {coord_str} - {e}")
            return None
    
    def seed(self) -> None:
        """
        Execute the full seeding process.
        
        Reads the CSV file, clears existing data, and populates
        all dimension tables with validation and error handling.
        
        Raises:
            FileNotFoundError: If CSV file doesn't exist.
            DatabaseConnectionError: If database operations fail.
        """
        logger.info(f"Starting database seeding from {self._csv_path}")
        
        # Clear existing data
        self.clear_tables()
        
        # Track inserted data
        zones: Dict[str, int] = {}
        states: Dict[str, int] = {}
        processed_cities: Set[str] = set()
        
        success_count = 0
        skip_count = 0
        
        try:
            with open(self._csv_path, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row_num, row in enumerate(reader, start=2):
                    try:
                        # Extract required fields
                        zone_name = row.get('geopolitical_zone', '').strip()
                        state_name = row.get('state', '').strip()
                        city_name = row.get('name', '').strip()
                        city_id_str = row.get('id', '').strip()
                        coords_str = row.get('coord', '').strip()
                        
                        # Validate essential fields
                        if not all([zone_name, state_name, city_name, city_id_str, coords_str]):
                            logger.warning(f"Row {row_num}: Missing essential data, skipping")
                            skip_count += 1
                            continue
                        
                        # Check for duplicates
                        if city_name in processed_cities:
                            logger.warning(f"Row {row_num}: Duplicate city '{city_name}', skipping")
                            skip_count += 1
                            continue
                        
                        # Parse coordinates
                        coords = self.parse_coordinates(coords_str)
                        if not coords:
                            logger.warning(f"Row {row_num}: Invalid coordinates for '{city_name}', skipping")
                            skip_count += 1
                            continue
                        
                        lon, lat = coords
                        city_id = int(city_id_str)
                        
                        # Insert zone if not exists
                        if zone_name not in zones:
                            zones[zone_name] = self.insert_zone(zone_name)
                        
                        # Insert state if not exists
                        if state_name not in states:
                            zone_id = zones[zone_name]
                            states[state_name] = self.insert_state(state_name, zone_id)
                        
                        # Insert city
                        state_id = states[state_name]
                        if self.insert_city(city_name, city_id, state_id, lat, lon):
                            processed_cities.add(city_name)
                            success_count += 1
                        else:
                            skip_count += 1
                        
                    except (ValueError, KeyError) as e:
                        logger.error(f"Row {row_num}: Processing error - {e}")
                        skip_count += 1
                        continue
            
            logger.info(
                f"Seeding completed: {success_count} cities inserted, "
                f"{skip_count} skipped, "
                f"{len(zones)} zones, {len(states)} states"
            )
            
        except FileNotFoundError:
            logger.error(f"CSV file not found: {self._csv_path}")
            raise
        except Exception as e:
            logger.error(f"Seeding failed: {e}")
            raise
    
    def close(self) -> None:
        """Clean up database resources."""
        self._db.close()
        logger.info("Seeder resources cleaned up")
    
    def __enter__(self):
        """Support context manager protocol."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up on context manager exit."""
        self.close()
    
    def __repr__(self) -> str:
        return f"DatabaseSeeder(csv_path='{self._csv_path}')"
