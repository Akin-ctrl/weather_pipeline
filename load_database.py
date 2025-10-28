import os
import csv
import logging
import ast  # Safely evaluate string representations of Python literals
import psycopg2
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database credentials
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
CSV_FILE_PATH = 'nigerian_cities.csv'

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        return psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to the database: {e}")
        return None

def seed_data():
    """Reads the CSV and populates the dimension tables."""
    conn = get_db_connection()
    if not conn:
        return

    with conn.cursor() as cur:
        # For idempotency, clear the tables before seeding to avoid duplicates on re-run
        logging.info("Clearing existing dimension data...")
        # Ensure CASCADE is used if there are foreign key constraints referencing these tables
        cur.execute("""
            TRUNCATE TABLE cities RESTART IDENTITY CASCADE;
            TRUNCATE TABLE states RESTART IDENTITY CASCADE;
            TRUNCATE TABLE geo_political_zones RESTART IDENTITY CASCADE;
        """)

        # Use dictionaries to store the IDs of inserted regions/states to avoid re-querying
        regions = {}
        states = {}
        processed_cities = set()  # To track already processed city names across the entire CSV
        
        logging.info(f"Reading data from {CSV_FILE_PATH}...")
        try:
            with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for i, row in enumerate(reader):
                    row_num = i + 2 # Account for header and 0-based index
                    try:
                        region_name = row.get('geopolitical_zone', '').strip()
                        state_name = row.get('state', '').strip()
                        city_name = row.get('name', '').strip()
                        city_ext_id = row.get('id', '').strip() # Renamed to avoid confusion with DB primary key 'id'

                        # Basic validation for essential fields
                        if not all([region_name, state_name, city_name, city_ext_id]):
                            logging.warning(f"Skipping row {row_num} due to missing essential data: {row}")
                            continue

                        # Check for duplicate city names within the CSV
                        if city_name in processed_cities:
                            logging.warning(f"Skipping duplicate city entry in CSV on row {row_num} for: '{city_name}'")
                            continue
                        
                        # Safely evaluate the 'coord' string
                        coords_str = row.get('coord')
                        if not coords_str:
                            logging.warning(f"Skipping row {row_num} for '{city_name}' due to missing 'coord' data.")
                            continue

                        try:
                            coords = ast.literal_eval(coords_str)
                            lon, lat = coords['lon'], coords['lat']
                        except (SyntaxError, ValueError, KeyError) as e:
                            logging.error(f"Skipping row {row_num} for '{city_name}' due to invalid 'coord' format: {coords_str}. Error: {e}")
                            continue

                        # Insert Geo Political Zone if not exists
                        if region_name not in regions:
                            cur.execute("INSERT INTO geo_political_zones (zone_name) VALUES (%s) ON CONFLICT (zone_name) DO NOTHING RETURNING id;", (region_name,))
                            result = cur.fetchone()
                            if result:
                                region_id = result[0]
                                regions[region_name] = region_id
                            else: # Already exists, retrieve its ID
                                cur.execute("SELECT id FROM geo_political_zones WHERE zone_name = %s;", (region_name,))
                                regions[region_name] = cur.fetchone()[0]
                        
                        # Insert State if not exists
                        if state_name not in states:
                            region_id = regions[region_name]
                            cur.execute("INSERT INTO states (state_name, zone_id) VALUES (%s, %s) ON CONFLICT (state_name) DO NOTHING RETURNING id;", (state_name, region_id))
                            result = cur.fetchone()
                            if result:
                                state_id = result[0]
                                states[state_name] = state_id
                            else: # Already exists, retrieve its ID
                                cur.execute("SELECT id FROM states WHERE state_name = %s;", (state_name,))
                                states[state_name] = cur.fetchone()[0]

                        # Insert City
                        state_id = states[state_name]
                        cur.execute(
                            "INSERT INTO cities (city_name, city_id, state_id, latitude, longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (city_id) DO NOTHING;",
                            (city_name, city_ext_id, state_id, lat, lon)
                        )
                        processed_cities.add(city_name) # Add to set after successful processing

                    except (KeyError, SyntaxError, ValueError) as e:
                        logging.error(f"Skipping row {row_num} due to error: {row}. Error: {e}")
                        continue
            
            conn.commit()
            logging.info(f"Database seeding completed successfully. Processed {len(processed_cities)} unique cities.")
        except FileNotFoundError:
            logging.error(f"CSV file not found at {CSV_FILE_PATH}")
        except Exception as e:
            conn.rollback() # Rollback on any other unhandled error
            logging.error(f"An unexpected error occurred during seeding: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    seed_data()