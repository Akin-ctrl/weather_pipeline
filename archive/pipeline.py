# pipeline.py
import os
import time
import logging
from datetime import datetime, timezone
import requests
import psycopg2
import schedule
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")

# --- DATABASE CONNECTION ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        return psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to the database: {e}")
        return None

# --- DATA LOADING ---
def load_cities_from_db() -> dict:
    """Loads the list of cities and their IDs from the database."""
    conn = get_db_connection()
    if not conn:
        logging.critical("Cannot load cities from DB. Pipeline cannot run.")
        return {}
    
    cities_map = {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, city_name FROM cities;")
        for row in cur.fetchall():
            cities_map[row[1]] = row[0] # { "city_name": city_id }
    conn.close()
    logging.info(f"Loaded {len(cities_map)} cities from the database.")
    return cities_map

# --- API AND DATA PROCESSING ---
def fetch_weather(city_name: str) -> dict | None:
    """Fetches weather data for a given city from the OpenWeatherMap API."""
    api_url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={OPENWEATHER_API_KEY}"
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for {city_name}: {e}")
        return None

def process_weather_data(data: dict, city_id: int) -> dict | None:
    """Processes raw JSON data into a structured dictionary for storage."""
    if not data:
        return None
    try:
        return {
            "city_id": city_id,
            "temperature": round(data['main']['temp'] - 273.15, 2),
            "humidity": data['main']['humidity'],
            "pressure": data['main']['pressure'],
            "wind_speed": data['wind']['speed'],
            "weather_main": data['weather'][0]['main'],
            "weather_desc": data['weather'][0]['description'],
            "reading_timestamp": datetime.fromtimestamp(data['dt'], tz=timezone.utc)
        }
    except (KeyError, IndexError) as e:
        logging.error(f"Error processing data. Missing key: {e}")
        return None

def store_weather_data(weather_data: dict):
    """Inserts processed weather data into the weather_readings table."""
    if not weather_data:
        return

    conn = get_db_connection()
    if not conn:
        logging.error("Could not store data due to DB connection failure.")
        return

    sql = """
        INSERT INTO weather_readings (city_id, temperature, humidity, pressure, wind_speed, weather_main, weather_desc, reading_timestamp)
        VALUES (%(city_id)s, %(temperature)s, %(humidity)s, %(pressure)s, %(wind_speed)s, %(weather_main)s, %(weather_desc)s, %(reading_timestamp)s);
    """
    
    with conn.cursor() as cur:
        try:
            cur.execute(sql, weather_data)
            conn.commit()
            logging.info(f"Successfully stored weather data for city_id: {weather_data['city_id']}.")
        except psycopg2.Error as e:
            logging.error(f"Database insertion failed for city_id {weather_data['city_id']}: {e}")
            conn.rollback()
    conn.close()

# --- MAIN JOB AND SCHEDULING ---
def weather_pipeline_job(cities_map: dict):
    """The main job that runs the ETL process for all cities."""
    logging.info("Starting weather data pipeline job...")
    for city_name, city_id in cities_map.items():
        raw_data = fetch_weather(city_name)
        if raw_data:
            processed_data = process_weather_data(raw_data, city_id)
            store_weather_data(processed_data)
    logging.info("Weather data pipeline job finished.")

if __name__ == "__main__":
    if not OPENWEATHER_API_KEY:
        raise ValueError("OPENWEATHER_API_KEY environment variable not set.")

    logging.info("Waiting for the database to be ready...")
    time.sleep(10)

    # Load the cities to monitor from our newly seeded database
    cities_to_monitor = load_cities_from_db()
    if not cities_to_monitor:
        logging.critical("No cities found in the database. Please run the seeding script. Exiting.")
        exit(1)

    # Schedule the job, passing the city map as an argument
    schedule.every(10).minutes.do(weather_pipeline_job, cities_map=cities_to_monitor)
    
    logging.info("Running the first job immediately...")
    weather_pipeline_job(cities_to_monitor)

    logging.info("Scheduler started. Waiting for the next run...")
    while True:
        schedule.run_pending()
        time.sleep(1)