import os
import time
import logging
from datetime import datetime, timezone

import requests
import psycopg2
import schedule
from dotenv import load_dotenv
import json



# --- CONFIGURATION ---
# Load environment variables from .env file
load_dotenv()

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load credentials and settings from environment variables
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST") # This will be 'db' from docker-compose

# List of cities to monitor
with open("city.list.json", "r", encoding="utf-8") as read:
    cities = json.load(read)

# Filter cities for Nigeria (country code = 'NG')
nigeria_cities = [c for c in cities if c["country"] == "NG"]

# Extract just the city names into a list
nigeria_city_names = [c["name"] for c in nigeria_cities]

def fetch_weather(city: str) -> dict | None:
    """Fetches weather data for a given city from the OpenWeatherMap API."""
    api_url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}"
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for {city}: {e}")
        return None

def process_weather_data(city: str, data: dict) -> dict | None:
    """Processes raw JSON data into a structured dictionary."""
    if not data:
        return None
    try:
        processed = {
            "city_name": city,
            # Convert temperature from Kelvin to Celsius
            "temperature": round(data['main']['temp'] - 273.15, 2),
            "humidity": data['main']['humidity'],
            "pressure": data['main']['pressure'],
            "wind_speed": data['wind']['speed'],
            "weather_main": data['weather'][0]['main'],
            "weather_desc": data['weather'][0]['description'],
            # Convert Unix timestamp to a timezone-aware datetime object
            "reading_timestamp": datetime.fromtimestamp(data['dt'], tz=timezone.utc)
        }
        return processed
    except (KeyError, IndexError) as e:
        logging.error(f"Error processing data for {city}. Missing key: {e}")
        return None

# --- DATABASE SETUP ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST
        )
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to the database: {e}")
        return None

def initialize_database():
    """Creates the weather_readings table if it doesn't exist."""
    conn = get_db_connection()
    if conn is None:
        logging.error("Database connection failed. Table initialization skipped.")
        return

    # Use a 'with' statement for cursor management
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_readings (
                id SERIAL PRIMARY KEY,
                city_name VARCHAR(255) NOT NULL,
                temperature FLOAT NOT NULL,
                humidity INTEGER,
                pressure INTEGER,
                wind_speed FLOAT,
                weather_main VARCHAR(255),
                weather_desc VARCHAR(255),
                reading_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        conn.commit()
        logging.info("Database initialized. 'weather_readings' table is ready.")
    conn.close()

def store_weather_data(weather_data: dict):
    """Inserts processed weather data into the PostgreSQL database."""
    if not weather_data:
        return

    conn = get_db_connection()
    if conn is None:
        logging.error(f"Could not store data for {weather_data['city_name']} due to DB connection failure.")
        return

    sql = """
        INSERT INTO weather_readings (city_name, temperature, humidity, pressure, wind_speed, weather_main, weather_desc, reading_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    with conn.cursor() as cur:
        try:
            cur.execute(sql, (
                weather_data['city_name'],
                weather_data['temperature'],
                weather_data['humidity'],
                weather_data['pressure'],
                weather_data['wind_speed'],
                weather_data['weather_main'],
                weather_data['weather_desc'],
                weather_data['reading_timestamp']
            ))
            conn.commit()
            logging.info(f"Successfully stored weather data for {weather_data['city_name']}.")
        except psycopg2.Error as e:
            logging.error(f"Database insertion failed for {weather_data['city_name']}: {e}")
            conn.rollback() # Roll back the transaction on error
    conn.close()

# --- MAIN JOB AND SCHEDULING ---
def weather_pipeline_job():
    """The main job that runs the ETL process for all cities."""
    logging.info("Starting weather data pipeline job...")
    for city in nigeria_city_names:
        raw_data = fetch_weather(city)
        if raw_data:
            processed_data = process_weather_data(city, raw_data)
            store_weather_data(processed_data)
    logging.info("Weather data pipeline job finished.")

if __name__ == "__main__":

    # Check for the essential API key before starting
    if not OPENWEATHER_API_KEY:
        raise ValueError("OPENWEATHER_API_KEY environment variable not set. The pipeline cannot start.")

    # Initialize the database and create the table on the first run
    # A small delay to ensure the DB container is fully ready
    logging.info("Waiting for the database container to be ready...")
    time.sleep(10) 
    initialize_database()

    # 3. Schedule the job to run periodically
    # For demonstration, we run it every 10 minutes.
    schedule.every(10).minutes.do(weather_pipeline_job)
    
    # 4. Run the job once immediately at the start
    logging.info("Running the first job immediately...")
    weather_pipeline_job()

    # 5. Start the scheduler loop
    logging.info("Scheduler started. Waiting for the next run...")
    while True:
        schedule.run_pending()
        time.sleep(1)