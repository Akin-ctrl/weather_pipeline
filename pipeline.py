# pipeline.py

import os
import time
import logging
import schedule
import requests
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# --- 1. SETUP AND CONFIGURATION ---

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# Get credentials and configurations from environment variables
API_KEY = os.getenv("OPENWEATHER_API_KEY")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST") # This will be 'db' from docker-compose

# List of cities to monitor
CITIES = ["London", "New York", "Tokyo", "Sydney", "Paris"]
API_BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# --- 2. DATABASE HELPER FUNCTIONS ---

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    while True:
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST
            )
            logging.info("Database connection successful.")
            return conn
        except psycopg2.OperationalError as e:
            logging.error(f"Database connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def create_table_if_not_exists(conn):
    """Creates the weather_readings table if it doesn't already exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_readings (
                id SERIAL PRIMARY KEY,
                city_name VARCHAR(255) NOT NULL,
                temperature_celsius FLOAT NOT NULL,
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
        logging.info("Table 'weather_readings' is ready.")

def store_data(conn, weather_data):
    """Inserts processed weather data into the database."""
    with conn.cursor() as cur:
        # Using psycopg2.sql for safe dynamic table/column names (not needed here but good practice)
        # and parameterized queries (%s) to prevent SQL injection.
        insert_query = sql.SQL("""
            INSERT INTO weather_readings (city_name, temperature_celsius, humidity, pressure, wind_speed, weather_main, weather_desc, reading_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, TO_TIMESTAMP(%s));
        """)
        try:
            cur.execute(insert_query, (
                weather_data['city'],
                weather_data['temp_celsius'],
                weather_data['humidity'],
                weather_data['pressure'],
                weather_data['wind_speed'],
                weather_data['weather_main'],
                weather_data['weather_desc'],
                weather_data['dt']
            ))
            conn.commit()
            logging.info(f"Successfully stored data for {weather_data['city']}.")
        except Exception as e:
            logging.error(f"Error storing data for {weather_data['city']}: {e}")
            conn.rollback() # Rollback the transaction on error

# --- 3. API AND PROCESSING FUNCTIONS ---

def fetch_weather(city):
    """Fetches weather data for a given city from the OpenWeatherMap API."""
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric' # Request temperature in Celsius
    }
    try:
        response = requests.get(API_BASE_URL, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        logging.info(f"Successfully fetched data for {city}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for {city}: {e}")
        return None

def process_weather(raw_data):
    """Processes raw JSON data into a clean dictionary."""
    if not raw_data:
        return None
    
    return {
        'city': raw_data['name'],
        'temp_celsius': raw_data['main']['temp'],
        'humidity': raw_data['main']['humidity'],
        'pressure': raw_data['main']['pressure'],
        'wind_speed': raw_data['wind']['speed'],
        'weather_main': raw_data['weather'][0]['main'],
        'weather_desc': raw_data['weather'][0]['description'],
        'dt': raw_data['dt'] # Timestamp from API (seconds since epoch)
    }

# --- 4. MAIN PIPELINE JOB ---

def pipeline_job():
    """The main job that orchestrates the fetch, process, and store steps."""
    logging.info("--- Starting pipeline job ---")
    conn = get_db_connection()
    if conn is None:
        logging.error("Could not establish database connection. Aborting job.")
        return

    try:
        for city in CITIES:
            raw_data = fetch_weather(city)
            if raw_data:
                processed_data = process_weather(raw_data)
                if processed_data:
                    store_data(conn, processed_data)
            time.sleep(1) # Small delay to avoid overwhelming the API
    finally:
        conn.close()
        logging.info("Database connection closed.")
        logging.info("--- Pipeline job finished ---")

# --- 5. SCHEDULING AND EXECUTION ---

if __name__ == "__main__":
    # On the very first run, set up the database table
    conn = get_db_connection()
    if conn:
        create_table_if_not_exists(conn)
        conn.close()
    
    # Schedule the job to run every 10 minutes
    schedule.every(10).minutes.do(pipeline_job)
    
    # Run the job once immediately at the start
    logging.info("Running initial pipeline job...")
    pipeline_job()
    
    logging.info("Scheduler started. Waiting for the next run...")
    while True:
        schedule.run_pending()
        time.sleep(1)
