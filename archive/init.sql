-- init.sql

-- Create the geo_political_zone table (Dimension)
CREATE TABLE IF NOT EXISTS geo_political_zones (
    id SERIAL PRIMARY KEY,
    zone_name VARCHAR(255) UNIQUE NOT NULL
);

-- Create the states table (Dimension)
CREATE TABLE IF NOT EXISTS states (
    id SERIAL PRIMARY KEY,
    state_name VARCHAR(255) UNIQUE NOT NULL,
    zone_id INTEGER NOT NULL REFERENCES geo_political_zones(id)
);

-- Create the cities table (Dimension)
CREATE TABLE IF NOT EXISTS cities (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(255) UNIQUE NOT NULL,
    city_id INTEGER UNIQUE NOT NULL,
    state_id INTEGER NOT NULL REFERENCES states(id),
    latitude FLOAT,
    longitude FLOAT
);

-- Create the weather_readings table (Fact Table)
-- This table will store the time-series data
CREATE TABLE IF NOT EXISTS weather_readings (
    id BIGSERIAL PRIMARY KEY,
    city_id INTEGER NOT NULL REFERENCES cities(id),
    temperature FLOAT NOT NULL,
    humidity INTEGER,
    pressure INTEGER,
    wind_speed FLOAT,
    weather_main VARCHAR(255),
    weather_desc VARCHAR(255),
    reading_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Optional: Create an index for faster queries on city_id and timestamp
CREATE INDEX IF NOT EXISTS idx_weather_readings_city_id_timestamp
ON weather_readings (city_id, reading_timestamp DESC);