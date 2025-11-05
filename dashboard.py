
"""
Streamlit dashboard for the local weather pipeline.

Features:
- Search/filter by geopolitical zone, state, or city (data pulled from the project's Postgres DB).
- Overview: major Nigerian cities with latest readings.
- Click a city to view detailed latest reading + map.

If the DB is unavailable the app will show a helpful error and fall back to city listing from `nigerian_cities.csv` for filter population (no readings).
"""

from typing import List, Dict, Optional

import logging

import os
import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st
import pandas as pd
import ast
from dotenv import load_dotenv

load_dotenv()
try:
    # optional helper for periodic auto-refresh
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None


# Helper rerun 
def _rerun_app():
    """Attempt to rerun the Streamlit app in a compatible way.
    Tries st.experimental_rerun(), falls back to raising Streamlit's internal
    RerunException, and finally falls back to setting a session flag and stopping.
    """
    try:
        # Preferred public API
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun()
            return
    except Exception:
        # continue to fallback
        pass

    # Try internal rerun exception (may vary by Streamlit version)
    try:
        from streamlit.runtime.scriptrunner.script_runner import RerunException

        raise RerunException()
    except Exception:
        # Last resort: set a marker and stop; UI will refresh on next interaction
        st.session_state["_needs_rerun"] = True
        st.stop()


    # Navigation helpers using callbacks (avoid experimental_rerun)
def goto_city(city_id, city_name=None):
    st.session_state['selected_city'] = city_id
    st.session_state['view'] = 'city'
    if city_name:
        st.session_state['view_city_name'] = city_name


def back_to_overview():
    st.session_state['view'] = 'main'


def apply_filters():
    # Read current sidebar selections (they are stored in st.session_state by keys)
    zone_choice = st.session_state.get('sidebar_zone')
    state_choice = st.session_state.get('sidebar_state')
    city_choice = st.session_state.get('sidebar_city')

    # resolve ids/names using fetch helpers
    zones = fetch_zones()
    selected_zone_id = None
    if zone_choice and zone_choice != 'All':
        for z in zones:
            if z.get('zone_name') == zone_choice:
                selected_zone_id = z.get('id')
                break
        # If zones came from CSV, id will be the same as name
        if selected_zone_id is None:
            selected_zone_id = zone_choice

    states = fetch_states(selected_zone_id)
    selected_state_id = None
    if state_choice and state_choice != 'All':
        for s in states:
            if s.get('state_name') == state_choice:
                selected_state_id = s.get('id')
                break
        if selected_state_id is None:
            selected_state_id = state_choice

    cities = fetch_cities(selected_state_id)
    selected_city_id = None
    if city_choice and city_choice != 'All':
        for c in cities:
            if c.get('city_name') == city_choice:
                selected_city_id = c.get('id')
                break
        if selected_city_id is None:
            selected_city_id = city_choice

    # Decide view
    if selected_city_id:
        st.session_state['selected_city'] = selected_city_id
        st.session_state['view'] = 'city'
        st.session_state['view_city_name'] = city_choice
    elif selected_state_id:
        st.session_state['view'] = 'state'
        st.session_state['view_state_id'] = selected_state_id
        st.session_state['view_state_name'] = state_choice
    elif selected_zone_id:
        st.session_state['view'] = 'zone'
        st.session_state['view_zone_id'] = selected_zone_id
        st.session_state['view_zone_name'] = zone_choice
    else:
        st.session_state['view'] = 'main'


def sidebar_controls():
    """Render sidebar controls and store temporary selections in session_state keys.
    The Apply button triggers apply_filters() which updates navigation state.
    """
    st.sidebar.header("Filters")
    zones = fetch_zones()
    zone_names = [z['zone_name'] for z in zones] if zones else []
    # store selections under explicit keys so apply_filters can read them
    st.sidebar.selectbox("Geopolitical zone", options=["All"] + zone_names, key='sidebar_zone')

    # states depend on chosen zone; read current choice from session_state
    current_zone = st.session_state.get('sidebar_zone')
    sel_zone_id = None
    if current_zone and current_zone != 'All':
        for z in zones:
            if z.get('zone_name') == current_zone:
                sel_zone_id = z.get('id')
                break
        if sel_zone_id is None:
            sel_zone_id = current_zone

    states = fetch_states(sel_zone_id)
    state_names = [s['state_name'] for s in states] if states else []
    st.sidebar.selectbox("State", options=["All"] + state_names, key='sidebar_state')

    # cities depend on state selection
    current_state = st.session_state.get('sidebar_state')
    sel_state_id = None
    if current_state and current_state != 'All':
        for s in states:
            if s.get('state_name') == current_state:
                sel_state_id = s.get('id')
                break
        if sel_state_id is None:
            sel_state_id = current_state

    cities = fetch_cities(sel_state_id)
    city_names = [c['city_name'] for c in cities] if cities else []
    st.sidebar.selectbox("City", options=["All"] + city_names, key='sidebar_city')

    # Auto-refresh toggle (defaults to enabled)
    if 'auto_refresh' not in st.session_state:
        st.session_state['auto_refresh'] = True
    st.sidebar.checkbox("Auto-refresh every minute", value=st.session_state['auto_refresh'], key='auto_refresh')

    st.sidebar.button("Apply filters", on_click=apply_filters)


# DB config via environment (used by pipeline and seeding scripts)
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")

MAJOR_CITIES = [
    "Lagos", "Abeokuta", "Kano", "Ibadan", "Abuja", "Port Harcourt", "Benin City",
    "Kaduna", "Enugu", "Jos", "Maiduguri", "Ilorin", "Sokoto",
    "Owerri", "Warri", "Aba"
]


@st.cache_resource
def get_db_connection():
    """Return a psycopg2 connection or None if configuration is missing."""
    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST]):
        return None
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
        return conn
    except Exception as e:
        st.session_state.setdefault("db_error", str(e))
        return None


@st.cache_data(ttl=60)
def fetch_zones() -> List[Dict]:
    """Fetch geopolitical zones from the DB. The DB connection is acquired internally to avoid
    passing unhashable objects (psycopg2 connections) into Streamlit-cached functions."""
    conn = get_db_connection()
    if not conn:
        # fallback to local CSV
        df = load_local_city_list()
        if df.empty or 'geopolitical_zone' not in df.columns:
            return []
        zones = sorted(df['geopolitical_zone'].dropna().unique())
        return [{'id': z, 'zone_name': z} for z in zones]
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, zone_name FROM geo_political_zones ORDER BY zone_name;")
        return cur.fetchall()


@st.cache_data(ttl=60)
def fetch_states(zone_id: Optional[int] = None) -> List[Dict]:
    conn = get_db_connection()
    if not conn:
        # fallback to local CSV; zone_id in this mode is actually zone_name
        df = load_local_city_list()
        if df.empty or 'state' not in df.columns:
            return []
        if zone_id:
            states = df[df['geopolitical_zone'] == zone_id]['state'].dropna().unique()
        else:
            states = df['state'].dropna().unique()
        states = sorted(states)
        return [{'id': s, 'state_name': s} for s in states]
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if zone_id:
            cur.execute("SELECT id, state_name FROM states WHERE zone_id = %s ORDER BY state_name;", (zone_id,))
        else:
            cur.execute("SELECT id, state_name FROM states ORDER BY state_name;")
        return cur.fetchall()


@st.cache_data(ttl=60)
def fetch_cities(state_id: Optional[int] = None) -> List[Dict]:
    conn = get_db_connection()
    if not conn:
        # fallback to local CSV; state_id in this mode is state name
        df = load_local_city_list()
        if df.empty:
            return []
        if state_id:
            sub = df[df['state'] == state_id]
        else:
            sub = df
        # build dicts consistent with DB shape where possible
        cities = []
        for _, row in sub.sort_values('name').iterrows():
            coord = row.get('coord', '')
            lat = lon = None
            try:
                coord_d = ast.literal_eval(coord) if isinstance(coord, str) else {}
                lon = coord_d.get('lon')
                lat = coord_d.get('lat')
            except Exception:
                lat = lon = None
            cities.append({'id': row['name'], 'city_name': row['name'], 'latitude': lat, 'longitude': lon})
        return cities
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if state_id:
            cur.execute("SELECT id, city_name, latitude, longitude FROM cities WHERE state_id = %s ORDER BY city_name;", (state_id,))
        else:
            cur.execute("SELECT id, city_name, latitude, longitude FROM cities ORDER BY city_name;")
        return cur.fetchall()


@st.cache_data(ttl=60)
def fetch_cities_by_zone(zone_id: int) -> List[Dict]:
    """Return cities belonging to a geopolitical zone (via states->cities join)."""
    conn = get_db_connection()
    if not conn:
        # fallback to local CSV; zone_id is zone name
        df = load_local_city_list()
        if df.empty:
            return []
        sub = df[df['geopolitical_zone'] == zone_id]
        cities = []
        for _, row in sub.sort_values('name').iterrows():
            coord = row.get('coord', '')
            lat = lon = None
            try:
                coord_d = ast.literal_eval(coord) if isinstance(coord, str) else {}
                lon = coord_d.get('lon')
                lat = coord_d.get('lat')
            except Exception:
                lat = lon = None
            cities.append({'id': row['name'], 'city_name': row['name'], 'latitude': lat, 'longitude': lon})
        return cities
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT c.id, c.city_name, c.latitude, c.longitude
            FROM cities c
            JOIN states s ON c.state_id = s.id
            WHERE s.zone_id = %s
            ORDER BY c.city_name;
            """,
            (zone_id,)
        )
        return cur.fetchall()


@st.cache_data(ttl=30)
def fetch_latest_reading(city_id: int) -> Optional[Dict]:
    conn = get_db_connection()
    if not conn:
        return None
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT temperature, humidity, pressure, wind_speed, weather_main, weather_desc, reading_timestamp
            FROM weather_readings
            WHERE city_id = %s
            ORDER BY reading_timestamp DESC
            LIMIT 1;
            """,
            (city_id,)
        )
        row = cur.fetchone()
        return row


def load_local_city_list(csv_path: str = "nigerian_cities.csv") -> pd.DataFrame:
    try:
        df = pd.read_csv(csv_path)
        return df
    except Exception:
        return pd.DataFrame()


def show_overview():
    st.header("Major Nigerian Cities")
    cols = st.columns(3)
    conn = get_db_connection()
    # Show major cities grid; if DB has readings show them, otherwise show placeholders
    for i, city in enumerate(MAJOR_CITIES):
        col = cols[i % 3]
        with col:
            st.subheader(city)
            # Try to find city id from DB
            city_id = None
            if conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT id FROM cities WHERE city_name ILIKE %s LIMIT 1;", (city,))
                    res = cur.fetchone()
                    if res:
                        city_id = res["id"]
            if city_id:
                reading = fetch_latest_reading(city_id)
                if reading:
                    st.metric("Temperature", f"{reading['temperature']} °C")
                    st.write(f"**{reading['weather_main']}**: {reading['weather_desc']}")
                    st.write(f"Humidity: {reading['humidity']}%")
                    # clickable detail
                    st.button("View details", key=f"view_{city_id}", on_click=goto_city, args=(city_id, city))
                else:
                    st.info("No readings yet")
                    st.button("Select city", key=f"select_{city}", on_click=goto_city, args=(city_id, city))
            else:
                st.info("No city record in DB")


def show_city_detail(city_id: int):
    st.header("City detail")
    if not city_id:
        st.info("Select a city to view details")
        return
    # Load city metadata
    conn = get_db_connection()
    city_meta = None
    if conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, city_name, latitude, longitude FROM cities WHERE id = %s;", (city_id,))
            city_meta = cur.fetchone()
    else:
        # fallback to CSV using name or id
        df = load_local_city_list()
        if df.empty:
            st.error("City metadata not found (DB down and local CSV missing).")
            return
        # Try match by numeric id first
        match = None
        try:
            # city_id may be numeric or a name; handle both
            match = df[df['id'] == int(city_id)] if isinstance(city_id, (int,)) or (isinstance(city_id, str) and city_id.isdigit()) else None
        except Exception:
            match = None
        if match is None or match.empty:
            # try match by name
            match = df[df['name'].str.lower() == str(city_id).lower()]
        if match is None or match.empty:
            st.error("City metadata not found in local CSV")
            return
        row = match.iloc[0]
        coord = row.get('coord', '')
        lat = lon = None
        try:
            coord_d = ast.literal_eval(coord) if isinstance(coord, str) else {}
            lon = coord_d.get('lon')
            lat = coord_d.get('lat')
        except Exception:
            lat = lon = None
        city_meta = {'id': row['id'], 'city_name': row['name'], 'latitude': lat, 'longitude': lon}

    st.subheader(city_meta['city_name'] if isinstance(city_meta, dict) else (city_meta[1] if isinstance(city_meta, (list,tuple)) else str(city_id)))
    reading = fetch_latest_reading(city_id)
    if not reading:
        st.warning("No readings available for this city yet.")

    col1, col2, col3 = st.columns(3)
    if reading:
        with col1:
            st.metric("Temperature", f"{reading['temperature']} °C")
        with col2:
            st.metric("Humidity", f"{reading['humidity']}%")
        with col3:
            st.metric("Wind Speed", f"{reading['wind_speed']} m/s")

        st.markdown(f"**Description:** {reading['weather_main']} — {reading['weather_desc']}")
        st.markdown(f"**Pressure:** {reading['pressure']} hPa")
        st.markdown(f"**Last updated:** {reading['reading_timestamp']}")
    else:
        with col1:
            st.metric("Temperature", "—")
        with col2:
            st.metric("Humidity", "—")
        with col3:
            st.metric("Wind Speed", "—")
        st.markdown("**Description:** —")
        st.markdown("**Pressure:** —")
        st.markdown("**Last updated:** —")

    # show map
    lat = city_meta.get("latitude") if isinstance(city_meta, dict) else city_meta[2]
    lon = city_meta.get("longitude") if isinstance(city_meta, dict) else city_meta[3]
    if lat and lon:
        df = pd.DataFrame([{"lat": lat, "lon": lon}])
        st.map(df)


def show_zone_overview(zone_id: int):
    st.header("Overview: Cities in Zone")
    zone_name = None
    zones = fetch_zones()
    for z in zones:
        if z["id"] == zone_id or z.get("id") == zone_id:
            zone_name = z.get("zone_name")
            break
    st.subheader(zone_name or f"Zone {zone_id}")
    cities = fetch_cities_by_zone(zone_id)
    if not cities:
        st.info("No cities found for this zone.")
        return
    cols = st.columns(3)
    for i, c in enumerate(cities):
        col = cols[i % 3]
        with col:
            st.subheader(c["city_name"] if isinstance(c, dict) else c[1])
            reading = None
            cid = c.get("id") if isinstance(c, dict) else c[0]
            reading = fetch_latest_reading(cid)
            if reading:
                st.metric("Temperature", f"{reading['temperature']} °C")
                st.write(f"**{reading['weather_main']}**: {reading['weather_desc']}")
                st.button("View details", key=f"zone_view_{cid}", on_click=goto_city, args=(cid, c.get('city_name') if isinstance(c, dict) else c[1]))
            else:
                st.info("No readings yet")


def show_state_overview(state_id: int):
    st.header("Overview: Cities in state")
    state_name = None
    states = fetch_states()
    for s in states:
        if s["id"] == state_id or s.get("id") == state_id:
            state_name = s.get("state_name")
            break
    st.subheader(state_name or f"State {state_id}")
    cities = fetch_cities(state_id)
    if not cities:
        st.info("No cities found for this state.")
        return
    cols = st.columns(3)
    for i, c in enumerate(cities):
        col = cols[i % 3]
        with col:
            st.subheader(c["city_name"] if isinstance(c, dict) else c[1])
            cid = c.get("id") if isinstance(c, dict) else c[0]
            reading = fetch_latest_reading(cid)
            if reading:
                st.metric("Temperature", f"{reading['temperature']} °C")
                st.write(f"**{reading['weather_main']}**: {reading['weather_desc']}")
                st.button("View details", key=f"state_view_{cid}", on_click=goto_city, args=(cid, c.get('city_name') if isinstance(c, dict) else c[1]))
            else:
                st.info("No readings yet")


def main():
    st.set_page_config(page_title="Nigeria Weather Dashboard", page_icon="🌦️", layout="wide")
    st.title("Nigeria Weather Dashboard")
    st.write("Browse weather by geopolitical zone, state, or city. Data is read from the project's Postgres database.")

    conn = get_db_connection()
    if conn is None:
        st.error("Database connection not configured or failed. Please set DB_NAME, DB_USER, DB_PASSWORD, and DB_HOST environment variables. The app will still show city lists if available.")

    # Sidebar controls (use callback-driven navigation)
    sidebar_controls()

    # If user enabled auto-refresh, trigger a periodic rerun every 60s
    if st.session_state.get('auto_refresh'):
        if st_autorefresh is not None:
            # interval in milliseconds
            st_autorefresh(interval=60 * 1000, key="autorefresh")
        else:
            # developer environment: advise that the optional package is missing
            st.sidebar.info("Auto-refresh disabled: install 'streamlit-autorefresh' to enable automatic updates every minute.")

    # initialize navigation/session keys
    if 'view' not in st.session_state:
        st.session_state.view = 'main'
    if 'selected_city' not in st.session_state:
        st.session_state.selected_city = None

    # Render the appropriate page
    if st.session_state.view == 'main':
        left, right = st.columns([2, 1])
        with left:
            show_overview()
        with right:
            show_city_detail(st.session_state.get('selected_city'))
    elif st.session_state.view == 'zone':
        zid = st.session_state.get('view_zone_id')
        zname = st.session_state.get('view_zone_name')
        st.sidebar.markdown(f"### Viewing zone: {zname or zid}")
        st.sidebar.button("Back to overview", on_click=back_to_overview)
        show_zone_overview(zid)
    elif st.session_state.view == 'state':
        sid = st.session_state.get('view_state_id')
        sname = st.session_state.get('view_state_name')
        st.sidebar.markdown(f"### Viewing state: {sname or sid}")
        st.sidebar.button("Back to overview", on_click=back_to_overview)
        show_state_overview(sid)
    elif st.session_state.view == 'city':
        cid = st.session_state.get('selected_city')
        cname = st.session_state.get('view_city_name')
        st.sidebar.markdown(f"### Viewing city: {cname or cid}")
        st.sidebar.button("Back to overview", on_click=back_to_overview)
        show_city_detail(cid)


if __name__ == "__main__":
    main()
