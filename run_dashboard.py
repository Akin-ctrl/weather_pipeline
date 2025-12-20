"""
Streamlit dashboard entry point.
================================
Weather monitoring dashboard for Nigerian cities.
"""

import os
import sys
import streamlit as st
from datetime import datetime, timezone
import pytz

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from app.utils.dashboard_service import DashboardDataService

# Load environment variables
load_dotenv()

# Initialize data service (cached at module level)
@st.cache_resource
def get_data_service():
    """Get cached data service instance."""
    return DashboardDataService(
        db_host=os.getenv("DB_HOST"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        csv_path="data/nigerian_cities.csv"
    )


# Cache data fetching functions
@st.cache_data(ttl=60)
def fetch_zones():
    """Fetch zones with 60s cache."""
    service = get_data_service()
    return service.fetch_zones()


@st.cache_data(ttl=60)
def fetch_states(zone_id=None):
    """Fetch states with 60s cache."""
    service = get_data_service()
    return service.fetch_states(zone_id)


@st.cache_data(ttl=60)
def fetch_cities(state_id=None):
    """Fetch cities with 60s cache."""
    service = get_data_service()
    return service.fetch_cities(state_id)


@st.cache_data(ttl=60)
def fetch_cities_by_zone(zone_id):
    """Fetch cities by zone with 60s cache."""
    service = get_data_service()
    return service.fetch_cities_by_zone(zone_id)


@st.cache_data(ttl=30)
def fetch_latest_reading(city_id):
    """Fetch latest reading with 30s cache."""
    service = get_data_service()
    return service.fetch_latest_reading(city_id)


def format_timestamp(timestamp_str):
    """Convert UTC timestamp to Nigerian time (WAT = UTC+1) and format it."""
    if not timestamp_str:
        return "N/A"
    
    try:
        # Parse the timestamp string
        if isinstance(timestamp_str, str):
            # Handle both with and without timezone info
            if '+' in timestamp_str or timestamp_str.endswith('Z'):
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(timestamp_str).replace(tzinfo=timezone.utc)
        else:
            # Already a datetime object
            dt = timestamp_str
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        
        # Convert to Nigerian time (Africa/Lagos = WAT = UTC+1)
        lagos_tz = pytz.timezone('Africa/Lagos')
        local_time = dt.astimezone(lagos_tz)
        
        # Format as readable string
        return local_time.strftime('%Y-%m-%d %H:%M:%S WAT')
    except Exception as e:
        return str(timestamp_str)


# Major cities to display
MAJOR_CITIES = [
    "Lagos", "Abeokuta", "Kano", "Ibadan", "Abuja", "Port Harcourt", "Benin City",
    "Kaduna", "Enugu", "Jos", "Maiduguri", "Ilorin", "Sokoto",
    "Owerri", "Warri", "Aba"
]


def apply_styles():
    """Apply custom CSS styles."""
    st.markdown("""
        <style>
        /* Main theme */
        .stApp {
            background: radial-gradient(circle at top left, #0a0a0a, #101820);
        }
        
        /* Hero banner */
        .hero {
            position: relative;
            background-image: linear-gradient(to bottom, rgba(0,0,0,0.4), rgba(0,0,0,0.85)), 
                              url('https://images.unsplash.com/photo-1504384308090-c894fdcc538d?auto=format&fit=crop&w=1400&q=80');
            background-size: cover;
            background-position: center;
            min-height: 280px;
            border-radius: 15px;
            margin: 0 0 2rem 0;
            padding: 2rem;
            display: flex;
            flex-direction: column;
            justify-content: flex-end;
        }
        .hero h1 {
            font-size: 2.5rem;
            font-weight: 700;
            color: #00aaff;
            margin: 0 0 0.5rem 0;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.8);
        }
        .hero p {
            font-size: 1.1rem;
            color: #e0e0e0;
            margin: 0;
            text-shadow: 1px 1px 2px rgba(0,0,0,0.8);
        }
        
        /* Section titles */
        .section-title {
            font-size: 1.5rem;
            font-weight: 600;
            margin-top: 1.5rem;
            margin-bottom: 1rem;
            color: #00aaff;
        }
        
        /* Enhanced metric cards */
        div[data-testid="stMetric"] {
            background: linear-gradient(135deg, #1a1f2e 0%, #0f1419 100%);
            padding: 1.2rem;
            border-radius: 12px;
            border: 1px solid rgba(0, 170, 255, 0.2);
            box-shadow: 0 4px 10px rgba(0, 122, 255, 0.15);
            transition: all 0.3s ease;
        }
        div[data-testid="stMetric"]:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 20px rgba(0, 170, 255, 0.3);
            border-color: rgba(0, 170, 255, 0.5);
        }
        div[data-testid="stMetric"] label {
            color: #b0b0b0 !important;
            font-weight: 500;
        }
        div[data-testid="stMetric"] [data-testid="stMetricValue"] {
            color: #00aaff !important;
            font-size: 1.8rem !important;
        }
        
        /* Enhanced buttons */
        .stButton>button {
            background: linear-gradient(135deg, #007bff 0%, #0056b3 100%);
            border: none;
            border-radius: 8px;
            padding: 0.6rem 1.5rem;
            font-weight: 600;
            color: white;
            transition: all 0.3s ease;
            box-shadow: 0 2px 8px rgba(0, 123, 255, 0.3);
        }
        .stButton>button:hover {
            background: linear-gradient(135deg, #0056b3 0%, #003d82 100%);
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0, 170, 255, 0.5);
        }
        
        /* Sidebar styling */
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0f1419 0%, #1a1f2e 100%);
            border-right: 1px solid rgba(0, 170, 255, 0.2);
        }
        section[data-testid="stSidebar"] .stSelectbox label,
        section[data-testid="stSidebar"] .stCheckbox label {
            color: #00aaff !important;
            font-weight: 500;
        }
        
        /* Subheaders */
        h2, h3 {
            color: #00aaff !important;
        }
        
        /* Info/warning boxes */
        .stAlert {
            border-radius: 8px;
            border-left: 4px solid #00aaff;
        }
        </style>
    """, unsafe_allow_html=True)


def show_hero():
    """Display hero banner."""
    st.markdown("""
        <div class="hero">
            <h1>SkyPulse - Nigeria Weather Intelligence</h1>
            <p>Real-time weather monitoring across 523 Nigerian cities • Powered by OpenWeatherMap & PostgreSQL</p>
        </div>
    """, unsafe_allow_html=True)


def show_overview(filtered_cities=None):
    """Display major cities overview or filtered cities."""
    
    if filtered_cities:
        st.markdown('<div class="section-title"> Filtered Cities - Latest Readings</div>', unsafe_allow_html=True)
        st.info(f"Showing {len(filtered_cities)} cities based on your filter selection")
        
        cols = st.columns(3)
        for i, city in enumerate(filtered_cities[:30]):
            col = cols[i % 3]
            with col:
                city_name = city['city_name']
                city_id = city['id']
                st.subheader(city_name)
                
                reading = fetch_latest_reading(city_id)
                if reading:
                    st.metric("Temperature", f"{reading['temperature']:.1f} °C")
                    st.write(f"**{reading['weather_main']}**: {reading['weather_desc']}")
                    st.write(f"Humidity: {reading['humidity']}%")
                    st.button("View details", key=f"view_{city_id}", on_click=goto_city, args=(city_id, city_name))
                else:
                    st.info("No readings yet")
        
        if len(filtered_cities) > 30:
            st.info(f"Showing first 30 of {len(filtered_cities)} cities. Use city selector for more.")
        return
    
    # Default view: show major cities
    st.markdown('<div class="section-title"> Major Nigerian Cities - Latest Readings</div>', unsafe_allow_html=True)
    
    cols = st.columns(3)
    cities = fetch_cities()
    
    for i, city_name in enumerate(MAJOR_CITIES):
        col = cols[i % 3]
        with col:
            st.subheader(city_name)
            
            # Find city id
            city_id = None
            for c in cities:
                if c['city_name'].lower() == city_name.lower():
                    city_id = c['id']
                    break
            
            if city_id:
                reading = fetch_latest_reading(city_id)
                if reading:
                    st.metric("Temperature", f"{reading['temperature']} °C")
                    st.write(f"**{reading['weather_main']}**: {reading['weather_desc']}")
                    st.write(f"Humidity: {reading['humidity']}%")
                    st.button("View details", key=f"view_{city_id}", on_click=goto_city, args=(city_id, city_name))
                else:
                    st.info("No readings yet")
            else:
                st.info("City not in database")


def show_city_detail(city_id):
    """Display detailed city view."""
    st.markdown('<div class="section-title"> City Details</div>', unsafe_allow_html=True)
    
    if not city_id:
        st.info("Select a city to view details")
        return
    
    cities = fetch_cities()
    city_meta = next((c for c in cities if c['id'] == city_id), None)
    
    if not city_meta:
        st.error("City not found")
        return
    
    st.subheader(city_meta['city_name'])
    reading = fetch_latest_reading(city_id)
    
    if reading:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Temperature", f"{reading['temperature']} °C")
        with col2:
            st.metric("Humidity", f"{reading['humidity']}%")
        with col3:
            st.metric("Wind Speed", f"{reading['wind_speed']} m/s")
        
        st.markdown(f"**Description:** {reading['weather_main']} — {reading['weather_desc']}")
        st.markdown(f"**Pressure:** {reading['pressure']} hPa")
        st.markdown(f"**Last updated:** {format_timestamp(reading['reading_timestamp'])}")
    else:
        st.warning("No readings available for this city yet.")
    
    # Show map if coordinates available
    if city_meta.get('latitude') and city_meta.get('longitude'):
        import pandas as pd
        df = pd.DataFrame([{
            'lat': city_meta['latitude'],
            'lon': city_meta['longitude']
        }])
        st.map(df)


def sidebar_controls():
    """Render sidebar filter controls."""
    st.sidebar.header("Filters")
    
    zones = fetch_zones()
    zone_names = [z['zone_name'] for z in zones] if zones else []
    st.sidebar.selectbox("Geopolitical zone", options=["All"] + zone_names, key='sidebar_zone')
    
    # States depend on zone selection
    current_zone = st.session_state.get('sidebar_zone')
    sel_zone_id = None
    if current_zone and current_zone != 'All':
        for z in zones:
            if z.get('zone_name') == current_zone:
                sel_zone_id = z.get('id')
                break
    
    states = fetch_states(sel_zone_id)
    state_names = [s['state_name'] for s in states] if states else []
    st.sidebar.selectbox("State", options=["All"] + state_names, key='sidebar_state')
    
    # Cities depend on state selection
    current_state = st.session_state.get('sidebar_state')
    sel_state_id = None
    if current_state and current_state != 'All':
        for s in states:
            if s.get('state_name') == current_state:
                sel_state_id = s.get('id')
                break
    
    cities = fetch_cities(sel_state_id)
    city_names = [c['city_name'] for c in cities] if cities else []
    st.sidebar.selectbox("City", options=["All"] + city_names, key='sidebar_city')
    
    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox("Auto-refresh every minute", value=True, key='auto_refresh')
    
    st.sidebar.button("Apply filters", on_click=apply_filters)


def apply_filters():
    """Apply filter selections and navigate."""
    zone_choice = st.session_state.get('sidebar_zone')
    state_choice = st.session_state.get('sidebar_state')
    city_choice = st.session_state.get('sidebar_city')
    
    zones = fetch_zones()
    states = fetch_states()
    cities = fetch_cities()
    
    # If specific city is selected, go to city detail view
    if city_choice and city_choice != 'All':
        selected_city_id = None
        for c in cities:
            if c.get('city_name') == city_choice:
                selected_city_id = c.get('id')
                break
        
        if selected_city_id:
            st.session_state['selected_city'] = selected_city_id
            st.session_state['view'] = 'city'
            st.session_state['view_city_name'] = city_choice
            st.session_state['filtered_cities'] = None
            return
    
    # Otherwise, filter cities by zone or state
    filtered = None
    
    if state_choice and state_choice != 'All':
        # Filter by state
        state_id = None
        for s in states:
            if s.get('state_name') == state_choice:
                state_id = s.get('id')
                break
        if state_id:
            filtered = fetch_cities(state_id)
    elif zone_choice and zone_choice != 'All':
        # Filter by zone
        zone_id = None
        for z in zones:
            if z.get('zone_name') == zone_choice:
                zone_id = z.get('id')
                break
        if zone_id:
            filtered = fetch_cities_by_zone(zone_id)
    
    st.session_state['filtered_cities'] = filtered
    st.session_state['view'] = 'main'


def goto_city(city_id, city_name=None):
    """Navigate to city detail view."""
    st.session_state['selected_city'] = city_id
    st.session_state['view'] = 'city'
    if city_name:
        st.session_state['view_city_name'] = city_name


def back_to_overview():
    """Navigate back to main overview."""
    st.session_state['view'] = 'main'


def main():
    """Main dashboard application."""
    st.set_page_config(
        page_title="SkyPulse - Nigeria Weather Intelligence",
        page_icon="🌦️",
        layout="wide"
    )
    
    apply_styles()
    show_hero()
    
    # Check database status
    service = get_data_service()
    if not service.is_db_available():
        st.error("Database connection not available. Using CSV fallback for city listings. Weather readings unavailable.")
    
    sidebar_controls()
    
    # Auto-refresh if enabled
    if st.session_state.get('auto_refresh', False):
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=60 * 1000, key="autorefresh")
        except ImportError:
            st.sidebar.info("Auto-refresh disabled: install 'streamlit-autorefresh' to enable.")
    
    # Initialize navigation state
    if 'view' not in st.session_state:
        st.session_state.view = 'main'
    if 'selected_city' not in st.session_state:
        st.session_state.selected_city = None
    if 'filtered_cities' not in st.session_state:
        st.session_state.filtered_cities = None
    
    # Render appropriate view
    if st.session_state.view == 'main':
        left, right = st.columns([2, 1])
        with left:
            show_overview(st.session_state.get('filtered_cities'))
        with right:
            show_city_detail(st.session_state.get('selected_city'))
    elif st.session_state.view == 'city':
        cid = st.session_state.get('selected_city')
        cname = st.session_state.get('view_city_name')
        st.sidebar.markdown(f"### Viewing city: {cname or cid}")
        st.sidebar.button("Back to overview", on_click=back_to_overview)
        show_city_detail(cid)


if __name__ == "__main__":
    main()
