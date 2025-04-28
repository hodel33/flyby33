# Built-in libraries
from datetime import datetime
import time
import asyncio
import sqlite3
import os

# Third-party libraries -> requirements.txt
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from streamlit_folium import st_folium
from FlightRadar24 import FlightRadar24API
import reverse_geocode
from configupdater import ConfigUpdater

# Custom made libraries
from main import Utils
import sql_utils as sql

# Setup logging (not used here, just dummy)
Utils.setup_logging("NUL" if os.name == "nt" else "/dev/null")



# ------------------------------------------------------------
# Page Configuration
# ------------------------------------------------------------

st.set_page_config(page_title="Flyby33", page_icon="favicon.svg", layout="wide")

# Load map colors from Utils
colors = Utils.get_custom_colors()

# Add Streamlit specific colors
colors.update({
    "pink_color": "#AF5FAF",
    "cloud_white": "#ECF0F1"
})

# Inject custom CSS - Streamlit
st.markdown(f"""
<style>
            
/* Remove padding from top of the main container */
.block-container {{
    padding-top: 2rem !important;
}}
       
h1 {{
    font-size: 24px !important;
    color: {colors["cloud_white"]} !important;
}}  
            
h4 {{
    color: {colors["cloud_white"]} !important;
    font-size: 22px !important;
    font-weight: bold !important;
}}
                   
p {{
    font-size: 14px !important;
    color: {colors["cloud_white"]} !important;
}}
            
/* Modify top padding for the stSidebarHeader */
[data-testid="stSidebarHeader"] {{
    padding: 0.5rem !important;
}} 
            
/* Remove top padding from stSidebarContent */
[data-testid="stSidebarContent"] {{
    padding-top: 0rem !important;
    background-color: #1d1e25;     
}}         

/* The <pre> tag ensures the ASCII spacing is preserved exactly. */
[data-testid="stMarkdownPre"] {{
    font-family: monospace !important;
    font-size: 11px !important;
    line-height: 1.0 !important;
    color: {colors["pink_color"]} !important; 
    margin: 0 !important;
    padding: 0 !important;
    white-space: pre !important;
}}
            
/* Remove the anchor links */            
[data-testid="stHeaderActionElements"] {{display: none;}}

/* Change the fill color of the progress bar */
[data-baseweb="progress-bar"] > div > div > div {{
    background-color: {colors["pink_color"]} !important;
}}

/* Change the appearance of the gradient bar at the top */
[data-testid="stDecoration"] {{
    background-image: linear-gradient(90deg, #a089a0, #AF5FAF) !important;
}} 

/* Hide the Streamlit status updates */ 
div[data-testid="stStatusWidget"] {{
visibility: hidden;
height: 0%;
position: fixed;
}}

/* Hide the Deploy button */ 
[data-testid="stAppDeployButton"] {{
    display:none;
}}

/* Hide the auto-refresh component iframe */
iframe[title="streamlit_autorefresh.st_autorefresh"] {{
    height: 0 !important;
    position: absolute;
    visibility: hidden;
}}
.element-container:has(iframe[title="streamlit_autorefresh.st_autorefresh"]) {{
    height: 0 !important;
    min-height: 0 !important;
    margin: 0 !important;
    padding: 0 !important;
    visibility: hidden;
}}
    
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------
# Load Configuration Before Initializing Session State
# ------------------------------------------------------------

config_file = 'config.ini'
db_path = "sql_database.db" # Database location
db_init_tables = sql.DatabaseUtils.db_tables # Initialize database tables
flyby_radius_km = 10
flyby_radius_m = flyby_radius_km * 1000

try:
    # Attempt to load and validate the configuration
    origin_location_coords, origin_radius_m, auto_refresh_interval, ignore_airport_proximity, DEBUG_MODE = Utils.load_and_validate_config(config_file)
    origin_radius_km = origin_radius_m / 1000

# Display error message within Streamlit app and halt execution
except ValueError as e:
    st.error(f"**Configuration Error:** {e}\n\nPlease correct the Config file and re-run Streamlit.")
    st.stop()
except FileNotFoundError as e:
    st.error(f"**Configuration Missing:** {e}\n\nPlease download the config.ini file and re-run Streamlit.")
    st.stop()
    exit()

# Checks if database exists - creates and initializes it if not
try: 
    sql.execute(db_path, "SELECT * FROM flights LIMIT 1")
except sqlite3.OperationalError:
    for query in db_init_tables:
        sql.execute(db_path, query)

# ------------------------------------------------------------
# Initialize Session State with Config Data
# ------------------------------------------------------------

# Always update session state with current config.ini values
st.session_state.cfg_location_coords = origin_location_coords
st.session_state.cfg_location_radius_m = origin_radius_m
st.session_state.cfg_location_radius_km = origin_radius_km
st.session_state.ignore_airport_proximity = ignore_airport_proximity
st.session_state.auto_refresh_interval = auto_refresh_interval
st.session_state.debug_mode = DEBUG_MODE

# First initialize essential session state variables with origin coordinates
if 'map_center' not in st.session_state:
    st.session_state.map_center = st.session_state.cfg_location_coords
if 'last_saved_origin_coords' not in st.session_state:
    st.session_state.last_saved_origin_coords = st.session_state.cfg_location_coords
if 'coords_changed' not in st.session_state:
    st.session_state.coords_changed = False

# Check if coordinates have changed from config file
if st.session_state.last_saved_origin_coords != st.session_state.cfg_location_coords:
    st.session_state.last_saved_origin_coords = st.session_state.cfg_location_coords
    st.session_state.map_center = st.session_state.cfg_location_coords
    st.session_state.coords_changed = True
    st.session_state.last_st_data = None # Clear saved map state
else:
    st.session_state.coords_changed = False
    
# Initialize the FlightRadar24API object if not already in session state
if 'fr_api' not in st.session_state:
    st.session_state.fr_api = FlightRadar24API()

# Initialize map_zoom with 6
if 'map_zoom' not in st.session_state:
    st.session_state.map_zoom = 6

# Initialize flight_list to store fetched flight data
if 'flight_list' not in st.session_state:
    st.session_state.flight_list = None

# Store the last known st_data from map interactions
if 'last_st_data' not in st.session_state:
    st.session_state.last_st_data = None

# Initialize the last fetch timestamp if not set
if "last_fetch" not in st.session_state:
    st.session_state.last_fetch = time.time()

# Track whether initial flight data fetch has been completed
if 'initial_fetch_done' not in st.session_state:
    st.session_state.initial_fetch_done = False

# Initialize the page selection state
if 'page_selection' not in st.session_state:
    st.session_state.page_selection = "Map"  # Default page

# Define a callback function for when page selection changes
def on_page_change():

    # Preserve map positioning if available and not forcing map center update (coords changed)
    preserve_map_position()
            
    # Update the page selection
    st.session_state.page_selection = st.session_state.current_page_selection

# Message to display when no flights are found
no_flights_message = "No flight data available. Try refreshing or adjusting your settings."

    

# ------------------------------------------------------------
# Functions
# ------------------------------------------------------------

def preserve_map_position():
    """Helper function to preserve map position when appropriate"""
    if not st.session_state.coords_changed and st.session_state.last_st_data and "center" in st.session_state.last_st_data and "zoom" in st.session_state.last_st_data:
        new_center = [
            st.session_state.last_st_data["center"]["lat"],
            st.session_state.last_st_data["center"]["lng"],
        ]
        new_zoom = st.session_state.last_st_data["zoom"]
        st.session_state.map_center = new_center
        st.session_state.map_zoom = new_zoom

def fetch_flight_data():
    """
    Fetch flight data from API, process it and update session state.
    Used for both initial and auto-refresh fetches.
    
    :return bool: True if fetch completed successfully, regardless of whether flights were found or not.
    """
    # Create progress placeholder in the sidebar
    progress_placeholder = st.sidebar.empty()
    progress_bar = progress_placeholder.progress(0)
    
    # Database cleanup and reference data refresh
    sql.DatabaseUtils.cleanup_old_flights(db_path) # Remove old flight data which is older than 1 week
    Utils.refresh_reference_data(db_path, st.session_state.fr_api, DEBUG_MODE=False) # Refresh airport/airline reference data if older than 1 month
    existing_flights_with_details = [] # Empty list since we are not using the api detailed flight functionality here
    
    async def async_fetch_flight_data():
        completed_tasks = 0
        flight_list = []
        
        async for flight_data, _, total in Utils.fetch_api_flights(
                existing_flights_with_details, 
                st.session_state.cfg_location_coords, 
                st.session_state.cfg_location_radius_m, 
                run_detailed_api=False, 
                fr_api=st.session_state.fr_api, 
                DEBUG_MODE=False):
                
            flight_list.append(flight_data)
            completed_tasks += 1
            progress = int((completed_tasks / total) * 100)
            progress_bar.progress(progress)
            
        return flight_list
    
    # Run the async function in a synchronous context
    flight_active_list = asyncio.run(async_fetch_flight_data())
    
    # Process the flight data and update session state
    if not flight_active_list: # Check if flight_active_list is empty (no active flights in region)
        st.session_state.flight_list = []
    else:
        flight_active_list_ids = [flight.get('flight_id') for flight in flight_active_list] # Extract flight IDs for all active flights
        sql.DatabaseUtils.save_flights_to_db(db_path, flight_active_list) # Save flight data to DB
        enriched_flight_list = sql.DatabaseUtils.enrich_missing_flight_data_from_db(db_path, flight_active_list) # Enrich missing flight data
        sql.DatabaseUtils.save_enriched_flights_to_db(db_path, enriched_flight_list) # Save the enriched data back to the database
        flight_active_list_db_load = sql.DatabaseUtils.load_flights_from_db(db_path, flight_active_list_ids) # Load flight data for active flights
        
        flight_active_list_prepared = Utils.prepare_flight_list(
            flight_active_list_db_load, 
            st.session_state.cfg_location_coords, 
            st.session_state.cfg_location_radius_km, 
            flyby_radius_km, 
            st.session_state.ignore_airport_proximity, 
            DEBUG_MODE=False)
            
        flight_active_list_final = Utils.standardize_flight_keys(flight_active_list_prepared) # Standardize the keys - rename dict keys for display
        st.session_state.flight_list = flight_active_list_final # Store processed flight list in session state
    
    # Preserve map positioning if available and not forcing map center update (coords changed)
    preserve_map_position()
    
    progress_placeholder.empty() # Remove the progress bar when done
    
    # Update fetch timestamp
    st.session_state.last_fetch = time.time()
    
    return True

def generate_and_display_map():
    """
    Generate the Folium map based on the flight data and display it using st_folium.
    Preserve map state (zoom and center) between reruns.
    """

    flight_list = st.session_state.flight_list

    # Generate the Folium map using the imported Utils class
    plane_map_folium = Utils.generate_folium_map(
        flight_list, 
        st.session_state.cfg_location_coords, 
        st.session_state.cfg_location_radius_m, 
        flyby_radius_m,
        view_center=st.session_state.map_center,
        view_zoom=st.session_state.map_zoom,
        ignore_airport_proximity=st.session_state.ignore_airport_proximity
    )

    # Display the map using st_folium with adjusted size
    st_data = st_folium(plane_map_folium, width="100%")

    return st_data

def prepare_dataframe_for_display(flight_list, is_schedule_page=False):
    """
    Prepares a flight dataframe for display with common formatting applied.
    Returns the formatted dataframe or None if filtered result is empty.
    """
    # Convert the list to a DataFrame
    df = pd.DataFrame(flight_list)
    
    # Create the Google Maps link together with a dummy parameter for extracting location name (Streamlit LinkColumn is finnicky)
    if "Location Coords" in df.columns and "Location" in df.columns:
        df["Location"] = df.apply(
            lambda row: (
                f"https://www.google.com/maps/search/?q=loc:{str(row['Location Coords']).strip('()').replace(', ', ',')}"
                f"&loc_name={row['Location']}"
            ) if row["Location Coords"] != "-" else row["Location"],
            axis=1
        )
    
    # Remove columns we don't want for display
    df = df.drop(
        columns=[col for col in df_columns_to_remove if col in df.columns],
        errors="ignore"
    )
    
    # Format "Flyby Chance" as %
    df['Flyby Chance'] = df['Flyby Chance'].apply(
        lambda x: f"{int(round(x * 100))} %" if pd.notna(x) and x != "-" else x
    )
    
    # Rename "Aircraft Code" -> "Aircraft"
    if "Aircraft Code" in df.columns:
        df.rename(columns={"Aircraft Code": "Aircraft"}, inplace=True)
    
    # Format "ETA" to only show "HH:MM:SS"
    if "ETA" in df.columns:
        df["ETA"] = df["ETA"].apply(
            lambda x: x.strftime("%H:%M:%S") if isinstance(x, datetime) else x
        )
    
    # For Schedule page, filter and rearrange columns
    if is_schedule_page:

        # Filter out rows with 'ETA' == '-' (missing ETAs)
        df = df[df["ETA"] != "-"]
        
        # If there's still data left, sort/format and display
        if df.empty:
            return None
            
        # Sort by ETA
        df = df.sort_values(by="ETA", ascending=True)
        
        # Re-arrange columns to bring certain columns to the front
        cols_to_move_schedule = ["ETA", "Flyby Chance", "Flyby Info", "Distance (km)"]
        front_cols = [col for col in cols_to_move_schedule if col in df.columns]
        other_cols = [col for col in df.columns if col not in front_cols]
        df = df[front_cols + other_cols]

    else:
        # Sort by Distance (km) in ascending order, ignoring non-numeric
        if "Distance (km)" in df.columns:

            # Convert distance to numeric, coerce invalids to NaN
            df["Distance (km)"] = pd.to_numeric(df["Distance (km)"], errors="coerce")

            # Sort by numeric distance (NaNs go to bottom by default)
            df = df.sort_values(by="Distance (km)", ascending=True)

            # Convert numeric distances to integer (Int64) where possible
            df["Distance (km)"] = df["Distance (km)"].astype("Int64")

            # Replace NaN with "-"
            df["Distance (km)"] = df["Distance (km)"].fillna("-")
    
    # Reset index to start from 1
    df.index = range(1, len(df) + 1)
    
    # Convert "Tail No" values into clickable Flightradar24 URLs
    if "Tail No" in df.columns:
        df["Tail No"] = df["Tail No"].apply(
            lambda x: f"{url_flightradar_reg_no_base}{x}" if x != "-" else ""
        )
    
    # Convert "Aircraft" values into clickable Skybrary URLs
    if "Aircraft" in df.columns:
        df["Aircraft"] = df["Aircraft"].apply(
            lambda x: f"{url_skybrary_aircraft_icao_base}{x}" if x != "-" else ""
        )
    
    # Replace all "-" entries with empty strings to enforce the same format for missing values.
    df.replace("-", "", inplace=True)
    
    return df



# ------------------------------------------------------------
# Sidebar
# ------------------------------------------------------------

with st.sidebar:

    # Custom CSS for sidebar styling
    st.markdown(f"""
    <style>
              
    /* Increase font size and add spacing for radio button labels */
    div[data-testid="stRadio"] div[data-testid="stMarkdownContainer"] p {{
        font-size: 20px !important;
        margin-left: -8px;
        margin-top: 3px;
    }}
                
    /* On hover: Change label text color */
    div[data-testid="stRadio"] div[data-testid="stMarkdownContainer"] p:hover {{
        color: #a089a0 !important;
    }}
                
    /* On select: Change label text color */
    div[data-testid="stRadio"] label input[type="radio"]:checked ~ div > div[data-testid="stMarkdownContainer"] p {{
        color: #af75af !important;
    }} 
                
    /* Increase font size and add spacing for radio button labels */
    div[data-testid="stRadio"] div[data-testid="stCaptionContainer"] p {{
        font-size: 12px !important;
        margin-left: -8px;
    }}
                               
    /* Hide the actual buttons */
    div[data-testid="stRadio"] label > div:first-child {{
        display: none
    }}              
                
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="overflow: hidden;">
    <pre>
    ░█▀▀░█░░░█░█░█▀▄░█░█░▀▀█░▀▀█
    ░█▀▀░█░░░░█░░█▀▄░░█░░░▀▄░░▀▄
    ░▀░░░▀▀▀░░▀░░▀▀░░░▀░░▀▀░░▀▀░
    <span style="display:block; margin-top:-8px; font-size:12px;">
    © 2025 Hodel33
    </span>
    </pre>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='height: 48px;'></div>", unsafe_allow_html=True)

    # Get the closest known city to the origin coordinates
    location = reverse_geocode.get((st.session_state.cfg_location_coords))["city"]
    loc_radius_km = int(st.session_state.cfg_location_radius_km)

    # Define menu items
    menu_items = ["Map", "List", "Schedule", "Settings"]

    # Create captions list: None for other menu items, captions for "Settings"
    page_captions = [None, None, None, f"{location} &nbsp;|&nbsp; {loc_radius_km} km"]

    # Radio button for page selection
    page_selection = st.radio("Pages", 
                            menu_items, 
                            label_visibility="collapsed",
                            captions=page_captions,
                            index=menu_items.index(st.session_state.page_selection),
                            key="current_page_selection",
                            on_change=on_page_change)
    
    st.markdown("<div style='height: 48px;'></div>", unsafe_allow_html=True)

    icon_margin_right = 12

    # SVG icons
    svg_github = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0,0,256,256" width="24px" height="24px" fill-rule="nonzero" style="opacity: 0.7; transition: opacity 0.3s;">
                <g fill="{colors["cloud_white"]}" fill-rule="nonzero" stroke="none" stroke-width="1" stroke-linecap="butt" stroke-linejoin="miter" stroke-miterlimit="10" stroke-dasharray="" stroke-dashoffset="0" font-family="none" font-weight="none" font-size="none" text-anchor="none" style="mix-blend-mode: normal"><g transform="scale(10.8,10.8)"><path d="M12,2c-5.52344,0 -10,4.47656 -10,10c0,5.52344 4.47656,10 10,10c5.52344,0 10,-4.47656 10,-10c0,-5.52344 -4.47656,-10 -10,-10zM12,4c4.41016,0 8,3.58984 8,8c0,0.46875 -0.04687,0.92969 -0.125,1.375c-0.24609,-0.05469 -0.60937,-0.12109 -1.03125,-0.125c-0.3125,-0.00391 -0.70312,0.04688 -1.03125,0.09375c0.11328,-0.34766 0.1875,-0.73047 0.1875,-1.125c0,-0.96094 -0.46875,-1.85547 -1.21875,-2.59375c0.20703,-0.76953 0.41016,-2.08984 -0.125,-2.625c-1.58203,0 -2.45703,1.12891 -2.5,1.1875c-0.48828,-0.11719 -0.99219,-0.1875 -1.53125,-0.1875c-0.69141,0 -1.35156,0.125 -1.96875,0.3125l0.1875,-0.15625c0,0 -0.87891,-1.21875 -2.5,-1.21875c-0.56641,0.57031 -0.30859,2.01563 -0.09375,2.75c-0.76562,0.73047 -1.25,1.59375 -1.25,2.53125c0,0.32813 0.07813,0.64063 0.15625,0.9375c-0.27734,-0.03125 -1.27734,-0.125 -1.6875,-0.125c-0.36328,0 -0.92578,0.08594 -1.375,0.1875c-0.0625,-0.39844 -0.09375,-0.80469 -0.09375,-1.21875c0,-4.41016 3.58984,-8 8,-8zM5.46875,13.28125c0.39453,0 1.59375,0.14063 1.75,0.15625c0.01953,0.05469 0.03906,0.10547 0.0625,0.15625c-0.42969,-0.03906 -1.26172,-0.09766 -1.8125,-0.03125c-0.36719,0.04297 -0.83594,0.17578 -1.25,0.28125c-0.03125,-0.125 -0.07031,-0.24609 -0.09375,-0.375c0.4375,-0.09375 1.01172,-0.1875 1.34375,-0.1875zM18.84375,13.5c0.39844,0.00391 0.76172,0.07031 1,0.125c-0.01172,0.06641 -0.04687,0.12109 -0.0625,0.1875c-0.25391,-0.05859 -0.67187,-0.14453 -1.15625,-0.15625c-0.23437,-0.00391 -0.60937,0.00781 -0.9375,0.03125c0.01563,-0.03125 0.01953,-0.0625 0.03125,-0.09375c0.33984,-0.04687 0.77344,-0.09766 1.125,-0.09375zM6.09375,13.78125c0.5625,0.00391 1.08984,0.04297 1.3125,0.0625c0.52344,0.97656 1.58203,1.69922 3.21875,2c-0.40234,0.22266 -0.76172,0.53516 -1.03125,0.90625c-0.23437,0.01953 -0.48047,0.03125 -0.71875,0.03125c-0.69531,0 -1.12891,-0.62109 -1.5,-1.15625c-0.375,-0.53516 -0.83594,-0.59375 -1.09375,-0.625c-0.26172,-0.03125 -0.35156,0.11719 -0.21875,0.21875c0.76172,0.58594 1.03516,1.28125 1.34375,1.90625c0.27734,0.5625 0.85938,0.875 1.5,0.875h0.125c-0.01953,0.10938 -0.03125,0.21094 -0.03125,0.3125v1.09375c-2.30859,-0.93359 -4.06641,-2.90625 -4.71875,-5.34375c0.41016,-0.10156 0.87109,-0.20703 1.21875,-0.25c0.16016,-0.01953 0.36328,-0.03516 0.59375,-0.03125zM18.625,13.90625c0.44922,0.01172 0.84766,0.09766 1.09375,0.15625c-0.55078,2.07031 -1.91016,3.79297 -3.71875,4.84375v-0.59375c0,-0.85156 -0.67187,-1.94531 -1.625,-2.46875c1.58203,-0.28906 2.61328,-0.98047 3.15625,-1.90625c0.37891,-0.02734 0.82422,-0.03906 1.09375,-0.03125zM12.5,18c0.27344,0 0.5,0.22656 0.5,0.5v1.4375c-0.32812,0.04297 -0.66016,0.0625 -1,0.0625v-1.5c0,-0.27344 0.22656,-0.5 0.5,-0.5zM10.5,19c0.27344,0 0.5,0.22656 0.5,0.5v0.4375c-0.33594,-0.04297 -0.67578,-0.10547 -1,-0.1875v-0.25c0,-0.27344 0.22656,-0.5 0.5,-0.5zM14.5,19c0.24219,0 0.45313,0.17578 0.5,0.40625c-0.32422,0.13281 -0.65625,0.25391 -1,0.34375v-0.25c0,-0.27344 0.22656,-0.5 0.5,-0.5z"></path></g></g>
            </svg>"""

    svg_linkedin = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0,0,256,256" width="24px" height="24px" fill-rule="nonzero" style="opacity: 0.7; transition: opacity 0.3s;">
                <g fill="{colors["cloud_white"]}" fill-rule="nonzero" stroke="none" stroke-width="1" stroke-linecap="butt" stroke-linejoin="miter" stroke-miterlimit="10" stroke-dasharray="" stroke-dashoffset="0" font-family="none" font-weight="none" font-size="none" text-anchor="none" style="mix-blend-mode: normal"><g transform="scale(10.8,10.8)"><path d="M19,3h-14c-1.105,0 -2,0.895 -2,2v14c0,1.105 0.895,2 2,2h14c1.105,0 2,-0.895 2,-2v-14c0,-1.105 -0.895,-2 -2,-2zM9,17h-2.523v-7h2.523zM7.694,8.717c-0.771,0 -1.286,-0.514 -1.286,-1.2c0,-0.686 0.514,-1.2 1.371,-1.2c0.771,0 1.286,0.514 1.286,1.2c0,0.686 -0.514,1.2 -1.371,1.2zM18,17h-2.442v-3.826c0,-1.058 -0.651,-1.302 -0.895,-1.302c-0.244,0 -1.058,0.163 -1.058,1.302c0,0.163 0,3.826 0,3.826h-2.523v-7h2.523v0.977c0.325,-0.57 0.976,-0.977 2.197,-0.977c1.221,0 2.198,0.977 2.198,3.174z"></path></g></g>
            </svg>"""

    svg_email = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0,0,256,256" width="24px" height="24px" fill-rule="nonzero" style="opacity: 0.7; transition: opacity 0.3s; transform: translateY(-1px);">
                <g fill="{colors["cloud_white"]}" fill-rule="nonzero" stroke="none" stroke-width="1" stroke-linecap="butt" stroke-linejoin="miter" stroke-miterlimit="10" stroke-dasharray="" stroke-dashoffset="0" font-family="none" font-weight="none" font-size="none" text-anchor="none" style="mix-blend-mode: normal"><g transform="scale(11.8,11.8)"><path d="M20,4h-16c-1.105,0 -2,0.895 -2,2v12c0,1.105 0.895,2 2,2h16c1.105,0 2,-0.895 2,-2v-12c0,-1.105 -0.895,-2 -2,-2zM20,8l-8,5l-8,-5v-2l8,5l8,-5z"></path></g></g>
            </svg>"""

    # Social links using the SVG variables
    st.markdown(f"""
    <div style="display: flex; justify-content: flex-start; align-items: center;">
        <a href="https://github.com/hodel33" target="_blank" style="margin-right: {icon_margin_right}px;">
            {svg_github}
        </a>
        <a href="https://www.linkedin.com/in/bjornhodel" target="_blank" style="margin-right: {icon_margin_right}px;">
            {svg_linkedin}
        </a>
        <a href="mailto:hodel33@gmail.com" target="_blank" style="margin-right: {icon_margin_right}px;">
            {svg_email}
        </a>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br><br>", unsafe_allow_html=True)


# ------------------------------------------------------------
# Data Fetching and Auto-Refresh Logic
# ------------------------------------------------------------

current_time = time.time()
time_since_last_fetch = current_time - st.session_state.last_fetch

# Handle initial fetch and coordinate changes
if not st.session_state.initial_fetch_done: # This will trigger the initial fetch only once when the app first loads
    st.session_state.initial_fetch_done = fetch_flight_data()

elif st.session_state.coords_changed: # Check if we need to fetch due to coordinate change
    fetch_flight_data()
    st.session_state.coords_changed = False
    st.rerun()

# Auto-refresh logic
if st.session_state.auto_refresh_interval > 0:
    st_autorefresh(
        interval=st.session_state.auto_refresh_interval * 1000,
        limit=None,
        key="auto_refresh_counter"
    )

    # Only fetch if enough time has passed (-2 since the st_autorefresh can trigger slightly earlier than the specified interval)
    if time_since_last_fetch >= (st.session_state.auto_refresh_interval - 2): # 
        fetch_flight_data()


# ------------------------------------------------------------
# Pages
# ------------------------------------------------------------

# Define the base URLs for Flightradar links
url_flightradar_reg_no_base = "https://www.flightradar24.com/data/aircraft/"
url_skybrary_aircraft_icao_base = "https://skybrary.aero/aircraft/"

# Define column configurations with tooltips
column_configs = {
    "Callsign": st.column_config.TextColumn(
        help="The flight's callsign identifier.",
        width="small"
    ),
    "Tail No": st.column_config.LinkColumn(
        help="The flight's tail number. Click to view aircraft details on Flightradar.",
        display_text=r".*/data/aircraft/(.*)",  # Extract Tail No for display
        width="small"
    ),
    "Aircraft": st.column_config.LinkColumn(
        help="The ICAO model code of the aircraft. Click to view aircraft details on Skybrary.",
        display_text=r".*/aircraft/(.*)",  # Extract ICAO code for display
        width="small"
    ),
    "Airline": st.column_config.TextColumn(
        help="The airline operating the flight.",
        width="small"
    ),
    "Alt (m)": st.column_config.NumberColumn(
        help="Current altitude of the aircraft in meters.",
        width="small"
    ),
    "Speed (km/h)": st.column_config.NumberColumn(
        help="Current speed of the aircraft in km/h.",
        width="small"
    ),
    "Heading (°)": st.column_config.NumberColumn(
        help="Current heading of the aircraft in degrees.",
        width="small"
    ),
    "Location": st.column_config.LinkColumn(
        help="Nearest city to aircraft's current position. Click to view coordinates on Google Maps.",
        display_text=r".*&loc_name=(.*)", # Retrieve the dummy parameter for extracting location name for DataFrame display
        width="small"
    ),
    "Origin": st.column_config.TextColumn(
        help="Origin airport and city.",
        width="small"
    ),
    "Destination": st.column_config.TextColumn(
        help="Destination airport and city.",
        width="small"
    ),
    "Distance (km)": st.column_config.NumberColumn(
        help="Distance from the origin point in km.",
        width="small"
    ),
    "Flyby Chance": st.column_config.TextColumn(
        help="Probability of a flyby event, expressed as a percentage.",
        width="small"
    ),
    "Flyby Info": st.column_config.TextColumn(
        help="Closest approach distance and direction from your location.",
        width="small"
    ),
    "ETA": st.column_config.TextColumn(
        help="Estimated Time of Arrival (HH:MM:SS).",
        width="small"
    )
}

# Columns to remove
df_columns_to_remove = [
    "Flight No", "Aircraft", "Airline Code", "Origin Airport", "Origin City", "Destination Airport", "Destination Airport Location", 
    "Destination City", "Location Coords", "Timestamp", "is_heading_towards_origin", "will_pass_within_radius", 
    "is_closer_to_airport_than_origin", "latest_trail_data",
    ]


if page_selection == "Map":

    st.markdown("<h4>Flyby33 - Flight Map</h4>", unsafe_allow_html=True)
    st.markdown("") # add space

    # If there's flight data, generate & show it
    if st.session_state.flight_list:

        st_data = generate_and_display_map()

        # Only store user interactions if coordinates haven't changed
        if st_data and not st.session_state.coords_changed:
            st.session_state.last_st_data = st_data
            
        # Reset the coordinates changed flag after generating the map
        if st.session_state.coords_changed:
            st.session_state.coords_changed = False
            
    else:
        st.info(no_flights_message)


elif page_selection == "List":

    st.markdown("<h4>Flyby33 - Flight List</h4>", unsafe_allow_html=True)
    st.markdown("") # add space

    if st.session_state.flight_list:
        df_flights_list = prepare_dataframe_for_display(st.session_state.flight_list)
        
        # Display the DataFrame using st.dataframe with column_config
        st.dataframe(
            df_flights_list,
            column_config=column_configs,
            hide_index=True,
            use_container_width=True
        )
    else:
        st.info(no_flights_message)


elif page_selection == "Schedule":

    st.markdown("<h4>Flyby33 - Flight Schedule</h4>", unsafe_allow_html=True)
    st.markdown("") # add space

    if st.session_state.flight_list:
        df_flights_schedule = prepare_dataframe_for_display(st.session_state.flight_list, is_schedule_page=True)
        
        if df_flights_schedule is not None:
            # Display the DataFrame using st.dataframe with column_config
            st.dataframe(
                df_flights_schedule,
                column_config=column_configs,
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info("No flights with valid ETA found for scheduling.")
    else:
        st.info(no_flights_message)


elif page_selection == "Settings":

    st.markdown("<h4>Flyby33 - Settings</h4>", unsafe_allow_html=True)
    st.markdown("") # add space

    # Prepare config updater for saving changes later
    config_file = 'config.ini'
    updater = ConfigUpdater()
    updater.read(config_file)

    # Expecting a string format: "lat, lng"
    coords_default = ", ".join(map(str, st.session_state.cfg_location_coords))
    location_coords_input = st.text_input("Origin Location Coordinates (Lat, Lng)", value=coords_default)
    st.markdown("") # add space
    
    # Slider for Origin Location Radius
    location_radius_input = st.select_slider(
        "Origin Location Radius (km)",
        options=list(range(50, 201, 50)),
        value=int(st.session_state.cfg_location_radius_km),
        format_func=lambda x: f"{x} km"
    )
    st.markdown("") # add space

    # Format function to display slider values
    def format_auto_refresh(x):
        if x > 30:
            return f"{x//60} min"
        else:
            return f"{x} sec"
        
    auto_refresh_options = [0, 15, 30, 60]

    # Slider for Auto Refresh Interval
    interval_seconds = st.select_slider(
        "Auto Refresh Interval (seconds, 0: Off)",
        options=auto_refresh_options,
        value=st.session_state.auto_refresh_interval,
        format_func=format_auto_refresh
    )
    st.markdown("") # add space

    # Checkbox for Ignore Airport Proximity
    ignore_airport_proximity_input = st.checkbox(
        "Ignore Airport Proximity",
        value=st.session_state.ignore_airport_proximity,
        help="Process flyby calculations even for planes that appear to be on final approach to nearby airports (those closer to their destination than to the origin location)."
    )
    st.markdown("") # add space
    st.markdown("") # add space

    # Save Button to update config and session state
    if st.button("Save Changes"):
        # Validate and parse the coordinates input
        try:
            cleaned_coords = location_coords_input.replace('(', '').replace(')', '').strip()
            lat_str, lng_str = cleaned_coords.split(',')
            lat = float(lat_str.strip())
            lng = float(lng_str.strip())
            new_coords = (lat, lng)

        except Exception as e:
            st.error("Invalid format for coordinates. Please use the format: 'lat, lng'.")

        else:

            # Get new radius
            new_radius_km = location_radius_input

            # Write the new settings back to the config.ini file
            updater['Settings']['location_coords'].value = f"{lat}, {lng}"
            updater['Settings']['location_radius'].value = str(new_radius_km)
            updater['Settings']['auto_refresh_interval'].value = str(interval_seconds)
            updater['Settings']['ignore_airport_proximity'].value = str(ignore_airport_proximity_input)

            with open(config_file, 'w') as f:
                updater.write(f)
          
            # Reset refresh timer to start countdown from this moment
            st.session_state.last_fetch = time.time()

            st.success("Configuration updated successfully!")