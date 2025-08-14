# streamlit_app.py
import streamlit as st
import duckdb
import pandas as pd
import altair as alt

# -----------------------------
# Database Connection
# -----------------------------
# def get_connection():
#     return duckdb.connect("nyc_taxi.duckdb")

# conn = get_connection()

import os
import requests
import duckdb
import streamlit as st

import os
import requests
import duckdb
import streamlit as st

# --- MotherDuck Token ---
MD_TOKEN = "taxidata eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...."  # your token
DB_NAME = "nyc_taxi_db"
GDRIVE_FILE_ID = "10XmGyzqzZvjznaIOfkjhbmj9hr4Zzmix"  # Google Drive file ID
LOCAL_DB_PATH = "nyc_taxi.duckdb"

# --- Google Drive download helpers ---
def download_file_from_google_drive(file_id, destination):
    URL = "https://docs.google.com/uc?export=download"
    session = requests.Session()
    response = session.get(URL, params={'id': file_id}, stream=True)
    token = get_confirm_token(response)

    if token:
        params = {'id': file_id, 'confirm': token}
        response = session.get(URL, params=params, stream=True)

    save_response_content(response, destination)

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value
    return None

def save_response_content(response, destination):
    CHUNK_SIZE = 32768
    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk:
                f.write(chunk)

@st.cache_resource
def setup_motherduck():
    # Install & load MotherDuck extension
    duckdb.sql("INSTALL motherduck;")
    duckdb.sql("LOAD motherduck;")
    duckdb.sql(f"SET motherduck_token='{MD_TOKEN}'")

    # Step 1: If MotherDuck DB not present, upload from Google Drive
    # You can check with duckdb.sql("SHOW DATABASES") if DB exists
    try:
        con = duckdb.connect(f"md:{DB_NAME}", config={"motherduck_token": MD_TOKEN})
        con.execute("SELECT 1").fetchone()
        st.info("Connected to existing MotherDuck database ✅")
        return con
    except Exception:
        st.warning("MotherDuck DB not found, uploading from Google Drive...")

        # Download DuckDB file locally
        download_file_from_google_drive(GDRIVE_FILE_ID, LOCAL_DB_PATH)

        # Attach local DB
        duckdb.sql(f"ATTACH '{LOCAL_DB_PATH}' AS local_db (READ_ONLY)")

        # Create MotherDuck DB from local
        duckdb.sql(f"CREATE DATABASE md:{DB_NAME} AS local_db")

        st.success("Uploaded to MotherDuck 🎉")
        return duckdb.connect(f"md:{DB_NAME}", config={"motherduck_token": MD_TOKEN})

# --- Main ---
st.title("NYC Taxi Data - MotherDuck Demo")
conn = setup_motherduck()

# Example query
df = conn.execute("SELECT * FROM chicago_taxi_2019 LIMIT 10").fetchdf()
st.dataframe(df)



# -----------------------------
# Streamlit Page Config
# -----------------------------
st.set_page_config(page_title="Chicago & NYC Transportation Dashboard", layout="wide")
st.markdown(
    """
    <style>
        body {
            background-color: black;
            color: white;
        }
        .stMarkdown, .stDataFrame, .stMetric {
            color: white;
        }
        .stPlotlyChart, .stAltairChart {
            background-color: black;
        }
    </style>
    """,
    unsafe_allow_html=True
)

st.title("🚖 Chicago & NYC Transportation Analysis (Black & White)")

# -----------------------------
# Data Loading Functions
# -----------------------------
def load_table(table_name):
    query = f"SELECT * FROM {table_name} LIMIT 5000"  # limit to avoid heavy load
    return conn.execute(query).fetchdf()

# -----------------------------
# Tabs for Different Views
# -----------------------------
tab1, tab2, tab3, tab4 = st.tabs(["NYC Taxi Data", "Chicago Taxi Data", "Chicago Traffic", "CTA Ridership"])

with tab1:
    st.subheader("NYC Taxi Ridership Trends")
    year = st.selectbox("Select Year", [2019, 2023])
    df_nyc = load_table(f"yellow_taxi_{year}_1" if year == 2019 else "yellow_taxi_2023")

    st.write("Sample Data", df_nyc.head())

    df_nyc['pickup_hour'] = pd.to_datetime(df_nyc['tpep_pickup_datetime']).dt.hour
    hourly_chart = alt.Chart(df_nyc).mark_bar(color="white").encode(
        x=alt.X('pickup_hour:O', title='Hour of Day'),
        y=alt.Y('count()', title='Number of Trips')
    ).properties(width=700, height=400).configure_axis(
        labelColor='white',
        titleColor='white'
    ).configure_view(
        strokeWidth=0
    )

    st.altair_chart(hourly_chart)

with tab2:
    st.subheader("Chicago Taxi Ridership Trends")
    year = st.selectbox("Select Year", [2019, 2023], key="chi")
    df_chi = load_table(f"chicago_taxi_{year}")

    st.write("Sample Data", df_chi.head())

    df_chi['pickup_hour'] = pd.to_datetime(df_chi['trip_start_timestamp']).dt.hour
    hourly_chart_chi = alt.Chart(df_chi).mark_bar(color="white").encode(
        x=alt.X('pickup_hour:O', title='Hour of Day'),
        y=alt.Y('count()', title='Number of Trips')
    ).properties(width=700, height=400).configure_axis(
        labelColor='white',
        titleColor='white'
    )

    st.altair_chart(hourly_chart_chi)

with tab3:
    st.subheader("Chicago Traffic Speed Trends")
    year = st.selectbox("Select Year", [2019, 2023], key="traffic")
    df_traffic = load_table(f"chicago_traffic_{year}")

    st.write("Sample Data", df_traffic.head())

    df_traffic['hour'] = pd.to_datetime(df_traffic['time']).dt.hour
    speed_chart = alt.Chart(df_traffic).mark_line(color="white").encode(
        x=alt.X('hour:O', title='Hour of Day'),
        y=alt.Y('mean(speed)', title='Average Speed (mph)')
    ).properties(width=700, height=400).configure_axis(
        labelColor='white',
        titleColor='white'
    )

    st.altair_chart(speed_chart)

with tab4:
    st.subheader("CTA - L Stations Daily Entries")
    df_cta = load_table("CTA_L_Stations_Daily_Entries")

    st.write("Sample Data", df_cta.head())

    station_selected = st.selectbox("Select Station", df_cta['stationname'].unique())
    df_station = df_cta[df_cta['stationname'] == station_selected]

    ridership_chart = alt.Chart(df_station).mark_line(color="white").encode(
        x=alt.X('date:T', title='Date'),
        y=alt.Y('rides', title='Number of Rides')
    ).properties(width=700, height=400).configure_axis(
        labelColor='white',
        titleColor='white'
    )

    st.altair_chart(ridership_chart)
