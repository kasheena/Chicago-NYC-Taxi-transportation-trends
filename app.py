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

@st.cache_resource
def get_connection():
    db_path = "nyc_taxi.duckdb"
    if not os.path.exists(db_path):
        st.info("Downloading database file... This may take a while ⏳")
        url = "https://drive.google.com/file/d/10XmGyzqzZvjznaIOfkjhbmj9hr4Zzmix/view?usp=drive_link"
        r = requests.get(url, stream=True)
        with open(db_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        st.success("Database downloaded!")
    return duckdb.connect(db_path)

conn = get_connection()


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
