
import os
import duckdb
import pandas as pd
import streamlit as st
import altair as alt

# -----------------------------
# Page config (must be first)
# -----------------------------
st.set_page_config(
    page_title="CommutePulse — Chicago & NYC Transportation Analytics",
    layout="wide",
    page_icon="🚉"
)

# -----------------------------
# Design System — Urban Transit Theme
# -----------------------------
st.markdown("""
<style>
:root {
  --bg: #0b0f14;
  --panel: #111723;
  --panel-2: #0E141F;
  --text: #e6eef9;
  --muted: #9db1c9;
  --accent: #FF7A00;   /* transit orange */
  --primary: #0A84FF;  /* deep blue */
  --success: #17B26A;  /* success green */
  --warn: #FFD166;
  --danger: #EF476F;
  --shadow: 0 10px 30px rgba(0,0,0,0.35);
}
html, body, [class*="css"]  {
  background: var(--bg) !important;
  color: var(--text) !important;
  font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial, "Noto Sans", "Liberation Sans", sans-serif;
}
section.main > div { padding-top: 12px; }
h1, h2, h3, h4 { color: var(--text); letter-spacing: 0.2px; }
hr { border: none; border-top: 1px solid #202a39; margin: 0.5rem 0 1rem; }
.block { background: linear-gradient(140deg, var(--panel), var(--panel-2)); border:1px solid #182235; border-radius:18px; box-shadow: var(--shadow); padding: 16px 18px; }
.metric {
  display:flex; align-items:center; gap:14px; background: #0e1622; border:1px solid #1b2a40; border-radius:16px; padding:14px 16px;
}
.metric .value { font-size: 26px; font-weight: 700; color: var(--text); }
.metric .label { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.7px; }
.badge { display:inline-block; padding:2px 8px; border-radius:12px; font-size:12px; border:1px solid #1c2a40; color: var(--muted); }
.kpi-up { color: var(--success); }
.kpi-down { color: var(--danger); }
.header {
  background: radial-gradient(80% 120% at 20% 0%, rgba(10,132,255,0.25), rgba(255,122,0,0.08) 60%, transparent 90%);
  border:1px solid #162236; border-radius:22px; padding:18px; margin-bottom: 12px;
}
.caption { color: var(--muted); font-size: 13px; }
</style>
""", unsafe_allow_html=True)

# -----------------------------
# MotherDuck connection
# -----------------------------
MD_TOKEN = st.secrets.get("MOTHERDUCK_TOKEN", os.getenv("MOTHERDUCK_TOKEN", ""))  # set in Streamlit secrets or env
DB_ALIAS = "motherduck_db"
MD_DB_NAME = "taxi_assign" 
MD_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6Imthc2hlZW5hcGVyc29uYWxAZ21haWwuY29tIiwic2Vzc2lvbiI6Imthc2hlZW5hcGVyc29uYWwuZ21haWwuY29tIiwicGF0IjoiUEk4WnZwcC1zNEFDZFYtRWYxaEtoX0k2aFZoZmhDTTJQRTRGY2Y5UVJQWSIsInVzZXJJZCI6Ijk4MWZiMjYzLTQ1NzEtNDk2OS04NWNkLWM0ZjA3MGE0ZTg4YSIsImlzcyI6Im1kX3BhdCIsInJlYWRPbmx5IjpmYWxzZSwidG9rZW5UeXBlIjoicmVhZF93cml0ZSIsImlhdCI6MTc1NTE3ODI4Mn0.bjWdIVWu-3suCbmyRu0UEr-jSu8kPmfpYrZ5xPH_-xo"  # replace with your actual token

if not MD_TOKEN:
    st.error("MotherDuck token not found. Add MOTHERDUCK_TOKEN to your Streamlit secrets or environment.")
    st.stop()

@st.cache_resource(show_spinner=False)
def connect_md():
    # Install & load extension (idempotent)
    duckdb.sql("INSTALL motherduck;")
    duckdb.sql("LOAD motherduck;")
    duckdb.sql(f"SET motherduck_token='{MD_TOKEN}';")
    # Attach the database
    duckdb.sql(f"ATTACH 'md:{MD_DB_NAME}' AS {DB_ALIAS};")
    # Return a connection bound to the current DuckDB context
    return duckdb.connect()

conn = connect_md()

def qdf(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Query helper -> DataFrame."""
    if params:
        return conn.execute(sql, params).fetchdf()
    return conn.execute(sql).fetchdf()

# -----------------------------
# Header
# -----------------------------
colL, colR = st.columns([0.75, 0.25])
with colL:
    st.markdown(f"""
    <div class="header">
      <div style="display:flex; align-items:center; gap:14px;">
        <div class="badge">CommutePulse</div>
        <h1 style="margin:0;">Chicago & NYC Transportation Analytics</h1>
      </div>
      <div class="caption">Operational insights for CTA & NYC: taxi demand, traffic congestion, and L ridership — 2019 vs 2023 recovery.</div>
    </div>
    """, unsafe_allow_html=True)
with colR:
    # Global controls
    city = st.selectbox("City focus (for maps/spotlights)", ["Chicago", "NYC"], index=0)
    agg_level = st.selectbox("Time aggregation", ["Hourly", "Daily", "Weekly", "Monthly"], index=3)

# -----------------------------
# Filters & time bounds
# -----------------------------
years = st.multiselect("Year(s)", [2019, 2023], default=[2019, 2023])
st.caption("Tip: You can filter down to a single year to zoom in.")

# -----------------------------
# KPIs (all computed from your schemas only)
# -----------------------------
# NYC trips 2019/2023 (from pickup timestamp string)
sql_nyc_kpi = f"""
WITH y19 AS (
    SELECT COUNT(*) AS trips_2019
    FROM {DB_ALIAS}.main.yellow_taxi_2019_1
),
y23 AS (
    SELECT COUNT(*) AS trips_2023
    FROM {DB_ALIAS}.main.yellow_taxi_2023
)
SELECT 
    trips_2019, 
    trips_2023,
    CASE 
        WHEN trips_2019 > 0 
        THEN 100.0 * trips_2023 / trips_2019 
        ELSE NULL 
    END AS recovery_pct
FROM y19, y23;
"""

# Execute the query and fetch the first row of results
nyc_kpi_df = qdf(sql_nyc_kpi)

# Extract the first row for KPI values
nyc_kpi = nyc_kpi_df.iloc[0]

# Chicago trips 2019/2023
sql_chi_kpi = f"""
WITH y19 AS (SELECT COUNT(*) trips_2019 FROM {DB_ALIAS}.main.chicago_taxi_2019),
     y23 AS (SELECT COUNT(*) trips_2023 FROM {DB_ALIAS}.main.chicago_taxi_2023)
SELECT trips_2019, trips_2023,
       CASE WHEN trips_2019>0 THEN 100.0 * trips_2023 / trips_2019 ELSE NULL END AS recovery_pct
FROM y19, y23;
"""
chi_kpi = qdf(sql_chi_kpi).iloc[0]

# CTA total rides (all-time in table; you can later filter by date widgets if needed)
sql_cta = f"SELECT SUM(rides)::BIGINT AS total_rides FROM {DB_ALIAS}.main.\"CTA - L Stations Daily Entries\";"
cta_total = qdf(sql_cta).iloc[0]["total_rides"]

# Traffic: Chicago average speed by year (for quick context)
sql_traffic_kpi = f"""
SELECT
  2019 AS year, AVG(speed) AS avg_speed
FROM {DB_ALIAS}.main.chicago_traffic_2019
UNION ALL
SELECT
  2023 AS year, AVG(speed) AS avg_speed
FROM {DB_ALIAS}.main.chicago_traffic_2023
ORDER BY year;
"""
traffic_kpi = qdf(sql_traffic_kpi)

# KPI row
k1, k2, k3, k4 = st.columns(4)
with k1:
    delta = (nyc_kpi["recovery_pct"] or 0) - 100
    cls = "kpi-up" if delta >= 0 else "kpi-down"
    st.markdown(f"""
    <div class="metric">
      <div>
        <div class="label">NYC Taxi Trips (Recovery)</div>
        <div class="value">{nyc_kpi['trips_2023']:,} / {nyc_kpi['trips_2019']:,}</div>
        <div class="{cls}">{nyc_kpi['recovery_pct']:.1f}% vs 2019</div>
      </div>
    </div>
    """, unsafe_allow_html=True)
with k2:
    delta = (chi_kpi["recovery_pct"] or 0) - 100
    cls = "kpi-up" if delta >= 0 else "kpi-down"
    st.markdown(f"""
    <div class="metric">
      <div>
        <div class="label">Chicago Taxi Trips (Recovery)</div>
        <div class="value">{chi_kpi['trips_2023']:,} / {chi_kpi['trips_2019']:,}</div>
        <div class="{cls}">{chi_kpi['recovery_pct']:.1f}% vs 2019</div>
      </div>
    </div>
    """, unsafe_allow_html=True)
with k3:
    st.markdown(f"""
    <div class="metric">
      <div>
        <div class="label">CTA — Total Recorded L Entries</div>
        <div class="value">{cta_total:,}</div>
        <div class="caption">All dates available in source</div>
      </div>
    </div>
    """, unsafe_allow_html=True)
with k4:
    sp19 = float(traffic_kpi.loc[traffic_kpi["year"]==2019, "avg_speed"].values[0]) if (traffic_kpi["year"]==2019).any() else None
    sp23 = float(traffic_kpi.loc[traffic_kpi["year"]==2023, "avg_speed"].values[0]) if (traffic_kpi["year"]==2023).any() else None
    if sp19 and sp23:
        delta = sp23 - sp19
        cls = "kpi-up" if delta >= 0 else "kpi-down"
        st.markdown(f"""
        <div class="metric">
          <div>
            <div class="label">Chicago Traffic Avg Speed</div>
            <div class="value">{sp23:.1f} mph</div>
            <div class="{cls}">{delta:+.1f} vs 2019</div>
          </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="metric">
          <div>
            <div class="label">Chicago Traffic Avg Speed</div>
            <div class="value">—</div>
            <div class="caption">Insufficient data</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<hr/>", unsafe_allow_html=True)

# -----------------------------
# Ridership Recovery — Monthly (2019 vs 2023)
# -----------------------------
# NYC monthly counts (parse timestamps from VARCHAR)
sql_nyc_monthly = f"""
WITH base AS (
  SELECT
    2019 AS year,
    DATE_TRUNC('month', CAST(tpep_pickup_datetime AS TIMESTAMP)) AS month,
    1 AS cnt
  FROM {DB_ALIAS}.main.yellow_taxi_2019_1
  UNION ALL
  SELECT
    2023 AS year,
    DATE_TRUNC('month', CAST(tpep_pickup_datetime AS TIMESTAMP)) AS month,
    1 AS cnt
  FROM {DB_ALIAS}.main.yellow_taxi_2023
)
SELECT year, month, SUM(cnt) AS trips
FROM base
WHERE year IN ({",".join([str(y) for y in years])})
GROUP BY 1,2
ORDER BY 2,1;
"""
nyc_monthly = qdf(sql_nyc_monthly)

# Chicago monthly counts
sql_chi_monthly = f"""
WITH base AS (
  SELECT 2019 AS year, DATE_TRUNC('month', trip_start_timestamp) AS month, 1 AS cnt
  FROM {DB_ALIAS}.main.chicago_taxi_2019
  UNION ALL
  SELECT 2023 AS year, DATE_TRUNC('month', trip_start_timestamp) AS month, 1 AS cnt
  FROM {DB_ALIAS}.main.chicago_taxi_2023
)
SELECT year, month, SUM(cnt) AS trips
FROM base
WHERE year IN ({",".join([str(y) for y in years])})
GROUP BY 1,2
ORDER BY 2,1;
"""
chi_monthly = qdf(sql_chi_monthly)

lcol, rcol = st.columns(2)
with lcol:
    st.subheader("NYC — Monthly Taxi Trips (2019 vs 2023)")
    if not nyc_monthly.empty:
        c = alt.Chart(nyc_monthly).mark_line(point=True).encode(
            x=alt.X('month:T', title='Month'),
            y=alt.Y('trips:Q', title='Trips'),
            color=alt.Color('year:N', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
            tooltip=['year', alt.Tooltip('month:T'), 'trips:Q']
        ).properties(height=320).configure_axis(
            labelColor='#e6eef9', titleColor='#e6eef9'
        ).configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No NYC data for selected year(s).")
with rcol:
    st.subheader("Chicago — Monthly Taxi Trips (2019 vs 2023)")
    if not chi_monthly.empty:
        c = alt.Chart(chi_monthly).mark_line(point=True).encode(
            x=alt.X('month:T', title='Month'),
            y=alt.Y('trips:Q', title='Trips'),
            color=alt.Color('year:N', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
            tooltip=['year', alt.Tooltip('month:T'), 'trips:Q']
        ).properties(height=320).configure_axis(
            labelColor='#e6eef9', titleColor='#e6eef9'
        ).configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No Chicago data for selected year(s).")

# -----------------------------
# Peak Patterns — Hour-of-Day & Day-of-Week
# -----------------------------
# NYC hourly (cast pickup)
sql_nyc_hour = f"""
SELECT
  year,
  hour,
  trips
FROM (
  SELECT 2019 AS year, EXTRACT(hour FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS hour, COUNT(*) AS trips
  FROM {DB_ALIAS}.main.yellow_taxi_2019_1
  GROUP BY 1,2
  UNION ALL
  SELECT 2023 AS year, EXTRACT(hour FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS hour, COUNT(*) AS trips
  FROM {DB_ALIAS}.main.yellow_taxi_2023
  GROUP BY 1,2
) t
WHERE year IN ({",".join([str(y) for y in years])})
ORDER BY 1,2;
"""
nyc_hour = qdf(sql_nyc_hour)

# Chicago hourly
sql_chi_hour = f"""
SELECT
  year,
  hour,
  trips
FROM (
  SELECT 2019 AS year, EXTRACT(hour FROM trip_start_timestamp) AS hour, COUNT(*) AS trips
  FROM {DB_ALIAS}.main.chicago_taxi_2019
  GROUP BY 1,2
  UNION ALL
  SELECT 2023 AS year, EXTRACT(hour FROM trip_start_timestamp) AS hour, COUNT(*) AS trips
  FROM {DB_ALIAS}.main.chicago_taxi_2023
  GROUP BY 1,2
) t
WHERE year IN ({",".join([str(y) for y in years])})
ORDER BY 1,2;
"""
chi_hour = qdf(sql_chi_hour)

lcol, rcol = st.columns(2)
with lcol:
    st.subheader("NYC — Hourly Demand")
    if not nyc_hour.empty:
        c = alt.Chart(nyc_hour).mark_bar().encode(
            x=alt.X('hour:O', title='Hour (0–23)'),
            y=alt.Y('trips:Q', title='Trips'),
            column=alt.Column('year:N', header=alt.Header(labelColor='#e6eef9')),
            tooltip=['year','hour','trips']
        ).configure_axis(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No NYC hourly data.")
with rcol:
    st.subheader("Chicago — Hourly Demand")
    if not chi_hour.empty:
        c = alt.Chart(chi_hour).mark_bar().encode(
            x=alt.X('hour:O', title='Hour (0–23)'),
            y=alt.Y('trips:Q', title='Trips'),
            column=alt.Column('year:N', header=alt.Header(labelColor='#e6eef9')),
            tooltip=['year','hour','trips']
        ).configure_axis(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No Chicago hourly data.")

# -----------------------------
# Traffic — Chicago: Average Speed by Hour (congestion proxy)
# -----------------------------
sql_chi_speed = f"""
WITH unioned AS (
  SELECT 2019 AS year, time, speed FROM {DB_ALIAS}.main.chicago_traffic_2019
  UNION ALL
  SELECT 2023 AS year, time, speed FROM {DB_ALIAS}.main.chicago_traffic_2023
)
SELECT year, EXTRACT(hour FROM time) AS hour, AVG(speed) AS avg_speed
FROM unioned
WHERE year IN ({",".join([str(y) for y in years])})
GROUP BY 1,2
ORDER BY 1,2;
"""
chi_speed = qdf(sql_chi_speed)
st.subheader("Chicago Traffic — Avg Speed by Hour (2019 vs 2023)")
if not chi_speed.empty:
    c = alt.Chart(chi_speed).mark_line(point=True).encode(
        x=alt.X('hour:O', title='Hour (0–23)'),
        y=alt.Y('avg_speed:Q', title='Avg Speed (mph)'),
        color=alt.Color('year:N', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
        tooltip=['year','hour','avg_speed']
    ).properties(height=320).configure_axis(labelColor='#e6eef9', titleColor='#e6eef9') \
     .configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
    st.altair_chart(c, use_container_width=True)
else:
    st.info("No traffic data for selected year(s).")

# -----------------------------
# CTA — Top Stations Trend
# -----------------------------
st.subheader("CTA — L Stations: Daily Entries (Top Stations)")
top_n = st.slider("Top N stations", 3, 20, 8, 1)
sql_cta_topstations = f"""
WITH agg AS (
  SELECT stationname, SUM(rides) AS total_rides
  FROM {DB_ALIAS}.main."CTA - L Stations Daily Entries"
  GROUP BY 1
),
top AS (
  SELECT stationname FROM agg ORDER BY total_rides DESC LIMIT {top_n}
)
SELECT t.stationname, date::DATE AS date, rides
FROM {DB_ALIAS}.main."CTA - L Stations Daily Entries" t
JOIN top USING (stationname)
ORDER BY stationname, date;
"""
cta_ts = qdf(sql_cta_topstations)
if not cta_ts.empty:
    c = alt.Chart(cta_ts).mark_line().encode(
        x=alt.X('date:T', title='Date'),
        y=alt.Y('rides:Q', title='Rides'),
        color=alt.Color('stationname:N', legend=alt.Legend(columns=1)),
        tooltip=['stationname', alt.Tooltip('date:T'), 'rides:Q']
    ).properties(height=340).configure_axis(labelColor='#e6eef9', titleColor='#e6eef9') \
     .configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
    st.altair_chart(c, use_container_width=True)
else:
    st.info("CTA rides not available.")

# -----------------------------
# NYC — Pickup Hotspots by PULocationID (rank table)
# -----------------------------
st.subheader("NYC — Pickup Hotspots (PULocationID)")
sql_nyc_pu = f"""
WITH base AS (
  SELECT 2019 AS year, PULocationID AS zone, 1 AS cnt FROM {DB_ALIAS}.main.yellow_taxi_2019_1
  UNION ALL
  SELECT 2023 AS year, PULocationID AS zone, 1 AS cnt FROM {DB_ALIAS}.main.yellow_taxi_2023
)
SELECT year, zone, SUM(cnt) AS trips
FROM base
WHERE year IN ({",".join([str(y) for y in years])})
GROUP BY 1,2
ORDER BY year, trips DESC
LIMIT 50;
"""
nyc_pu = qdf(sql_nyc_pu)
st.dataframe(nyc_pu, use_container_width=True)

# -----------------------------
# Chicago — Pickup Density (lat/lon scatter; requires 2023 table columns)
# -----------------------------
st.subheader("Chicago — Pickup Points (sample)")
sql_chi_pts = f"""
SELECT pickup_centroid_latitude AS lat, pickup_centroid_longitude AS lon
FROM {DB_ALIAS}.main.chicago_taxi_2023
WHERE pickup_centroid_latitude IS NOT NULL AND pickup_centroid_longitude IS NOT NULL
LIMIT 5000;
"""
chi_pts = qdf(sql_chi_pts)
if not chi_pts.empty:
    st.map(chi_pts.rename(columns={"lat":"latitude","lon":"longitude"}), use_container_width=True)
else:
    st.info("No Chicago pickup coordinates available.")

# -----------------------------
# Actionable Notes (auto)
# -----------------------------
st.markdown("""
<div class="block">
  <h3 style="margin-top:0;">Insights & Recommendations</h3>
  <ul>
    <li><b>Peak Management:</b> Use hourly peaks (above) to align <i>train frequency</i> and <i>bus headways</i>, especially where Chicago traffic shows <i>lower avg speeds</i> in 2023 vs 2019.</li>
    <li><b>Station Ops:</b> Top CTA stations with consistent growth should be prioritized for <i>platform staffing</i> and <i>crowd control</i> during peak windows.</li>
    <li><b>Rideshare Zones:</b> NYC PULocationID hotspots (ranked above) suggest <i>dedicated curb zones</i> and <i>pickup signage</i> to reduce conflicts.</li>
    <li><b>Congestion Relief:</b> Where <i>traffic avg speed dips</i> coincide with <i>high CTA ridership</i>, consider <i>bus-only lanes</i> and <i>TSP (signal priority)</i>.</li>
    <li><b>Subsidy Tuning:</b> Compare 2023/2019 recovery by month; target incentives to <i>off-peak</i> or <i>under-recovered corridors</i>.</li>
  </ul>
</div>
""", unsafe_allow_html=True)

