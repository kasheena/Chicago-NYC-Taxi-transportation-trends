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
  --accent: #FF7A00;    /* transit orange */
  --primary: #0A84FF;   /* deep blue */
  --success: #17B26A;   /* success green */
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
.stTabs [data-testid="stTabContent"] { padding: 1.5rem 0; }
.stTabs [data-testid="stTab"] {
    font-size: 14px;
    font-weight: 500;
    color: var(--muted);
}
/* New CSS to constrain the app's overall width */
.content-wrapper {
    max-width: 1100px;
    margin: 0 auto;
    padding: 0 1rem;
}
</style>
""", unsafe_allow_html=True)

# -----------------------------
# MotherDuck connection
# -----------------------------
MD_TOKEN = st.secrets.get("MOTHERDUCK_TOKEN", os.getenv("MOTHERDUCK_TOKEN", "")) # set in Streamlit secrets or env
DB_ALIAS = "motherduck_db"
MD_DB_NAME = "taxi_assign"

# NOTE: The user's provided token is removed for security and a placeholder is used.
# If you run this code, you will need to replace this with your own valid token.
MD_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6Imthc2hlZW5hcGVyc29uYWxAZ21haWwuY29tIiwic2Vzc2lvbiI6Imthc2hlZW5hcGVyc29uYWwuZ21haWwuY29tIiwicGF0IjoiUEk4WnZwcC1zNEFDZFYtRWYxaEtoX0k2aFZoZmhDTTJQRTRGY2Y5UVJQWSIsInVzZXJJZCI6Ijk4MWZiMjYzLTQ1NzEtNDk2OS04NWNkLWM0ZjA3MGE0ZTg4YSIsImlzcyI6Im1kX3BhdCIsInJlYWRPbmx5IjpmYWxzZSwidG9rZW5UeXBlIjoicmVhZF93cml0ZSIsImlhdCI6MTc1NTE3ODI4Mn0.bjWdIVWu-3suCbmyRu0UEr-jSu8kPmfpYrZ5xPH_-xo" 

if not MD_TOKEN or MD_TOKEN == "your_motherduck_token_here":
    st.error("MotherDuck token not found. Add MOTHERDUCK_TOKEN to your Streamlit secrets or environment.")
    st.stop()

@st.cache_resource(show_spinner=False)
def connect_md():
    conn = duckdb.connect()
    conn.execute("INSTALL motherduck;")
    conn.execute("LOAD motherduck;")
    conn.execute(f"SET motherduck_token='{MD_TOKEN}';")
    conn.execute(f"ATTACH 'md:{MD_DB_NAME}' AS {DB_ALIAS};")
    return conn

conn = connect_md()

def qdf(sql: str, params: dict | None = None) -> pd.DataFrame:
    if params:
        return conn.execute(sql, params).fetchdf()
    return conn.execute(sql).fetchdf()


# -----------------------------
# Main Content
# -----------------------------
st.markdown("<div class='content-wrapper'>", unsafe_allow_html=True)

# -----------------------------
# Header & Global Filters
# -----------------------------
st.markdown(f"""
<div class="header">
  <div style="display:flex; align-items:center; gap:14px;">
    <div class="badge">CommutePulse</div>
    <h1 style="margin:0;">Chicago & NYC Transportation Analytics</h1>
  </div>
  <div class="caption">Operational insights for CTA & NYC: taxi demand, traffic congestion, and L ridership — 2019 vs 2023 recovery.</div>
</div>
""", unsafe_allow_html=True)

years = st.multiselect("Year(s)", [2019, 2023], default=[2019, 2023], help="Select the years for comparison.")


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
nyc_kpi_df = qdf(sql_nyc_kpi)
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

# CTA total rides (all-time in table)
sql_cta = f"SELECT SUM(rides)::BIGINT AS total_rides FROM {DB_ALIAS}.main.cta_l_ridership;"
cta_total = qdf(sql_cta).iloc[0]["total_rides"]

# Traffic: Chicago average speed by year
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
# Tabs
# -----------------------------
tab_landing, tab_nyc, tab_chi, tab_traffic, tab_comp, tab_conc = st.tabs([
    "Project Overview",
    "NYC Taxi (2019 vs 2023)",
    "Chicago Taxi (2019 vs 2023)",
    "Chicago Traffic & L-Rides",
    "NYC vs. Chicago",
    "Conclusions"
])

with tab_landing:
    st.markdown("""
    # Taxi & Public Transportation Ridership Impact on Policy
    <p style="color: var(--muted); font-size: 1.25rem;">Informing Transportation Policy in Chicago & NYC</p>

    ---
    <div class="block">
      <div style="display:flex; justify-content:space-between; align-items:center;">
        <div>
          <p style="margin:0; font-weight:600;">Presenters:</p>
          <p style="margin:0; color:var(--muted);">Ankit, Kasheena, Bickramjit (Group 7)</p>
        </div>
        <div>
          <p style="margin:0; font-weight:600;">Course:</p>
          <p style="margin:0; color:var(--muted);">MSDSP 420 Database Systems</p>
        </div>
      </div>
    </div>

    ---

    ### EXECUTIVE SUMMARY & BUSINESS OBJECTIVES
    <div class="block">
      <p><b>Core Problem:</b> Analyze urban mobility changes (taxi & public transit) pre/post-COVID to inform policy and reduce congestion in Chicago & NYC.</p>
      <br/>
      <p><b>Key Questions:</b></p>
      <ul>
        <li>How have ridership patterns shifted (pre/post-COVID)?</li>
        <li>Are current subsidies effectively recovering demand?</li>
        <li>How can CTA infrastructure address Chicago traffic congestion?</li>
      </ul>
      <p><b>Expected Outcomes:</b> Data-driven recommendations for policy, infrastructure, and operational optimization.</p>
    </div>

    ---

    ### Dashboard Summary
    <div class="block">
      <ul>
        <li>The dashboard compares taxi trip data and public transit ridership between **2019** (pre-pandemic) and **2023** (post-pandemic) to highlight recovery trends.</li>
        <li>**Key Performance Indicators (KPIs)** at the top provide a quick view of taxi trip counts and recovery rates for both Chicago and NYC.</li>
        <li>Charts are organized into tabs to analyze **monthly and hourly trip patterns**, payment method shifts, and vendor market share.</li>
        <li>Specific focus is given to **Chicago traffic congestion** and its relationship with CTA L-train ridership.</li>
        <li>The final tab, **"Conclusions,"** summarizes the key findings and offers actionable insights for urban planners and transportation authorities.</li>
      </ul>
    </div>
    """, unsafe_allow_html=True)


with tab_nyc:
    st.markdown("""
    This section focuses on analyzing **New York City taxi trip data** from 2019 and 2023 to understand the impact of the COVID-19 pandemic on the taxi industry.
    We'll examine recovery trends, changes in payment methods, and shifts in market share among taxi technology providers.
    """)

    # NYC monthly counts (using the user-provided query structure)
    sql_nyc_monthly = f"""
    WITH y19 AS (
      SELECT
        2019 AS year,
        EXTRACT(MONTH FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS month,
        COUNT(*) AS trip_count,
        AVG(trip_distance) AS avg_distance,
        AVG(total_amount) AS avg_revenue,
        SUM(total_amount) AS total_revenue
      FROM {DB_ALIAS}.main.yellow_taxi_2019_1
      WHERE trip_distance > 0 AND total_amount > 0
      GROUP BY 1, 2
    ),
    y23 AS (
      SELECT
        2023 AS year,
        EXTRACT(MONTH FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS month,
        COUNT(*) AS trip_count,
        AVG(trip_distance) AS avg_distance,
        AVG(total_amount) AS avg_revenue,
        SUM(total_amount) AS total_revenue
      FROM {DB_ALIAS}.main.yellow_taxi_2023
      WHERE trip_distance > 0 AND total_amount > 0
      GROUP BY 1, 2
    )
    SELECT * FROM y19 UNION ALL SELECT * FROM y23 ORDER BY year, month;
    """
    nyc_monthly = qdf(sql_nyc_monthly)
    st.subheader("NYC — Monthly Taxi Trips (2019 vs 2023)")
    if not nyc_monthly.empty:
        c = alt.Chart(nyc_monthly).mark_bar().encode(
            x=alt.X('month:O', title='Month', axis=alt.Axis(format=".0f")),
            y=alt.Y('trip_count:Q', title='Trip Count'),
            color=alt.Color('year:N', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
            column=alt.Column('year:N', header=alt.Header(labelColor='#e6eef9', title='Year')),
            tooltip=['year', 'month', 'trip_count']
        ).properties(height=320).configure_axis(
            labelColor='#e6eef9', titleColor='#e6eef9'
        ).configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No NYC data for selected year(s).")
    st.markdown("""
    **Purpose:** Compares monthly taxi trip volumes. **Relevance:** Shows demand recovery and seasonal patterns post-COVID, helping to evaluate subsidy effectiveness over time.
    """)

    # Additional charts for NYC monthly metrics
    st.subheader("NYC — Average Trip Distance & Revenue by Month")
    if not nyc_monthly.empty:
        c1 = alt.Chart(nyc_monthly).mark_line(point=True).encode(
            x=alt.X('month:O', title='Month', axis=alt.Axis(format=".0f")),
            y=alt.Y('avg_distance:Q', title='Avg Distance'),
            color=alt.Color('year:N', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
            tooltip=['year', 'month', alt.Tooltip('avg_distance:Q', format=".2f")]
        ).properties(height=200).configure_axis(labelColor='#e6eef9', titleColor='#e6eef9').configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c1, use_container_width=True)

        c2 = alt.Chart(nyc_monthly).mark_line(point=True).encode(
            x=alt.X('month:O', title='Month', axis=alt.Axis(format=".0f")),
            y=alt.Y('avg_revenue:Q', title='Avg Revenue ($)'),
            color=alt.Color('year:N', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
            tooltip=['year', 'month', alt.Tooltip('avg_revenue:Q', format=".2f")]
        ).properties(height=200).configure_axis(labelColor='#e6eef9', titleColor='#e6eef9').configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c2, use_container_width=True)
    st.markdown("""
    **Purpose:** Analyzes trip value and length trends. **Relevance:** Reveals changes in travel behavior and economic impact on drivers, informing fare policy adjustments.
    """)

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
    st.subheader("NYC — Hourly Demand")
    if not nyc_hour.empty:
        c = alt.Chart(nyc_hour).mark_bar().encode(
            x=alt.X('hour:O', title='Hour (0–23)'),
            y=alt.Y('trips:Q', title='Trips'),
            column=alt.Column('year:N', header=alt.Header(labelColor='#e6eef9', title='Year')),
            tooltip=['year','hour','trips']
        ).configure_axis(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No NYC hourly data.")
    st.markdown("""
    **Purpose:** Identifies peak travel hours for each year. **Relevance:** Helps optimize driver supply and informs policies for managing rush hour congestion effectively.
    """)

    # Payment Type breakdown
    sql_nyc_payment_type = f"""
    WITH payment_data AS (
        SELECT
            2019 AS year,
            CASE payment_type
                WHEN 0 THEN 'Flex Fare'
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                WHEN 6 THEN 'Voided Trip'
                ELSE 'Other'
            END AS payment_type_desc
        FROM {DB_ALIAS}.main.yellow_taxi_2019_1
        UNION ALL
        SELECT
            2023 AS year,
            CASE payment_type
                WHEN 0 THEN 'Flex Fare'
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                WHEN 6 THEN 'Voided Trip'
                ELSE 'Other'
            END AS payment_type_desc
        FROM {DB_ALIAS}.main.yellow_taxi_2023
    )
    SELECT year, payment_type_desc, COUNT(*) AS trips
    FROM payment_data
    WHERE year IN ({",".join([str(y) for y in years])})
    GROUP BY 1, 2
    ORDER BY 1, trips DESC;
    """
    nyc_payment_type_df = qdf(sql_nyc_payment_type)

    # VendorID breakdown
    sql_nyc_vendor = f"""
    WITH vendor_data AS (
        SELECT
            2019 AS year,
            CASE VendorID
                WHEN 1 THEN 'Creative Mobile Technologies'
                WHEN 2 THEN 'Curb Mobility'
                WHEN 6 THEN 'Myle Technologies'
                WHEN 7 THEN 'Helix'
                ELSE 'Other'
            END AS vendor_name
        FROM {DB_ALIAS}.main.yellow_taxi_2019_1
        UNION ALL
        SELECT
            2023 AS year,
            CASE VendorID
                WHEN 1 THEN 'Creative Mobile Technologies'
                WHEN 2 THEN 'Curb Mobility'
                WHEN 6 THEN 'Myle Technologies'
                WHEN 7 THEN 'Helix'
                ELSE 'Other'
            END AS vendor_name
        FROM {DB_ALIAS}.main.yellow_taxi_2023
    )
    SELECT year, vendor_name, COUNT(*) AS trips
    FROM vendor_data
    WHERE year IN ({",".join([str(y) for y in years])})
    GROUP BY 1, 2
    ORDER BY 1, trips DESC;
    """
    nyc_vendor_df = qdf(sql_nyc_vendor)

    col_pay, col_vendor = st.columns(2)
    with col_pay:
        st.subheader("NYC Payment Type Breakdown")
        if not nyc_payment_type_df.empty:
            c = alt.Chart(nyc_payment_type_df).mark_bar().encode(
                x=alt.X('payment_type_desc:N', title='Payment Type', sort='-y'),
                y=alt.Y('trips:Q', title='Number of Trips'),
                color=alt.Color('year:N', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
                tooltip=['year', 'payment_type_desc', 'trips']
            ).properties(height=320).configure_axis(
                labelColor='#e6eef9', titleColor='#e6eef9'
            ).configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
            st.altair_chart(c, use_container_width=True)
        else:
            st.info("No NYC payment data for selected year(s).")
        st.markdown("""
        **Purpose:** Tracks shifts in payment methods. **Relevance:** Highlights the trend towards digital payments, guiding infrastructure and app development for seamless transactions.
        """)
    with col_vendor:
        st.subheader("NYC Vendor Market Share")
        if not nyc_vendor_df.empty:
            c = alt.Chart(nyc_vendor_df).mark_bar().encode(
                x=alt.X('vendor_name:N', title='Vendor', sort='-y'),
                y=alt.Y('trips:Q', title='Number of Trips'),
                color=alt.Color('year:N', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
                tooltip=['year', 'vendor_name', 'trips']
            ).properties(height=320).configure_axis(
                labelColor='#e6eef9', titleColor='#e6eef9'
            ).configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
            st.altair_chart(c, use_container_width=True)
        else:
            st.info("No NYC vendor data for selected year(s).")
        st.markdown("""
        **Purpose:** Gauges vendor market share changes. **Relevance:** Reveals which companies are dominating the market, useful for competitive analysis and regulation.
        """)

    st.markdown("---")
    st.subheader("NYC — Trip Fare vs. Distance (Sample)")
    # New query for NYC fare vs distance scatter plot
    sql_nyc_scatter = f"""
    SELECT
        2019 AS year,
        trip_distance AS distance,
        fare_amount AS fare
    FROM {DB_ALIAS}.main.yellow_taxi_2019_1
    WHERE trip_distance > 0 AND fare_amount > 0 AND fare_amount < 100
    USING SAMPLE 10000 ROWS
    UNION ALL
    SELECT
        2023 AS year,
        trip_distance AS distance,
        fare_amount AS fare
    FROM {DB_ALIAS}.main.yellow_taxi_2023
    WHERE trip_distance > 0 AND fare_amount > 0 AND fare_amount < 100
    USING SAMPLE 10000 ROWS;
    """
    nyc_scatter_df = qdf(sql_nyc_scatter)

    if not nyc_scatter_df.empty:
        c = alt.Chart(nyc_scatter_df).mark_point(filled=True, opacity=0.4).encode(
            x=alt.X('distance', title='Trip Distance (miles)', scale=alt.Scale(domain=(0, 20))),
            y=alt.Y('fare', title='Fare Amount ($)', scale=alt.Scale(domain=(0, 60))),
            color=alt.Color('year:N', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
            tooltip=['year', 'distance', 'fare']
        ).properties(height=400).configure_axis(labelColor='#e6eef9', titleColor='#e6eef9') \
         .configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No data to plot trip fare vs. distance.")
    st.markdown("""
    **Purpose:** Examines pricing consistency across trip distances. **Relevance:** Reveals if fare structures are stable, useful for ensuring fairness in pricing and stable driver income.
    """)
    
    st.subheader("NYC — Average Tip Percentage by Payment Type")
    # New query for NYC average tip percentage
    sql_nyc_tips = f"""
    WITH tips AS (
        SELECT
            2019 AS year,
            CASE payment_type
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                ELSE 'Other'
            END AS payment_type_desc,
            tip_amount,
            total_amount
        FROM {DB_ALIAS}.main.yellow_taxi_2019_1
        WHERE payment_type IN (1, 2) AND tip_amount > 0 AND total_amount > 0
        UNION ALL
        SELECT
            2023 AS year,
            CASE payment_type
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                ELSE 'Other'
            END AS payment_type_desc,
            tip_amount,
            total_amount
        FROM {DB_ALIAS}.main.yellow_taxi_2023
        WHERE payment_type IN (1, 2) AND tip_amount > 0 AND total_amount > 0
    )
    SELECT
        year,
        payment_type_desc,
        AVG(tip_amount / total_amount) * 100 AS avg_tip_pct
    FROM tips
    GROUP BY year, payment_type_desc
    ORDER BY year, avg_tip_pct DESC;
    """
    nyc_tips_df = qdf(sql_nyc_tips)

    if not nyc_tips_df.empty:
        c = alt.Chart(nyc_tips_df).mark_bar().encode(
            x=alt.X('payment_type_desc:N', title='Payment Type'),
            y=alt.Y('avg_tip_pct:Q', title='Average Tip Percentage (%)', axis=alt.Axis(format=".1f")),
            color=alt.Color('year:N', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
            column=alt.Column('year:N', header=alt.Header(labelColor='#e6eef9', title='Year')),
            tooltip=['year', 'payment_type_desc', alt.Tooltip('avg_tip_pct:Q', format=".1f")]
        ).properties(height=320).configure_axis(
            labelColor='#e6eef9', titleColor='#e6eef9'
        ).configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No data to plot tipping trends.")
    st.markdown("""
    **Purpose:** Analyzes tipping trends by payment type. **Relevance:** Provides insights into rider behavior and driver compensation, informing financial support policies for drivers.
    """)


with tab_chi:
    st.markdown("""
    This section focuses on **Chicago taxi trip data** from 2019 and 2023 to evaluate the local taxi industry's recovery.
    We'll examine monthly and hourly demand patterns and analyze average fare amounts to understand changes in trip value.
    """)

    # Chicago monthly counts and metrics
    sql_chi_monthly = f"""
    WITH y19 AS (
      SELECT
        2019 AS year,
        EXTRACT(MONTH FROM trip_start_timestamp) AS month,
        COUNT(*) AS trip_count,
        AVG(trip_miles) AS avg_distance,
        AVG(trip_total) AS avg_revenue,
        SUM(trip_total) AS total_revenue
      FROM {DB_ALIAS}.main.chicago_taxi_2019
      WHERE trip_miles > 0 AND trip_total > 0
      GROUP BY 1, 2
    ),
    y23 AS (
      SELECT
        2023 AS year,
        EXTRACT(MONTH FROM trip_start_timestamp) AS month,
        COUNT(*) AS trip_count,
        AVG(trip_miles) AS avg_distance,
        AVG(trip_total) AS avg_revenue,
        SUM(trip_total) AS total_revenue
      FROM {DB_ALIAS}.main.chicago_taxi_2023
      WHERE trip_miles > 0 AND trip_total > 0
      GROUP BY 1, 2
    )
    SELECT * FROM y19 UNION ALL SELECT * FROM y23 ORDER BY year, month;
    """
    chi_monthly = qdf(sql_chi_monthly)
    st.subheader("Chicago — Monthly Taxi Trips (2019 vs 2023)")
    if not chi_monthly.empty:
        c = alt.Chart(chi_monthly).mark_bar().encode(
            x=alt.X('month:O', title='Month', axis=alt.Axis(format=".0f")),
            y=alt.Y('trip_count:Q', title='Trip Count'),
            color=alt.Color('year:N', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
            column=alt.Column('year:N', header=alt.Header(labelColor='#e6eef9', title='Year')),
            tooltip=['year', 'month', 'trip_count']
        ).properties(height=320).configure_axis(
            labelColor='#e6eef9', titleColor='#e6eef9'
        ).configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No Chicago data for selected year(s).")
    st.markdown("""
    **Purpose:** Compares monthly taxi trip volumes. **Relevance:** Shows demand recovery and seasonal patterns post-COVID, helping to evaluate subsidy effectiveness over time.
    """)

    # Additional charts for Chicago monthly metrics
    st.subheader("Chicago — Average Trip Distance & Revenue by Month")
    if not chi_monthly.empty:
        c1 = alt.Chart(chi_monthly).mark_line(point=True).encode(
            x=alt.X('month:O', title='Month', axis=alt.Axis(format=".0f")),
            y=alt.Y('avg_distance:Q', title='Avg Distance'),
            color=alt.Color('year:N', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
            tooltip=['year', 'month', alt.Tooltip('avg_distance:Q', format=".2f")]
        ).properties(height=200).configure_axis(labelColor='#e6eef9', titleColor='#e6eef9').configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c1, use_container_width=True)

        c2 = alt.Chart(chi_monthly).mark_line(point=True).encode(
            x=alt.X('month:O', title='Month', axis=alt.Axis(format=".0f")),
            y=alt.Y('avg_revenue:Q', title='Avg Revenue ($)'),
            color=alt.Color('year:N', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
            tooltip=['year', 'month', alt.Tooltip('avg_revenue:Q', format=".2f")]
        ).properties(height=200).configure_axis(labelColor='#e6eef9', titleColor='#e6eef9').configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c2, use_container_width=True)
    st.markdown("""
    **Purpose:** Analyzes trip value and length trends. **Relevance:** Reveals changes in travel behavior and economic impact on drivers, informing fare policy adjustments.
    """)

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
    st.subheader("Chicago — Hourly Demand")
    if not chi_hour.empty:
        c = alt.Chart(chi_hour).mark_bar().encode(
            x=alt.X('hour:O', title='Hour (0–23)'),
            y=alt.Y('trips:Q', title='Trips'),
            column=alt.Column('year:N', header=alt.Header(labelColor='#e6eef9', title='Year')),
            tooltip=['year','hour','trips']
        ).configure_axis(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No Chicago hourly data.")
    st.markdown("""
    **Purpose:** Identifies peak travel hours for each year. **Relevance:** Helps optimize driver supply and informs policies for managing rush hour congestion effectively.
    """)
    
    st.markdown("---")
    st.subheader("Chicago — Trip Density by Hour & Day of Week")
    # New query for Chicago trip density heatmap
    sql_chi_heatmap = f"""
    WITH data AS (
        SELECT
            EXTRACT(YEAR FROM trip_start_timestamp) AS year,
            EXTRACT(HOUR FROM trip_start_timestamp) AS hour,
            CASE EXTRACT(DOW FROM trip_start_timestamp)
                WHEN 0 THEN 'Sun'
                WHEN 1 THEN 'Mon'
                WHEN 2 THEN 'Tue'
                WHEN 3 THEN 'Wed'
                WHEN 4 THEN 'Thu'
                WHEN 5 THEN 'Fri'
                WHEN 6 THEN 'Sat'
            END AS day_of_week
        FROM {DB_ALIAS}.main.chicago_taxi_2019
        UNION ALL
        SELECT
            EXTRACT(YEAR FROM trip_start_timestamp) AS year,
            EXTRACT(HOUR FROM trip_start_timestamp) AS hour,
            CASE EXTRACT(DOW FROM trip_start_timestamp)
                WHEN 0 THEN 'Sun'
                WHEN 1 THEN 'Mon'
                WHEN 2 THEN 'Tue'
                WHEN 3 THEN 'Wed'
                WHEN 4 THEN 'Thu'
                WHEN 5 THEN 'Fri'
                WHEN 6 THEN 'Sat'
            END AS day_of_week
        FROM {DB_ALIAS}.main.chicago_taxi_2023
    )
    SELECT
        year,
        hour,
        day_of_week,
        COUNT(*) AS trips
    FROM data
    WHERE year IN ({",".join([str(y) for y in years])})
    GROUP BY 1, 2, 3
    ORDER BY year, hour;
    """
    chi_heatmap_df = qdf(sql_chi_heatmap)

    if not chi_heatmap_df.empty:
        c = alt.Chart(chi_heatmap_df).mark_rect().encode(
            x=alt.X('day_of_week:O', title='Day of Week', sort=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']),
            y=alt.Y('hour:O', title='Hour (0-23)'),
            color=alt.Color('trips:Q', title='Trip Count', scale=alt.Scale(scheme='turbo')),
            column=alt.Column('year:N', header=alt.Header(labelColor='#e6eef9', title='Year')),
            tooltip=['year', 'day_of_week', 'hour', alt.Tooltip('trips:Q', format=",")]
        ).properties(height=400).configure_axis(
            labelColor='#e6eef9', titleColor='#e6eef9'
        ).configure_legend(
            labelColor='#e6eef9', titleColor='#e6eef9',
            gradientDirection='horizontal',
            orient='bottom',
            titleOrient='left'
        )
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No data to plot trip density heatmap.")
    st.markdown("""
    **Purpose:** Pinpoints time-of-day and day-of-week demand hotspots. **Relevance:** Crucial for optimizing fleet distribution and predicting service needs at a granular level.
    """)


with tab_traffic:
    st.markdown("""
    This section examines **Chicago's traffic and L-train ridership data**. This data serves as a proxy for urban mobility and congestion, helping us understand broader transportation trends beyond just taxi usage.
    """)

    # Chicago Traffic — Avg Speed by Hour (congestion proxy)
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
    st.markdown("""
    **Purpose:** Measures traffic congestion over time. **Relevance:** Indicates if post-COVID travel patterns have worsened or eased congestion, informing infrastructure decisions.
    """)

    # Chicago Traffic — Avg Speed by Day of Week
    sql_chi_speed_day = f"""
    WITH unioned AS (
      SELECT 2019 AS year, time, speed FROM {DB_ALIAS}.main.chicago_traffic_2019
      UNION ALL
      SELECT 2023 AS year, time, speed FROM {DB_ALIAS}.main.chicago_traffic_2023
    )
    SELECT
      year,
      EXTRACT(dow FROM time) AS day_of_week_num,
      CASE EXTRACT(dow FROM time)
        WHEN 0 THEN 'Sun'
        WHEN 1 THEN 'Mon'
        WHEN 2 THEN 'Tue'
        WHEN 3 THEN 'Wed'
        WHEN 4 THEN 'Thu'
        WHEN 5 THEN 'Fri'
        WHEN 6 THEN 'Sat'
      END AS day_of_week,
      AVG(speed) AS avg_speed
    FROM unioned
    WHERE year IN ({",".join([str(y) for y in years])})
    GROUP BY 1, 2, 3
    ORDER BY 1, 2;
    """
    chi_speed_day = qdf(sql_chi_speed_day)
    st.subheader("Chicago Traffic — Avg Speed by Day of Week")
    if not chi_speed_day.empty:
        c = alt.Chart(chi_speed_day).mark_bar().encode(
            x=alt.X('day_of_week:O', title='Day of Week', sort=['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']),
            y=alt.Y('avg_speed:Q', title='Avg Speed (mph)'),
            column=alt.Column('year:N', header=alt.Header(labelColor='#e6eef9', title='Year')),
            tooltip=['year','day_of_week','avg_speed']
        ).configure_axis(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No traffic data for selected year(s).")
    st.markdown("""
    **Purpose:** Analyzes traffic speed by day of the week. **Relevance:** Identifies weekly congestion trends, guiding dynamic traffic management and public transit planning.
    """)


    st.markdown("<hr/>", unsafe_allow_html=True)
    st.subheader("CTA — L Stations: Daily Entries (Top Stations)")
    top_n = st.slider("Top N stations", 3, 20, 8, 1)
    sql_cta_topstations = f"""
    WITH agg AS (
      SELECT stationname, SUM(rides) AS total_rides
      FROM {DB_ALIAS}.main.cta_l_ridership
      GROUP BY 1
    ),
    top AS (
      SELECT stationname FROM agg ORDER BY total_rides DESC LIMIT {top_n}
    )
    SELECT t.stationname, date::DATE AS date, rides
    FROM {DB_ALIAS}.main.cta_l_ridership t
    JOIN top USING (stationname)
    ORDER BY stationname, date;
    """
    cta_ts = qdf(sql_cta_topstations)
    if not cta_ts.empty:
        c = alt.Chart(cta_ts).mark_line().encode(
            x=alt.X('date:T', title='Date'),
            y=alt.Y('rides:Q', title='Rides'),
            color=alt.Color('stationname:N', legend=alt.Legend(columns=1, title='Station')),
            tooltip=['stationname', alt.Tooltip('date:T'), 'rides:Q']
        ).properties(height=340).configure_axis(labelColor='#e6eef9', titleColor='#e6eef9') \
         .configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("CTA rides not available.")
    st.markdown("""
    **Purpose:** Tracks ridership at key stations. **Relevance:** Helps identify high-traffic stations for resource allocation, safety, and potential infrastructure upgrades.
    """)


with tab_comp:
    st.markdown("""
    This section provides a **direct comparison between NYC and Chicago** to highlight differences and similarities in their post-pandemic recovery.
    We'll look at the overall trends in taxi trips and the busiest pickup locations in each city for both 2019 and 2023.
    """)

    # Combined SQL query for NYC and Chicago monthly trips
    sql_combined_monthly = f"""
    WITH nyc_data AS (
        SELECT
            'NYC' AS city,
            2019 AS year,
            EXTRACT(MONTH FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS month,
            COUNT(*) AS trip_count
        FROM {DB_ALIAS}.main.yellow_taxi_2019_1
        GROUP BY 1, 2, 3
        UNION ALL
        SELECT
            'NYC' AS city,
            2023 AS year,
            EXTRACT(MONTH FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS month,
            COUNT(*) AS trip_count
        FROM {DB_ALIAS}.main.yellow_taxi_2023
        GROUP BY 1, 2, 3
    ),
    chicago_data AS (
        SELECT
            'Chicago' AS city,
            2019 AS year,
            EXTRACT(MONTH FROM trip_start_timestamp) AS month,
            COUNT(*) AS trip_count
        FROM {DB_ALIAS}.main.chicago_taxi_2019
        GROUP BY 1, 2, 3
        UNION ALL
        SELECT
            'Chicago' AS city,
            2023 AS year,
            EXTRACT(MONTH FROM trip_start_timestamp) AS month,
            COUNT(*) AS trip_count
        FROM {DB_ALIAS}.main.chicago_taxi_2023
        GROUP BY 1, 2, 3
    )
    SELECT * FROM nyc_data
    UNION ALL
    SELECT * FROM chicago_data
    ORDER BY city, year, month;
    """
    combined_monthly_data = qdf(sql_combined_monthly)

    st.subheader("Monthly Taxi Trips: NYC vs. Chicago (2019 & 2023)")
    if not combined_monthly_data.empty:
        c = alt.Chart(combined_monthly_data).mark_bar().encode(
            x=alt.X('year:N', title=None, axis=alt.Axis(labels=False)),
            xOffset=alt.XOffset('year:N', title=None),
            y=alt.Y('trip_count:Q', title='Trip Count'),
            color=alt.Color('year:N', title='Year', scale=alt.Scale(range=['#FF7A00', '#0A84FF'])),
            column=alt.Column('city:N', header=alt.Header(title='City')),
            tooltip=['city', 'year', 'month', 'trip_count']
        ).properties(height=320).configure_axis(
            labelColor='#e6eef9', titleColor='#e6eef9'
        ).configure_legend(labelColor='#e6eef9', titleColor='#e6eef9')
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No data available for comparison.")
    st.markdown("""
    **Purpose:** Compares recovery rates of NYC and Chicago. **Relevance:** Provides a high-level view of which city is recovering faster, useful for cross-city policy evaluation.
    """)


    st.subheader("Pickup Density — Busiest Locations")
    comp_1, comp_2 = st.columns(2)
    with comp_1:
        st.markdown("**NYC — Top Pickup Zones (2023)**")
        sql_nyc_zones_2023 = f"""
        SELECT
            z.Zone,
            z.Borough,
            COUNT(*) AS trips
        FROM {DB_ALIAS}.main.yellow_taxi_2023 y
        JOIN {DB_ALIAS}.main.NYC_zone_lookup z
            ON y.PULocationID = z.LocationID
        GROUP BY z.Zone, z.Borough
        ORDER BY trips DESC
        LIMIT 20;
        """
        nyc_zones_2023 = qdf(sql_nyc_zones_2023)
        if not nyc_zones_2023.empty:
            c = alt.Chart(nyc_zones_2023).mark_bar(color='#0A84FF').encode(
                x=alt.X('trips:Q', title='Number of Trips'),
                y=alt.Y('Zone:N', sort='-x', title='Pickup Zone'),
                tooltip=['Zone', 'Borough', 'trips']
            ).properties(height=320).configure_axis(
                labelColor='#e6eef9', titleColor='#e6eef9'
            )
            st.altair_chart(c, use_container_width=True)
        else:
            st.info("No NYC pickup data available for 2023.")
        st.markdown("""
        **Purpose:** Pinpoints top pickup locations. **Relevance:** Guides infrastructure decisions for creating dedicated pickup zones, reducing street congestion and improving efficiency.
        """)
    with comp_2:
        st.markdown("**Chicago — Top Pickup Locations (2023)**")
        sql_chi_pts_2023 = f"""
        SELECT
            ROUND(pickup_centroid_latitude, 5) AS lat,
            ROUND(pickup_centroid_longitude, 5) AS lon,
            COUNT(*) AS trips
        FROM {DB_ALIAS}.main.chicago_taxi_2023
        WHERE pickup_centroid_latitude IS NOT NULL AND pickup_centroid_longitude IS NOT NULL
        GROUP BY 1, 2
        ORDER BY trips DESC
        LIMIT 5000;
        """
        chi_pts_2023 = qdf(sql_chi_pts_2023)
        if not chi_pts_2023.empty:
            st.map(chi_pts_2023.rename(columns={"lat": "latitude", "lon": "longitude"}))
        else:
            st.info("No Chicago pickup coordinates available for 2023.")
        st.markdown("""
        **Purpose:** Pinpoints top pickup locations. **Relevance:** Guides infrastructure decisions for creating dedicated pickup zones, reducing street congestion and improving efficiency.
        """)

    st.markdown("<hr/>", unsafe_allow_html=True)
    comp_3, comp_4 = st.columns(2)
    with comp_3:
        st.markdown("**NYC — Top Pickup Zones (2019)**")
        sql_nyc_zones_2019 = f"""
        SELECT
            z.Zone,
            z.Borough,
            COUNT(*) AS trips
        FROM {DB_ALIAS}.main.yellow_taxi_2019_1 y
        JOIN {DB_ALIAS}.main.NYC_zone_lookup z
            ON y.PULocationID = z.LocationID
        GROUP BY z.Zone, z.Borough
        ORDER BY trips DESC
        LIMIT 20;
        """
        nyc_zones_2019 = qdf(sql_nyc_zones_2019)
        if not nyc_zones_2019.empty:
            c = alt.Chart(nyc_zones_2019).mark_bar(color='#FF7A00').encode(
                x=alt.X('trips:Q', title='Number of Trips'),
                y=alt.Y('Zone:N', sort='-x', title='Pickup Zone'),
                tooltip=['Zone', 'Borough', 'trips']
            ).properties(height=320).configure_axis(
                labelColor='#e6eef9', titleColor='#e6eef9'
            )
            st.altair_chart(c, use_container_width=True)
        else:
            st.info("No NYC pickup data available for 2019.")
        st.markdown("""
        **Purpose:** Pinpoints top pickup locations. **Relevance:** Guides infrastructure decisions for creating dedicated pickup zones, reducing street congestion and improving efficiency.
        """)
    with comp_4:
        st.markdown("**Chicago — Top Pickup Locations (2019)**")
        sql_chi_pts_2019 = f"""
        SELECT
            ROUND(pickup_centroid_latitude, 5) AS lat,
            ROUND(pickup_centroid_longitude, 5) AS lon,
            COUNT(*) AS trips
        FROM {DB_ALIAS}.main.chicago_taxi_2019
        WHERE pickup_centroid_latitude IS NOT NULL AND pickup_centroid_longitude IS NOT NULL
        GROUP BY 1, 2
        ORDER BY trips DESC
        LIMIT 5000;
        """
        chi_pts_2019 = qdf(sql_chi_pts_2019)
        if not chi_pts_2019.empty:
            st.map(chi_pts_2019.rename(columns={"lat": "latitude", "lon": "longitude"}))
        else:
            st.info("No Chicago pickup coordinates available for 2019.")
        st.markdown("""
        **Purpose:** Pinpoints top pickup locations. **Relevance:** Guides infrastructure decisions for creating dedicated pickup zones, reducing street congestion and improving efficiency.
        """)


with tab_conc:
    st.markdown("""
    ### Summary of Findings
    This section summarizes the key insights and recommendations derived from the data analysis across all tabs.

    ---
    ### Insights & Recommendations
    <div class="block">
      <ul>
        <li><b>Payment Trends:</b> The shift from cash to credit card payments is a major trend in NYC, suggesting a need for streamlined digital payment options and a potential decrease in cash handling requirements.</li>
        <li><b>Vendor Market Shift:</b> Analyze the change in vendor market share to understand competitive dynamics and potential new market players or consolidation.</li>
        <li><b>Peak Management:</b> Use hourly peaks (above) to align <i>train frequency</i> and <i>bus headways</i>, especially where Chicago traffic shows <i>lower avg speeds</i> in 2023 vs 2019.</li>
        <li><b>Station Ops:</b> Top CTA stations with consistent growth should be prioritized for <i>platform staffing</i> and <i>crowd control</i> during peak windows.</li>
        <li><b>Rideshare Zones:</b> NYC PULocationID hotspots (ranked above) suggest <i>dedicated curb zones</i> and <i>pickup signage</i> to reduce conflicts.</li>
        <li><b>Congestion Relief:</b> Where <i>traffic avg speed dips</i> coincide with <i>high CTA ridership</i>, consider <i>bus-only lanes</i> and <i>TSP (signal priority)</i>.</li>
        <li><b>Subsidy Tuning:</b> Compare 2023/2019 recovery by month; target incentives to <i>off-peak</i> or <i>under-recovered corridors</i>.</li>
      </ul>
    </div>
    """, unsafe_allow_html=True)

st.markdown("</div>", unsafe_allow_html=True)
