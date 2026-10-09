import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Sarvagram Drought Early Warning", layout="wide")

session = get_active_session()

st.title("Drought & Crop-Stress Early Warning")
st.caption("Prioritization tiers are rule-based demo signals, not predicted crop loss or credit default.")

# --- Sidebar filters ---
with st.sidebar:
    st.header("Filters")

    dates = session.sql("""
        SELECT MIN(AS_OF_DATE) AS min_d, MAX(AS_OF_DATE) AS max_d
        FROM SARVAGRAM_DEMO.ANALYTICS.PLOT_EARLY_WARNING
    """).collect()
    min_date = dates[0]["MIN_D"]
    max_date = dates[0]["MAX_D"]

    date_range = st.date_input("Date range", value=(min_date, max_date),
                               min_value=min_date, max_value=max_date)

    districts = [r["DISTRICT"] for r in session.sql(
        "SELECT DISTINCT DISTRICT FROM SARVAGRAM_DEMO.ANALYTICS.PLOT_EARLY_WARNING ORDER BY 1"
    ).collect()]
    selected_districts = st.multiselect("District", districts, default=districts)

    tiers = ["CRITICAL", "HIGH", "MODERATE", "WATCH", "NORMAL"]
    selected_tiers = st.multiselect("Priority Tier", tiers,
                                    default=["CRITICAL", "HIGH", "MODERATE"])

# --- Build filters ---
dist_filter = ",".join(f"'{d}'" for d in selected_districts) if selected_districts else "''"
tier_filter = ",".join(f"'{t}'" for t in selected_tiers) if selected_tiers else "''"
start_date = date_range[0] if len(date_range) == 2 else min_date
end_date = date_range[1] if len(date_range) == 2 else max_date

# --- KPI row ---
kpi_df = session.sql(f"""
    SELECT
        COUNT(DISTINCT PLOT_ID) AS total_plots,
        SUM(CASE WHEN PRIORITY_TIER = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
        SUM(CASE WHEN PRIORITY_TIER = 'HIGH' THEN 1 ELSE 0 END) AS high_count,
        SUM(CASE WHEN RAINFALL_ANOMALY_FLAG = TRUE THEN 1 ELSE 0 END) AS anomaly_count
    FROM SARVAGRAM_DEMO.ANALYTICS.PLOT_EARLY_WARNING
    WHERE AS_OF_DATE BETWEEN '{start_date}' AND '{end_date}'
      AND DISTRICT IN ({dist_filter})
      AND PRIORITY_TIER IN ({tier_filter})
""").collect()

row = kpi_df[0]
with st.container(horizontal=True):
    st.metric("Plots Monitored", f"{row['TOTAL_PLOTS']:,}", border=True)
    st.metric("Critical Alerts", f"{row['CRITICAL_COUNT']:,}", border=True)
    st.metric("High Alerts", f"{row['HIGH_COUNT']:,}", border=True)
    st.metric("Rainfall Anomalies", f"{row['ANOMALY_COUNT']:,}", border=True)

# --- District summary ---
st.subheader("District Watchlist")

district_summary = session.sql(f"""
    SELECT DISTRICT, PRIORITY_TIER,
           COUNT(DISTINCT PLOT_ID) AS plot_count,
           ROUND(AVG(NDVI), 3) AS avg_ndvi,
           ROUND(AVG(RAIN_30D_MM), 1) AS avg_rain_30d,
           ROUND(AVG(DRY_DAYS_15D), 1) AS avg_dry_days
    FROM SARVAGRAM_DEMO.ANALYTICS.PLOT_EARLY_WARNING
    WHERE AS_OF_DATE = '{end_date}'
      AND DISTRICT IN ({dist_filter})
      AND PRIORITY_TIER IN ({tier_filter})
    GROUP BY DISTRICT, PRIORITY_TIER
    ORDER BY CASE PRIORITY_TIER
        WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MODERATE' THEN 3
        WHEN 'WATCH' THEN 4 ELSE 5 END, DISTRICT
""").to_pandas()

if not district_summary.empty:
    st.dataframe(district_summary, use_container_width=True, hide_index=True,
        column_config={
            "DISTRICT": "District", "PRIORITY_TIER": st.column_config.TextColumn("Tier"),
            "PLOT_COUNT": st.column_config.NumberColumn("Plots"),
            "AVG_NDVI": st.column_config.NumberColumn("Avg NDVI", format="%.3f"),
            "AVG_RAIN_30D": st.column_config.NumberColumn("30d Rain (mm)", format="%.1f"),
            "AVG_DRY_DAYS": st.column_config.NumberColumn("Dry Days (15d)", format="%.1f"),
        })
else:
    st.info("No data for selected filters.")

# --- Trend charts ---
st.subheader("Time Series Trends")
col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.markdown("**Rainfall by Tehsil (daily)**")
        rain_ts = session.sql(f"""
            SELECT DISTINCT TEHSIL, OBS_DATE, RAINFALL_MM
            FROM SARVAGRAM_DEMO.CURATED.PLOT_DAILY_FEATURES
            WHERE OBS_DATE BETWEEN '{start_date}' AND '{end_date}'
              AND DISTRICT IN ({dist_filter})
            ORDER BY OBS_DATE
        """).to_pandas()
        if not rain_ts.empty:
            import pandas as pd
            rain_pivot = rain_ts.pivot_table(index="OBS_DATE", columns="TEHSIL",
                                             values="RAINFALL_MM")
            st.line_chart(rain_pivot, height=300)

with col2:
    with st.container(border=True):
        st.markdown("**NDVI by Tehsil (daily)**")
        ndvi_ts = session.sql(f"""
            SELECT DISTINCT TEHSIL, OBS_DATE, NDVI
            FROM SARVAGRAM_DEMO.CURATED.PLOT_DAILY_FEATURES
            WHERE OBS_DATE BETWEEN '{start_date}' AND '{end_date}'
              AND DISTRICT IN ({dist_filter})
            ORDER BY OBS_DATE
        """).to_pandas()
        if not ndvi_ts.empty:
            import pandas as pd
            ndvi_pivot = ndvi_ts.pivot_table(index="OBS_DATE", columns="TEHSIL",
                                             values="NDVI")
            st.line_chart(ndvi_pivot, height=300)

# --- Plot detail ---
st.subheader("Plot Detail")

plots_list = session.sql(f"""
    SELECT DISTINCT PLOT_ID, DISTRICT, TEHSIL, CROP_TYPE, PRIORITY_TIER
    FROM SARVAGRAM_DEMO.ANALYTICS.PLOT_EARLY_WARNING
    WHERE AS_OF_DATE = '{end_date}'
      AND DISTRICT IN ({dist_filter})
      AND PRIORITY_TIER IN ('CRITICAL', 'HIGH')
    ORDER BY CASE PRIORITY_TIER WHEN 'CRITICAL' THEN 1 ELSE 2 END, PLOT_ID
    LIMIT 50
""").to_pandas()

if not plots_list.empty:
    selected_plot = st.selectbox(
        "Select a plot to inspect",
        plots_list["PLOT_ID"].tolist(),
        format_func=lambda x: (
            f"{x} — {plots_list[plots_list['PLOT_ID']==x]['DISTRICT'].values[0]}"
            f" / {plots_list[plots_list['PLOT_ID']==x]['PRIORITY_TIER'].values[0]}"
        ))

    if selected_plot:
        detail = session.sql(f"""
            SELECT AS_OF_DATE, PRIORITY_TIER, RAINFALL_ANOMALY_FLAG, ANOMALY_DISTANCE,
                   EXPECTED_RAINFALL_MM, ACTUAL_RAINFALL_MM, RAIN_7D_MM, RAIN_30D_MM,
                   RAIN_DEVIATION_PCT, DRY_DAYS_15D, NDVI, NDVI_7D_CHANGE_PCT,
                   SOIL_MOISTURE_INDEX, MAX_TEMP_C, CONTRIBUTING_SIGNALS,
                   MODEL_ID, RUN_ID, WEATHER_HAS_MISSING
            FROM SARVAGRAM_DEMO.ANALYTICS.PLOT_EARLY_WARNING
            WHERE PLOT_ID = '{selected_plot}'
            ORDER BY AS_OF_DATE DESC LIMIT 30
        """).to_pandas()

        if not detail.empty:
            latest = detail.iloc[0]
            with st.container(horizontal=True):
                st.metric("Priority", latest["PRIORITY_TIER"], border=True)
                st.metric("NDVI", f"{latest['NDVI']:.3f}", border=True)
                st.metric("30d Rain (mm)", f"{latest['RAIN_30D_MM']:.1f}", border=True)
                st.metric("Dry Days (15d)", str(int(latest["DRY_DAYS_15D"])), border=True)

            with st.container(border=True):
                st.markdown("**Contributing Signals**")
                signals = latest["CONTRIBUTING_SIGNALS"]
                st.write(signals if signals else "No active signals")

            with st.container(border=True):
                st.markdown("**Model & Data Metadata**")
                st.write(f"Model: `{latest['MODEL_ID']}` | Run: `{latest['RUN_ID']}`"
                         f" | Missing weather data: `{latest['WEATHER_HAS_MISSING']}`")

            with st.container(border=True):
                st.markdown("**Recent History (last 30 days)**")
                st.dataframe(
                    detail[["AS_OF_DATE", "PRIORITY_TIER", "RAINFALL_ANOMALY_FLAG",
                            "NDVI", "RAIN_30D_MM", "DRY_DAYS_15D"]],
                    use_container_width=True, hide_index=True)
else:
    st.info("No CRITICAL or HIGH tier plots found for selected filters.")

st.divider()
st.caption("Data: Synthetic demo | Model: Snowflake ML Anomaly Detection"
           " | Tier: Rule-based prioritization, not a credit/loss prediction")
