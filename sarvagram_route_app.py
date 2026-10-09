import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Sarvagram Route Optimization", layout="wide")

session = get_active_session()

st.title("Collection Agent Route Optimization")
st.caption("Distances are straight-line (great-circle) proxies, NOT road distance or travel time.")

# --- Sidebar ---
with st.sidebar:
    st.header("Controls")

    runs = session.sql("""
        SELECT DISTINCT RUN_ID, RUN_DATE
        FROM SARVAGRAM_DEMO.ANALYTICS.ROUTE_RUNS
        ORDER BY RUN_DATE DESC
    """).to_pandas()

    if runs.empty:
        st.warning("No route runs found.")
        st.stop()

    selected_run = st.selectbox(
        "Route Run", runs["RUN_ID"].tolist(),
        format_func=lambda x: f"{x} ({runs[runs['RUN_ID']==x]['RUN_DATE'].values[0]})")

    territories = session.sql(f"""
        SELECT DISTINCT TERRITORY FROM SARVAGRAM_DEMO.ANALYTICS.ROUTE_RUNS
        WHERE RUN_ID = '{selected_run}' ORDER BY 1
    """).collect()
    territory_list = [r["TERRITORY"] for r in territories]
    selected_territories = st.multiselect("Territory", territory_list, default=territory_list)

    st.divider()
    if st.button("Generate New Routes", type="primary"):
        with st.spinner("Running solver..."):
            session.sql("CALL SARVAGRAM_DEMO.ANALYTICS.GENERATE_ROUTES('2024-09-15')").collect()
            st.success("Routes generated!")
            st.rerun()

terr_filter = ",".join(f"'{t}'" for t in selected_territories) if selected_territories else "''"

# --- KPI row ---
kpi = session.sql(f"""
    SELECT COUNT(*) AS total_routes, SUM(TOTAL_STOPS) AS total_stops,
        ROUND(SUM(TOTAL_DISTANCE_M) / 1000, 1) AS total_dist_km,
        ROUND(AVG(PRIORITY_COVERAGE_PCT), 1) AS avg_priority_cov,
        SUM(CONSTRAINT_VIOLATIONS) AS total_violations
    FROM SARVAGRAM_DEMO.ANALYTICS.ROUTE_RUNS
    WHERE RUN_ID = '{selected_run}' AND TERRITORY IN ({terr_filter})
""").collect()[0]

unassigned_count = session.sql(f"""
    SELECT COUNT(*) AS cnt FROM SARVAGRAM_DEMO.ANALYTICS.UNASSIGNED_TASKS
    WHERE RUN_ID = '{selected_run}' AND TERRITORY IN ({terr_filter})
""").collect()[0]["CNT"]

with st.container(horizontal=True):
    st.metric("Routes", str(kpi["TOTAL_ROUTES"]), border=True)
    st.metric("Assigned Stops", str(kpi["TOTAL_STOPS"]), border=True)
    st.metric("Total Distance (km)", f"{kpi['TOTAL_DIST_KM']}", border=True)
    st.metric("P1 Coverage", f"{kpi['AVG_PRIORITY_COV']}%", border=True)
    st.metric("Violations", str(kpi["TOTAL_VIOLATIONS"]), border=True)
    st.metric("Unassigned", str(unassigned_count), border=True)

# --- Baseline comparison ---
st.subheader("Optimized vs Baseline Comparison")

comparison = session.sql(f"""
    SELECT * FROM SARVAGRAM_DEMO.ANALYTICS.ROUTE_BASELINE_COMPARISON
    WHERE TERRITORY IN ({terr_filter}) ORDER BY TERRITORY
""").to_pandas()

if not comparison.empty:
    st.dataframe(comparison, use_container_width=True, hide_index=True,
        column_config={
            "TERRITORY": "Territory", "AGENT_TOKEN": "Agent", "TOTAL_STOPS": "Stops",
            "OPTIMIZED_DIST_KM": st.column_config.NumberColumn("Optimized (km)", format="%.1f"),
            "NAIVE_DIST_KM": st.column_config.NumberColumn("Naive (km)", format="%.1f"),
            "DISTANCE_SAVINGS_PCT": st.column_config.NumberColumn("Savings %", format="%.1f%%"),
            "PRIORITY_COVERAGE_PCT": st.column_config.NumberColumn("P1 Coverage", format="%.0f%%"),
            "SOLVER_METHOD": "Solver",
            "DISTANCE_NOTE": st.column_config.TextColumn("Note", width="large"),
        })

    with st.container(border=True):
        st.markdown("**Distance Savings by Territory**")
        chart_data = comparison[["TERRITORY", "OPTIMIZED_DIST_KM", "NAIVE_DIST_KM"]].set_index("TERRITORY")
        st.bar_chart(chart_data, height=300)

# --- Route details ---
st.subheader("Route Details")

route_summary = session.sql(f"""
    SELECT TERRITORY, AGENT_TOKEN, TOTAL_STOPS,
           ROUND(TOTAL_DISTANCE_M / 1000, 1) AS DIST_KM,
           TOTAL_SERVICE_MIN, ROUND(TOTAL_ESTIMATED_AMOUNT) AS EST_AMOUNT,
           PRIORITY_COVERAGE_PCT, SOLVER_METHOD
    FROM SARVAGRAM_DEMO.ANALYTICS.ROUTE_RUNS
    WHERE RUN_ID = '{selected_run}' AND TERRITORY IN ({terr_filter})
    ORDER BY TERRITORY
""").to_pandas()

if not route_summary.empty:
    selected_agent = st.selectbox(
        "Select agent route", route_summary["AGENT_TOKEN"].tolist(),
        format_func=lambda x: (
            f"{x} — {route_summary[route_summary['AGENT_TOKEN']==x]['TERRITORY'].values[0]}"
            f" ({route_summary[route_summary['AGENT_TOKEN']==x]['TOTAL_STOPS'].values[0]} stops)"
        ))

    if selected_agent:
        stops = session.sql(f"""
            SELECT STOP_NUMBER, TASK_ID, ACCOUNT_TOKEN,
                   ROUND(LEG_DISTANCE_M / 1000, 2) AS LEG_KM,
                   ROUND(CUMULATIVE_DISTANCE_M / 1000, 2) AS CUM_KM,
                   EXPECTED_ARRIVAL_MIN, SERVICE_DURATION_MIN,
                   TASK_PRIORITY, ROUND(ESTIMATED_AMOUNT_INR) AS EST_AMT
            FROM SARVAGRAM_DEMO.ANALYTICS.ROUTE_STOPS
            WHERE RUN_ID = '{selected_run}' AND AGENT_TOKEN = '{selected_agent}'
            ORDER BY STOP_NUMBER
        """).to_pandas()

        with st.container(border=True):
            st.markdown(f"**Ordered Stops for {selected_agent}**")
            st.dataframe(stops, use_container_width=True, hide_index=True,
                column_config={
                    "STOP_NUMBER": "#", "TASK_ID": "Task", "ACCOUNT_TOKEN": "Account",
                    "LEG_KM": st.column_config.NumberColumn("Leg (km)", format="%.2f",
                        help="Straight-line distance, NOT road distance"),
                    "CUM_KM": st.column_config.NumberColumn("Cumulative (km)", format="%.2f"),
                    "EXPECTED_ARRIVAL_MIN": st.column_config.NumberColumn("ETA (min)", format="%d"),
                    "SERVICE_DURATION_MIN": st.column_config.NumberColumn("Service (min)", format="%d"),
                    "TASK_PRIORITY": st.column_config.NumberColumn("Priority"),
                    "EST_AMT": st.column_config.NumberColumn("Amount (INR)", format="₹%.0f"),
                })

        with st.container(border=True):
            st.markdown("**Route Map (approximate positions)**")
            import pandas as pd
            agent_base = session.sql(f"""
                SELECT BASE_LATITUDE AS lat, BASE_LONGITUDE AS lon
                FROM SARVAGRAM_DEMO.RAW.AGENTS WHERE AGENT_TOKEN = '{selected_agent}'
            """).to_pandas()
            stop_coords = session.sql(f"""
                SELECT CUSTOMER_LATITUDE AS lat, CUSTOMER_LONGITUDE AS lon
                FROM SARVAGRAM_DEMO.ANALYTICS.ROUTE_STOPS
                WHERE RUN_ID = '{selected_run}' AND AGENT_TOKEN = '{selected_agent}'
                ORDER BY STOP_NUMBER
            """).to_pandas()
            all_points = pd.concat([agent_base, stop_coords], ignore_index=True)
            st.map(all_points, size=50)

# --- Unassigned tasks ---
st.subheader("Unassigned Tasks")

unassigned = session.sql(f"""
    SELECT TASK_ID, ACCOUNT_TOKEN, TERRITORY, REASON, TASK_PRIORITY,
           ROUND(ESTIMATED_AMOUNT_INR) AS EST_AMT
    FROM SARVAGRAM_DEMO.ANALYTICS.UNASSIGNED_TASKS
    WHERE RUN_ID = '{selected_run}' AND TERRITORY IN ({terr_filter})
    ORDER BY TASK_PRIORITY, TERRITORY
""").to_pandas()

if not unassigned.empty:
    st.dataframe(unassigned, use_container_width=True, hide_index=True,
        column_config={
            "TASK_ID": "Task", "ACCOUNT_TOKEN": "Account", "TERRITORY": "Territory",
            "REASON": "Reason", "TASK_PRIORITY": "Priority",
            "EST_AMT": st.column_config.NumberColumn("Amount (INR)", format="₹%.0f"),
        })
else:
    st.success("All tasks assigned!")

st.divider()
st.caption("Distance: Straight-line proxy (great-circle), NOT road distance or travel time"
           " | Solver: Nearest-neighbor + 2-opt local search | Data: Synthetic demo")
