"""Optional Streamlit dashboard. Reads saved results (run `python app.py` first).
Start with: streamlit run dashboard.py"""
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from src import config, database

ROOT = Path(__file__).resolve().parent
OUT, FIG = ROOT / "output", ROOT / "figures"

st.set_page_config(page_title="Indian Air Quality - Spark", layout="wide")
st.title("Indian Air Quality Big Data Analytics (Apache Spark)")

meta_path = OUT / "summary.json"
if config.DATABASE_PATH.exists():
    meta = database.load_latest_run(config.DATABASE_PATH)
    if meta is None:
        st.error("The SQLite database contains no pipeline runs. Run `python app.py` first.")
        st.stop()
    database_run_id = meta["run_id"]
    runs = database.list_runs(config.DATABASE_PATH)
    with st.expander("Recent database runs"):
        st.dataframe(pd.DataFrame(runs), hide_index=True, width="stretch")

    def load_table(name):
        return database.load_result_table(config.DATABASE_PATH, database_run_id, name)

    database_mode = True
elif meta_path.exists():
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    database_mode = False
else:
    st.error("No results found. Run `python app.py` in the project folder first.")
    st.stop()

if meta["is_demo_synthetic_data"]:
    st.warning("These results come from a small SYNTHETIC demonstration dataset, not real measurements.")
st.caption(meta["aqi_note"])

s, q = meta["dataset_summary"], meta["data_quality_report"]
c = st.columns(5)
c[0].metric("Rows read", q["rows_read"])
c[1].metric("Rows kept", q["rows_after_cleaning"])
c[2].metric("Cities", s["cities"])
c[3].metric("Stations", s["stations"])
c[4].metric("Duplicates removed", q["duplicates_removed"])
st.write(f"Time range: {s['first_timestamp']} to {s['last_timestamp']}")

with st.expander("Data quality details"):
    st.json(q)

t1, t2, t3, t4 = st.tabs(["Cities", "Stations", "Figures", "Cleaned data"])
with t1:
    st.subheader("Average AQI by city")
    st.dataframe(load_table("city_avg_aqi") if database_mode else pd.read_csv(OUT / "city_avg_aqi.csv"),
                 width="stretch")
    st.subheader("PM2.5 vs PM10")
    st.dataframe(load_table("pm_comparison") if database_mode else pd.read_csv(OUT / "pm_comparison.csv"),
                 width="stretch")
    st.subheader("AQI and pollutant trends")
    st.dataframe(load_table("trends") if database_mode else pd.read_csv(OUT / "trends.csv"),
                 width="stretch")
    st.subheader("Pollutant correlations")
    st.dataframe(load_table("correlation_long") if database_mode else pd.read_csv(OUT / "correlation_long.csv"),
                 width="stretch")
with t2:
    st.subheader("Station summary")
    st.dataframe(load_table("station_summary") if database_mode else pd.read_csv(OUT / "station_summary.csv"),
                 width="stretch")
    st.subheader("Highest observed AQI (in this dataset only)")
    st.dataframe(load_table("top_stations") if database_mode else pd.read_csv(OUT / "top_stations.csv"),
                 width="stretch")
with t3:
    pngs = sorted(FIG.glob("*.png"))
    if not pngs:
        st.info("No figures yet. Run `python app.py`.")
    for p in pngs:
        st.image(str(p), width="stretch")
with t4:
    st.caption("A preview of up to 1,000 cleaned records from the latest SQLite-backed run.")
    if database_mode:
        records = database.load_cleaned_records(config.DATABASE_PATH, database_run_id)
        st.dataframe(records, width="stretch")
    else:
        cleaned_path = OUT / meta["cleaned_data_file"]
        if cleaned_path.exists():
            cleaned = pd.read_parquet(cleaned_path) if cleaned_path.suffix == ".parquet" else pd.read_csv(cleaned_path)
            st.dataframe(cleaned.head(1000), width="stretch")
        else:
            st.info("Cleaned-data export not found. Run `python app.py`.")
