"""SQLite persistence for cleaned records and analytics from each pipeline run."""
import json
import math
import sqlite3
import uuid
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd


RESULT_COLUMNS = {
    "city_avg_aqi": ("city", "avg_aqi", "min_aqi", "max_aqi", "aqi_observations"),
    "pm_comparison": ("city", "avg_pm25", "avg_pm10", "pm25_to_pm10_ratio", "paired_observations"),
    "trends": ("period", "city", "avg_aqi", "avg_pm25", "observations"),
    "station_summary": (
        "city", "station", "observations", "avg_aqi", "max_aqi", "avg_pm25", "avg_pm10", "avg_no2", "avg_so2",
    ),
    "correlation_long": ("var1", "var2", "paired_observations", "correlation"),
    "top_stations": ("city", "station", "max_observed_aqi", "avg_aqi", "aqi_observations"),
    "top_pm25_stations": ("city", "station", "max_observed_pm25", "avg_pm25", "pm25_observations"),
}

RESULT_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    input_file TEXT NOT NULL,
    is_demo_synthetic_data INTEGER NOT NULL,
    metadata_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS air_quality_records (
    run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    timestamp TEXT NOT NULL,
    city TEXT NOT NULL,
    station TEXT NOT NULL,
    pm25 REAL,
    pm10 REAL,
    no2 REAL,
    so2 REAL,
    aqi REAL,
    aqi_category_of_provided_aqi TEXT,
    PRIMARY KEY (run_id, timestamp, city, station)
);
CREATE INDEX IF NOT EXISTS idx_records_run_city ON air_quality_records(run_id, city);
CREATE INDEX IF NOT EXISTS idx_records_run_station ON air_quality_records(run_id, station);
CREATE TABLE IF NOT EXISTS city_avg_aqi (
    run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    city TEXT NOT NULL,
    avg_aqi REAL, min_aqi REAL, max_aqi REAL, aqi_observations INTEGER,
    PRIMARY KEY (run_id, city)
);
CREATE TABLE IF NOT EXISTS pm_comparison (
    run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    city TEXT NOT NULL,
    avg_pm25 REAL, avg_pm10 REAL, pm25_to_pm10_ratio REAL, paired_observations INTEGER,
    PRIMARY KEY (run_id, city)
);
CREATE TABLE IF NOT EXISTS trends (
    run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    period TEXT NOT NULL,
    city TEXT NOT NULL,
    avg_aqi REAL, avg_pm25 REAL, observations INTEGER,
    PRIMARY KEY (run_id, period, city)
);
CREATE TABLE IF NOT EXISTS station_summary (
    run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    city TEXT NOT NULL,
    station TEXT NOT NULL,
    observations INTEGER, avg_aqi REAL, max_aqi REAL, avg_pm25 REAL, avg_pm10 REAL, avg_no2 REAL, avg_so2 REAL,
    PRIMARY KEY (run_id, city, station)
);
CREATE TABLE IF NOT EXISTS correlation_long (
    run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    var1 TEXT NOT NULL,
    var2 TEXT NOT NULL,
    paired_observations INTEGER,
    correlation REAL,
    PRIMARY KEY (run_id, var1, var2)
);
CREATE TABLE IF NOT EXISTS top_stations (
    run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    city TEXT NOT NULL,
    station TEXT NOT NULL,
    max_observed_aqi REAL, avg_aqi REAL, aqi_observations INTEGER,
    PRIMARY KEY (run_id, city, station)
);
CREATE TABLE IF NOT EXISTS top_pm25_stations (
    run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    city TEXT NOT NULL,
    station TEXT NOT NULL,
    max_observed_pm25 REAL, avg_pm25 REAL, pm25_observations INTEGER,
    PRIMARY KEY (run_id, city, station)
);
"""


def _sqlite_value(value):
    """Convert pandas/numpy values to values accepted by sqlite3."""
    if pd.isna(value):
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime().isoformat(sep=" ")
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _rows(frame, columns):
    return [tuple(_sqlite_value(value) for value in row)
            for row in frame.loc[:, columns].itertuples(index=False, name=None)]


def save_run(database_path, cleaned_records, results, metadata):
    """Persist one complete pipeline run in a transaction and return its identifier."""
    path = Path(database_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(RESULT_SCHEMA)
        with connection:
            connection.execute(
                """INSERT INTO pipeline_runs
                   (run_id, created_at, input_file, is_demo_synthetic_data, metadata_json)
                   VALUES (?, ?, ?, ?, ?)""",
                (run_id, created_at, metadata["input_file"],
                 int(metadata["is_demo_synthetic_data"]), json.dumps(metadata, allow_nan=False)),
            )

            record_columns = (
                "timestamp", "city", "station", "pm25", "pm10", "no2", "so2", "aqi",
                "aqi_category_of_provided_aqi",
            )
            if cleaned_records is not None:
                connection.executemany(
                    """INSERT INTO air_quality_records
                       (run_id, timestamp, city, station, pm25, pm10, no2, so2, aqi,
                        aqi_category_of_provided_aqi)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [(run_id, *row) for row in _rows(cleaned_records, record_columns)],
                )

            for table_name, columns in RESULT_COLUMNS.items():
                placeholders = ", ".join("?" for _ in range(len(columns) + 1))
                column_list = ", ".join(("run_id", *columns))
                connection.executemany(
                    f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})",
                    [(run_id, *row) for row in _rows(results[table_name], columns)],
                )
    return run_id


def list_runs(database_path, limit=20):
    """Return recent run metadata, newest first."""
    with closing(sqlite3.connect(database_path)) as connection:
        rows = connection.execute(
            """SELECT run_id, created_at, input_file, is_demo_synthetic_data
               FROM pipeline_runs ORDER BY created_at DESC, rowid DESC LIMIT ?""",
            (limit,),
        ).fetchall()
    return [
        {"run_id": row[0], "created_at": row[1], "input_file": row[2],
         "is_demo_synthetic_data": bool(row[3])}
        for row in rows
    ]


def load_latest_run(database_path):
    """Load the most recently saved run and its original pipeline metadata."""
    with closing(sqlite3.connect(database_path)) as connection:
        row = connection.execute(
            """SELECT run_id, created_at, input_file, is_demo_synthetic_data, metadata_json
               FROM pipeline_runs ORDER BY created_at DESC, rowid DESC LIMIT 1"""
        ).fetchone()
    if row is None:
        return None
    return {
        "run_id": row[0],
        "created_at": row[1],
        "input_file": row[2],
        "is_demo_synthetic_data": bool(row[3]),
        **json.loads(row[4]),
    }


def load_result_table(database_path, run_id, table_name):
    """Load a known analytics table for a run."""
    if table_name not in RESULT_COLUMNS:
        raise ValueError(f"Unknown analytics table: {table_name}")
    with closing(sqlite3.connect(database_path)) as connection:
        return pd.read_sql_query(
            f"SELECT {', '.join(RESULT_COLUMNS[table_name])} FROM {table_name} WHERE run_id = ?",
            connection,
            params=(run_id,),
        )


def load_cleaned_records(database_path, run_id, limit=1000):
    """Load a bounded preview of the cleaned records for a run."""
    with closing(sqlite3.connect(database_path)) as connection:
        return pd.read_sql_query(
            """SELECT timestamp, city, station, pm25, pm10, no2, so2, aqi,
                      aqi_category_of_provided_aqi
               FROM air_quality_records WHERE run_id = ?
               ORDER BY timestamp, city, station LIMIT ?""",
            connection,
            params=(run_id, limit),
        )
