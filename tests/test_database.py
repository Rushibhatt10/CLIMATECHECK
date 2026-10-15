import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

import pandas as pd

from src import database


class DatabasePersistenceTests(unittest.TestCase):
    def test_saves_run_records_and_analytics(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "air_quality.sqlite3"
            cleaned = pd.DataFrame([{
                "timestamp": pd.Timestamp("2024-01-01 12:00:00"),
                "city": "Delhi",
                "station": "Station 1",
                "pm25": 10.0,
                "pm10": 20.0,
                "no2": 5.0,
                "so2": 2.0,
                "aqi": 50.0,
                "aqi_category_of_provided_aqi": "Good",
            }])
            results = {
                name: pd.DataFrame(columns=columns)
                for name, columns in database.RESULT_COLUMNS.items()
            }
            results["city_avg_aqi"] = pd.DataFrame([{
                "city": "Delhi", "avg_aqi": 50.0, "min_aqi": 50.0, "max_aqi": 50.0,
                "aqi_observations": 1,
            }])
            results["correlation_long"] = pd.DataFrame([{
                "var1": "pm25", "var2": "pm10", "paired_observations": 1, "correlation": float("nan"),
            }])
            metadata = {
                "input_file": "sample.csv",
                "is_demo_synthetic_data": True,
                "dataset_summary": {"rows": 1},
                "data_quality_report": {"rows_after_cleaning": 1},
            }

            run_id = database.save_run(path, cleaned, results, metadata)

            stored_run = database.load_latest_run(path)
            self.assertEqual(stored_run["run_id"], run_id)
            self.assertEqual(stored_run["dataset_summary"], {"rows": 1})
            self.assertEqual(database.list_runs(path)[0]["input_file"], "sample.csv")
            self.assertEqual(database.load_result_table(path, run_id, "city_avg_aqi").iloc[0]["city"], "Delhi")
            correlation = database.load_result_table(path, run_id, "correlation_long").iloc[0]["correlation"]
            self.assertTrue(pd.isna(correlation))
            records = database.load_cleaned_records(path, run_id)
            self.assertEqual(len(records), 1)
            self.assertEqual(records.iloc[0]["city"], "Delhi")
            self.assertEqual(records.iloc[0]["timestamp"], "2024-01-01 12:00:00")

            with closing(sqlite3.connect(path)) as connection:
                tables = {
                    row[0] for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
            self.assertTrue(set(database.RESULT_COLUMNS).issubset(tables))

    def test_rejects_unknown_result_table(self):
        with self.assertRaises(ValueError):
            database.load_result_table(":memory:", "run-id", "not_a_table")


if __name__ == "__main__":
    unittest.main()
