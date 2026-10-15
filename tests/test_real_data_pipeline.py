import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

from app import save_cleaned_spark
from src import config, data_cleaning, data_loader


class RealDataPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = data_loader.get_spark(config.SPARK_LOCAL_DIR / "tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_cpcb_schema_maps_without_inventing_aqi_and_streams_parquet(self):
        contents = (
            "Station ID,State,City,Station Name,Timestamp,PM2.5 (µg/m³),"
            "PM10 (µg/m³),NO2 (µg/m³),SO2 (µg/m³),NO (µg/m³)\n"
            'site_6010,Andaman and Nicobar,Sri Vijaya Puram,"Police Line, Sri Vijaya Puram",'
            "2025-01-01T00:00:00.000000+0000,12.5,24.0,9.0,3.0,7.0\n"
            "site_6011,Andaman and Nicobar,Sri Vijaya Puram,Station 2,"
            "2025-01-01T00:15:00.000000+0000,,,,,\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.csv"
            source.write_text(contents, encoding="utf-8")
            raw, schema_report = data_loader.load_dataframe_with_report(self.spark, source)

            self.assertEqual(schema_report["missing_measure_columns"], ["aqi"])
            self.assertIn("NO (µg/m³)", schema_report["unsupported_source_columns"])
            cleaned, quality_report = data_cleaning.clean(raw)
            self.assertEqual(quality_report["rows_read"], 2)
            self.assertEqual(quality_report["rows_after_cleaning"], 1)
            self.assertEqual(quality_report["rows_without_any_valid_measurement"], 1)

            try:
                row = cleaned.first()
                self.assertEqual(row["station"], "site_6010")
                self.assertEqual(row["station_name"], "Police Line, Sri Vijaya Puram")
                self.assertEqual(row["state"], "Andaman and Nicobar")
                self.assertEqual(row["pm25"], 12.5)
                self.assertIsNone(row["aqi"])

                parquet_path = Path(directory) / "cleaned.parquet"
                self.assertEqual(save_cleaned_spark(cleaned, "parquet", parquet_path), 1)
                saved = pq.read_table(parquet_path)
                self.assertEqual(saved.num_rows, 1)
                self.assertEqual(saved.column("timestamp")[0].as_py(), datetime(2025, 1, 1, 0, 0))
                self.assertIsNone(saved.column("aqi")[0].as_py())
                self.assertEqual(saved.column("station")[0].as_py(), "site_6010")
            finally:
                cleaned.unpersist(blocking=True)


if __name__ == "__main__":
    unittest.main()
