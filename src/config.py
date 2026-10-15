"""Single place for all data-source and pipeline settings."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ---- Data source -----------------------------------------------------------
DEFAULT_INPUT = ROOT / "data" / "sample" / "sample_air_quality_DEMO.csv"
REAL_DATA_INPUT = ROOT / "data" / "raw" / "cpcb-aqi-2025.csv"
REAL_DATA_ARCHIVE = ROOT / "data" / "raw" / "cpcb-air-quality-2025.csv.zip"
REAL_DATA_SOURCE = {
    "name": "India CPCB 15-minute air-quality observations, 2025",
    "url": "https://github.com/Vonter/india-cpcb-aqi/releases/tag/2025",
    "documentation_url": "https://github.com/Vonter/india-cpcb-aqi/blob/main/DATA.md",
    "upstream_url": "https://airquality.cpcb.gov.in/ccr/#/caaqm-dashboard-all/caaqm-landing/caaqm-data-repository",
    "license": "Open Database License (ODbL) v1.0",
    "attribution": "CPCB Data Repository; compiled by Vonter. Attribute CPCB and the dataset; share adapted databases under ODbL.",
    "archive_sha256": "f6b15ffcf7490986c6e0e1fae9f7fcbbde57d2e010a5ec2466ce1d9e5f0cf09b",
}

# Maps source column names to canonical names (case-insensitive).
COLUMN_MAP = {
    "Station ID": "station_id",
    "State": "state",
    "City": "city",
    "Station Name": "station_name",
    "Timestamp": "timestamp",
    "PM2.5 (µg/m³)": "pm25",
    "PM10 (µg/m³)": "pm10",
    "NO2 (µg/m³)": "no2",
    "SO2 (µg/m³)": "so2",
}

# Canonical keys are required; measures may be absent in a source and become NULL.
KEY_COLUMNS = ["timestamp", "city", "station"]
PRESERVED_COLUMNS = ["state", "station_id", "station_name"]
MEASURE_COLUMNS = ["pm25", "pm10", "no2", "so2", "aqi"]
REQUIRED_COLUMNS = KEY_COLUMNS
STATION_KEY_CANDIDATES = ["station", "station_id", "station_name"]

# Timestamp patterns tried in order (Spark datetime patterns).
TIMESTAMP_FORMATS = [
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXX",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd",
    "dd-MM-yyyy HH:mm:ss",
    "dd-MM-yyyy HH:mm",
    "dd-MM-yyyy",
    "dd/MM/yyyy HH:mm",
    "dd/MM/yyyy",
]

# Values outside these ranges are treated as invalid and set to missing.
VALID_RANGES = {
    "pm25": (0.0, 1000.0),
    "pm10": (0.0, 2000.0),
    "no2": (0.0, 1000.0),
    "so2": (0.0, 1000.0),
    "aqi": (0.0, 500.0),
}

UNITS = {"pm25": "µg/m³", "pm10": "µg/m³", "no2": "µg/m³", "so2": "µg/m³", "aqi": "index"}
LABELS = {"pm25": "PM2.5", "pm10": "PM10", "no2": "NO2", "so2": "SO2", "aqi": "AQI"}

# ---- Analytics settings ----------------------------------------------------
MIN_CORR_OBSERVATIONS = 10   # correlation reported only if >= this many paired values
TOP_N_STATIONS = 10
TREND_MONTHLY_IF_DAYS_AT_LEAST = 90  # otherwise trends are daily

# ---- Spark (kept light for a laptop) ----------------------------------------
SPARK_APP_NAME = "IndianAirQualityBigData"
SPARK_MASTER = "local[2]"
SPARK_DRIVER_MEMORY = "1g"
SPARK_SHUFFLE_PARTITIONS = "4"
SPARK_EXPORT_PARTITIONS = int(os.environ.get("SPARK_EXPORT_PARTITIONS", "64"))

# ---- Output locations -------------------------------------------------------
OUTPUT_DIR = ROOT / "output"
FIGURES_DIR = ROOT / "figures"
CLEANED_BASENAME = "cleaned_air_quality"   # .parquet or .csv is appended
DATABASE_PATH = OUTPUT_DIR / "air_quality.sqlite3"
REAL_OUTPUT_DIR = OUTPUT_DIR / "real"
REAL_FIGURES_DIR = FIGURES_DIR / "real"
REAL_DATABASE_PATH = REAL_OUTPUT_DIR / "air_quality.sqlite3"
SPARK_LOCAL_DIR = Path(os.environ.get("SPARK_LOCAL_DIR", ROOT / "tmp" / "spark"))


def is_demo(path) -> bool:
    """True when the input is the bundled synthetic demonstration file."""
    return "DEMO" in Path(path).name.upper()
