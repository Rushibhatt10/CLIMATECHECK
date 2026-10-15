"""Spark session creation and data loading/validation."""
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config


def get_spark(local_dir=None) -> SparkSession:
    """Create a lightweight local SparkSession."""
    # Make Spark workers use the same Python as this script (important on Windows).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    local_dir = Path(local_dir) if local_dir else config.SPARK_LOCAL_DIR
    local_dir.mkdir(parents=True, exist_ok=True)
    os.environ["SPARK_LOCAL_DIRS"] = str(local_dir)
    spark = (
        SparkSession.builder.appName(config.SPARK_APP_NAME)
        .master(config.SPARK_MASTER)
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .config("spark.local.dir", str(local_dir))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # Invalid casts become NULL instead of raising errors (we count them ourselves).
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _rename_columns(df: DataFrame) -> DataFrame:
    """Apply COLUMN_MAP and normalise names to lower case (case-insensitive)."""
    mapping = {k.lower(): v for k, v in config.COLUMN_MAP.items()}
    for col in df.columns:
        target = mapping.get(col.lower(), col.strip().lower())
        if target != col:
            df = df.withColumnRenamed(col, target)
    return df


def load_dataframe(spark: SparkSession, path=None) -> DataFrame:
    """Load a CSV or Parquet file and check that required columns exist."""
    frame, _ = load_dataframe_with_report(spark, path)
    return frame


def load_dataframe_with_report(spark: SparkSession, path=None):
    """Load canonical columns and describe source fields not represented by the pipeline."""
    path = Path(path) if path else config.DEFAULT_INPUT
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    suffix = path.suffix.lower()
    if suffix == ".csv":
        # Everything is read as text; types are applied (safely) in data_cleaning.
        df = spark.read.option("header", True).option("inferSchema", False).csv(str(path))
    elif suffix == ".parquet":
        df = spark.read.parquet(str(path))
    else:
        raise ValueError(f"Unsupported file type '{suffix}'. Use .csv or .parquet")

    source_columns = list(df.columns)
    df = _rename_columns(df)
    if "station" not in df.columns:
        station_source = next(
            (column for column in config.STATION_KEY_CANDIDATES if column in df.columns),
            None,
        )
        if station_source is None:
            raise ValueError(
                f"Missing station identifier. Expected one of {config.STATION_KEY_CANDIDATES}; "
                f"found: {df.columns}"
            )
        df = df.withColumn("station", F.col(station_source).cast("string"))

    missing = [c for c in config.REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns: {missing}. Found: {df.columns}. "
            "Fix the file or edit COLUMN_MAP in src/config.py."
        )

    available_columns = set(df.columns)
    missing_measure_columns = [c for c in config.MEASURE_COLUMNS if c not in df.columns]
    for column in missing_measure_columns:
        df = df.withColumn(column, F.lit(None).cast("string"))
    for column in config.PRESERVED_COLUMNS:
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None).cast("string"))

    mapping = {key.casefold(): value for key, value in config.COLUMN_MAP.items()}
    recognized = set(config.KEY_COLUMNS + config.PRESERVED_COLUMNS + config.MEASURE_COLUMNS)
    unsupported_source_columns = []
    for source_column in source_columns:
        canonical = mapping.get(source_column.casefold(), source_column.strip().lower())
        if canonical not in recognized:
            unsupported_source_columns.append(source_column)

    selected = config.KEY_COLUMNS + config.PRESERVED_COLUMNS + config.MEASURE_COLUMNS
    report = {
        "source_columns": source_columns,
        "mapped_columns": [column for column in selected if column in available_columns],
        "missing_measure_columns": missing_measure_columns,
        "unsupported_source_columns": unsupported_source_columns,
    }
    return df.select(*selected), report
