"""Cleaning with PySpark: types, invalid values, duplicates, quality report."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark import StorageLevel

from src import config

CPCB_BANDS = [  # Category of an AQI value, using the published Indian AQI bands
    (50, "Good"), (100, "Satisfactory"), (200, "Moderate"),
    (300, "Poor"), (400, "Very Poor"), (500, "Severe"),
]


def _parse_timestamp(col: str):
    parsed = [F.to_timestamp(F.trim(F.col(col)), fmt) for fmt in config.TIMESTAMP_FORMATS]
    return F.coalesce(*parsed)


def _aqi_category(col):
    expr = F.when(col.isNull(), F.lit(None).cast("string"))
    for upper, name in CPCB_BANDS:
        expr = expr.when(col <= upper, F.lit(name))
    return expr.otherwise(F.lit("Severe"))


def clean(df: DataFrame):
    """Return (clean_df, report). `report` explains what was fixed or removed."""
    typed = df.withColumn("timestamp_parsed", _parse_timestamp("timestamp"))
    for c in ("city", "station"):
        blank = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
        typed = typed.withColumn(c, F.when(blank, F.lit(None)).otherwise(F.trim(F.col(c).cast("string"))))
    for c in config.PRESERVED_COLUMNS:
        blank = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
        typed = typed.withColumn(c, F.when(blank, F.lit(None)).otherwise(F.trim(F.col(c).cast("string"))))

    # Per measurement: missing = empty in file, invalid = non-numeric or outside valid range.
    stats_exprs = []
    for c in config.MEASURE_COLUMNS:
        lo, hi = config.VALID_RANGES[c]
        raw = F.trim(F.col(c).cast("string"))
        num = raw.cast("double")
        is_empty = F.col(c).isNull() | (raw == "")
        is_bad = (~is_empty) & (num.isNull() | F.isnan(num) | (num < lo) | (num > hi))
        typed = typed.withColumn(f"{c}_num", F.when(is_empty | is_bad, F.lit(None)).otherwise(num))
        stats_exprs += [
            F.sum(is_empty.cast("int")).alias(f"{c}__missing"),
            F.sum(is_bad.cast("int")).alias(f"{c}__invalid"),
        ]
    valid_keys = (
        F.col("timestamp_parsed").isNotNull()
        & F.col("city").isNotNull()
        & F.col("station").isNotNull()
    )
    no_typed_values = F.lit(True)
    for c in config.MEASURE_COLUMNS:
        no_typed_values = no_typed_values & F.col(f"{c}_num").isNull()
    stats_exprs.extend([
        F.count("*").alias("rows_read"),
        F.sum(F.col("timestamp_parsed").isNull().cast("int")).alias("bad_timestamp"),
        F.sum((F.col("city").isNull() | F.col("station").isNull()).cast("int")).alias("no_city_or_station"),
        F.sum(valid_keys.cast("int")).alias("rows_after_keys"),
        F.sum((valid_keys & ~no_typed_values).cast("int")).alias("rows_after_values"),
    ])
    s = typed.agg(*stats_exprs).collect()[0].asDict()
    rows_in = int(s["rows_read"] or 0)
    rows_after_keys = int(s["rows_after_keys"] or 0)
    rows_after_values = int(s["rows_after_values"] or 0)

    out = typed.select(
        F.col("timestamp_parsed").alias("timestamp"),
        F.initcap(F.col("city")).alias("city"),
        F.col("station"),
        *[F.col(c) for c in config.PRESERVED_COLUMNS],
        *[F.col(f"{c}_num").alias(c) for c in config.MEASURE_COLUMNS],
    )
    out = out.filter(F.col("timestamp").isNotNull() & F.col("city").isNotNull() & F.col("station").isNotNull())
    no_output_values = F.lit(True)
    for c in config.MEASURE_COLUMNS:
        no_output_values = no_output_values & F.col(c).isNull()
    out = out.filter(~no_output_values)
    out = out.dropDuplicates(config.KEY_COLUMNS)
    out = out.withColumn("aqi_category_of_provided_aqi", _aqi_category(F.col("aqi")))
    # Keep the cleaned data reusable without pinning a multi-gigabyte source in RAM.
    out = out.persist(StorageLevel.DISK_ONLY)
    rows_out = out.count()

    report = {
        "rows_read": rows_in,
        "dropped_bad_timestamp_or_missing_city_station": rows_in - rows_after_keys,
        "dropped_rows_with_no_valid_measurement": rows_after_keys - rows_after_values,
        "duplicates_removed": rows_after_values - rows_out,
        "rows_after_cleaning": rows_out,
        "rows_without_any_valid_measurement": rows_after_keys - rows_after_values,
        "bad_timestamp_values": int(s["bad_timestamp"] or 0),
        "per_column": {
            c: {"missing_in_file": int(s[f"{c}__missing"] or 0),
                "invalid_set_to_missing": int(s[f"{c}__invalid"] or 0)}
            for c in config.MEASURE_COLUMNS
        },
    }
    return out, report
