"""Spark DataFrame analytics. Each function returns a small pandas DataFrame."""
from itertools import combinations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import config

M = config.MEASURE_COLUMNS


def dataset_summary(df: DataFrame) -> dict:
    row = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("city").alias("cities"),
        F.countDistinct("station").alias("stations"),
        F.date_format(F.min("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("first"),
        F.date_format(F.max("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("last"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__missing") for c in M],
    ).collect()[0].asDict()
    return {
        "rows": int(row["rows"]), "cities": int(row["cities"]), "stations": int(row["stations"]),
        "first_timestamp": str(row["first"]), "last_timestamp": str(row["last"]),
        "missing_after_cleaning": {c: int(row[f"{c}__missing"] or 0) for c in M},
    }


def city_avg_aqi(df: DataFrame) -> pd.DataFrame:
    """Average of the AQI values PROVIDED in the file (AQI is not recalculated here)."""
    return (df.groupBy("city")
            .agg(F.round(F.avg("aqi"), 1).alias("avg_aqi"),
                 F.min("aqi").alias("min_aqi"), F.max("aqi").alias("max_aqi"),
                 F.count("aqi").alias("aqi_observations"))
            .filter(F.col("aqi_observations") > 0)
            .orderBy(F.desc("avg_aqi")).toPandas())


def pm_comparison(df: DataFrame) -> pd.DataFrame:
    """PM2.5 vs PM10 per city, using only rows where BOTH were measured."""
    paired = df.filter(F.col("pm25").isNotNull() & F.col("pm10").isNotNull())
    return (paired.groupBy("city")
            .agg(F.round(F.avg("pm25"), 1).alias("avg_pm25"),
                 F.round(F.avg("pm10"), 1).alias("avg_pm10"),
                 F.round(F.sum("pm25") / F.sum("pm10"), 3).alias("pm25_to_pm10_ratio"),
                 F.count("*").alias("paired_observations"))
            .orderBy(F.desc("avg_pm10")).toPandas())


def trends(df: DataFrame):
    """Average AQI/PM2.5 per city per period. Returns (pandas_df, granularity)."""
    span = df.agg(F.datediff(F.max("timestamp"), F.min("timestamp")).alias("d")).collect()[0]["d"] or 0
    unit = "month" if span >= config.TREND_MONTHLY_IF_DAYS_AT_LEAST else "day"
    out = (df.withColumn("period", F.date_trunc(unit, F.col("timestamp")))
           .groupBy("period", "city")
           .agg(F.round(F.avg("aqi"), 1).alias("avg_aqi"),
                F.round(F.avg("pm25"), 1).alias("avg_pm25"),
                F.count("*").alias("observations"))
           .orderBy("period", "city")
           .select(F.date_format("period", "yyyy-MM-dd HH:mm:ss").alias("period"),
                   "city", "avg_aqi", "avg_pm25", "observations")
           .toPandas())
    return out, unit


def station_summary(df: DataFrame) -> pd.DataFrame:
    return (df.groupBy("city", "station")
            .agg(F.count("*").alias("observations"),
                 F.round(F.avg("aqi"), 1).alias("avg_aqi"), F.max("aqi").alias("max_aqi"),
                 F.round(F.avg("pm25"), 1).alias("avg_pm25"), F.round(F.avg("pm10"), 1).alias("avg_pm10"),
                 F.round(F.avg("no2"), 1).alias("avg_no2"), F.round(F.avg("so2"), 1).alias("avg_so2"))
            .orderBy("city", "station").toPandas())


def correlations(df: DataFrame) -> pd.DataFrame:
    """Pairwise Pearson correlation (long format); NaN when too few paired values."""
    exprs = []
    pairs = list(combinations(M, 2))
    for a, b in pairs:
        exprs.append(F.corr(a, b).alias(f"{a}|{b}|corr"))
        exprs.append(F.sum((F.col(a).isNotNull() & F.col(b).isNotNull()).cast("int")).alias(f"{a}|{b}|n"))
    r = df.agg(*exprs).collect()[0].asDict()
    rows = []
    for a, b in pairs:
        n = int(r[f"{a}|{b}|n"] or 0)
        c = r[f"{a}|{b}|corr"]
        ok = n >= config.MIN_CORR_OBSERVATIONS and c is not None and c == c
        rows.append({"var1": a, "var2": b, "paired_observations": n,
                     "correlation": round(float(c), 3) if ok else None})
    return pd.DataFrame(rows)


def top_stations(df: DataFrame, n: int = None) -> pd.DataFrame:
    """Stations with the highest OBSERVED AQI in the loaded data (not an official ranking)."""
    n = n or config.TOP_N_STATIONS
    return (df.filter(F.col("aqi").isNotNull()).groupBy("city", "station")
            .agg(F.max("aqi").alias("max_observed_aqi"),
                 F.round(F.avg("aqi"), 1).alias("avg_aqi"),
                 F.count("aqi").alias("aqi_observations"))
            .orderBy(F.desc("max_observed_aqi"), F.desc("avg_aqi")).limit(n).toPandas())


def top_pm25_stations(df: DataFrame, n: int = None) -> pd.DataFrame:
    """Stations with the highest observed PM2.5 concentration in the loaded data."""
    n = n or config.TOP_N_STATIONS
    return (df.filter(F.col("pm25").isNotNull()).groupBy("city", "station")
            .agg(F.max("pm25").alias("max_observed_pm25"),
                 F.round(F.avg("pm25"), 1).alias("avg_pm25"),
                 F.count("pm25").alias("pm25_observations"))
            .orderBy(F.desc("max_observed_pm25"), F.desc("avg_pm25")).limit(n).toPandas())


def run_all(df: DataFrame) -> dict:
    t, unit = trends(df)
    return {
        "summary": dataset_summary(df),
        "city_avg_aqi": city_avg_aqi(df),
        "pm_comparison": pm_comparison(df),
        "trends": t, "trend_granularity": unit,
        "station_summary": station_summary(df),
        "correlation_long": correlations(df),
        "top_stations": top_stations(df),
        "top_pm25_stations": top_pm25_stations(df),
    }
