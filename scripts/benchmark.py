"""Pandas vs PySpark on the SAME cleaned records (run `python app.py` first).

What is timed: the three operations below, each run REPEAT times after one warm-up run,
reporting the median. What is NOT timed: Spark startup and reading the file (reported
separately). Operations (identical in both tools):
  1. average AQI/PM2.5/PM10 + row count per city
  2. max AQI + average pollutants per city/station
  3. pairwise correlations between pollutants and AQI
"""
import json
import os
import statistics
import sys
import time
from itertools import combinations

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark.sql import functions as F  # noqa: E402

from src import config, data_loader  # noqa: E402

REPEAT = 5
M = config.MEASURE_COLUMNS


def find_cleaned():
    for ext in ("parquet", "csv"):
        p = config.OUTPUT_DIR / f"{config.CLEANED_BASENAME}.{ext}"
        if p.exists():
            return p
    return None


def pandas_ops(pdf):
    a = pdf.groupby("city").agg(avg_aqi=("aqi", "mean"), avg_pm25=("pm25", "mean"),
                                avg_pm10=("pm10", "mean"), n=("aqi", "size")).reset_index()
    b = pdf.groupby(["city", "station"]).agg(max_aqi=("aqi", "max"), avg_pm25=("pm25", "mean"),
                                             avg_no2=("no2", "mean"), avg_so2=("so2", "mean")).reset_index()
    c = {(x, y): pdf[x].corr(pdf[y]) for x, y in combinations(M, 2)}  # pairwise-complete Pearson
    return a, b, c


def spark_ops(sdf):
    a = sdf.groupBy("city").agg(F.avg("aqi").alias("avg_aqi"), F.avg("pm25").alias("avg_pm25"),
                                F.avg("pm10").alias("avg_pm10"), F.count("*").alias("n")).toPandas()
    b = sdf.groupBy("city", "station").agg(F.max("aqi").alias("max_aqi"), F.avg("pm25").alias("avg_pm25"),
                                           F.avg("no2").alias("avg_no2"), F.avg("so2").alias("avg_so2")).toPandas()
    row = sdf.agg(*[F.corr(x, y).alias(f"{x}|{y}") for x, y in combinations(M, 2)]).collect()[0]
    c = {tuple(k.split("|")): v for k, v in row.asDict().items()}
    return a, b, c


def timed(fn, arg):
    fn(arg)  # warm-up (not counted)
    times = []
    for _ in range(REPEAT):
        t = time.perf_counter()
        fn(arg)
        times.append(time.perf_counter() - t)
    return statistics.median(times)


def check_same(p, s):
    """Confirm both tools produced the same numbers."""
    pa = p[0].sort_values("city").reset_index(drop=True)
    sa = s[0].sort_values("city").reset_index(drop=True)
    assert (pa["n"].values == sa["n"].values).all(), "row counts differ"
    for col in ("avg_aqi", "avg_pm25", "avg_pm10"):
        assert ((pa[col] - sa[col]).abs() < 1e-6).all(), f"{col} differs"
    for k, v in p[2].items():
        w = s[2][k]
        if pd.notna(v) and w is not None:
            assert abs(v - w) < 1e-6, f"correlation {k} differs"


def main():
    path = find_cleaned()
    if path is None:
        print("ERROR: no cleaned data in output/. Run `python app.py` first.")
        sys.exit(1)

    t = time.perf_counter()
    pdf = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
    pandas_load = time.perf_counter() - t

    t = time.perf_counter()
    spark = data_loader.get_spark()
    startup = time.perf_counter() - t
    try:
        t = time.perf_counter()
        sdf = spark.read.parquet(str(path)) if path.suffix == ".parquet" else \
            spark.read.option("header", True).option("inferSchema", True).csv(str(path))
        sdf = sdf.cache()
        n = sdf.count()
        spark_load = time.perf_counter() - t

        check_same(pandas_ops(pdf), spark_ops(sdf))
        p_med = timed(pandas_ops, pdf)
        s_med = timed(spark_ops, sdf)
    finally:
        spark.stop()

    res = {
        "records": int(n), "repeats": REPEAT, "input": path.name,
        "pandas_median_seconds": round(p_med, 5), "pyspark_median_seconds": round(s_med, 5),
        "pandas_file_load_seconds": round(pandas_load, 5),
        "spark_startup_seconds_excluded": round(startup, 3),
        "spark_file_load_and_cache_seconds_excluded": round(spark_load, 3),
        "results_identical": True,
        "notes": ("Timings are measured on this machine for this input only. Spark local mode runs on one "
                  "computer, so this does NOT demonstrate multi-machine distributed computing. On small "
                  "data Pandas is expected to be faster because Spark adds scheduling overhead."),
    }
    (config.OUTPUT_DIR / "benchmark_results.json").write_text(json.dumps(res, indent=2), encoding="utf-8")
    print(f"Records: {n}  (median of {REPEAT} runs after warm-up; results verified identical)")
    print(f"  Pandas  : {p_med * 1000:9.2f} ms")
    print(f"  PySpark : {s_med * 1000:9.2f} ms")
    print(f"  (excluded) Spark startup {startup:.1f}s, Spark load+cache {spark_load:.1f}s")
    print("Small data usually favours Pandas; local Spark is not proof of distributed speedup.")


if __name__ == "__main__":
    main()
