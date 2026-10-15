"""Run the full pipeline: load -> clean -> analyse -> save -> figures.

Usage (from the project root):
    python app.py
    python app.py --input data/raw/my_dataset.csv --format csv
"""
import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql.types import DoubleType, StringType, TimestampType

from src import analytics, config, data_cleaning, data_loader, database, visualizations


def save_cleaned(pdf, fmt, output_dir=None):
    """Save cleaned data. Uses pandas for the final write so no Hadoop/winutils is needed on Windows."""
    output_dir = Path(output_dir) if output_dir else config.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{config.CLEANED_BASENAME}.{fmt}"
    if fmt == "parquet":
        pdf.to_parquet(path, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)  # Spark cannot read nanosecond timestamps
    else:
        pdf.to_csv(path, index=False)
    return path


def save_cleaned_spark(df, fmt, output_path, batch_size=10000, output_partitions=None):
    """Stream Spark rows in bounded batches to avoid Windows Hadoop writes and full collects."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        if output_path.is_dir():
            raise ValueError(f"Expected a file path but found a directory: {output_path}")
        output_path.unlink()

    rows_written = 0
    if output_partitions and output_partitions > df.rdd.getNumPartitions():
        df = df.repartition(output_partitions)
    if fmt == "parquet":
        arrow_types = {
            StringType: pa.string(),
            DoubleType: pa.float64(),
            TimestampType: pa.timestamp("us"),
        }
        fields = []
        for field in df.schema.fields:
            arrow_type = next(
                (value for spark_type, value in arrow_types.items() if isinstance(field.dataType, spark_type)),
                None,
            )
            if arrow_type is None:
                raise TypeError(f"Unsupported Spark type for Parquet streaming: {field.name} {field.dataType}")
            fields.append(pa.field(field.name, arrow_type, nullable=field.nullable))
        schema = pa.schema(fields)
        with pq.ParquetWriter(output_path, schema, compression="snappy") as writer:
            batch = []
            for row in df.toLocalIterator():
                values = row.asDict(recursive=True)
                for key, value in values.items():
                    if isinstance(value, datetime):
                        values[key] = value.astimezone(timezone.utc).replace(tzinfo=None)
                batch.append(values)
                if len(batch) >= batch_size:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                    rows_written += len(batch)
                    batch.clear()
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                rows_written += len(batch)
    else:
        with output_path.open("w", encoding="utf-8", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=df.columns, extrasaction="ignore")
            writer.writeheader()
            for row in df.toLocalIterator():
                values = row.asDict(recursive=True)
                for key, value in values.items():
                    if isinstance(value, datetime):
                        values[key] = value.astimezone(timezone.utc).replace(tzinfo=None).isoformat(sep=" ")
                writer.writerow(values)
                rows_written += 1
    return rows_written


def main():
    p = argparse.ArgumentParser(description="Indian Air Quality Big Data pipeline (PySpark)")
    p.add_argument("--mode", choices=["demo", "real"], help="select demo or real-data defaults")
    p.add_argument("--input", help="CSV or Parquet file (defaults by mode)")
    p.add_argument("--format", choices=["parquet", "csv"], default="parquet", help="cleaned-data format")
    p.add_argument("--no-figures", action="store_true", help="skip PNG figures")
    p.add_argument("--output-dir", help="override the mode-specific output directory")
    p.add_argument("--figures-dir", help="override the mode-specific figures directory")
    p.add_argument("--spark-temp-dir", help="override Spark local temporary storage")
    args = p.parse_args()

    if args.input:
        input_path = Path(args.input)
    elif args.mode == "real":
        input_path = config.REAL_DATA_INPUT
    else:
        input_path = config.DEFAULT_INPUT
    if not input_path.is_absolute():
        input_path = Path.cwd() / input_path
    demo = args.mode == "demo" if args.mode else config.is_demo(input_path)
    if args.mode == "real" and config.is_demo(input_path):
        p.error("The bundled DEMO file cannot be run with --mode real.")
    output_dir = Path(args.output_dir) if args.output_dir else (
        config.OUTPUT_DIR if demo else config.REAL_OUTPUT_DIR
    )
    figures_dir = Path(args.figures_dir) if args.figures_dir else (
        config.FIGURES_DIR if demo else config.REAL_FIGURES_DIR
    )
    spark_temp_dir = Path(args.spark_temp_dir) if args.spark_temp_dir else config.SPARK_LOCAL_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)
    label = visualizations.source_label_for(input_path, demo)
    t0 = time.perf_counter()

    spark = data_loader.get_spark(spark_temp_dir)
    cleaned = None
    try:
        print(f"[1/5] Loading {input_path.name}" + ("  (DEMO synthetic data)" if demo else ""))
        raw, source_schema = data_loader.load_dataframe_with_report(spark, input_path)

        print("[2/5] Cleaning with Spark")
        cleaned, report = data_cleaning.clean(raw)

        print("[3/5] Running analytics")
        results = analytics.run_all(cleaned)

        print("[4/5] Saving results")
        cleaned_path = output_dir / f"{config.CLEANED_BASENAME}.{args.format}"
        if demo:
            cleaned_pdf = cleaned.toPandas()
            save_cleaned(cleaned_pdf, args.format, output_dir)
            cleaned_path = output_dir / f"{config.CLEANED_BASENAME}.{args.format}"
        else:
            cleaned_pdf = None
            rows_written = save_cleaned_spark(
                cleaned, args.format, cleaned_path,
                output_partitions=config.SPARK_EXPORT_PARTITIONS,
            )
            if rows_written != report["rows_after_cleaning"]:
                raise ValueError(
                    f"Cleaned output row count mismatch: wrote {rows_written}, "
                    f"expected {report['rows_after_cleaning']}"
                )

        out = output_dir
        for name in ["city_avg_aqi", "pm_comparison", "trends", "station_summary",
                     "correlation_long", "top_stations", "top_pm25_stations"]:
            results[name].to_csv(out / f"{name}.csv", index=False)
        source_archive = None
        source = None
        if not demo:
            source = dict(config.REAL_DATA_SOURCE) if input_path.name == config.REAL_DATA_INPUT.name else {
                "name": "Unspecified real-data source",
                "url": None,
                "license": None,
                "attribution": None,
            }
            source["file_name"] = input_path.name
            source["file_size_bytes"] = input_path.stat().st_size
            source["timestamp_timezone"] = "UTC" if source.get("license") else None
            if input_path.name == config.REAL_DATA_INPUT.name and config.REAL_DATA_ARCHIVE.exists():
                source_archive = {
                    "file_name": config.REAL_DATA_ARCHIVE.name,
                    "file_size_bytes": config.REAL_DATA_ARCHIVE.stat().st_size,
                    "sha256": config.REAL_DATA_SOURCE["archive_sha256"],
                    "extracted_member_name": input_path.name,
                    "extracted_member_size_bytes": input_path.stat().st_size,
                }
        meta = {
            "input_file": input_path.name,
            "is_demo_synthetic_data": demo,
            "data_mode": "demo" if demo else "real",
            "source_label": label,
            "source": source,
            "source_archive": source_archive,
            "source_schema": source_schema,
            "aqi_note": (
                "AQI values are taken as provided in the input file; they are not recalculated."
                if "aqi" in source_schema["mapped_columns"]
                else "The source contains no AQI field; AQI was not calculated or inferred."
            ),
            "trend_granularity": results["trend_granularity"],
            "data_quality_report": report,
            "dataset_summary": results["summary"],
            "cleaned_data_file": cleaned_path.name,
            "cleaned_data_format": args.format,
            "cleaned_data_is_directory": False,
            "cleaned_records_in_sqlite": demo,
            "spark_temp_dir": str(spark_temp_dir),
            "processing": {
                "spark_version": spark.version,
                "spark_master": config.SPARK_MASTER,
                "spark_shuffle_partitions": config.SPARK_SHUFFLE_PARTITIONS,
                "spark_export_partitions": config.SPARK_EXPORT_PARTITIONS,
                "timestamp_timezone": "UTC",
                "cleaned_output_method": "Pandas" if demo else "bounded PyArrow batches from a Spark iterator",
            },
        }
        database_path = (
            output_dir / "air_quality.sqlite3" if args.output_dir
            else config.DATABASE_PATH if demo
            else config.REAL_DATABASE_PATH
        )
        run_id = database.save_run(database_path, cleaned_pdf, results, meta)
        meta["run_id"] = run_id
        meta["source_file_size_bytes"] = input_path.stat().st_size
        meta["source_archive_size_bytes"] = (
            source_archive["file_size_bytes"] if source_archive else None
        )
        meta["cleaned_data_size_bytes"] = cleaned_path.stat().st_size
        meta["elapsed_seconds"] = round(time.perf_counter() - t0, 1)
        (out / "summary.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
        (out / "run_manifest.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
        print(f"      SQLite database: {database_path} (run {run_id})")

        if args.no_figures:
            print("[5/5] Figures skipped")
        else:
            print("[5/5] Creating figures")
            figs = visualizations.make_all(results, label, figures_dir)
            for f in figs:
                print(f"      {f}")
    except (FileNotFoundError, ValueError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if cleaned is not None:
            cleaned.unpersist(blocking=True)
        spark.stop()

    s = results["summary"]
    print(f"\nDone in {time.perf_counter() - t0:.1f}s: {report['rows_read']} rows read -> "
          f"{report['rows_after_cleaning']} kept ({s['cities']} cities, {s['stations']} stations)")
    print(f"Cleaned data: {cleaned_path}")
    print(f"Results and manifest: {output_dir}")
    print(f"Figures: {figures_dir}")
    if demo:
        print("NOTE: these results come from synthetic DEMO data, not real measurements.")


if __name__ == "__main__":
    main()
