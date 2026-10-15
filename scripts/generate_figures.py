"""Rebuild the PNG figures from saved results in output/ (no Spark needed).
Run `python app.py` first."""
import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src import config, visualizations  # noqa: E402


def main():
    out = config.OUTPUT_DIR
    meta_path = out / "summary.json"
    if not meta_path.exists():
        print("ERROR: output/summary.json not found. Run `python app.py` first.")
        sys.exit(1)
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    r = {n: pd.read_csv(out / f"{n}.csv") for n in
         ["city_avg_aqi", "pm_comparison", "trends", "correlation_long", "top_stations"]}
    r["correlation_long"]["correlation"] = pd.to_numeric(r["correlation_long"]["correlation"], errors="coerce")
    r["trend_granularity"] = meta["trend_granularity"]
    for f in visualizations.make_all(r, meta["source_label"]):
        print(f"Saved {f.relative_to(config.ROOT)}")


if __name__ == "__main__":
    main()
