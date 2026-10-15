"""Regenerates the small DEMO sample (synthetic numbers, NOT real measurements)."""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "data" / "sample" / "sample_air_quality_DEMO.csv"
random.seed(42)

# Made-up base levels, only to exercise the code.
STATIONS = {
    "Delhi": [("Demo Station D1", 190), ("Demo Station D2", 160)],
    "Mumbai": [("Demo Station M1", 95), ("Demo Station M2", 80)],
    "Chennai": [("Demo Station C1", 70), ("Demo Station C2", 60)],
    "Kolkata": [("Demo Station K1", 140), ("Demo Station K2", 120)],
}
start = datetime(2024, 1, 1)
rows = []
for city, stations in STATIONS.items():
    for station, base in stations:
        for day in range(30):
            ts = start + timedelta(days=day, hours=12)
            level = base * random.uniform(0.75, 1.25)
            pm25 = level * 0.55
            pm10 = level * 0.95 + random.uniform(-10, 10)
            no2 = 20 + level * 0.20 + random.uniform(-5, 5)
            so2 = 8 + level * 0.05 + random.uniform(-3, 3)
            rows.append([ts.strftime("%Y-%m-%d %H:%M:%S"), city, station,
                         round(pm25, 1), round(pm10, 1), round(no2, 1), round(so2, 1),
                         round(level, 0), "DEMO_SYNTHETIC"])

# Deliberate data problems so the cleaning code has something to do.
rows[3][3] = ""                 # missing PM2.5
rows[10][4] = ""                # missing PM10
rows[25][5] = "N/A"             # non-numeric NO2
rows[40][3] = -5                # invalid negative PM2.5
rows[55][6] = 9999              # impossible SO2
rows[70][7] = ""                # missing AQI
rows[90][0] = "not a date"      # bad timestamp
rows.append(list(rows[5]))      # exact duplicate
rows.append(list(rows[100]))    # exact duplicate
rows.append(list(rows[130]))    # exact duplicate

OUT.parent.mkdir(parents=True, exist_ok=True)
with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["timestamp", "city", "station", "pm25", "pm10", "no2", "so2", "aqi", "data_source"])
    w.writerows(rows)
print(f"Wrote {len(rows)} rows to {OUT}")
