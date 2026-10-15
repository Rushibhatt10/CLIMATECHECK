
"""Publication-ready charts for real and demonstration air-quality data."""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src import config

plt.rcParams.update({
    "font.size": 10,
    "axes.titlesize": 13,
    "axes.labelsize": 10,
    "figure.dpi": 130,
    "axes.grid": True,
    "grid.alpha": 0.25,
})


def _save(fig, filename, source_label, out_dir):
    fig.text(
        0.5, 0.015, source_label,
        ha="center", fontsize=8, color="gray",
        wrap=True,
    )
    fig.tight_layout(rect=(0, 0.055, 1, 1))
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    fig.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


def _empty(title, filename, source_label, out_dir):
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.axis("off")
    ax.set_title(title)
    ax.text(
        0.5, 0.5, "No valid observations available for this chart.",
        ha="center", va="center", transform=ax.transAxes,
    )
    return _save(fig, filename, source_label, out_dir)


def city_avg_aqi(df, source_label, out_dir):
    """Compatibility function: chart city-average PM2.5, never invent AQI."""
    title = "Cities with Highest Average PM2.5 Concentration"

    if df.empty or not {"city", "avg_pm25"}.issubset(df.columns):
        return _empty(title, "01_city_avg_pm25.png", source_label, out_dir)

    d = df.dropna(subset=["avg_pm25"]).nlargest(20, "avg_pm25")
    if d.empty:
        return _empty(title, "01_city_avg_pm25.png", source_label, out_dir)

    d = d.sort_values("avg_pm25")
    fig, ax = plt.subplots(figsize=(10, max(5, 0.34 * len(d))))
    bars = ax.barh(d["city"], d["avg_pm25"])
    ax.bar_label(bars, fmt="%.1f", padding=3, fontsize=8)
    ax.set_title(title)
    ax.set_xlabel("Average PM2.5 concentration (µg/m³)")
    ax.set_ylabel("City")
    return _save(fig, "01_city_avg_pm25.png", source_label, out_dir)


def pm_comparison(df, source_label, out_dir):
    title = "Average PM2.5 vs PM10 by City"

    if df.empty or not {
        "city", "avg_pm25", "avg_pm10"
    }.issubset(df.columns):
        return _empty(title, "02_pm25_vs_pm10.png", source_label, out_dir)

    d = df.dropna(subset=["avg_pm25", "avg_pm10"])
    d = d.nlargest(15, "avg_pm10").sort_values("avg_pm10")
    if d.empty:
        return _empty(title, "02_pm25_vs_pm10.png", source_label, out_dir)

    y = np.arange(len(d))
    fig, ax = plt.subplots(figsize=(10, max(5, 0.38 * len(d))))
    ax.barh(y - 0.18, d["avg_pm25"], 0.35, label="PM2.5")
    ax.barh(y + 0.18, d["avg_pm10"], 0.35, label="PM10")
    ax.set_yticks(y)
    ax.set_yticklabels(d["city"])
    ax.set_title(title + " (top 15 cities by PM10)")
    ax.set_xlabel("Average concentration (µg/m³)")
    ax.set_ylabel("City")
    ax.legend()
    return _save(fig, "02_pm25_vs_pm10.png", source_label, out_dir)


def trends(df, granularity, source_label, out_dir):
    title = "Monthly PM2.5 Concentration Trends"
    filename = "03_pm25_trends.png"

    required = {"period", "city", "avg_pm25"}
    if df.empty or not required.issubset(df.columns):
        return _empty(title, filename, source_label, out_dir)

    d = df.dropna(subset=["avg_pm25"]).copy()
    if d.empty:
        return _empty(title, filename, source_label, out_dir)

    d["period"] = pd.to_datetime(d["period"], errors="coerce")
    d = d.dropna(subset=["period"])

    # Keep the eight cities with the highest average PM2.5 so the chart
    # remains readable instead of drawing hundreds of overlapping lines.
    top_cities = (
        d.groupby("city")["avg_pm25"].mean()
        .nlargest(8).index
    )
    d = d[d["city"].isin(top_cities)]

    fig, ax = plt.subplots(figsize=(11, 6))
    for city, group in d.groupby("city"):
        group = group.sort_values("period")
        ax.plot(
            group["period"], group["avg_pm25"],
            marker="o", markersize=3, linewidth=1.6, label=city,
        )

    ax.xaxis.set_major_formatter(
        mdates.ConciseDateFormatter(mdates.AutoDateLocator())
    )
    ax.set_title(title)
    ax.set_xlabel("Date (UTC)")
    ax.set_ylabel("Average PM2.5 concentration (µg/m³)")
    ax.legend(title="City", loc="best", fontsize=8)
    return _save(fig, filename, source_label, out_dir)


def pivot_corr(long_df):
    cols = config.MEASURE_COLUMNS
    matrix = pd.DataFrame(np.nan, index=cols, columns=cols, dtype=float)

    for col in cols:
        matrix.loc[col, col] = 1.0

    if long_df.empty:
        return matrix

    for _, row in long_df.iterrows():
        value = row["correlation"]
        value = np.nan if pd.isna(value) else float(value)
        matrix.loc[row["var1"], row["var2"]] = value
        matrix.loc[row["var2"], row["var1"]] = value

    return matrix


def correlation_heatmap(long_df, source_label, out_dir):
    title = "Correlation Between Measured Pollutants"
    matrix = pivot_corr(long_df)

    off_diagonal = matrix.values[
        ~np.eye(len(matrix), dtype=bool)
    ]
    if np.isnan(off_diagonal).all():
        return _empty(
            title, "04_correlation_heatmap.png", source_label, out_dir
        )

    labels = [config.LABELS.get(c, c) for c in matrix.columns]
    fig, ax = plt.subplots(figsize=(7.5, 6.5))
    cmap = plt.get_cmap("RdBu_r").copy()
    cmap.set_bad("#dddddd")

    image = ax.imshow(
        np.ma.masked_invalid(matrix.values),
        vmin=-1, vmax=1, cmap=cmap,
    )
    ax.set_xticks(range(len(labels)))
    ax.set_yticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=25, ha="right")
    ax.set_yticklabels(labels)

    for i in range(len(labels)):
        for j in range(len(labels)):
            value = matrix.values[i, j]
            text = "n/a" if np.isnan(value) else f"{value:.2f}"
            ax.text(j, i, text, ha="center", va="center")

    fig.colorbar(image, ax=ax, label="Pearson correlation (-1 to +1)")
    ax.set_title(title)
    return _save(
        fig, "04_correlation_heatmap.png", source_label, out_dir
    )


def top_stations(df, source_label, out_dir):
    """Use station_summary to plot highest average PM10, not AQI."""
    title = "Stations with Highest Average PM10 Concentration"
    filename = "05_top_pm10_stations.png"

    required = {"city", "station", "avg_pm10"}
    if df.empty or not required.issubset(df.columns):
        return _empty(title, filename, source_label, out_dir)

    d = df.dropna(subset=["avg_pm10"]).nlargest(15, "avg_pm10")
    if d.empty:
        return _empty(title, filename, source_label, out_dir)

    d = d.sort_values("avg_pm10")
    labels = d["station"].astype(str) + " (" + d["city"].astype(str) + ")"
    fig, ax = plt.subplots(figsize=(11, max(5, 0.38 * len(d))))
    bars = ax.barh(labels, d["avg_pm10"])
    ax.bar_label(bars, fmt="%.1f", padding=3, fontsize=8)
    ax.set_title(title + " (top 15)")
    ax.set_xlabel("Average PM10 concentration (µg/m³)")
    ax.set_ylabel("Monitoring station (city)")
    return _save(fig, filename, source_label, out_dir)


def top_pm25_stations(df, source_label, out_dir):
    title = "Stations with Highest Observed PM2.5 Concentration"
    filename = "06_top_pm25_stations.png"

    if df.empty or not {
        "city", "station", "max_observed_pm25"
    }.issubset(df.columns):
        return _empty(title, filename, source_label, out_dir)

    d = df.dropna(subset=["max_observed_pm25"]).nlargest(
        15, "max_observed_pm25"
    )
    if d.empty:
        return _empty(title, filename, source_label, out_dir)

    d = d.sort_values("max_observed_pm25")
    labels = d["station"].astype(str) + " (" + d["city"].astype(str) + ")"
    fig, ax = plt.subplots(figsize=(11, max(5, 0.38 * len(d))))
    bars = ax.barh(labels, d["max_observed_pm25"])
    ax.bar_label(bars, fmt="%.1f", padding=3, fontsize=8)
    ax.set_title(title + " (top 15)")
    ax.set_xlabel("Maximum observed PM2.5 concentration (µg/m³)")
    ax.set_ylabel("Monitoring station (city)")
    return _save(fig, filename, source_label, out_dir)


def make_all(results, source_label, out_dir=None):
    out_dir = out_dir or config.FIGURES_DIR

    # These existing result keys are retained for database compatibility.
    # AQI-named legacy results are deliberately not presented as AQI.
    return [
        city_avg_aqi(results["pm_comparison"], source_label, out_dir),
        pm_comparison(results["pm_comparison"], source_label, out_dir),
        trends(
            results["trends"], results["trend_granularity"],
            source_label, out_dir,
        ),
        correlation_heatmap(
            results["correlation_long"], source_label, out_dir
        ),
        top_stations(results["station_summary"], source_label, out_dir),
        top_pm25_stations(
            results["top_pm25_stations"], source_label, out_dir
        ),
    ]


def source_label_for(input_path, is_demo):
    if is_demo:
        return "DEMONSTRATION DATA (synthetic) - NOT real measurements"
    if Path(input_path).name == config.REAL_DATA_INPUT.name:
        return (
            "CPCB CAAQM 15-minute observations, 2025 "
            "(ODbL-1.0; compiled by Vonter)"
        )
    return f"Source file: {Path(input_path).name}"
