# Indian Air Quality Big Data Analytics Using Apache Spark

> **Important:** the bundled data (`data/sample/sample_air_quality_DEMO.csv`) is a tiny **synthetic demonstration
> file**. It exists only to test the code. Its numbers are made up and must **not** be presented as real
> Indian air-quality findings. Replace it with a real open dataset (see section 8) for any real analysis.

## 1. What this project does
It loads air-quality records, cleans them with **Apache Spark (PySpark)**, calculates statistics
(city averages, PM2.5 vs PM10, trends, station summaries, correlations, highest-AQI stations), saves the
results and draws six PNG charts. An optional dashboard shows everything in a browser, and a benchmark
compares Pandas with PySpark.

**AQI note:** AQI values are used exactly as they appear in the input file. This project does **not**
recalculate AQI. The category column (Good, Satisfactory, ...) is only a label for the AQI number provided.

## 2. What each tool does
| Tool | Purpose |
|---|---|
| Python | The programming language everything is written in |
| Java 17 | Spark runs on the Java Virtual Machine, so Java is required |
| PySpark | Python interface to Apache Spark, used for loading, cleaning and analysis |
| Pandas | Holds small result tables; also used in the benchmark |
| SQLite | Stores cleaned observations, analysis results, and pipeline-run history locally |
| Matplotlib | Draws the PNG figures |
| PyArrow | Lets Pandas read/write Parquet files |
| Streamlit (optional) | Simple browser dashboard |

## 3. Folder structure
```
app.py                  run the demo or real-data pipeline
dashboard.py            optional Streamlit dashboard
requirements.txt        core packages     requirements-dashboard.txt  adds Streamlit
src/config.py           ALL settings: input file, column names, valid ranges, Spark options
src/data_loader.py      SparkSession + CSV/Parquet loading + column check
src/data_cleaning.py    types, invalid values, duplicates, quality report
src/database.py         SQLite schema and per-run persistence/query helpers
src/analytics.py        the six analyses
src/visualizations.py   the six figures
scripts/spark_test.py       checks Java + Spark work
scripts/generate_figures.py redraws figures from saved results (no Spark)
scripts/benchmark.py        Pandas vs PySpark timing
scripts/make_sample_data.py recreates the demo CSV
data/sample/  demo data     data/raw/  downloaded/extracted real data (ignored by git)
figures/  demo and real PNG charts     output/  demo and real results/manifests
```

## 4. Set up (Windows PowerShell, from the project folder)
The verified local setup uses Python 3.10.11, Java 17, and PySpark 4.0.1. Do not recreate the environment
or reinstall requirements when the existing `.venv` is working. From this project folder:
```powershell
cd "C:\Users\Rushi Bhatt\Downloads\indian-air-quality-bigdata\indian-air-quality-bigdata"
java -version
.\.venv\Scripts\python.exe --version
```

## 5. Test Spark
```powershell
.\.venv\Scripts\python.exe scripts\spark_test.py
```
Expected last line: `Spark job ran correctly: PASS`. (Harmless Windows warnings about "winutils" or
"native-hadoop" may appear; this project avoids Hadoop file writes, so they do not matter.)

## 6. Run the analytics pipeline
```powershell
.\.venv\Scripts\python.exe app.py
```
This runs the bundled synthetic demo. Real data is a separate mode:
```powershell
.\.venv\Scripts\python.exe app.py --mode real
```
The default real input is the extracted CSV at `data/raw/cpcb-aqi-2025.csv`. Options include
`--input <file.csv|file.parquet>`, `--format csv`, `--no-figures`, `--output-dir <path>`,
`--figures-dir <path>`, and `--spark-temp-dir <path>`. Real processing uses bounded PyArrow batches for
the cleaned export; it does not collect the full Spark DataFrame into Pandas or SQLite.

Then (optional):
```powershell
.\.venv\Scripts\python.exe scripts\benchmark.py
.\.venv\Scripts\python.exe scripts\generate_figures.py
.\.venv\Scripts\python.exe -m pip install -r requirements-dashboard.txt
.\.venv\Scripts\python.exe -m streamlit run dashboard.py
```

## 7. Where results are saved
- `figures/` and `output/` - demo figures/results
- `figures/real/` and `output/real/` - separate real-data figures/results
- `output/cleaned_air_quality.parquet` - cleaned demo data
- `output/real/cleaned_air_quality.parquet` - cleaned real observations as a Parquet file
- Each mode has analytics CSVs, `summary.json`, and `run_manifest.json` with mode, source, byte sizes,
  schema mapping, counts, UTC date coverage, and processing metadata.
- Demo SQLite stores cleaned observations and analyses. Real SQLite stores run metadata and small analytics
  tables only; the 16-million-row cleaned dataset stays in Parquet.
- `output/benchmark_results.json` - benchmark timings

The optional dashboard uses the latest SQLite run for its tables. Real cleaned observations are not copied
into SQLite; use the Parquet output for full-resolution records.

## 8. Verified real dataset and results

### Selected source, provenance, and licence

The integrated source is the 2025 15-minute station-observation release from
[Vonter/india-cpcb-aqi](https://github.com/Vonter/india-cpcb-aqi/releases/tag/2025). Its
[data dictionary](https://github.com/Vonter/india-cpcb-aqi/blob/main/DATA.md) describes station-level
readings as sourced from the [CPCB Data Repository](https://airquality.cpcb.gov.in/ccr/#/caaqm-dashboard-all/caaqm-landing/caaqm-data-repository).
The repository identifies the dataset licence as **Open Database License (ODbL) v1.0**. Attribute CPCB and
the dataset compiler; publicly used adapted databases must comply with ODbL share-alike requirements.
CPCB may retain copyright over individual records.

The approved archive and extracted source were verified locally:

| Artifact | Exact size | Verified details |
|---|---:|---|
| `cpcb-air-quality-2025.csv.zip` | 542,560,783 bytes (517.4 MiB) | GitHub release asset; SHA-256 `f6b15ffcf7490986c6e0e1fae9f7fcbbde57d2e010a5ec2466ce1d9e5f0cf09b` |
| `cpcb-aqi-2025.csv` extracted from that ZIP | 3,388,981,301 bytes (3.156 GiB) | ZIP directory metadata and extracted file size agree; this exceeds 1 GiB |

The archive contains one CSV with 18,838,322 records, 29 source columns, station identifiers/names, state,
city, UTC timestamps, and pollutant/weather measurements at 15-minute intervals. The archive is available at
the linked GitHub release. The downloaded archive is retained at `data/raw/cpcb-air-quality-2025.csv.zip`
and the extracted CSV at `data/raw/cpcb-aqi-2025.csv`; these large files are intentionally git-ignored.
To reproduce the download and extraction (check that you have the required free disk space first):

```powershell
New-Item -ItemType Directory -Force data\raw
curl.exe -fL --retry 3 -o data\raw\cpcb-air-quality-2025.csv.zip `
  https://github.com/Vonter/india-cpcb-aqi/releases/download/2025/cpcb-air-quality-2025.csv.zip
Get-FileHash data\raw\cpcb-air-quality-2025.csv.zip -Algorithm SHA256
Expand-Archive -LiteralPath data\raw\cpcb-air-quality-2025.csv.zip -DestinationPath data\raw -Force
Get-Item data\raw\cpcb-air-quality-2025.csv.zip,data\raw\cpcb-aqi-2025.csv |
  Select-Object Name,Length
.\.venv\Scripts\python.exe app.py --mode real
```
Verify the hash against the value in the artifact table and the two file sizes against their listed byte counts.

Other sources examined:

- [India Air Quality API / XKDR Forum](https://airquality.xkdr.org/) documents genuine CPCB CAAQM and US
  Embassy/AirNow readings under **CC BY 4.0**, with attribution to India Air Quality Database, XKDR Forum,
  CPCB, and US Department of State/AirNow. It advertises API/bulk Parquet access, but the accessible source
  page did not provide a verifiable downloadable artifact size, so it was not used to satisfy the size test.
- The official [CPCB Central Control Room](https://airquality.cpcb.gov.in/ccr/) provides a data-repository
  interface, but no downloadable archival file size was exposed in this check. The
  [data.gov.in real-time AQI catalog](https://www.data.gov.in/catalog/real-time-air-quality-index) returned
  HTTP 403 to the metadata request, so its dataset size and downloadable snapshot could not be verified.

### Mapping, processing, and measured results

Run the real source with `.\.venv\Scripts\python.exe app.py --mode real`. The source timestamp offset is
parsed as UTC; station ID is the station key, while station name and state are retained. PM2.5, PM10, NO2,
and SO2 are mapped to the existing analyses. **This 15-minute file has no AQI field**: no AQI is calculated,
derived, or inferred. Its other 20 pollutant/weather columns are listed as unsupported in the run manifest
rather than silently treated as AQI or included in current project analyses.

Verified full-file run:

- 18,838,322 rows read; 16,258,560 retained after cleaning.
- 2,579,762 rows had no valid value in the four mapped measurements and were removed; no rows had invalid
  timestamps/keys, no duplicates were removed, and no out-of-range/non-numeric values were observed in the
  mapped measurements.
- 288 cities and 549 distinct station IDs.
- UTC coverage: 2025-01-01 00:00 through 2025-12-31 23:45 (15-minute source intervals).
- The cleaned Parquet, analytics CSVs, and `output/real/run_manifest.json` are generated from the full source.
- Figures are in `figures/real/`: PM comparison, PM2.5 trend, correlation heatmap, and top-PM2.5 stations
  use real measurements. AQI-specific charts explicitly show that the source has no AQI values.

The full data was processed locally with PySpark in local mode. The cleaned output is streamed to Parquet
in bounded batches; Spark temporary storage and output directories can be overridden with CLI options.
`output/real/run_manifest.json` is the record of the actual run and includes source/archive sizes, licence,
source schema, missing/unsupported fields, counts, date coverage, Spark settings, and output size.

## 9. Known limitations
- Sample results are synthetic. Its very high correlations are artefacts of how the fake numbers were generated.
- Spark runs in **local mode** on one computer. That is real Spark, but it does not demonstrate multi-machine distributed computing.
- For small data Pandas is faster than Spark because of Spark's startup and scheduling overhead. The benchmark measures
  only the analysis operations (after one warm-up run, median of 5) and excludes Spark startup and file loading; it checks both tools return identical results.
- AQI is never recomputed. The selected 15-minute source does not contain AQI; AQI analytics are therefore
  empty for the real run, and no conversion from pollutant concentrations is attempted.
- "Highest observed AQI" means highest in the loaded data only, not an official pollution ranking.
- Current analytics use PM2.5, PM10, NO2, and SO2. Other source pollutants and meteorological columns are
  reported as unsupported and are not analyzed; rows with no valid values in those four mapped fields are dropped.
- Real cleaned records are not stored in SQLite. Use the Parquet file for full-resolution data.
- SQLite is a local, single-machine database and is not intended for concurrent multi-user production workloads.
- Duplicates are defined as the same timestamp + city + station; one arbitrary row is kept.
- Large local Spark runs require sufficient free disk for input, Spark spill/cache, and outputs. The verified
  run had over 399 GB free before download; disk space was checked before and after processing.
