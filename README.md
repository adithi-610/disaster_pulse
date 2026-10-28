# 🔥 DisasterPulse

**Satellite-based wildfire detection and regional analysis pipeline**  
*Academic project — NASA FIRMS near-real-time fire detection data, India bounding box*

---

## Table of Contents

- [Overview](#overview)
- [Dataset](#dataset)
- [Project Structure](#project-structure)
- [Pipeline Architecture](#pipeline-architecture)
- [Setup & Installation](#setup--installation)
- [Running the Pipeline](#running-the-pipeline)
- [Script Reference](#script-reference)
- [Outputs](#outputs)
- [Key Findings](#key-findings)
- [Technology Stack](#technology-stack)
- [Notes & Limitations](#notes--limitations)

---

## Overview

DisasterPulse is a data engineering and analysis pipeline that ingests ~7.3 million NASA FIRMS satellite fire-detection records, filters them to a regional bounding box covering India and surrounding areas (lat 6°–37° N, lon 68°–98° E), cleans and enriches the data, loads it into MongoDB, runs a Hadoop Streaming MapReduce job, and serves a live Streamlit dashboard.

The full pipeline takes raw multi-satellite CSV downloads all the way to an interactive web dashboard using PySpark (local mode), MongoDB, Hadoop Streaming, pandas, Plotly, and Streamlit.

---

## Dataset

| Field | Value |
|---|---|
| **Source** | NASA FIRMS (Fire Information for Resource Management System) |
| **Download** | [firms.modaps.eosdis.nasa.gov](https://firms.modaps.eosdis.nasa.gov/) |
| **Documentation** | [Readme.txt](https://firms.modaps.eosdis.nasa.gov/download/Readme.txt) |
| **Date range** | 2024-11-01 → 2025-01-13 (~74 days; 2025-01-13 is a partial day) |
| **Total raw rows** | 7,312,164 |
| **Regional rows** | 294,374 (4.03% of total) |

### Raw files (`data/raw/`)

| File | Satellite | Instrument | Rows |
|---|---|---|---|
| `fire_nrt_M-C61_565334.csv` | Terra + Aqua | MODIS | 731,138 |
| `fire_nrt_J1V-C2_565335.csv` | NOAA-20 (N20) | VIIRS | 3,419,726 |
| `fire_nrt_SV-C2_565336.csv` | Suomi NPP (N) | VIIRS | 3,161,300 |

### Columns

```
latitude, longitude, brightness, scan, track, acq_date, acq_time,
satellite, instrument, confidence, version, bright_t31, frp, daynight
```

> **Note:** `confidence` is a numeric string (`0`–`100`) for MODIS and a letter (`l`/`n`/`h`) for VIIRS. The pipeline normalises both to `low` / `nominal` / `high`.

---

## Project Structure

```
DisasterPulse/
├── app.py                          # Streamlit dashboard
├── env.sh                          # Environment setup (Java, Hadoop, venv)
├── requirements.txt                # Python dependencies (pinned)
│
├── data/
│   ├── raw/                        # Source CSVs — never written to by scripts
│   │   ├── fire_nrt_M-C61_565334.csv
│   │   ├── fire_nrt_J1V-C2_565335.csv
│   │   ├── fire_nrt_SV-C2_565336.csv
│   │   └── Readme.txt
│   └── processed/                  # All pipeline outputs land here
│       ├── regional_fire.csv       # Bounding-box filtered, ~294k rows
│       ├── clean_regional.csv      # Cleaned + engineered features (plain CSV)
│       ├── clean_parquet/          # Parquet, partitioned by year_month
│       └── summaries/
│           ├── daily_counts.parquet
│           ├── grid_totals.parquet
│           └── grid_week_counts.parquet
│
├── results/                        # Text reports from each pipeline stage
│   ├── inspection_report.txt
│   ├── region_check.txt
│   ├── cleaning_report.txt
│   ├── full_mr_pipe.txt            # MapReduce full-run output
│   ├── sample20_mr.txt             # MapReduce sample output
│   ├── full_awk.txt
│   ├── sample20_awk.txt
│   └── sample20.csv
│
├── src/                            # All pipeline scripts
│   ├── inspect_data.py             # Stage 1 – EDA / data quality report
│   ├── region_check.py             # Stage 2 – Bounding-box statistics
│   ├── make_regional_dataset.py    # Stage 3 – Filter & save regional CSV
│   ├── clean_transform.py          # Stage 4 – Clean, enrich, write Parquet/CSV
│   ├── ingest_mongodb.py           # Stage 5 – Load into MongoDB
│   ├── mapper.py                   # Stage 6a – Hadoop Streaming mapper
│   └── reducer.py                  # Stage 6b – Hadoop Streaming reducer
│
└── report/
    └── report_notes.md             # Lab-notebook style decisions & findings
```

---

## Pipeline Architecture

```
data/raw/fire_nrt_*.csv   (7.3 M rows, 3 satellites)
          │
          ▼
┌─────────────────────┐
│  inspect_data.py    │  PySpark – EDA, schema, nulls, duplicates, date range
└─────────────────────┘
          │
          ▼
┌─────────────────────┐
│  region_check.py    │  PySpark – Count rows inside India bounding box
└─────────────────────┘
          │
          ▼
┌──────────────────────────┐
│  make_regional_dataset.py│  PySpark – Explicit schema, source_file col,
│                          │  filter → data/processed/regional_fire.csv
└──────────────────────────┘
          │
          ▼
┌─────────────────────┐
│  clean_transform.py │  PySpark – 5 cleaning steps, confidence_level,
│                     │  grid_id, week, acq_datetime, Parquet + CSV
└─────────────────────┘
          │
          ▼
┌─────────────────────┐
│  ingest_mongodb.py  │  pymongo – Drop & reload disaster_pulse.raw_detections
│                     │  in 10k-row chunks; create 3 indexes
└─────────────────────┘
          │
          ▼
┌────────────────────────────────┐
│  mapper.py  │  reducer.py      │  Hadoop Streaming – grid_id|week → count
└────────────────────────────────┘
          │
          ▼
┌─────────────────────┐
│  app.py (Streamlit) │  Interactive dashboard – filters, charts, map
└─────────────────────┘
```

---

## Setup & Installation

### Prerequisites

- Python 3.11+
- Java 17 (required by PySpark/Spark)
- MongoDB 9.x running on `localhost:27017`
- Hadoop (optional, for the MapReduce stage — installable via Homebrew)

### 1. Clone / open the project

```bash
cd /path/to/DisasterPulse
```

### 2. Create and activate the virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Set environment variables (Java + Hadoop)

```bash
source env.sh
```

`env.sh` exports:
- `JAVA_HOME` → OpenJDK 17 (Homebrew)
- `HADOOP_HOME` → Hadoop (Homebrew)
- Activates the `venv`

---

## Running the Pipeline

Run each stage in order from the project root. All scripts build their own paths from `__file__` and can be called from any working directory.

```bash
# Stage 1 – Inspect raw data quality
python src/inspect_data.py

# Stage 2 – Bounding-box row statistics
python src/region_check.py

# Stage 3 – Filter to India region, save regional CSV
python src/make_regional_dataset.py

# Stage 4 – Clean, transform, write Parquet + CSV
python src/clean_transform.py

# Stage 5 – Load into MongoDB
python src/ingest_mongodb.py

# Stage 6 – Hadoop Streaming MapReduce (local pipe simulation)
cat data/processed/clean_regional.csv \
  | python src/mapper.py \
  | sort \
  | python src/reducer.py

# Dashboard
python -m streamlit run app.py
```

---

## Script Reference

### [`src/inspect_data.py`](src/inspect_data.py)
PySpark EDA script. Reads all `data/raw/fire_nrt_*.csv` with inferred schema. Reports:
- Total row count and rows per satellite
- Schema and column data types
- Null counts per column
- Exact duplicate count
- Latitude / longitude min-max
- Acquisition date range and daily row counts
- Distinct values for `satellite`, `instrument`, `daynight`, `confidence`

Output saved to `results/inspection_report.txt`.

---

### [`src/region_check.py`](src/region_check.py)
PySpark script that applies the India bounding box filter on the raw 7.3 M row dataset. Reports:
- Total rows, rows in box, percentage
- Rows per satellite, rows per year-month
- Run time

Output saved to `results/region_check.txt`.

---

### [`src/make_regional_dataset.py`](src/make_regional_dataset.py)
PySpark script with an **explicit schema** (no type inference). Adds `source_file` column (CSV basename via `input_file_name()`). Filters to bounding box. Reports row counts and count of `acq_time` values with length ≠ 4. Writes `data/processed/regional_fire.csv` via pandas (single file).

---

### [`src/clean_transform.py`](src/clean_transform.py)
The main cleaning and feature-engineering stage. Applies five sequential steps and records rows removed at each:

| Step | Action |
|---|---|
| 1 | Drop exact duplicates |
| 2 | Drop rows with null in `latitude`, `longitude`, `acq_date`, `acq_time`, or `confidence` |
| 3 | Drop rows with `latitude ∉ [-90, 90]` or `longitude ∉ [-180, 180]` |
| 4 | Left-pad `acq_time` to 4 chars; validate hour ∈ [0,23] and minute ∈ [0,59]; add `acq_datetime` |
| 5 | Drop rows where `acq_date = 2025-01-13` (partial day) |

**Feature columns added:**

| Column | Description |
|---|---|
| `confidence_level` | `low` / `nominal` / `high` — normalises MODIS numeric and VIIRS letter scales |
| `year`, `month` | Integer year and month |
| `year_month` | String `yyyy-MM` (used as Parquet partition key) |
| `hour` | Acquisition hour (0–23) |
| `week` | Monday of the ISO week containing `acq_date` (as a date) |
| `grid_id` | `"lat_lon"` where each coordinate is floored to nearest 0.5° cell, e.g. `"12.5_77.0"` |

**Outputs:**
- `data/processed/clean_parquet/` — Parquet partitioned by `year_month` (overwrite)
- `data/processed/clean_regional.csv` — Single plain CSV via pandas
- `data/processed/summaries/daily_counts.parquet`
- `data/processed/summaries/grid_totals.parquet`
- `data/processed/summaries/grid_week_counts.parquet`
- `results/cleaning_report.txt` — Full report with top-10 grid_ids

---

### [`src/ingest_mongodb.py`](src/ingest_mongodb.py)
Loads `data/processed/regional_fire.csv` into MongoDB.

| Parameter | Value |
|---|---|
| URI | `mongodb://localhost:27017` |
| Database | `disaster_pulse` |
| Collection | `raw_detections` |
| Chunk size | 10,000 rows |
| Idempotency | Collection is **dropped** before each load |

Indexes created: `acq_date`, `satellite`, `(latitude, longitude)` compound.

---

### [`src/mapper.py`](src/mapper.py)
Hadoop Streaming mapper (stdlib only). Reads lines from stdin, skips the header (`latitude,…`), blank lines, and lines that don't split into exactly 23 fields. Emits:
```
grid_id|week<TAB>1
```
Skipped lines reported via `reporter:counter:DisasterPulse,SkippedLines,1` on stderr.

---

### [`src/reducer.py`](src/reducer.py)
Hadoop Streaming reducer (stdlib only). Reads sorted `key<TAB>value` lines, sums values per key, emits `key<TAB>total`. Malformed lines (no TAB, non-integer value) reported via `reporter:counter:DisasterPulse,BadReducerLines,1` on stderr.

---

### [`app.py`](app.py)
Streamlit dashboard. Reads `data/processed/clean_regional.csv`.

**Features:**
- Sidebar filters: date range, satellite, confidence
- KPI metrics: total detections, satellites, average FRP, high-confidence count
- Monthly detection trend (line chart)
- Detections by satellite (bar chart)
- FRP distribution (histogram)
- Confidence category breakdown (donut chart)
- Geographic scatter map (up to 5,000 sampled points, coloured by FRP)
- Expandable raw data table

---

## Outputs

| Path | Description |
|---|---|
| `results/inspection_report.txt` | Full EDA report on raw data |
| `results/region_check.txt` | Bounding-box filter statistics |
| `results/cleaning_report.txt` | Per-step removal counts, top-10 grids |
| `data/processed/regional_fire.csv` | Filtered regional dataset (~294k rows) |
| `data/processed/clean_regional.csv` | Cleaned + enriched dataset (~290k rows) |
| `data/processed/clean_parquet/` | Parquet partitioned by `year_month` |
| `data/processed/summaries/*.parquet` | Pre-aggregated daily / grid summaries |
| MongoDB `disaster_pulse.raw_detections` | 294,374 documents, 3 indexes |

---

## Key Findings

- **294,374 rows** (4.03% of 7.3 M) fall inside the India bounding box
- **NOAA-20 (N20)** is the dominant satellite: 143,549 detections (48.8%)
- **November 2024** has the highest fire activity: 133,673 detections
- **2025-01-13** is excluded (partial day — only 420 rows)
- MODIS confidence is numeric (`0`–`100`); VIIRS uses letters (`l`/`n`/`h`) — normalised to a common `confidence_level` column
- `acq_time` loses leading zeros when pandas infers integer type — always read as string and left-pad to 4 characters

---

## Technology Stack

| Technology | Version | Purpose |
|---|---|---|
| Python | 3.11+ | All scripts |
| PySpark | 4.2.0 | Distributed data processing (local mode) |
| pandas | 2.3.3 | Single-file CSV I/O, MongoDB chunk ingestion |
| pymongo | 4.19.0 | MongoDB driver |
| Streamlit | 1.66.1 | Dashboard |
| Plotly | 7.1.0 | Interactive charts |
| pyarrow | 26.0.0 | Parquet I/O |
| Hadoop | Homebrew | Streaming MapReduce runtime |
| MongoDB | 9.x | Document store for raw regional detections |
| Java | OpenJDK 17 | Required by Spark and Hadoop |

---

## Notes & Limitations

- **Local mode only** — Spark runs on a single machine (`local[*]`). The pipeline is designed for a MacBook Air; cluster deployment would require Spark cluster config changes only.
- **A detection ≠ a fire** — Each row is one satellite overpass observation of a thermal anomaly, not a confirmed unique wildfire incident.
- **Bounding box ≠ India** — The box (lat 6°–37°, lon 68°–98°) includes parts of Pakistan, Bangladesh, Nepal, Bhutan, Sri Lanka, and Myanmar.
- **~74 days of data** — The dataset covers 2024-11-01 to 2025-01-12 (after removing the partial day). Long-term seasonal trends cannot be drawn from this window.
- **MongoDB must be running** — `ingest_mongodb.py` requires a local MongoDB instance on port 27017 before it is called.
- **`data/raw/` is read-only** — No script ever writes into `data/raw/`.
