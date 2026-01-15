# USGS Flow Percentile Monitor

## 1. Project Overview

This project is a hydrological monitoring system that tracks streamflow conditions across the United States. It ingests data from USGS NWIS, calculates robust flow percentiles based on historical daily values, and determines the current status (e.g., "Normal", "Below Normal") for ~10,000 active gauges.

**Core Metric:** Flow Percentile (Ranking current flow against the historical distribution for the specific Day of Year).

## 2. Architecture

### Infrastructure

- **Compute:** AWS EC2 (t3.medium recommended for memory).
- **Storage:** AWS S3 (Stores historical statistical lookup tables and current live outputs).
- **Scheduling:** Linux Crontab on EC2.
- **Language:** Python 3.10+.

### Data Pipeline

The system consists of two distinct data pipelines:

#### Pipeline A: The "Reference Generator" (Slow / Batch)

- **Frequency:** Runs Monthly (or Annually).
- **Purpose:** Builds the statistical baseline.
- **Logic:**
  1.  Fetch full period-of-record daily mean data for all USGS sites using `dataretrieval`.
  2.  Calculate percentile thresholds (P0, P10, P25, P50, P75, P90, P100) for every Day of Year (1-366) using `hyswap`.
  3.  Aggregate this into efficient lookup tables (Parquet format), partitioned by State.
  4.  Upload lookup tables to S3 (`s3://my-bucket/reference_stats/state=VT/stats.parquet`).

#### Pipeline B: The "Live Monitor" (Fast / Stream)

- **Frequency:** Runs Hourly.
- **Purpose:** Assesses current conditions.
- **Logic:**
  1.  Download the latest reference stats from S3 (cached in memory or local disk).
  2.  Fetch current Instantaneous Values (IV) for all sites using `dataretrieval`.
  3.  Compare current IV against the reference DOY stats to derive the exact percentile.
  4.  Save results to S3 (`s3://my-bucket/live_output/latest_conditions.json`) for frontend consumption.

## 3. Tech Stack

- **USGS Data:** `dataretrieval`, `hyswap`
- **Data Manipulation:** `pandas`, `numpy`, `pyarrow` (for Parquet)
- **AWS SDK:** `boto3`
- **Parallelization:** `concurrent.futures` (Critical for fetching 10k sites)

## 4. S3 Bucket Structure

```text
my-flow-bucket/
├── reference_stats/             # Output of Pipeline A
│   ├── state=AL/data.parquet
│   ├── state=AK/data.parquet
│   └── ...
├── live_output/                 # Output of Pipeline B
│   ├── current_status.json      # The latest snapshot
│   └── history/                 # Archival of hourly runs (optional)
│       └── 2026-01-15T1200.json
└── logs/
```
