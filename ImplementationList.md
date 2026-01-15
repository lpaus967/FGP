### File 2: `PROJECT_TICKETS.md`

This file gives Claude the step-by-step checklist to write the code.

```markdown
# Implementation Tickets

## Epic 1: Environment & Setup

**Goal:** Configure the AWS environment and Python project structure.

- **[TICKET-1.1] EC2 & IAM Configuration**
  - _Action:_ Launch EC2 instance (Ubuntu). Create IAM role with `S3FullAccess` (or scoped policy) and attach to EC2.
  - _Deliverable:_ SSH access verified; `aws s3 ls` works from the instance.
- **[TICKET-1.2] Python Environment**
  - _Action:_ Setup `venv`. Install requirements: `dataretrieval`, `hyswap`, `pandas`, `boto3`, `pyarrow`, `tqdm`.
  - _Deliverable:_ `requirements.txt` created and packages installed.

## Epic 2: Pipeline A (Reference Generator)

**Goal:** Build the script that calculates historical baselines using the Weibull method.

- **[TICKET-2.1] Single Site History Fetcher**
  - _Action:_ Write function to fetch Daily Values (DV) '00060' for a single site (start date: 1950-01-01). Handle empty returns.
  - _Deliverable:_ Function `fetch_site_history(site_id)`.
- **[TICKET-2.2] Hyswap Stats Calculator**
  - _Action:_ Implement `hyswap` logic to calculate percentiles (5, 10, 25, 50, 75, 90, 95) for every day of the year (1-366) for that site.
  - _Deliverable:_ DataFrame with index (DOY) and columns (Percentiles).
- **[TICKET-2.3] State-Level Batch Processor**
  - _Action:_ Create a wrapper to:
    1. Get all sites for a specific State.
    2. Loop through sites (use multiprocessing).
    3. Combine all site stats into one State DataFrame.
    4. Save as Parquet.
  - _Deliverable:_ `generate_state_reference(state_code)` saving `VT_stats.parquet`.
- **[TICKET-2.4] S3 Uploader**
  - _Action:_ Integrate `boto3` to upload the generated Parquet files to `s3://.../reference_stats/`.
  - _Deliverable:_ Validated files appearing in S3.

## Epic 3: Pipeline B (Live Monitor)

**Goal:** Build the script that calculates real-time status.

- **[TICKET-3.1] Reference Loader**
  - _Action:_ Write function to download/read the State Parquet files from S3 into a memory-efficient Pandas DataFrame.
  - _Deliverable:_ `load_reference_data(state_code)` returning a fast lookup table.
- **[TICKET-3.2] Live Data Fetcher**
  - _Action:_ Use `dataretrieval.get_iv()` to fetch the latest instantaneous value for all sites in a State.
  - _Deliverable:_ DataFrame of current flows.
- **[TICKET-3.3] Percentile Calculation Logic**
  - _Action:_ Implement the formula $P = \frac{m}{n+1} \times 100$. For the live script, this means interpolating the current flow value against the pre-calculated percentile thresholds for Today's DOY.
  - _Deliverable:_ Final DataFrame with `site_id`, `flow`, `percentile`, `status_label`.
- **[TICKET-3.4] JSON Output & Upload**
  - _Action:_ Convert results to JSON. Upload to `s3://.../live_output/`.
  - _Deliverable:_ Script runs end-to-end and updates S3.

## Epic 4: Deployment & Automation

**Goal:** Automate the scripts to run without human intervention.

- **[TICKET-4.1] Master Orchestrator Script**
  - _Action:_ Create `main.py` with arguments (e.g., `--mode=slow` or `--mode=fast`).
- **[TICKET-4.2] Crontab Setup**
  - _Action:_ Configure cron.
  - _Schedule:_
    - `0 * * * * /usr/bin/python3 /home/ubuntu/project/main.py --mode=fast` (Hourly)
    - `0 0 1 * * /usr/bin/python3 /home/ubuntu/project/main.py --mode=slow` (Monthly)
  - _Deliverable:_ Verified execution via `syslog`.
```

PIPELINE A TESTS

# Step 1: Test fetching history for a single site

python scripts/test_pipeline_a.py --step 1

# Step 2: Test percentile calculation with hyswap

python scripts/test_pipeline_a.py --step 2

# Step 3: Test listing sites for a state (Vermont)

python scripts/test_pipeline_a.py --step 3

# Step 4: Test full single-site pipeline

python scripts/test_pipeline_a.py --step 4

# Step 5: Test S3 upload (requires AWS credentials)

python scripts/test_pipeline_a.py --step 5

PIPELINE B TESTS

# Step 1: Test loading your local VT_stats.parquet

python scripts/test_pipeline_b.py --step 1

# Step 2: Test fetching live IV data from USGS

python scripts/test_pipeline_b.py --step 2

# Step 3: Test percentile calculation (compares live to reference)

python scripts/test_pipeline_b.py --step 3

# Step 4: Full pipeline test

python scripts/test_pipeline_b.py --step 4

# Or run all

python scripts/test_pipeline_b.py --step all
