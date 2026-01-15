#!/usr/bin/env python3
"""
Interactive testing script for Pipeline B components.

Run each step individually to verify functionality before running the full pipeline.

Usage:
    python scripts/test_pipeline_b.py --step 1  # Test reference loader (local)
    python scripts/test_pipeline_b.py --step 2  # Test live data fetch
    python scripts/test_pipeline_b.py --step 3  # Test percentile calculation
    python scripts/test_pipeline_b.py --step 4  # Test full live monitor
    python scripts/test_pipeline_b.py --step all  # Run all tests
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Set local reference directory for testing
LOCAL_REF_DIR = Path(__file__).parent.parent / "output"


def test_step_1_load_reference():
    """TICKET-3.1: Test loading reference data from local parquet."""
    print("\n" + "="*60)
    print("STEP 1: Testing Reference Loader (TICKET-3.1)")
    print("="*60)

    from src.pipeline_b.reference_loader import load_reference_data, set_local_reference_dir

    # Point to local output directory
    set_local_reference_dir(LOCAL_REF_DIR)

    print(f"\nLoading reference data for VT from: {LOCAL_REF_DIR}")

    df = load_reference_data("VT")

    if df is None:
        print("FAILED: Could not load reference data")
        print(f"Make sure {LOCAL_REF_DIR}/VT_stats.parquet exists")
        return False

    print(f"\nSUCCESS! Loaded reference data")
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"Unique sites: {df['site_id'].nunique()}")
    print(f"\nIndex type: {type(df.index)}")
    print(f"Index sample: {df.index[:5].tolist()}")
    print(f"\nSample data (first 5 rows):")
    print(df.head())

    return df


def test_step_2_fetch_live():
    """TICKET-3.2: Test fetching current instantaneous values."""
    print("\n" + "="*60)
    print("STEP 2: Testing Live Data Fetch (TICKET-3.2)")
    print("="*60)

    from src.pipeline_b.live_fetcher import fetch_state_current_conditions, extract_latest_values

    print("\nFetching current conditions for VT...")
    print("This may take a moment...")

    df = fetch_state_current_conditions("VT")

    if df is None:
        print("FAILED: Could not fetch current conditions")
        return False

    print(f"\nSUCCESS! Fetched current conditions")
    print(f"Raw shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")

    # Extract latest values
    latest = extract_latest_values(df)
    print(f"\nLatest values shape: {latest.shape}")
    print(f"\nSample (first 5 sites):")
    print(latest.head())

    return latest


def test_step_3_calculate_percentiles(reference_df=None, current_df=None):
    """TICKET-3.3: Test percentile calculation logic."""
    print("\n" + "="*60)
    print("STEP 3: Testing Percentile Calculation (TICKET-3.3)")
    print("="*60)

    from src.pipeline_b.reference_loader import load_reference_data, set_local_reference_dir
    from src.pipeline_b.live_fetcher import fetch_state_current_conditions, extract_latest_values
    from src.pipeline_b.percentile_calc import calculate_live_percentiles, interpolate_percentile, get_status_label

    # Load reference if not provided
    if reference_df is None:
        set_local_reference_dir(LOCAL_REF_DIR)
        reference_df = load_reference_data("VT")
        if reference_df is None:
            print("FAILED: Could not load reference data")
            return False

    # Fetch current if not provided
    if current_df is None:
        raw_df = fetch_state_current_conditions("VT")
        if raw_df is None:
            print("FAILED: Could not fetch current conditions")
            return False
        current_df = extract_latest_values(raw_df)

    print(f"\nReference data: {reference_df.shape}")
    print(f"Current data: {current_df.shape}")

    # Test interpolation function directly
    print("\n--- Testing interpolation function ---")
    import pandas as pd
    from datetime import datetime

    # Get today's DOY
    today_doy = datetime.now().timetuple().tm_yday
    today_month_day = datetime.now().strftime("%m-%d")
    print(f"Today's DOY: {today_doy} ({today_month_day})")

    # Get a sample site from reference data
    sample_site = reference_df['site_id'].iloc[0]
    print(f"Sample site: {sample_site}")

    # Check if reference uses month_day index
    print(f"Reference index type: {type(reference_df.index[0])}")

    # Try to get reference for this site and day
    if today_month_day in reference_df.index:
        site_ref = reference_df[
            (reference_df["site_id"] == sample_site) &
            (reference_df.index == today_month_day)
        ]
        if not site_ref.empty:
            print(f"\nReference for {sample_site} on {today_month_day}:")
            print(site_ref.iloc[0][['p05', 'p10', 'p25', 'p50', 'p75', 'p90', 'p95']])

    # Calculate percentiles
    print("\n--- Calculating live percentiles ---")
    results = calculate_live_percentiles(current_df, reference_df)

    if results.empty:
        print("WARNING: No results calculated")
        print("This might indicate a mismatch between site IDs or index format")
        return False

    print(f"\nSUCCESS! Calculated percentiles for {len(results)} sites")
    print(f"Columns: {list(results.columns)}")
    print(f"\nSample results:")
    print(results.head(10))

    # Show status distribution
    if 'status_label' in results.columns:
        print(f"\nStatus distribution:")
        print(results['status_label'].value_counts())

    return results


def test_step_4_full_monitor():
    """Test the complete live monitoring pipeline."""
    print("\n" + "="*60)
    print("STEP 4: Testing Full Live Monitor Pipeline")
    print("="*60)

    from src.pipeline_b.reference_loader import set_local_reference_dir
    from src.pipeline_b.percentile_calc import run_live_monitor

    # Set local reference directory
    set_local_reference_dir(LOCAL_REF_DIR)

    print("\nRunning full live monitor for VT (no S3 upload)...")

    # We need to modify run_live_monitor to skip S3 upload for testing
    # For now, let's just run steps 1-3 in sequence
    from src.pipeline_b.reference_loader import load_reference_data
    from src.pipeline_b.live_fetcher import fetch_state_current_conditions, extract_latest_values
    from src.pipeline_b.percentile_calc import calculate_live_percentiles

    # Load reference
    reference_df = load_reference_data("VT")
    if reference_df is None:
        print("FAILED: Could not load reference data")
        return False

    # Fetch current
    raw_df = fetch_state_current_conditions("VT")
    if raw_df is None:
        print("FAILED: Could not fetch current conditions")
        return False

    current_df = extract_latest_values(raw_df)

    # Calculate percentiles
    results = calculate_live_percentiles(current_df, reference_df)

    if results.empty:
        print("FAILED: No results generated")
        return False

    print(f"\nSUCCESS! Full pipeline completed")
    print(f"Processed {len(results)} sites")

    # Save locally for inspection
    output_path = LOCAL_REF_DIR / "live_results_test.json"
    results.to_json(output_path, orient="records", indent=2)
    print(f"\nSaved results to: {output_path}")

    print(f"\nSample output:")
    print(results.head(10))

    return results


def run_all_tests():
    """Run all test steps in sequence."""
    print("\n" + "#"*60)
    print("# RUNNING ALL PIPELINE B TESTS")
    print("#"*60)

    results = {}

    # Step 1
    ref_df = test_step_1_load_reference()
    results["step1_load_ref"] = ref_df is not None and ref_df is not False

    # Step 2
    current_df = test_step_2_fetch_live()
    results["step2_fetch_live"] = current_df is not None and current_df is not False

    # Step 3
    if results["step1_load_ref"] and results["step2_fetch_live"]:
        calc_results = test_step_3_calculate_percentiles(ref_df, current_df)
        results["step3_calc_pct"] = calc_results is not None and calc_results is not False
    else:
        results["step3_calc_pct"] = False

    # Step 4
    if results["step3_calc_pct"]:
        final_results = test_step_4_full_monitor()
        results["step4_full_pipeline"] = final_results is not None and final_results is not False
    else:
        results["step4_full_pipeline"] = False

    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    for step, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {step}: {status}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Test Pipeline B components")
    parser.add_argument(
        "--step",
        choices=["1", "2", "3", "4", "all"],
        default="all",
        help="Which test step to run"
    )
    args = parser.parse_args()

    if args.step == "1":
        test_step_1_load_reference()
    elif args.step == "2":
        test_step_2_fetch_live()
    elif args.step == "3":
        test_step_3_calculate_percentiles()
    elif args.step == "4":
        test_step_4_full_monitor()
    else:
        run_all_tests()


if __name__ == "__main__":
    main()
