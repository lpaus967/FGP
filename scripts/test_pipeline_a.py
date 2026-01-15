#!/usr/bin/env python3
"""
Interactive testing script for Pipeline A components.

Run each step individually to verify functionality before running the full pipeline.

Usage:
    python scripts/test_pipeline_a.py --step 1  # Test single site fetch
    python scripts/test_pipeline_a.py --step 2  # Test percentile calculation
    python scripts/test_pipeline_a.py --step 3  # Test state site listing
    python scripts/test_pipeline_a.py --step 4  # Test single site end-to-end
    python scripts/test_pipeline_a.py --step 5  # Test S3 upload (requires AWS)
    python scripts/test_pipeline_a.py --step all  # Run all tests
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_step_1_fetch_history():
    """TICKET-2.1: Test fetching historical data for a single site."""
    print("\n" + "="*60)
    print("STEP 1: Testing Single Site History Fetch (TICKET-2.1)")
    print("="*60)

    from src.pipeline_a.fetch_history import fetch_site_history

    # Test with a well-known USGS site (Merrimack River at Lowell, MA)
    test_site = "01100000"
    print(f"\nFetching data for site: {test_site}")
    print("This may take a moment...")

    df = fetch_site_history(test_site)

    if df is None:
        print("FAILED: No data returned")
        return False

    print(f"\nSUCCESS! Retrieved {len(df)} daily records")
    print(f"Date range: {df.index.min()} to {df.index.max()}")
    print(f"Columns: {list(df.columns)}")
    print(f"\nFirst 5 rows:")
    print(df.head())
    print(f"\nBasic stats:")
    print(df.describe())

    return df


def test_step_2_calculate_stats(df=None):
    """TICKET-2.2: Test percentile calculation using hyswap."""
    print("\n" + "="*60)
    print("STEP 2: Testing Percentile Calculation (TICKET-2.2)")
    print("="*60)

    # If no df provided, fetch one
    if df is None:
        from src.pipeline_a.fetch_history import fetch_site_history
        print("\nNo data provided, fetching test site first...")
        df = fetch_site_history("01100000")
        if df is None:
            print("FAILED: Could not fetch test data")
            return False

    from src.pipeline_a.calculate_stats import calculate_site_percentiles

    print(f"\nCalculating percentiles for {len(df)} daily records...")

    stats_df = calculate_site_percentiles(df, site_id="01100000")

    if stats_df is None:
        print("FAILED: Percentile calculation returned None")
        print("\nTIP: Check if hyswap API has changed. You may need to adjust")
        print("     calculate_stats.py to match the actual hyswap interface.")
        return False

    print(f"\nSUCCESS! Generated percentile stats")
    print(f"Shape: {stats_df.shape}")
    print(f"Columns: {list(stats_df.columns)}")
    print(f"\nSample (DOY 1-5):")
    print(stats_df.head())

    return stats_df


def test_step_3_list_state_sites():
    """Test fetching all sites for a state."""
    print("\n" + "="*60)
    print("STEP 3: Testing State Site Listing")
    print("="*60)

    from src.pipeline_a.fetch_history import get_sites_for_state

    # Use a small state for testing
    test_state = "VT"  # Vermont - relatively few sites
    print(f"\nFetching active streamflow sites for state: {test_state}")

    sites = get_sites_for_state(test_state)

    if not sites:
        print("FAILED: No sites returned")
        return False

    print(f"\nSUCCESS! Found {len(sites)} active sites in {test_state}")
    print(f"First 10 site IDs: {sites[:10]}")

    return sites


def test_step_4_single_site_pipeline():
    """Test the complete single-site processing pipeline."""
    print("\n" + "="*60)
    print("STEP 4: Testing Single Site End-to-End Pipeline")
    print("="*60)

    from src.pipeline_a.batch_processor import process_single_site

    test_site = "01100000"
    print(f"\nProcessing site {test_site} through full pipeline...")

    result = process_single_site(test_site)

    if result is None:
        print("FAILED: Pipeline returned None")
        return False

    print(f"\nSUCCESS! Full pipeline completed")
    print(f"Result shape: {result.shape}")
    print(f"Columns: {list(result.columns)}")
    print(f"\nSample output:")
    print(result.head(10))

    return result


def test_step_5_s3_upload(df=None):
    """TICKET-2.4: Test S3 upload (requires AWS credentials)."""
    print("\n" + "="*60)
    print("STEP 5: Testing S3 Upload (TICKET-2.4)")
    print("="*60)

    import pandas as pd
    from src.utils.s3_client import S3Client
    from src.utils.config import config

    print(f"\nTarget bucket: {config.s3.bucket_name}")
    print(f"Reference prefix: {config.s3.reference_prefix}")

    # Create test data if not provided
    if df is None:
        print("\nCreating test DataFrame...")
        df = pd.DataFrame({
            "site_id": ["TEST001", "TEST001", "TEST002", "TEST002"],
            "day_of_year": [1, 2, 1, 2],
            "p5": [10.0, 11.0, 20.0, 21.0],
            "p50": [50.0, 51.0, 60.0, 61.0],
            "p95": [100.0, 101.0, 110.0, 111.0]
        })

    print(f"Test data shape: {df.shape}")

    # Check AWS credentials
    try:
        import boto3
        sts = boto3.client("sts")
        identity = sts.get_caller_identity()
        print(f"\nAWS Identity: {identity.get('Arn', 'Unknown')}")
    except Exception as e:
        print(f"\nWARNING: Could not verify AWS credentials: {e}")
        print("Make sure AWS credentials are configured.")
        return False

    # Attempt upload
    s3_client = S3Client()
    test_state = "TEST"

    print(f"\nUploading to s3://{config.s3.bucket_name}/{config.s3.reference_prefix}/state={test_state}/...")

    success = s3_client.upload_reference_stats(df, test_state)

    if success:
        print("SUCCESS! Upload completed")

        # Try to read it back
        print("\nVerifying by downloading...")
        downloaded = s3_client.download_reference_stats(test_state)
        if downloaded is not None:
            print(f"Downloaded {len(downloaded)} rows - verification passed!")
        else:
            print("WARNING: Could not verify download")
    else:
        print("FAILED: Upload unsuccessful")

    return success


def run_all_tests():
    """Run all test steps in sequence."""
    print("\n" + "#"*60)
    print("# RUNNING ALL PIPELINE A TESTS")
    print("#"*60)

    results = {}

    # Step 1
    df = test_step_1_fetch_history()
    results["step1_fetch"] = df is not None and df is not False

    # Step 2
    if results["step1_fetch"]:
        stats = test_step_2_calculate_stats(df)
        results["step2_stats"] = stats is not None and stats is not False
    else:
        results["step2_stats"] = False

    # Step 3
    sites = test_step_3_list_state_sites()
    results["step3_sites"] = sites is not None and sites is not False

    # Step 4
    result = test_step_4_single_site_pipeline()
    results["step4_pipeline"] = result is not None and result is not False

    # Step 5 (optional - requires AWS)
    print("\n" + "-"*60)
    response = input("Run S3 upload test? (requires AWS credentials) [y/N]: ")
    if response.lower() == "y":
        results["step5_s3"] = test_step_5_s3_upload()
    else:
        results["step5_s3"] = "skipped"

    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    for step, passed in results.items():
        status = "PASS" if passed is True else ("SKIP" if passed == "skipped" else "FAIL")
        print(f"  {step}: {status}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Test Pipeline A components")
    parser.add_argument(
        "--step",
        choices=["1", "2", "3", "4", "5", "all"],
        default="all",
        help="Which test step to run"
    )
    args = parser.parse_args()

    if args.step == "1":
        test_step_1_fetch_history()
    elif args.step == "2":
        test_step_2_calculate_stats()
    elif args.step == "3":
        test_step_3_list_state_sites()
    elif args.step == "4":
        test_step_4_single_site_pipeline()
    elif args.step == "5":
        test_step_5_s3_upload()
    else:
        run_all_tests()


if __name__ == "__main__":
    main()
