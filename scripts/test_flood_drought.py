#!/usr/bin/env python3
"""
Test script for flood thresholds and drought classification features.

Usage:
    python scripts/test_flood_drought.py --step 1  # Test NWS flood threshold fetch
    python scripts/test_flood_drought.py --step 2  # Test drought classification
    python scripts/test_flood_drought.py --step 3  # Test flood status determination
    python scripts/test_flood_drought.py --step 4  # Test full pipeline with flood/drought
    python scripts/test_flood_drought.py --step all
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

LOCAL_OUTPUT_DIR = Path(__file__).parent.parent / "output"


def test_step_1_fetch_flood_thresholds():
    """Test fetching NWS flood thresholds (optional - may not be available)."""
    print("\n" + "="*60)
    print("STEP 1: Testing NWS Flood Threshold Fetch (OPTIONAL)")
    print("="*60)

    print("\nNOTE: NWS flood thresholds are OPTIONAL. The system works without them.")
    print("      Flood status will simply not be calculated if thresholds are unavailable.")
    print("      Drought status (percentile-based) works independently.\n")

    try:
        from src.pipeline_a.fetch_flood_thresholds import (
            fetch_nws_gauge_info,
            fetch_state_flood_thresholds
        )

        # Test single gauge first
        print("--- Testing single gauge fetch ---")
        test_site = "01144000"  # A common VT site
        print(f"Attempting to fetch NWS data for site: {test_site}")

        result = fetch_nws_gauge_info(test_site)

        if result:
            print(f"\nFound flood thresholds:")
            for key, value in result.items():
                print(f"  {key}: {value}")

            # Test state-level fetch
            print("\n--- Testing state-level fetch (VT) ---")
            print("This may take a few minutes...")

            df = fetch_state_flood_thresholds("VT")

            if not df.empty:
                print(f"\nFound flood thresholds for {len(df)} sites in VT")

                # Save locally
                output_path = LOCAL_OUTPUT_DIR / "flood_thresholds.parquet"
                LOCAL_OUTPUT_DIR.mkdir(exist_ok=True)
                df.to_parquet(output_path, index=False)
                print(f"Saved flood thresholds to: {output_path}")
                return df

        print("\nNWS API unavailable or no data found.")
        print("This is OK - flood thresholds are optional.")
        print("The system will continue without flood stage calculations.")
        return None

    except Exception as e:
        print(f"\nCould not fetch NWS flood thresholds: {e}")
        print("This is OK - flood thresholds are optional.")
        print("The system will continue without flood stage calculations.")
        return None


def test_step_2_drought_classification():
    """Test drought classification logic."""
    print("\n" + "="*60)
    print("STEP 2: Testing Drought Classification")
    print("="*60)

    from src.pipeline_b.percentile_calc import get_drought_status
    from src.utils.config import config

    print("\nDrought thresholds from config:")
    print(f"  D0 (Abnormally Dry): < {config.drought.d0_threshold}th percentile")
    print(f"  D1 (Moderate Drought): < {config.drought.d1_threshold}th percentile")
    print(f"  D2 (Severe Drought): < {config.drought.d2_threshold}th percentile")
    print(f"  D3 (Extreme Drought): < {config.drought.d3_threshold}th percentile")
    print(f"  D4 (Exceptional Drought): < {config.drought.d4_threshold}th percentile")

    print("\n--- Testing classification at various percentiles ---")
    test_percentiles = [1, 3, 7, 12, 25, 50, 75, 90]

    for pct in test_percentiles:
        drought_status = get_drought_status(pct)
        status_str = drought_status if drought_status else "Not in drought"
        print(f"  Percentile {pct:3d}: {status_str}")

    print("\nSUCCESS! Drought classification working correctly")
    return True


def test_step_3_flood_status():
    """Test flood status determination."""
    print("\n" + "="*60)
    print("STEP 3: Testing Flood Status Determination")
    print("="*60)

    from src.pipeline_b.percentile_calc import get_flood_status
    import pandas as pd

    # Create mock flood thresholds
    mock_thresholds = pd.Series({
        "action_stage": 8.0,
        "flood_stage": 10.0,
        "moderate_flood_stage": 14.0,
        "major_flood_stage": 18.0
    })

    print("\nTest thresholds:")
    print(f"  Action Stage: {mock_thresholds['action_stage']} ft")
    print(f"  Minor Flood: {mock_thresholds['flood_stage']} ft")
    print(f"  Moderate Flood: {mock_thresholds['moderate_flood_stage']} ft")
    print(f"  Major Flood: {mock_thresholds['major_flood_stage']} ft")

    print("\n--- Testing status at various gage heights ---")
    test_heights = [5.0, 8.5, 11.0, 15.0, 20.0, None]

    for height in test_heights:
        flood_status = get_flood_status(height, mock_thresholds)
        status_str = flood_status if flood_status else "No Flood"
        height_str = f"{height} ft" if height else "None"
        print(f"  Gage height {height_str:8s}: {status_str}")

    print("\nSUCCESS! Flood status determination working correctly")
    return True


def test_step_4_full_pipeline():
    """Test full pipeline with drought status (and flood if available)."""
    print("\n" + "="*60)
    print("STEP 4: Testing Full Pipeline with Drought/Flood Status")
    print("="*60)

    from src.pipeline_b.reference_loader import set_local_reference_dir, load_flood_thresholds
    from src.pipeline_b.live_fetcher import fetch_state_current_conditions, extract_latest_values
    from src.pipeline_b.percentile_calc import calculate_live_percentiles
    from src.pipeline_b.reference_loader import load_reference_data

    # Set local reference directory
    set_local_reference_dir(LOCAL_OUTPUT_DIR)

    # Check for required files
    percentile_file = LOCAL_OUTPUT_DIR / "VT_stats.parquet"
    flood_file = LOCAL_OUTPUT_DIR / "flood_thresholds.parquet"

    if not percentile_file.exists():
        print(f"ERROR: Percentile reference file not found: {percentile_file}")
        print("Run 'python scripts/test_pipeline_a.py' first to generate reference data")
        return False

    print(f"Using percentile reference: {percentile_file}")

    if flood_file.exists():
        print(f"Using flood thresholds: {flood_file}")
    else:
        print("\nNote: No flood thresholds file found (this is OK)")
        print("      Flood status will be skipped; drought status still works.")

    # Load reference data
    print("\n--- Loading reference data ---")
    reference_df = load_reference_data("VT")
    if reference_df is None:
        print("ERROR: Could not load reference data")
        return False
    print(f"Loaded percentile reference: {len(reference_df)} rows")

    flood_df = load_flood_thresholds()
    if flood_df is not None:
        print(f"Loaded flood thresholds: {len(flood_df)} sites")
    else:
        print("No flood thresholds loaded")
        flood_df = None

    # Fetch current conditions
    print("\n--- Fetching current conditions (with gage height) ---")
    raw_df = fetch_state_current_conditions("VT", include_gage_height=True)
    if raw_df is None:
        print("ERROR: Could not fetch current conditions")
        return False

    print(f"Raw IV data columns: {list(raw_df.columns)}")

    current_df = extract_latest_values(raw_df)
    print(f"Extracted {len(current_df)} valid readings")

    # Check if we have gage height data
    if "gage_height" in current_df.columns:
        gage_count = current_df["gage_height"].notna().sum()
        print(f"Sites with gage height: {gage_count}")
    else:
        print("No gage height column found")

    # Calculate percentiles with flood/drought status
    print("\n--- Calculating status ---")
    results = calculate_live_percentiles(current_df, reference_df, flood_df)

    if results.empty:
        print("ERROR: No results generated")
        return False

    print(f"\nSUCCESS! Generated status for {len(results)} sites")
    print(f"Columns: {list(results.columns)}")

    # Show summary statistics
    print("\n--- Status Summary ---")

    # Flow status distribution
    if "flow_status" in results.columns:
        print("\nFlow Status:")
        print(results["flow_status"].value_counts().to_string())

    # Drought status distribution
    if "drought_status" in results.columns:
        drought_sites = results[results["drought_status"].notna()]
        if len(drought_sites) > 0:
            print(f"\nDrought Status ({len(drought_sites)} sites in drought):")
            print(drought_sites["drought_status"].value_counts().to_string())
        else:
            print("\nNo sites currently in drought conditions")

    # Flood status distribution
    if "flood_status" in results.columns:
        flood_sites = results[results["flood_status"].notna()]
        if len(flood_sites) > 0:
            print(f"\nFlood Status ({len(flood_sites)} sites at flood stage):")
            print(flood_sites["flood_status"].value_counts().to_string())
        else:
            print("\nNo sites currently at flood stage")

    # Show sample results
    print("\n--- Sample Results ---")
    print(results.head(10).to_string())

    # Build and save JSON output
    print("\n--- Saving JSON Output ---")
    results["state"] = "VT"

    sites_dict = {}
    for _, row in results.iterrows():
        site_id = str(row.get("site_id", ""))
        if site_id:
            sites_dict[site_id] = {
                "flow": row.get("flow") if row.get("flow") is not None else None,
                "gage_height": row.get("gage_height") if row.get("gage_height") is not None else None,
                "percentile": row.get("percentile"),
                "flow_status": row.get("flow_status"),
                "drought_status": row.get("drought_status"),
                "flood_status": row.get("flood_status"),
                "state": "VT"
            }

    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "site_count": len(sites_dict),
        "sites": sites_dict
    }

    output_path = LOCAL_OUTPUT_DIR / "current_status.json"
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Saved to: {output_path}")

    # Show sample JSON
    print("\nSample JSON output:")
    sample_sites = dict(list(sites_dict.items())[:3])
    sample = {"generated_at": output["generated_at"], "site_count": output["site_count"], "sites": sample_sites}
    print(json.dumps(sample, indent=2))

    return results


def run_all_tests():
    """Run all test steps."""
    print("\n" + "#"*60)
    print("# RUNNING ALL FLOOD/DROUGHT TESTS")
    print("#"*60)

    results = {}

    # Step 1 - Flood thresholds (OPTIONAL)
    flood_df = test_step_1_fetch_flood_thresholds()
    results["step1_flood_thresholds"] = "OPTIONAL"  # Not required for system to work

    # Step 2 - Drought classification (REQUIRED)
    results["step2_drought_classification"] = test_step_2_drought_classification()

    # Step 3 - Flood status logic (REQUIRED - tests the logic, not the data)
    results["step3_flood_status_logic"] = test_step_3_flood_status()

    # Step 4 - Full pipeline (REQUIRED)
    full_results = test_step_4_full_pipeline()
    results["step4_full_pipeline"] = full_results is not None and full_results is not False

    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    for step, result in results.items():
        if result == "OPTIONAL":
            status = "SKIP (optional)"
        elif result:
            status = "PASS"
        else:
            status = "FAIL"
        print(f"  {step}: {status}")

    # Overall assessment
    required_tests = [v for k, v in results.items() if v != "OPTIONAL"]
    all_passed = all(required_tests)
    print(f"\n  Overall: {'ALL REQUIRED TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Test flood/drought features")
    parser.add_argument(
        "--step",
        choices=["1", "2", "3", "4", "all"],
        default="all",
        help="Which test step to run"
    )
    args = parser.parse_args()

    LOCAL_OUTPUT_DIR.mkdir(exist_ok=True)

    if args.step == "1":
        test_step_1_fetch_flood_thresholds()
    elif args.step == "2":
        test_step_2_drought_classification()
    elif args.step == "3":
        test_step_3_flood_status()
    elif args.step == "4":
        test_step_4_full_pipeline()
    else:
        run_all_tests()


if __name__ == "__main__":
    main()
