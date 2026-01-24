#!/usr/bin/env python3
"""
Run Pipeline B with the new bulk-readings API.

Fetches current conditions, calculates percentiles against historical reference,
and outputs to local JSON and/or S3.

Usage:
    python scripts/run_pipeline_b.py                    # Full run with percentiles, upload to S3
    python scripts/run_pipeline_b.py --local-only       # Save to output/ only, no S3
    python scripts/run_pipeline_b.py --states VT,NH,MA  # Process specific states only
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.pipeline_b.percentile_calc import run_live_monitor
from src.pipeline_b.reference_loader import set_local_reference_dir

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent / "output"


def save_local_output(df, output_path: Path):
    """
    Save results to local JSON file in the standard format.

    Format matches s3_client.upload_live_output() for consistency.
    """
    import pandas as pd

    timestamp = datetime.now(timezone.utc)

    # Build optimized JSON structure keyed by site_id
    sites_dict = {}
    for _, row in df.iterrows():
        site_id = str(row.get("site_id", ""))
        if site_id:
            site_data = {
                "flow": row.get("flow") if pd.notna(row.get("flow")) else None,
                "gage_height": row.get("gage_height") if pd.notna(row.get("gage_height")) else None,
                "water_temp": row.get("water_temp") if pd.notna(row.get("water_temp")) else None,
                "percentile": row.get("percentile") if pd.notna(row.get("percentile")) else None,
                "flow_status": row.get("flow_status") if pd.notna(row.get("flow_status")) else None,
                "drought_status": row.get("drought_status") if pd.notna(row.get("drought_status")) else None,
                "flood_status": row.get("flood_status") if pd.notna(row.get("flood_status")) else None,
                "trend": row.get("trend") if pd.notna(row.get("trend")) else None,
                "trend_rate": round(row.get("trend_rate"), 2) if pd.notna(row.get("trend_rate")) else None,
                "hours_since_peak": round(row.get("hours_since_peak"), 1) if pd.notna(row.get("hours_since_peak")) else None,
                "water_temp_trend": row.get("water_temp_trend") if pd.notna(row.get("water_temp_trend")) else None,
            }
            # Include state if present
            if "state" in row and pd.notna(row.get("state")):
                site_data["state"] = row.get("state")

            sites_dict[site_id] = site_data

    output = {
        "generated_at": timestamp.isoformat().replace("+00:00", "Z"),
        "site_count": len(sites_dict),
        "sites": sites_dict
    }

    # Save to file
    output_path.parent.mkdir(exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    return output


def run_pipeline(states: list[str] = None, upload_to_s3: bool = True, local_only: bool = False):
    """
    Run the full pipeline with percentile calculations.

    Args:
        states: List of state codes to process (None = all states)
        upload_to_s3: Whether to upload to S3
        local_only: If True, only save locally (overrides upload_to_s3)
    """
    start_time = datetime.now()

    # Set local reference directory for loading reference data
    set_local_reference_dir(OUTPUT_DIR)

    # Determine S3 upload setting
    do_upload = upload_to_s3 and not local_only

    logger.info("=" * 60)
    logger.info("PIPELINE B: Live Monitor with Percentiles")
    logger.info("=" * 60)
    logger.info(f"States: {states or 'all'}")
    logger.info(f"Upload to S3: {do_upload}")

    # Run the full pipeline
    results_df = run_live_monitor(states=states, upload_to_s3=do_upload)

    if results_df.empty:
        logger.error("No results generated")
        return None

    elapsed = (datetime.now() - start_time).total_seconds()

    # Save locally
    output_path = OUTPUT_DIR / "current_status.json"
    output = save_local_output(results_df, output_path)

    # Print summary
    print("\n" + "=" * 60)
    print("PIPELINE B RESULTS SUMMARY")
    print("=" * 60)
    print(f"Generated at: {output['generated_at']}")
    print(f"Total time: {elapsed:.1f}s")
    print(f"Sites with percentiles: {output['site_count']}")

    # Status breakdown
    if not results_df.empty and "flow_status" in results_df.columns:
        status_counts = results_df["flow_status"].value_counts().to_dict()
        print(f"\nFlow status breakdown:")
        for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
            print(f"  {status}: {count}")

    # Trend breakdown
    if not results_df.empty and "trend" in results_df.columns:
        trend_counts = results_df["trend"].value_counts().to_dict()
        print(f"\nTrend breakdown:")
        for trend, count in sorted(trend_counts.items(), key=lambda x: -x[1]):
            print(f"  {trend}: {count}")

    # State breakdown
    if not results_df.empty and "state" in results_df.columns:
        state_counts = results_df["state"].value_counts()
        print(f"\nStates processed: {len(state_counts)}")
        print(f"Top states: {dict(state_counts.head(5))}")

    # Sample output
    print(f"\nSample sites (first 3):")
    for site_id, data in list(output["sites"].items())[:3]:
        print(f"  {site_id}: flow={data.get('flow')}, percentile={data.get('percentile')}, "
              f"status={data.get('flow_status')}, trend={data.get('trend')}")

    print(f"\nLocal output saved to: {output_path}")
    if do_upload:
        print("S3 output uploaded to: current_status.json")
    print("=" * 60)

    return results_df


def main():
    parser = argparse.ArgumentParser(
        description="Run Pipeline B with percentile calculations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Full run with S3 upload
    python scripts/run_pipeline_b.py

    # Local only (no S3)
    python scripts/run_pipeline_b.py --local-only

    # Specific states only
    python scripts/run_pipeline_b.py --states VT,NH,MA --local-only
        """
    )
    parser.add_argument(
        "--states",
        type=str,
        default=None,
        help="Comma-separated list of state codes (default: all states)"
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Only save locally, don't upload to S3"
    )
    parser.add_argument(
        "--no-s3",
        action="store_true",
        help="Alias for --local-only"
    )

    args = parser.parse_args()

    # Parse states
    states = None
    if args.states:
        states = [s.strip().upper() for s in args.states.split(",")]

    local_only = args.local_only or args.no_s3

    run_pipeline(states=states, local_only=local_only)


if __name__ == "__main__":
    main()
