#!/usr/bin/env python3
"""
TICKET-4.1: Master Orchestrator Script

Main entry point for the USGS Flow Percentile Monitor.

Usage:
    python -m src.main --mode=slow   # Run Pipeline A (Reference Generator)
    python -m src.main --mode=fast   # Run Pipeline B (Live Monitor)
    python -m src.main --mode=slow --states=VT,NH,ME  # Process specific states
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from typing import Optional

import boto3

from src.pipeline_a import generate_state_reference
from src.pipeline_a.batch_processor import run_full_reference_generation
from src.pipeline_b.percentile_calc import run_live_monitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"flow_monitor_{datetime.now().strftime('%Y%m%d')}.log")
    ]
)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="USGS Flow Percentile Monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run monthly reference generation for all states
    python -m src.main --mode=slow

    # Run hourly live monitoring
    python -m src.main --mode=fast

    # Process specific states only
    python -m src.main --mode=slow --states=VT,NH,ME
        """
    )

    parser.add_argument(
        "--mode",
        required=True,
        choices=["slow", "fast"],
        help="Pipeline mode: 'slow' for reference generation, 'fast' for live monitoring"
    )

    parser.add_argument(
        "--states",
        type=str,
        default=None,
        help="Comma-separated list of state codes to process (default: all states)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without uploading to S3"
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (debug) logging"
    )

    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse states if provided
    states: Optional[list[str]] = None
    if args.states:
        states = [s.strip().upper() for s in args.states.split(",")]

    logger.info(f"Starting USGS Flow Monitor in {args.mode} mode")
    logger.info(f"States: {states or 'all'}")

    start_time = time.time()

    try:
        if args.mode == "slow":
            # Pipeline A: Reference Generator
            logger.info("Running Pipeline A: Reference Generator")
            run_full_reference_generation(states=states)
            logger.info("Pipeline A completed successfully")

        elif args.mode == "fast":
            # Pipeline B: Live Monitor
            logger.info("Running Pipeline B: Live Monitor")
            results = run_live_monitor(states=states)
            logger.info(f"Pipeline B completed. Processed {len(results)} sites.")

        # Calculate and publish execution time
        execution_time = time.time() - start_time
        logger.info(f"Pipeline execution time: {execution_time:.1f} seconds")

        try:
            cloudwatch = boto3.client("cloudwatch", region_name="us-east-1")
            cloudwatch.put_metric_data(
                Namespace="FGP/Pipeline",
                MetricData=[
                    {
                        "MetricName": "ExecutionTimeSeconds",
                        "Value": execution_time,
                        "Unit": "Seconds",
                        "Dimensions": [
                            {"Name": "Environment", "Value": "dev"},
                            {"Name": "Mode", "Value": args.mode},
                        ]
                    }
                ]
            )
            logger.info("Published execution time metric to CloudWatch")
        except Exception as cw_err:
            logger.warning(f"Failed to publish execution time metric: {cw_err}")

        return 0

    except Exception as e:
        logger.exception(f"Error running pipeline: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
