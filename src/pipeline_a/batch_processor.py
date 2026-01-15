"""
TICKET-2.3: State-Level Batch Processor

Processes all sites for a state and generates reference statistics.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm import tqdm

from src.utils.config import config
from src.utils.s3_client import S3Client
from .fetch_history import fetch_site_history, get_sites_for_state
from .calculate_stats import calculate_site_percentiles

logger = logging.getLogger(__name__)


def process_single_site(site_id: str) -> Optional[pd.DataFrame]:
    """
    Process a single site: fetch history and calculate percentiles.

    Args:
        site_id: USGS site identifier

    Returns:
        DataFrame with percentile statistics for the site, or None if processing fails.
    """
    # Fetch historical data
    history_df = fetch_site_history(site_id)
    if history_df is None:
        return None

    # Calculate percentiles
    stats_df = calculate_site_percentiles(history_df, site_id)
    return stats_df


def generate_state_reference(
    state_code: str,
    output_dir: Optional[Path] = None,
    upload_to_s3: bool = True
) -> Optional[pd.DataFrame]:
    """
    Generate reference statistics for all sites in a state.

    Args:
        state_code: Two-letter state code (e.g., "VT")
        output_dir: Local directory to save Parquet file (optional)
        upload_to_s3: Whether to upload results to S3

    Returns:
        Combined DataFrame with all site statistics, or None if no data.
    """
    logger.info(f"Starting reference generation for state: {state_code}")

    # Get all sites for the state
    site_ids = get_sites_for_state(state_code)
    if not site_ids:
        logger.warning(f"No sites found for state {state_code}")
        return None

    logger.info(f"Found {len(site_ids)} sites for state {state_code}")

    # Process sites in parallel
    results = []
    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        futures = {executor.submit(process_single_site, site_id): site_id for site_id in site_ids}

        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Processing {state_code}"):
            site_id = futures[future]
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                logger.error(f"Error processing site {site_id}: {e}")

    if not results:
        logger.warning(f"No valid results for state {state_code}")
        return None

    # Combine all results
    combined_df = pd.concat(results, ignore_index=True)
    logger.info(f"Generated statistics for {len(results)} sites in {state_code}")

    # Save to Parquet
    if output_dir:
        output_path = output_dir / f"{state_code}_stats.parquet"
        combined_df.to_parquet(output_path, index=False)
        logger.info(f"Saved to {output_path}")

    # Upload to S3
    if upload_to_s3:
        s3_client = S3Client()
        s3_client.upload_reference_stats(combined_df, state_code)

    return combined_df


def run_full_reference_generation(states: Optional[list[str]] = None) -> None:
    """
    Run reference generation for multiple states.

    Args:
        states: List of state codes to process. If None, processes all US states.
    """
    if states is None:
        # All US state codes
        states = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            "DC", "PR", "VI"  # Include DC, Puerto Rico, Virgin Islands
        ]

    for state in states:
        try:
            generate_state_reference(state)
        except Exception as e:
            logger.error(f"Failed to process state {state}: {e}")
