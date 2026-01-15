"""
TICKET-3.1: Reference Loader

Loads pre-calculated reference statistics from S3 or local files.
"""

import logging
from pathlib import Path
from typing import Optional, Union

import pandas as pd

from src.utils.s3_client import S3Client

logger = logging.getLogger(__name__)

# Module-level cache for reference data
_reference_cache: dict[str, pd.DataFrame] = {}

# Local directory for reference files (set via set_local_reference_dir)
_local_reference_dir: Optional[Path] = None


def set_local_reference_dir(path: Union[str, Path]) -> None:
    """Set the local directory for reference files (for testing without S3)."""
    global _local_reference_dir
    _local_reference_dir = Path(path)
    logger.info(f"Using local reference directory: {_local_reference_dir}")


def load_reference_data(state_code: str, use_cache: bool = True) -> Optional[pd.DataFrame]:
    """
    Load reference statistics for a state from local files or S3.

    Args:
        state_code: Two-letter state code (e.g., "VT")
        use_cache: Whether to use in-memory cache

    Returns:
        DataFrame with reference statistics, or None if not found.
    """
    global _reference_cache

    # Check cache first
    if use_cache and state_code in _reference_cache:
        logger.debug(f"Using cached reference data for {state_code}")
        return _reference_cache[state_code]

    df = None

    # Try local file first if directory is set
    if _local_reference_dir is not None:
        local_path = _local_reference_dir / f"{state_code}_stats.parquet"
        if local_path.exists():
            logger.info(f"Loading reference data from local file: {local_path}")
            df = pd.read_parquet(local_path)

    # Fall back to S3
    if df is None:
        s3_client = S3Client()
        df = s3_client.download_reference_stats(state_code)

    if df is not None and use_cache:
        _reference_cache[state_code] = df
        logger.info(f"Cached reference data for {state_code}")

    return df


def load_all_reference_data(states: list[str]) -> dict[str, pd.DataFrame]:
    """
    Load reference statistics for multiple states.

    Args:
        states: List of state codes

    Returns:
        Dictionary mapping state codes to DataFrames.
    """
    result = {}
    for state in states:
        df = load_reference_data(state)
        if df is not None:
            result[state] = df
    return result


def get_site_reference(
    site_id: str,
    day_of_year: int,
    reference_df: pd.DataFrame
) -> Optional[pd.Series]:
    """
    Get the reference percentiles for a specific site and day of year.

    Args:
        site_id: USGS site identifier
        day_of_year: Day of year (1-366)
        reference_df: Reference DataFrame for the state

    Returns:
        Series with percentile thresholds, or None if not found.
    """
    try:
        site_data = reference_df[
            (reference_df["site_id"] == site_id) &
            (reference_df.index == day_of_year)
        ]

        if site_data.empty:
            return None

        return site_data.iloc[0]

    except Exception as e:
        logger.error(f"Error getting reference for site {site_id}, DOY {day_of_year}: {e}")
        return None


def clear_cache() -> None:
    """Clear the in-memory reference data cache."""
    global _reference_cache
    _reference_cache.clear()
    logger.info("Reference data cache cleared")
