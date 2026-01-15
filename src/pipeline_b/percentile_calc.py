"""
TICKET-3.3: Percentile Calculation Logic

Calculates real-time percentiles by comparing current flow to reference statistics.
"""

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

from src.utils.config import config
from src.utils.s3_client import S3Client
from .reference_loader import load_reference_data
from .live_fetcher import fetch_state_current_conditions, extract_latest_values

logger = logging.getLogger(__name__)

# Status labels based on percentile ranges
STATUS_LABELS = {
    (0, 5): "Much Below Normal",
    (5, 10): "Below Normal",
    (10, 25): "Below Normal",
    (25, 75): "Normal",
    (75, 90): "Above Normal",
    (90, 95): "Above Normal",
    (95, 100): "Much Above Normal"
}


def interpolate_percentile(
    current_flow: float,
    percentile_thresholds: pd.Series
) -> Optional[float]:
    """
    Interpolate the exact percentile for a given flow value.

    Uses linear interpolation between the pre-calculated percentile thresholds.

    Args:
        current_flow: Current discharge value
        percentile_thresholds: Series with percentile columns (p05, p10, p25, p50, p75, p90, p95)

    Returns:
        Interpolated percentile (0-100), or None if insufficient data.
    """
    # Map config percentiles to column names (5 -> 'p05', 10 -> 'p10', etc.)
    percentile_cols = [f"p{p:02d}" for p in config.usgs.percentiles]
    percentiles = np.array(config.usgs.percentiles)

    # Get threshold values, skipping NaN
    thresholds = []
    valid_percentiles = []
    for col, pct in zip(percentile_cols, percentiles):
        if col in percentile_thresholds and pd.notna(percentile_thresholds[col]):
            thresholds.append(percentile_thresholds[col])
            valid_percentiles.append(pct)

    if len(thresholds) < 2:
        # Not enough data points for interpolation
        return None

    thresholds = np.array(thresholds)
    valid_percentiles = np.array(valid_percentiles)

    # Handle edge cases
    if current_flow <= thresholds[0]:
        return float(valid_percentiles[0])
    if current_flow >= thresholds[-1]:
        return float(valid_percentiles[-1])

    # Linear interpolation
    return float(np.interp(current_flow, thresholds, valid_percentiles))


def get_status_label(percentile: float) -> str:
    """
    Get the status label for a given percentile.

    Args:
        percentile: Percentile value (0-100)

    Returns:
        Status label string.
    """
    for (low, high), label in STATUS_LABELS.items():
        if low <= percentile < high:
            return label
    return "Normal"


def calculate_live_percentiles(
    current_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    month_day: Optional[str] = None
) -> pd.DataFrame:
    """
    Calculate percentiles for current conditions.

    Args:
        current_df: DataFrame with current flow values (must have 'site_no' and 'discharge' columns)
        reference_df: DataFrame with reference statistics (must have 'site_id' and 'month_day' columns)
        month_day: Month-day string for comparison (e.g., '01-15'). Default: today.

    Returns:
        DataFrame with site_id, flow, percentile, and status_label.
    """
    if month_day is None:
        month_day = datetime.now().strftime("%m-%d")

    logger.info(f"Calculating percentiles for {month_day}")

    results = []

    for _, row in current_df.iterrows():
        site_id = row.get("site_no")
        current_flow = row.get("discharge")

        if site_id is None or current_flow is None or pd.isna(current_flow):
            continue

        # Get reference data for this site and month_day
        site_ref = reference_df[
            (reference_df["site_id"] == site_id) &
            (reference_df["month_day"] == month_day)
        ]

        if site_ref.empty:
            logger.debug(f"No reference data for site {site_id} on {month_day}")
            continue

        # Calculate percentile
        percentile = interpolate_percentile(current_flow, site_ref.iloc[0])

        if percentile is None:
            logger.debug(f"Could not calculate percentile for site {site_id} (insufficient reference data)")
            continue

        status = get_status_label(percentile)

        results.append({
            "site_id": site_id,
            "flow": current_flow,
            "percentile": round(percentile, 1),
            "status_label": status,
            "timestamp": datetime.utcnow().isoformat()
        })

    logger.info(f"Calculated percentiles for {len(results)} sites")
    return pd.DataFrame(results)


def run_live_monitor(states: Optional[list[str]] = None) -> pd.DataFrame:
    """
    Run the live monitoring pipeline for all specified states.

    Args:
        states: List of state codes to monitor. If None, monitors all states with reference data.

    Returns:
        Combined DataFrame with current conditions for all sites.
    """
    if states is None:
        states = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
        ]

    all_results = []

    for state in states:
        logger.info(f"Processing state: {state}")

        # Load reference data
        reference_df = load_reference_data(state)
        if reference_df is None:
            logger.warning(f"No reference data for state {state}")
            continue

        # Fetch current conditions
        current_df = fetch_state_current_conditions(state)
        if current_df is None:
            continue

        # Extract latest values
        latest_df = extract_latest_values(current_df)

        # Calculate percentiles
        results = calculate_live_percentiles(latest_df, reference_df)
        if not results.empty:
            results["state"] = state
            all_results.append(results)

    if not all_results:
        return pd.DataFrame()

    combined = pd.concat(all_results, ignore_index=True)

    # Upload to S3
    s3_client = S3Client()
    s3_client.upload_live_output(combined)

    return combined
