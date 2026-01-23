"""
Calculates real-time percentiles by comparing current flow to reference statistics.
Includes drought tier classification (USDM methodology) and flood status (NWS thresholds).
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.utils.config import config
from src.utils.s3_client import S3Client
from .reference_loader import load_reference_data, load_flood_thresholds
from .live_fetcher import fetch_state_current_conditions, extract_latest_values
from .trend_detector import detect_all_trends, TrendResult

logger = logging.getLogger(__name__)


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


def get_flow_status(percentile: float) -> str:
    """
    Get the basic flow status label for a given percentile.

    Args:
        percentile: Percentile value (0-100)

    Returns:
        Flow status string.
    """
    if percentile < 5:
        return "Much Below Normal"
    elif percentile < 10:
        return "Below Normal"
    elif percentile < 25:
        return "Below Normal"
    elif percentile < 75:
        return "Normal"
    elif percentile < 90:
        return "Above Normal"
    elif percentile < 95:
        return "Above Normal"
    else:
        return "Much Above Normal"


def get_drought_status(percentile: float) -> Optional[str]:
    """
    Get drought classification based on U.S. Drought Monitor methodology.

    Only returns a value if conditions indicate drought (below D0 threshold).

    Args:
        percentile: Percentile value (0-100)

    Returns:
        Drought classification string, or None if not in drought.
    """
    if percentile < config.drought.d4_threshold:
        return "D4 - Exceptional Drought"
    elif percentile < config.drought.d3_threshold:
        return "D3 - Extreme Drought"
    elif percentile < config.drought.d2_threshold:
        return "D2 - Severe Drought"
    elif percentile < config.drought.d1_threshold:
        return "D1 - Moderate Drought"
    elif percentile < config.drought.d0_threshold:
        return "D0 - Abnormally Dry"
    else:
        return None  # Not in drought


def get_flood_status(
    gage_height: Optional[float],
    flood_thresholds: Optional[pd.Series]
) -> Optional[str]:
    """
    Determine flood status based on current gage height and NWS thresholds.

    Args:
        gage_height: Current gage height in feet
        flood_thresholds: Series with action_stage, flood_stage, moderate_flood_stage, major_flood_stage

    Returns:
        Flood status string, or None if not at flood stage or no thresholds available.
    """
    if gage_height is None or pd.isna(gage_height):
        return None

    if flood_thresholds is None or flood_thresholds.empty:
        return None

    # Check from most severe to least severe
    major = flood_thresholds.get("major_flood_stage")
    if major is not None and not pd.isna(major) and gage_height >= major:
        return "Major Flood"

    moderate = flood_thresholds.get("moderate_flood_stage")
    if moderate is not None and not pd.isna(moderate) and gage_height >= moderate:
        return "Moderate Flood"

    minor = flood_thresholds.get("flood_stage")
    if minor is not None and not pd.isna(minor) and gage_height >= minor:
        return "Minor Flood"

    action = flood_thresholds.get("action_stage")
    if action is not None and not pd.isna(action) and gage_height >= action:
        return "Action Stage"

    return None  # Below action stage


def calculate_live_percentiles(
    current_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    flood_thresholds_df: Optional[pd.DataFrame] = None,
    month_day: Optional[str] = None,
    trends: Optional[dict[str, TrendResult]] = None
) -> pd.DataFrame:
    """
    Calculate percentiles and status for current conditions.

    Args:
        current_df: DataFrame with current values (site_no, discharge, gage_height columns)
        reference_df: DataFrame with percentile reference statistics
        flood_thresholds_df: DataFrame with NWS flood thresholds (optional)
        month_day: Month-day string for comparison (e.g., '01-15'). Default: today.
        trends: Dict mapping site_id to TrendResult (optional)

    Returns:
        DataFrame with site_id, flow, gage_height, percentile, flow_status, drought_status,
        flood_status, trend, trend_rate, hours_since_peak.
    """
    if month_day is None:
        month_day = datetime.now().strftime("%m-%d")

    logger.info(f"Calculating percentiles for {month_day}")

    # Index flood thresholds by site_id for fast lookup
    flood_lookup = {}
    if flood_thresholds_df is not None and not flood_thresholds_df.empty:
        for _, row in flood_thresholds_df.iterrows():
            site_id = str(row.get("site_id", ""))
            if site_id:
                flood_lookup[site_id] = row

    results = []

    for _, row in current_df.iterrows():
        site_id = row.get("site_no")
        current_flow = row.get("discharge")
        gage_height = row.get("gage_height")

        if site_id is None:
            continue

        # Initialize result
        result = {
            "site_id": site_id,
            "flow": current_flow if pd.notna(current_flow) else None,
            "gage_height": gage_height if pd.notna(gage_height) else None,
            "percentile": None,
            "flow_status": None,
            "drought_status": None,
            "flood_status": None,
            "trend": None,
            "trend_rate": None,
            "hours_since_peak": None,
            "timestamp": datetime.utcnow().isoformat()
        }

        # Add trend data if available
        if trends and site_id in trends:
            trend_result = trends[site_id]
            result["trend"] = trend_result.trend
            result["trend_rate"] = trend_result.trend_rate
            result["hours_since_peak"] = trend_result.hours_since_peak

        # Calculate percentile if we have flow data
        if current_flow is not None and not pd.isna(current_flow):
            # Get reference data for this site and month_day
            site_ref = reference_df[
                (reference_df["site_id"] == site_id) &
                (reference_df["month_day"] == month_day)
            ]

            if not site_ref.empty:
                percentile = interpolate_percentile(current_flow, site_ref.iloc[0])

                if percentile is not None:
                    result["percentile"] = round(percentile, 1)
                    result["flow_status"] = get_flow_status(percentile)
                    result["drought_status"] = get_drought_status(percentile)

        # Determine flood status if we have gage height and thresholds
        if gage_height is not None and not pd.isna(gage_height):
            site_flood_thresholds = flood_lookup.get(str(site_id))
            if site_flood_thresholds is not None:
                result["flood_status"] = get_flood_status(gage_height, site_flood_thresholds)

        # Only include if we have at least percentile or flood status
        if result["percentile"] is not None or result["flood_status"] is not None:
            results.append(result)

    logger.info(f"Calculated status for {len(results)} sites")
    return pd.DataFrame(results)


def run_live_monitor(
    states: Optional[list[str]] = None,
    upload_to_s3: bool = True
) -> pd.DataFrame:
    """
    Run the live monitoring pipeline for all specified states.

    Args:
        states: List of state codes to monitor. If None, monitors all states with reference data.
        upload_to_s3: Whether to upload results to S3.

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

    # Load flood thresholds once (shared across all states) - OPTIONAL
    flood_thresholds_df = load_flood_thresholds()
    if flood_thresholds_df is not None:
        logger.info(f"Loaded flood thresholds for {len(flood_thresholds_df)} sites")
    else:
        logger.info("Flood thresholds not available (optional) - flood_status will be null")

    # First pass: fetch current conditions for all states
    state_data = {}
    all_current_flows = {}

    for state in states:
        logger.info(f"Fetching current conditions for state: {state}")

        # Load reference data
        reference_df = load_reference_data(state)
        if reference_df is None:
            logger.warning(f"No reference data for state {state}")
            continue

        # Fetch current conditions (includes gage height)
        current_df = fetch_state_current_conditions(state, include_gage_height=True)
        if current_df is None:
            continue

        # Extract latest values
        latest_df = extract_latest_values(current_df)

        # Store for second pass
        state_data[state] = {
            "reference_df": reference_df,
            "latest_df": latest_df
        }

        # Build dict of current flows for trend detection
        for _, row in latest_df.iterrows():
            site_id = row.get("site_no")
            discharge = row.get("discharge")
            if site_id and discharge is not None and pd.notna(discharge):
                all_current_flows[str(site_id)] = float(discharge)

    # Detect trends for all sites at once (loads historical data from S3)
    logger.info(f"Detecting trends for {len(all_current_flows)} sites")
    try:
        trends = detect_all_trends(all_current_flows, hours=config.trend.window_hours)
    except Exception as e:
        logger.warning(f"Trend detection failed, continuing without trends: {e}")
        trends = {}

    # Second pass: calculate percentiles with trend data
    all_results = []

    for state, data in state_data.items():
        logger.info(f"Calculating percentiles for state: {state}")

        # Calculate percentiles and status with trends
        results = calculate_live_percentiles(
            data["latest_df"],
            data["reference_df"],
            flood_thresholds_df,
            trends=trends
        )

        if not results.empty:
            results["state"] = state
            all_results.append(results)

    if not all_results:
        return pd.DataFrame()

    combined = pd.concat(all_results, ignore_index=True)

    # Upload to S3
    if upload_to_s3:
        s3_client = S3Client()
        s3_client.upload_live_output(combined)

    return combined
