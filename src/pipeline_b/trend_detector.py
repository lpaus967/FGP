"""
Trend Detection Module for Rising/Falling Limb Analysis

Identifies whether streamflow at each site is on a rising limb (increasing),
falling limb (decreasing), or stable based on historical data from S3.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import numpy as np

from src.utils.config import config
from src.utils.s3_client import S3Client

logger = logging.getLogger(__name__)


@dataclass
class TrendResult:
    """Result of trend analysis for a single site."""
    trend: str                      # "rising" | "falling" | "stable" | "unknown"
    trend_rate: float               # % change per hour
    hours_since_peak: Optional[float]  # Hours since recent peak (if falling)
    data_points: int                # Number of readings used


def load_historical_flows(
    s3_client: S3Client,
    hours: int = 48
) -> dict[str, list[tuple[datetime, float]]]:
    """
    Load flow history from S3 snapshots for all sites.

    Args:
        s3_client: S3Client instance
        hours: Number of hours to look back

    Returns:
        Dict mapping site_id to list of (timestamp, flow) tuples sorted by time.
    """
    keys = s3_client.list_historical_snapshots(hours=hours)

    if not keys:
        logger.warning("No historical snapshots found in S3")
        return {}

    logger.info(f"Loading {len(keys)} historical snapshots for trend detection")

    # site_id -> list of (timestamp, flow)
    site_flows: dict[str, list[tuple[datetime, float]]] = {}

    for key in keys:
        snapshot = s3_client.download_historical_snapshot(key)
        if snapshot is None:
            continue

        # Parse timestamp from key (live_output/history/YYYY-MM-DDTHHMM.json)
        filename = key.split("/")[-1]
        try:
            timestamp_str = filename.replace(".json", "")
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H%M")
        except ValueError:
            continue

        # Extract flow values for each site
        sites = snapshot.get("sites", {})
        for site_id, site_data in sites.items():
            flow = site_data.get("flow")
            if flow is not None:
                if site_id not in site_flows:
                    site_flows[site_id] = []
                site_flows[site_id].append((timestamp, float(flow)))

    # Sort each site's flows by timestamp
    for site_id in site_flows:
        site_flows[site_id].sort(key=lambda x: x[0])

    logger.info(f"Loaded historical flows for {len(site_flows)} sites")
    return site_flows


def calculate_trend(
    flow_history: list[tuple[datetime, float]],
    rising_threshold: float = 5.0,
    falling_threshold: float = -5.0,
    min_data_points: int = 4
) -> TrendResult:
    """
    Calculate trend using linear regression on normalized flows.

    Algorithm:
    1. Normalize: (flow - median) / median * 100
    2. Linear regression: normalized_flow = slope * hours + intercept
    3. trend_rate = slope (% per hour)
    4. Find peak and calculate hours_since_peak
    5. Classify: rising (>threshold), falling (<threshold), stable (between)

    Args:
        flow_history: List of (timestamp, flow) tuples, sorted by time
        rising_threshold: % total change threshold for rising classification
        falling_threshold: % total change threshold for falling classification
        min_data_points: Minimum data points required for analysis

    Returns:
        TrendResult with trend classification and metrics.
    """
    data_points = len(flow_history)

    # Not enough data
    if data_points < min_data_points:
        return TrendResult(
            trend="unknown",
            trend_rate=0.0,
            hours_since_peak=None,
            data_points=data_points
        )

    # Extract timestamps and flows
    timestamps = [t for t, _ in flow_history]
    flows = np.array([f for _, f in flow_history])

    # Check for all identical flows
    if np.std(flows) < 1e-10:
        return TrendResult(
            trend="stable",
            trend_rate=0.0,
            hours_since_peak=None,
            data_points=data_points
        )

    # Calculate time in hours from first observation
    base_time = timestamps[0]
    hours_from_start = np.array([
        (t - base_time).total_seconds() / 3600.0 for t in timestamps
    ])

    # Total observation window
    total_hours = hours_from_start[-1] - hours_from_start[0]
    if total_hours < 0.1:  # Less than 6 minutes of data
        return TrendResult(
            trend="unknown",
            trend_rate=0.0,
            hours_since_peak=None,
            data_points=data_points
        )

    # Normalize flows: (flow - median) / median * 100
    median_flow = np.median(flows)
    if median_flow < 1e-10:  # Avoid division by zero
        return TrendResult(
            trend="unknown",
            trend_rate=0.0,
            hours_since_peak=None,
            data_points=data_points
        )

    normalized_flows = (flows - median_flow) / median_flow * 100

    # Linear regression: normalized = slope * hours + intercept
    # Using numpy's polyfit (degree 1)
    slope, intercept = np.polyfit(hours_from_start, normalized_flows, 1)

    trend_rate = float(slope)

    # Total change over observation period
    total_change = trend_rate * total_hours

    # Find peak and calculate hours_since_peak (for falling limb)
    peak_idx = np.argmax(flows)
    peak_time = timestamps[peak_idx]
    now = timestamps[-1]
    hours_since_peak = (now - peak_time).total_seconds() / 3600.0

    # Classify trend
    if total_change >= rising_threshold:
        trend = "rising"
        hours_since_peak_result = None  # Not relevant for rising
    elif total_change <= falling_threshold:
        trend = "falling"
        hours_since_peak_result = hours_since_peak if hours_since_peak > 0.5 else None
    else:
        trend = "stable"
        hours_since_peak_result = None

    return TrendResult(
        trend=trend,
        trend_rate=round(trend_rate, 3),
        hours_since_peak=round(hours_since_peak_result, 1) if hours_since_peak_result else None,
        data_points=data_points
    )


def detect_all_trends(
    current_flows: dict[str, float],
    s3_client: Optional[S3Client] = None,
    hours: int = None
) -> dict[str, TrendResult]:
    """
    Run trend detection for all sites.

    Args:
        current_flows: Dict mapping site_id to current flow value
        s3_client: S3Client instance (created if not provided)
        hours: Hours to look back (default: from config)

    Returns:
        Dict mapping site_id to TrendResult.
    """
    if hours is None:
        hours = config.trend.window_hours

    if s3_client is None:
        s3_client = S3Client()

    # Load historical flows from S3
    try:
        historical_flows = load_historical_flows(s3_client, hours=hours)
    except Exception as e:
        logger.warning(f"Failed to load historical flows for trend detection: {e}")
        return {}

    results = {}

    for site_id, current_flow in current_flows.items():
        # Get historical data for this site
        flow_history = historical_flows.get(site_id, [])

        # Add current flow as the latest data point
        now = datetime.utcnow()
        if current_flow is not None:
            flow_history.append((now, current_flow))

        # Calculate trend
        trend_result = calculate_trend(
            flow_history,
            rising_threshold=config.trend.rising_threshold,
            falling_threshold=config.trend.falling_threshold,
            min_data_points=config.trend.min_data_points
        )

        results[site_id] = trend_result

    logger.info(f"Calculated trends for {len(results)} sites")

    # Log summary statistics
    trends = [r.trend for r in results.values()]
    rising_count = trends.count("rising")
    falling_count = trends.count("falling")
    stable_count = trends.count("stable")
    unknown_count = trends.count("unknown")
    logger.info(
        f"Trend summary: {rising_count} rising, {falling_count} falling, "
        f"{stable_count} stable, {unknown_count} unknown"
    )

    return results
