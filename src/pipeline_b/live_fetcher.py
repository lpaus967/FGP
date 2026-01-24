"""
Fetches current station readings from the bulk-readings API.
Uses cursor-based pagination to fetch all stations.
Includes both discharge (flow) and gage height for flood stage determination.
Also provides 48-hour historical readings for trend detection.
"""

import logging
import time
from typing import Optional
from datetime import datetime

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# API Configuration
API_BASE_URL = "https://f7ae5iy51g.execute-api.us-west-2.amazonaws.com/v1"
BULK_READINGS_ENDPOINT = f"{API_BASE_URL}/stations/bulk-readings"
DEFAULT_LIMIT = 100

# Retry configuration
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 1.0  # 1s, 2s, 4s between retries
RETRY_STATUS_CODES = [500, 502, 503, 504]


def _create_session() -> requests.Session:
    """Create a requests session with retry logic and connection pooling."""
    session = requests.Session()

    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=RETRY_STATUS_CODES,
        allowed_methods=["GET"],
        raise_on_status=False  # Don't raise, let us handle it
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def fetch_all_stations(limit: int = DEFAULT_LIMIT) -> list[dict]:
    """
    Fetch all stations using cursor-based pagination.

    Args:
        limit: Number of stations per page (default 250)

    Returns:
        List of all station objects with readings.
    """
    all_stations = []
    cursor = None
    page_count = 0

    # Use session for connection pooling and retries
    session = _create_session()

    while True:
        page_count += 1

        # Build request URL
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor

        try:
            start_time = time.time()
            response = session.get(BULK_READINGS_ENDPOINT, params=params, timeout=90)

            if response.status_code != 200:
                logger.error(f"Error fetching stations (page {page_count}): HTTP {response.status_code}")
                # If we have some data, return what we got
                if all_stations:
                    logger.warning(f"Returning {len(all_stations)} stations fetched before error")
                break

            data = response.json()
            elapsed = time.time() - start_time

        except requests.RequestException as e:
            logger.error(f"Error fetching stations (page {page_count}): {e}")
            # If we have some data, return what we got
            if all_stations:
                logger.warning(f"Returning {len(all_stations)} stations fetched before error")
            break

        stations = data.get("stations", [])
        all_stations.extend(stations)

        stations_in_page = data.get("stationsInPage", len(stations))
        logger.info(f"Fetched page {page_count}: {stations_in_page} stations ({elapsed:.1f}s)")

        # Check for next page
        cursor = data.get("nextCursor")
        if cursor is None:
            logger.info(f"Pagination complete. Total stations: {len(all_stations)}")
            break

    session.close()
    return all_stations


def fetch_current_conditions(site_ids: list[str] = None, include_gage_height: bool = True) -> Optional[pd.DataFrame]:
    """
    Fetch current conditions for all stations.

    Note: The new API fetches all stations at once via pagination.
    The site_ids parameter is kept for API compatibility but filtering
    happens after fetching all data.

    Args:
        site_ids: Optional list of site IDs to filter (applied post-fetch)
        include_gage_height: Whether to include gage height (always True with new API)

    Returns:
        DataFrame with current discharge and gage height values, or None if fetch fails.
    """
    try:
        stations = fetch_all_stations()

        if not stations:
            logger.warning("No stations returned from API")
            return None

        # Convert to DataFrame format expected by the pipeline
        records = []
        for station in stations:
            provider_id = station.get("providerId", "")

            # Skip if we have a site filter and this station isn't in it
            if site_ids and provider_id not in site_ids:
                continue

            # Get the latest reading from the readings array
            readings = station.get("readings", [])
            latest_reading = _get_latest_reading(readings)

            if latest_reading:
                records.append({
                    "site_no": provider_id,
                    "station_id": station.get("stationId", ""),
                    "name": station.get("name", ""),
                    "provider": station.get("provider", ""),
                    "discharge": latest_reading.get("waterFlowCFS"),
                    "gage_height": latest_reading.get("riverDepthFT"),
                    "timestamp": latest_reading.get("timestamp"),
                    "_readings": readings  # Keep for trend detection
                })

        if not records:
            logger.warning("No valid readings found in station data")
            return None

        df = pd.DataFrame(records)
        logger.info(f"Fetched current conditions for {len(df)} stations")
        return df

    except Exception as e:
        logger.error(f"Error fetching current conditions: {e}")
        return None


def fetch_state_current_conditions(state_code: str, include_gage_height: bool = True) -> Optional[pd.DataFrame]:
    """
    Fetch current conditions for all stations.

    Note: The new API doesn't filter by state - it returns all stations.
    State filtering should be done in post-processing if needed.
    This function is kept for API compatibility with the existing pipeline.

    Args:
        state_code: Two-letter state code (ignored - all stations fetched)
        include_gage_height: Whether to include gage height (always True with new API)

    Returns:
        DataFrame with current discharge and gage height values.
    """
    logger.info(f"Fetching current conditions (API returns all states, requested: {state_code})")
    return fetch_current_conditions(include_gage_height=include_gage_height)


def extract_latest_values(iv_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract the most recent value for each site from the fetched data.

    With the new API, the data is already in the correct format.
    This function validates and filters the data.

    Args:
        iv_df: DataFrame from fetch_current_conditions

    Returns:
        DataFrame with one row per site containing the latest discharge and gage height.
    """
    if iv_df is None or iv_df.empty:
        return pd.DataFrame()

    # Data is already per-site from the new API
    latest = iv_df.copy()

    # Ensure we have the required columns
    if "discharge" not in latest.columns:
        latest["discharge"] = None
    if "gage_height" not in latest.columns:
        latest["gage_height"] = None

    # Filter out invalid discharge values
    # Keep rows where discharge is valid OR gage_height is valid (for flood monitoring)
    valid_discharge = (latest["discharge"].notna()) & (latest["discharge"] > 0)
    valid_gage = (latest["gage_height"].notna()) & (latest["gage_height"] > -100)
    latest = latest[valid_discharge | valid_gage].copy()

    # Replace invalid discharge with None
    if "discharge" in latest.columns:
        latest.loc[latest["discharge"] <= 0, "discharge"] = None

    logger.info(f"Extracted {len(latest)} valid readings")

    return latest


def get_readings_for_trends(iv_df: pd.DataFrame) -> dict[str, list[tuple[datetime, float]]]:
    """
    Extract historical readings from the fetched data for trend detection.

    The new API returns 48-hour readings for each station, which can be used
    directly for trend detection without looking up S3 history.

    Args:
        iv_df: DataFrame from fetch_current_conditions (must have _readings column)

    Returns:
        Dict mapping site_id to list of (timestamp, flow) tuples sorted by time.
    """
    site_flows: dict[str, list[tuple[datetime, float]]] = {}

    if iv_df is None or iv_df.empty:
        return site_flows

    if "_readings" not in iv_df.columns:
        logger.warning("No _readings column in DataFrame - trend detection may use S3 fallback")
        return site_flows

    for _, row in iv_df.iterrows():
        site_id = row.get("site_no")
        readings = row.get("_readings", [])

        if not site_id or not readings:
            continue

        flow_history = []
        for reading in readings:
            discharge = reading.get("waterFlowCFS")
            timestamp_str = reading.get("timestamp")

            if discharge is None or timestamp_str is None:
                continue

            try:
                # Parse ISO timestamp
                if timestamp_str.endswith("Z"):
                    timestamp_str = timestamp_str[:-1] + "+00:00"
                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                # Convert to naive UTC for consistency with existing trend_detector
                timestamp = timestamp.replace(tzinfo=None)
                flow_history.append((timestamp, float(discharge)))
            except (ValueError, TypeError) as e:
                logger.debug(f"Error parsing reading timestamp for {site_id}: {e}")
                continue

        if flow_history:
            # Sort by timestamp
            flow_history.sort(key=lambda x: x[0])
            site_flows[str(site_id)] = flow_history

    logger.info(f"Extracted historical readings for {len(site_flows)} sites for trend detection")
    return site_flows


def _get_latest_reading(readings: list[dict]) -> Optional[dict]:
    """
    Get the most recent reading from a list of readings.

    Args:
        readings: List of reading objects with timestamp field

    Returns:
        The most recent reading, or None if no valid readings.
    """
    if not readings:
        return None

    # Find the reading with the latest timestamp
    latest = None
    latest_time = None

    for reading in readings:
        timestamp_str = reading.get("timestamp")
        if not timestamp_str:
            continue

        try:
            if timestamp_str.endswith("Z"):
                timestamp_str = timestamp_str[:-1] + "+00:00"
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

            if latest_time is None or timestamp > latest_time:
                latest_time = timestamp
                latest = reading
        except (ValueError, TypeError):
            continue

    return latest
