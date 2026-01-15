"""
NWS Flood Threshold Fetcher

Fetches flood stage thresholds from NWS AHPS (Advanced Hydrologic Prediction Service).
These thresholds define when a site reaches Action Stage, Minor Flood, Moderate Flood, and Major Flood.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm

from src.utils.config import config

logger = logging.getLogger(__name__)

# NWS API endpoints
NWS_GAUGES_API = "https://api.water.weather.gov/v1/gauges"


def fetch_nws_gauge_info(nws_id: str) -> Optional[dict]:
    """
    Fetch flood stage thresholds for a single NWS gauge.

    Args:
        nws_id: NWS gauge identifier (usually same as USGS site_id)

    Returns:
        Dictionary with flood thresholds, or None if not available.
    """
    url = f"{NWS_GAUGES_API}/{nws_id}"

    try:
        response = requests.get(url, timeout=30)

        if response.status_code == 404:
            # Gauge not in NWS system
            return None

        response.raise_for_status()
        data = response.json()

        # Extract flood categories
        flood_cats = data.get("floodCategories", {})

        return {
            "nws_id": nws_id,
            "site_id": data.get("usgsId", nws_id),
            "name": data.get("name", ""),
            "state": data.get("state", ""),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "action_stage": flood_cats.get("action"),
            "flood_stage": flood_cats.get("minor"),
            "moderate_flood_stage": flood_cats.get("moderate"),
            "major_flood_stage": flood_cats.get("major"),
            "unit": data.get("stageUnits", "ft"),
        }

    except requests.exceptions.Timeout:
        logger.warning(f"Timeout fetching NWS data for {nws_id}")
        return None
    except requests.exceptions.RequestException as e:
        logger.debug(f"Error fetching NWS data for {nws_id}: {e}")
        return None


def fetch_all_nws_gauges() -> pd.DataFrame:
    """
    Fetch the list of all NWS gauges with flood information.

    Returns:
        DataFrame with all NWS gauges.
    """
    url = NWS_GAUGES_API
    all_gauges = []
    page = 1

    logger.info("Fetching NWS gauge list...")

    while True:
        try:
            response = requests.get(
                url,
                params={"page": page, "per_page": 500},
                timeout=60
            )
            response.raise_for_status()
            data = response.json()

            gauges = data.get("gauges", [])
            if not gauges:
                break

            all_gauges.extend(gauges)
            logger.info(f"Fetched page {page}: {len(gauges)} gauges (total: {len(all_gauges)})")

            # Check if there are more pages
            if len(gauges) < 500:
                break

            page += 1
            time.sleep(0.1)  # Rate limiting

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching NWS gauge list: {e}")
            break

    if not all_gauges:
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame(all_gauges)
    logger.info(f"Total NWS gauges found: {len(df)}")

    return df


def fetch_flood_thresholds_for_sites(site_ids: list[str], max_workers: int = 5) -> pd.DataFrame:
    """
    Fetch flood thresholds for a list of USGS site IDs.

    Args:
        site_ids: List of USGS site identifiers
        max_workers: Number of parallel workers (keep low to respect NWS rate limits)

    Returns:
        DataFrame with flood thresholds for sites that have them.
    """
    logger.info(f"Fetching flood thresholds for {len(site_ids)} sites...")

    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_nws_gauge_info, site_id): site_id for site_id in site_ids}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching flood thresholds"):
            site_id = futures[future]
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                logger.error(f"Error fetching flood threshold for {site_id}: {e}")

    if not results:
        logger.warning("No flood thresholds found")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    logger.info(f"Found flood thresholds for {len(df)} sites")

    return df


def fetch_state_flood_thresholds(state_code: str) -> pd.DataFrame:
    """
    Fetch flood thresholds for all sites in a state.

    Args:
        state_code: Two-letter state code

    Returns:
        DataFrame with flood thresholds.
    """
    from src.pipeline_a.fetch_history import get_sites_for_state

    # Get all USGS sites for the state
    site_ids = get_sites_for_state(state_code)

    if not site_ids:
        logger.warning(f"No sites found for state {state_code}")
        return pd.DataFrame()

    # Fetch flood thresholds
    df = fetch_flood_thresholds_for_sites(site_ids)

    if not df.empty:
        df["state"] = state_code

    return df


def generate_flood_threshold_reference(
    states: Optional[list[str]] = None,
    output_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Generate a reference table of flood thresholds for multiple states.

    Args:
        states: List of state codes (default: all US states)
        output_path: Path to save the parquet file

    Returns:
        Combined DataFrame with all flood thresholds.
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
        logger.info(f"Processing flood thresholds for {state}...")
        df = fetch_state_flood_thresholds(state)
        if not df.empty:
            all_results.append(df)

    if not all_results:
        return pd.DataFrame()

    combined = pd.concat(all_results, ignore_index=True)

    # Remove duplicates (same site might appear in multiple states for border sites)
    combined = combined.drop_duplicates(subset=["site_id"])

    if output_path:
        combined.to_parquet(output_path, index=False)
        logger.info(f"Saved flood thresholds to {output_path}")

    return combined


def determine_flood_status(
    current_stage: Optional[float],
    action_stage: Optional[float],
    flood_stage: Optional[float],
    moderate_flood_stage: Optional[float],
    major_flood_stage: Optional[float]
) -> str:
    """
    Determine flood status based on current stage and thresholds.

    Args:
        current_stage: Current gage height
        action_stage: Action stage threshold
        flood_stage: Minor flood stage threshold
        moderate_flood_stage: Moderate flood stage threshold
        major_flood_stage: Major flood stage threshold

    Returns:
        Flood status string.
    """
    if current_stage is None or pd.isna(current_stage):
        return "Unknown"

    # Check from most severe to least severe
    if major_flood_stage is not None and not pd.isna(major_flood_stage):
        if current_stage >= major_flood_stage:
            return "Major Flood"

    if moderate_flood_stage is not None and not pd.isna(moderate_flood_stage):
        if current_stage >= moderate_flood_stage:
            return "Moderate Flood"

    if flood_stage is not None and not pd.isna(flood_stage):
        if current_stage >= flood_stage:
            return "Minor Flood"

    if action_stage is not None and not pd.isna(action_stage):
        if current_stage >= action_stage:
            return "Action Stage"

    return "No Flood"
