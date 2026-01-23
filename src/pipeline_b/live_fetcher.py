"""
Fetches current instantaneous values from USGS.
Includes both discharge (00060) and gage height (00065) for flood stage determination.
"""

import logging
from typing import Optional

import pandas as pd
import dataretrieval.nwis as nwis

from src.utils.config import config

logger = logging.getLogger(__name__)

# Parameter codes
PARAM_DISCHARGE = "00060"  # Discharge (cubic feet per second)
PARAM_GAGE_HEIGHT = "00065"  # Gage height (feet)


def fetch_current_conditions(site_ids: list[str], include_gage_height: bool = True) -> Optional[pd.DataFrame]:
    """
    Fetch current instantaneous values for multiple sites.

    Args:
        site_ids: List of USGS site identifiers
        include_gage_height: Whether to also fetch gage height for flood determination

    Returns:
        DataFrame with current discharge and gage height values, or None if fetch fails.
    """
    if not site_ids:
        return None

    # Build parameter list
    params = [PARAM_DISCHARGE]
    if include_gage_height:
        params.append(PARAM_GAGE_HEIGHT)
    param_str = ",".join(params)

    try:
        # Fetch in batches to avoid API limits
        batch_size = 100
        all_results = []

        for i in range(0, len(site_ids), batch_size):
            batch = site_ids[i:i + batch_size]

            df, _ = nwis.get_iv(
                sites=batch,
                parameterCd=param_str
            )

            if not df.empty:
                all_results.append(df)

        if not all_results:
            logger.warning("No current data available for any sites")
            return None

        combined = pd.concat(all_results)
        return combined

    except Exception as e:
        logger.error(f"Error fetching current conditions: {e}")
        return None


def fetch_state_current_conditions(state_code: str, include_gage_height: bool = True) -> Optional[pd.DataFrame]:
    """
    Fetch current instantaneous values for all sites in a state.

    Args:
        state_code: Two-letter state code (e.g., "VT")
        include_gage_height: Whether to also fetch gage height for flood determination

    Returns:
        DataFrame with current discharge and gage height values.
    """
    # Build parameter list
    params = [PARAM_DISCHARGE]
    if include_gage_height:
        params.append(PARAM_GAGE_HEIGHT)
    param_str = ",".join(params)

    try:
        df, _ = nwis.get_iv(
            stateCd=state_code,
            parameterCd=param_str
        )

        if df.empty:
            logger.warning(f"No current data available for state {state_code}")
            return None

        return df

    except Exception as e:
        logger.error(f"Error fetching current conditions for {state_code}: {e}")
        return None


def extract_latest_values(iv_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract the most recent value for each site from IV data.

    Args:
        iv_df: DataFrame with instantaneous values

    Returns:
        DataFrame with one row per site containing the latest discharge and gage height.
    """
    # Group by site and get the most recent reading
    latest = iv_df.groupby("site_no").last().reset_index()

    # Find the primary discharge column (00060, not quality codes)
    discharge_cols = [c for c in latest.columns if c == "00060" or (c.startswith("00060") and "cd" not in c.lower())]

    if discharge_cols:
        # Use primary 00060 column
        primary_col = "00060" if "00060" in discharge_cols else discharge_cols[0]
        latest["discharge"] = latest[primary_col]
    else:
        latest["discharge"] = None

    # Find the gage height column (00065, not quality codes)
    gage_cols = [c for c in latest.columns if c == "00065" or (c.startswith("00065") and "cd" not in c.lower())]

    if gage_cols:
        primary_gage_col = "00065" if "00065" in gage_cols else gage_cols[0]
        latest["gage_height"] = latest[primary_gage_col]
    else:
        latest["gage_height"] = None

    # Filter out invalid discharge values (-999999 is USGS missing/ice code)
    # Keep rows where discharge is valid OR gage_height is valid (for flood monitoring)
    valid_discharge = (latest["discharge"].notna()) & (latest["discharge"] > 0)
    valid_gage = (latest["gage_height"].notna()) & (latest["gage_height"] > -100)  # Some gages can be negative
    latest = latest[valid_discharge | valid_gage].copy()

    # Replace invalid discharge with None
    latest.loc[latest["discharge"] <= 0, "discharge"] = None

    logger.info(f"Extracted {len(latest)} valid readings from IV data")

    return latest
