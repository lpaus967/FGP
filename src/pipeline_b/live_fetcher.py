"""
TICKET-3.2: Live Data Fetcher

Fetches current instantaneous values from USGS.
"""

import logging
from typing import Optional

import pandas as pd
import dataretrieval.nwis as nwis

from src.utils.config import config

logger = logging.getLogger(__name__)


def fetch_current_conditions(site_ids: list[str]) -> Optional[pd.DataFrame]:
    """
    Fetch current instantaneous values for multiple sites.

    Args:
        site_ids: List of USGS site identifiers

    Returns:
        DataFrame with current discharge values, or None if fetch fails.
    """
    if not site_ids:
        return None

    try:
        # Fetch in batches to avoid API limits
        batch_size = 100
        all_results = []

        for i in range(0, len(site_ids), batch_size):
            batch = site_ids[i:i + batch_size]

            df, _ = nwis.get_iv(
                sites=batch,
                parameterCd=config.usgs.parameter_code
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


def fetch_state_current_conditions(state_code: str) -> Optional[pd.DataFrame]:
    """
    Fetch current instantaneous values for all sites in a state.

    Args:
        state_code: Two-letter state code (e.g., "VT")

    Returns:
        DataFrame with current discharge values.
    """
    try:
        df, _ = nwis.get_iv(
            stateCd=state_code,
            parameterCd=config.usgs.parameter_code
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
        DataFrame with one row per site containing the latest value.
    """
    # Group by site and get the most recent reading
    latest = iv_df.groupby("site_no").last().reset_index()
    return latest
