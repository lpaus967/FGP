"""
TICKET-2.1: Single Site History Fetcher

Fetches Daily Values (DV) for a single USGS site.
"""

import logging
from typing import Optional

import pandas as pd
import dataretrieval.nwis as nwis

from src.utils.config import config

logger = logging.getLogger(__name__)


def fetch_site_history(site_id: str, start_date: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Fetch Daily Values (DV) for a single USGS site.

    Args:
        site_id: USGS site identifier (e.g., "01013500")
        start_date: Start date for data retrieval (default: 1950-01-01)

    Returns:
        DataFrame with daily mean discharge values, or None if no data available.
    """
    if start_date is None:
        start_date = config.usgs.start_date

    try:
        df, _ = nwis.get_dv(
            sites=site_id,
            parameterCd=config.usgs.parameter_code,
            start=start_date
        )

        if df.empty:
            logger.warning(f"No data available for site {site_id}")
            return None

        return df

    except Exception as e:
        logger.error(f"Error fetching data for site {site_id}: {e}")
        return None


def get_sites_for_state(state_code: str) -> list[str]:
    """
    Get all active streamflow sites for a given state.

    Args:
        state_code: Two-letter state code (e.g., "VT")

    Returns:
        List of site IDs.
    """
    try:
        sites, _ = nwis.get_info(
            stateCd=state_code,
            parameterCd=config.usgs.parameter_code,
            siteType="ST",  # Stream sites only
            siteStatus="active"
        )

        if sites.empty:
            return []

        return sites["site_no"].tolist()

    except Exception as e:
        logger.error(f"Error fetching sites for state {state_code}: {e}")
        return []
