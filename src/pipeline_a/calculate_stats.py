"""
TICKET-2.2: Hyswap Stats Calculator

Implements hyswap logic to calculate percentiles for every day of the year.
"""

import logging
from typing import Optional

import pandas as pd
import numpy as np
import hyswap

from src.utils.config import config

logger = logging.getLogger(__name__)


def calculate_site_percentiles(df: pd.DataFrame, site_id: str) -> Optional[pd.DataFrame]:
    """
    Calculate percentile thresholds for each Day of Year using hyswap.

    Args:
        df: DataFrame with daily discharge values (from fetch_site_history)
        site_id: USGS site identifier

    Returns:
        DataFrame with index (DOY 1-366) and columns for each percentile threshold,
        or None if calculation fails.
    """
    try:
        # Use hyswap to calculate streamflow percentiles by day of year
        percentile_df = hyswap.percentiles.calculate_variable_percentile_thresholds_by_day(
            df,
            data_column_name=df.columns[0],  # First data column
            percentiles=list(config.usgs.percentiles)
        )

        # Add site identifier
        percentile_df["site_id"] = site_id

        return percentile_df

    except Exception as e:
        logger.error(f"Error calculating percentiles for site {site_id}: {e}")
        return None


def add_day_of_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Day of Year column to a DataFrame with datetime index.

    Args:
        df: DataFrame with datetime index

    Returns:
        DataFrame with 'day_of_year' column added.
    """
    df = df.copy()
    df["day_of_year"] = df.index.dayofyear
    return df
