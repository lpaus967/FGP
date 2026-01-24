"""
Pipeline B: Live Monitor (Fast / Stream)

Assesses current streamflow conditions by comparing live data
against the pre-calculated reference statistics.
"""

from .reference_loader import load_reference_data
from .live_fetcher import fetch_current_conditions, fetch_all_stations, get_readings_for_trends, extract_latest_values
from .percentile_calc import calculate_live_percentiles, run_live_monitor
from .trend_detector import calculate_trend, TrendResult
