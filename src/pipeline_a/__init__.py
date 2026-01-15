"""
Pipeline A: Reference Generator (Slow / Batch)

Builds the statistical baseline by fetching historical USGS data
and calculating percentile thresholds for every Day of Year.
Also fetches NWS flood stage thresholds for flood status determination.
"""

from .fetch_history import fetch_site_history
from .calculate_stats import calculate_site_percentiles
from .batch_processor import generate_state_reference
from .fetch_flood_thresholds import (
    fetch_flood_thresholds_for_sites,
    fetch_state_flood_thresholds,
    generate_flood_threshold_reference,
    determine_flood_status
)
