"""
Pipeline A: Reference Generator (Slow / Batch)

Builds the statistical baseline by fetching historical USGS data
and calculating percentile thresholds for every Day of Year.
"""

from .fetch_history import fetch_site_history
from .calculate_stats import calculate_site_percentiles
from .batch_processor import generate_state_reference
