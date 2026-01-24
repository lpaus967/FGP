"""
Configuration management for USGS Flow Percentile Monitor.
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class S3Config:
    """S3 bucket configuration."""
    bucket_name: str = os.getenv("S3_BUCKET_NAME", "my-flow-bucket")
    reference_prefix: str = "reference_stats"
    flood_thresholds_prefix: str = "flood_thresholds"
    live_output_prefix: str = "live_output"
    logs_prefix: str = "logs"


@dataclass
class USGSConfig:
    """USGS data fetching configuration."""
    discharge_param: str = "00060"  # Discharge (cubic feet per second)
    gage_height_param: str = "00065"  # Gage height (feet)
    parameter_code: str = "00060"  # Legacy - kept for compatibility
    start_date: str = "2000-01-01"  # ~25 years of recent data
    percentiles: tuple = (5, 10, 25, 50, 75, 90, 95)


@dataclass
class DroughtConfig:
    """Drought classification thresholds based on U.S. Drought Monitor."""
    d0_threshold: int = 30  # Abnormally Dry
    d1_threshold: int = 20  # Moderate Drought
    d2_threshold: int = 10  # Severe Drought
    d3_threshold: int = 5   # Extreme Drought
    d4_threshold: int = 2   # Exceptional Drought


@dataclass
class TrendConfig:
    """Trend detection configuration for rising/falling limb analysis."""
    window_hours: int = 48          # Look back period
    min_data_points: int = 4        # Minimum readings required
    rising_threshold: float = 5.0   # % total change to classify as rising
    falling_threshold: float = -5.0 # % total change to classify as falling
    temp_rising_threshold: float = 1.0   # Degrees C change to classify as rising
    temp_falling_threshold: float = -1.0 # Degrees C change to classify as falling


@dataclass
class Config:
    """Main configuration container."""
    s3: S3Config
    usgs: USGSConfig
    drought: DroughtConfig
    trend: TrendConfig
    max_workers: int = 10  # For concurrent.futures parallelization

    @classmethod
    def load(cls) -> "Config":
        """Load configuration from environment."""
        return cls(
            s3=S3Config(),
            usgs=USGSConfig(),
            drought=DroughtConfig(),
            trend=TrendConfig(),
            max_workers=int(os.getenv("MAX_WORKERS", "10"))
        )


# Global config instance
config = Config.load()
