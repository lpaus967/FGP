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
    live_output_prefix: str = "live_output"
    logs_prefix: str = "logs"


@dataclass
class USGSConfig:
    """USGS data fetching configuration."""
    parameter_code: str = "00060"  # Discharge (cubic feet per second)
    start_date: str = "2000-01-01"  # ~25 years of recent data
    percentiles: tuple = (5, 10, 25, 50, 75, 90, 95)


@dataclass
class Config:
    """Main configuration container."""
    s3: S3Config
    usgs: USGSConfig
    max_workers: int = 10  # For concurrent.futures parallelization

    @classmethod
    def load(cls) -> "Config":
        """Load configuration from environment."""
        return cls(
            s3=S3Config(),
            usgs=USGSConfig(),
            max_workers=int(os.getenv("MAX_WORKERS", "10"))
        )


# Global config instance
config = Config.load()
