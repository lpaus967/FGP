"""
TICKET-2.4 & TICKET-3.4: S3 Client

Handles all S3 operations for uploading and downloading data.
"""

import io
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from .config import config

# CloudWatch client for custom metrics
cloudwatch = boto3.client("cloudwatch", region_name=os.getenv("AWS_REGION", "us-west-1"))

logger = logging.getLogger(__name__)


class S3Client:
    """Client for S3 operations."""

    def __init__(self, bucket_name: Optional[str] = None):
        """
        Initialize S3 client.

        Args:
            bucket_name: S3 bucket name (default: from config)
        """
        self.s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-west-1"))
        self.bucket = bucket_name or config.s3.bucket_name

    def upload_reference_stats(self, df: pd.DataFrame, state_code: str) -> bool:
        """
        Upload reference statistics to S3.

        Args:
            df: DataFrame with reference statistics
            state_code: Two-letter state code

        Returns:
            True if upload successful, False otherwise.
        """
        key = f"{config.s3.reference_prefix}/state={state_code}/data.parquet"

        try:
            # Convert DataFrame to Parquet bytes
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=True)
            buffer.seek(0)

            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=buffer.getvalue()
            )

            logger.info(f"Uploaded reference stats to s3://{self.bucket}/{key}")
            return True

        except ClientError as e:
            logger.error(f"Failed to upload reference stats for {state_code}: {e}")
            return False

    def download_reference_stats(self, state_code: str) -> Optional[pd.DataFrame]:
        """
        Download reference statistics from S3.

        Args:
            state_code: Two-letter state code

        Returns:
            DataFrame with reference statistics, or None if not found.
        """
        key = f"{config.s3.reference_prefix}/state={state_code}/data.parquet"

        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            buffer = io.BytesIO(response["Body"].read())
            df = pd.read_parquet(buffer)

            logger.info(f"Downloaded reference stats from s3://{self.bucket}/{key}")
            return df

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"Reference stats not found for state {state_code}")
            else:
                logger.error(f"Failed to download reference stats for {state_code}: {e}")
            return None

    def upload_live_output(self, df: pd.DataFrame) -> bool:
        """
        Upload live monitoring output to S3.

        Outputs JSON in a format optimized for frontend joins:
        {
            "generated_at": "2026-01-15T14:00:00Z",
            "site_count": 10000,
            "sites": {
                "01100000": {
                    "flow": 5920.0,
                    "gage_height": 8.5,
                    "percentile": 52.3,
                    "flow_status": "Normal",
                    "drought_status": null,
                    "flood_status": null,
                    "state": "MA"
                },
                ...
            }
        }

        Args:
            df: DataFrame with current conditions

        Returns:
            True if upload successful, False otherwise.
        """
        timestamp = datetime.utcnow()

        # Upload current snapshot
        current_key = f"{config.s3.live_output_prefix}/current_status.json"

        # Upload to history
        history_key = f"{config.s3.live_output_prefix}/history/{timestamp.strftime('%Y-%m-%dT%H%M')}.json"

        try:
            # Build optimized JSON structure keyed by site_id for fast frontend lookups
            sites_dict = {}
            for _, row in df.iterrows():
                site_id = str(row.get("site_id", ""))
                if site_id:
                    site_data = {
                        "flow": row.get("flow") if pd.notna(row.get("flow")) else None,
                        "gage_height": row.get("gage_height") if pd.notna(row.get("gage_height")) else None,
                        "water_temp": row.get("water_temp") if pd.notna(row.get("water_temp")) else None,
                        "percentile": row.get("percentile") if pd.notna(row.get("percentile")) else None,
                        "flow_status": row.get("flow_status") if pd.notna(row.get("flow_status")) else None,
                        "drought_status": row.get("drought_status") if pd.notna(row.get("drought_status")) else None,
                        "flood_status": row.get("flood_status") if pd.notna(row.get("flood_status")) else None,
                        "trend": row.get("trend") if pd.notna(row.get("trend")) else None,
                        "trend_rate": round(row.get("trend_rate"), 2) if pd.notna(row.get("trend_rate")) else None,
                        "hours_since_peak": round(row.get("hours_since_peak"), 1) if pd.notna(row.get("hours_since_peak")) else None,
                        "water_temp_trend": row.get("water_temp_trend") if pd.notna(row.get("water_temp_trend")) else None,
                    }
                    # Include state if present
                    if "state" in row and pd.notna(row.get("state")):
                        site_data["state"] = row.get("state")

                    sites_dict[site_id] = site_data

            output = {
                "generated_at": timestamp.isoformat() + "Z",
                "site_count": len(sites_dict),
                "sites": sites_dict
            }

            json_data = json.dumps(output, separators=(",", ":"))  # Compact JSON

            # Upload current snapshot with CORS-friendly headers
            self.s3.put_object(
                Bucket=self.bucket,
                Key=current_key,
                Body=json_data,
                ContentType="application/json",
                CacheControl="max-age=300",  # 5 minute cache
            )

            # Upload to history
            self.s3.put_object(
                Bucket=self.bucket,
                Key=history_key,
                Body=json_data,
                ContentType="application/json"
            )

            logger.info(f"Uploaded live output ({len(sites_dict)} sites) to s3://{self.bucket}/{current_key}")

            # Publish custom CloudWatch metrics
            try:
                # Count sites with temperature data
                sites_with_temp = sum(1 for s in sites_dict.values() if s.get("water_temp") is not None)

                cloudwatch.put_metric_data(
                    Namespace="FGP/Pipeline",
                    MetricData=[
                        {
                            "MetricName": "SitesUploaded",
                            "Value": len(sites_dict),
                            "Unit": "Count",
                            "Dimensions": [
                                {"Name": "Environment", "Value": "dev"},
                            ]
                        },
                        {
                            "MetricName": "SitesWithTemperature",
                            "Value": sites_with_temp,
                            "Unit": "Count",
                            "Dimensions": [
                                {"Name": "Environment", "Value": "dev"},
                            ]
                        },
                    ]
                )
                logger.debug(f"Published CloudWatch metrics: {len(sites_dict)} sites, {sites_with_temp} with temp")
            except Exception as cw_err:
                logger.warning(f"Failed to publish CloudWatch metrics: {cw_err}")

            return True

        except ClientError as e:
            logger.error(f"Failed to upload live output: {e}")
            return False

    def download_live_output(self) -> Optional[pd.DataFrame]:
        """
        Download the current live output from S3.

        Returns:
            DataFrame with current conditions, or None if not found.
        """
        key = f"{config.s3.live_output_prefix}/current_status.json"

        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            json_data = response["Body"].read().decode("utf-8")
            df = pd.read_json(io.StringIO(json_data))

            return df

        except ClientError as e:
            logger.error(f"Failed to download live output: {e}")
            return None

    def list_available_states(self) -> list[str]:
        """
        List all states with available reference data.

        Returns:
            List of state codes.
        """
        prefix = f"{config.s3.reference_prefix}/state="

        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            states = set()

            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/"):
                for prefix_obj in page.get("CommonPrefixes", []):
                    # Extract state code from prefix
                    state = prefix_obj["Prefix"].split("=")[-1].rstrip("/")
                    states.add(state)

            return sorted(states)

        except ClientError as e:
            logger.error(f"Failed to list available states: {e}")
            return []

    def upload_flood_thresholds(self, df: pd.DataFrame) -> bool:
        """
        Upload flood thresholds reference data to S3.

        Args:
            df: DataFrame with flood thresholds

        Returns:
            True if upload successful, False otherwise.
        """
        key = f"{config.s3.flood_thresholds_prefix}/flood_thresholds.parquet"

        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=buffer.getvalue()
            )

            logger.info(f"Uploaded flood thresholds ({len(df)} sites) to s3://{self.bucket}/{key}")
            return True

        except ClientError as e:
            logger.error(f"Failed to upload flood thresholds: {e}")
            return False

    def download_flood_thresholds(self) -> Optional[pd.DataFrame]:
        """
        Download flood thresholds reference data from S3.

        Returns:
            DataFrame with flood thresholds, or None if not found.
        """
        key = f"{config.s3.flood_thresholds_prefix}/flood_thresholds.parquet"

        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            buffer = io.BytesIO(response["Body"].read())
            df = pd.read_parquet(buffer)

            logger.info(f"Downloaded flood thresholds ({len(df)} sites) from s3://{self.bucket}/{key}")
            return df

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning("Flood thresholds not found in S3")
            else:
                logger.error(f"Failed to download flood thresholds: {e}")
            return None

    def list_historical_snapshots(self, hours: int = 48) -> list[str]:
        """
        List S3 keys for snapshots in the time window.

        Args:
            hours: Number of hours to look back (default: 48)

        Returns:
            List of S3 keys sorted oldest first.
        """
        prefix = f"{config.s3.live_output_prefix}/history/"
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            keys = []

            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    # Parse timestamp from filename (YYYY-MM-DDTHHMM.json)
                    filename = key.split("/")[-1]
                    if not filename.endswith(".json"):
                        continue

                    try:
                        timestamp_str = filename.replace(".json", "")
                        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H%M")

                        if timestamp >= cutoff_time:
                            keys.append((timestamp, key))
                    except ValueError:
                        continue

            # Sort by timestamp (oldest first)
            keys.sort(key=lambda x: x[0])
            return [key for _, key in keys]

        except ClientError as e:
            logger.error(f"Failed to list historical snapshots: {e}")
            return []

    def download_historical_snapshot(self, key: str) -> Optional[dict]:
        """
        Download and parse a single historical JSON snapshot.

        Args:
            key: S3 key for the snapshot

        Returns:
            Parsed JSON dict, or None if download failed.
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            json_data = response["Body"].read().decode("utf-8")
            return json.loads(json_data)

        except ClientError as e:
            logger.warning(f"Failed to download snapshot {key}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse snapshot {key}: {e}")
            return None
