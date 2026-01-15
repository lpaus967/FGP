"""
TICKET-2.4 & TICKET-3.4: S3 Client

Handles all S3 operations for uploading and downloading data.
"""

import io
import json
import logging
from datetime import datetime
from typing import Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from .config import config

logger = logging.getLogger(__name__)


class S3Client:
    """Client for S3 operations."""

    def __init__(self, bucket_name: Optional[str] = None):
        """
        Initialize S3 client.

        Args:
            bucket_name: S3 bucket name (default: from config)
        """
        self.s3 = boto3.client("s3")
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
            json_data = df.to_json(orient="records", date_format="iso")

            # Upload current snapshot
            self.s3.put_object(
                Bucket=self.bucket,
                Key=current_key,
                Body=json_data,
                ContentType="application/json"
            )

            # Upload to history
            self.s3.put_object(
                Bucket=self.bucket,
                Key=history_key,
                Body=json_data,
                ContentType="application/json"
            )

            logger.info(f"Uploaded live output to s3://{self.bucket}/{current_key}")
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
