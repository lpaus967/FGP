"""
DynamoDB client for fetching station data with cursor-based pagination.

Replicates the functionality of the TypeScript getStationsWithReadingsCursor method,
connecting directly to DynamoDB instead of going through the API.
"""

import asyncio
import base64
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Any

import aioboto3
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Configuration from environment
TABLE_NAME = os.getenv("HYDRA_TABLE_NAME", "HydraDataTable")
# Use HYDRA_TABLE_REGION for DynamoDB (may differ from AWS_REGION used for S3)
DYNAMODB_REGION = os.getenv("HYDRA_TABLE_REGION", "us-west-2")
GSI_NAME = "StationSyncScheduleIndex"


def _encode_cursor(cursor_data: dict) -> str:
    """Base64 encode cursor data as JSON."""
    json_str = json.dumps(cursor_data, separators=(",", ":"))
    return base64.b64encode(json_str.encode("utf-8")).decode("utf-8")


def _decode_cursor(cursor: str) -> Optional[dict]:
    """Decode Base64 cursor back to dict."""
    try:
        json_str = base64.b64decode(cursor.encode("utf-8")).decode("utf-8")
        return json.loads(json_str)
    except Exception as e:
        logger.warning(f"Failed to decode cursor: {e}")
        return None


def _extract_station_id(pk: str) -> str:
    """Extract station ID from PK (STATION#<stationId>)."""
    if pk.startswith("STATION#"):
        return pk[8:]
    return pk


def _parse_reading(item: dict) -> dict:
    """Parse a DynamoDB reading item into the expected format."""
    # Extract timestamp from SK (READING#<ISO8601 timestamp>)
    sk = item.get("SK", "")
    timestamp = sk[8:] if sk.startswith("READING#") else item.get("timestamp", "")

    return {
        "timestamp": timestamp,
        "water_flow_cfs": _safe_float(item.get("waterFlowCFS")),
        "river_depth_ft": _safe_float(item.get("riverDepthFT")),
        "water_temp_c": _safe_float(item.get("waterTempC")),
    }


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert a value to float, returning None if not possible."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


async def _fetch_readings_for_station(
    table,
    station_id: str,
    start_time: str,
    end_time: str,
    semaphore: asyncio.Semaphore
) -> list[dict]:
    """
    Fetch readings for a single station within the time range.

    Args:
        table: aioboto3 DynamoDB table resource
        station_id: The station ID
        start_time: ISO8601 start timestamp
        end_time: ISO8601 end timestamp
        semaphore: Semaphore for concurrency control

    Returns:
        List of reading dicts
    """
    async with semaphore:
        try:
            pk = f"STATION#{station_id}"
            sk_start = f"READING#{start_time}"
            sk_end = f"READING#{end_time}"

            readings = []
            last_evaluated_key = None

            while True:
                query_kwargs = {
                    "KeyConditionExpression": Key("PK").eq(pk) & Key("SK").between(sk_start, sk_end),
                }

                if last_evaluated_key:
                    query_kwargs["ExclusiveStartKey"] = last_evaluated_key

                response = await table.query(**query_kwargs)

                for item in response.get("Items", []):
                    readings.append(_parse_reading(item))

                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break

            # Sort by timestamp ascending
            readings.sort(key=lambda r: r.get("timestamp", ""))
            return readings

        except ClientError as e:
            logger.error(f"Error fetching readings for station {station_id}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching readings for station {station_id}: {e}")
            return []


async def get_stations_with_readings_cursor_async(
    limit: int = 100,
    cursor: Optional[str] = None,
    hours_back: int = 48,
    concurrency_limit: int = 10
) -> dict:
    """
    Fetch stations with their recent readings using cursor-based pagination.

    This async function connects directly to DynamoDB and replicates the
    functionality of the TypeScript getStationsWithReadingsCursor method.

    Args:
        limit: Number of stations per page (1-500)
        cursor: Base64-encoded cursor string or None for first page
        hours_back: Hours of readings to fetch (default: 48)
        concurrency_limit: Max parallel reading queries (default: 10)

    Returns:
        Dict with structure:
        {
            "stations": [...],
            "next_cursor": str | None,
            "stations_in_page": int
        }
    """
    # Validate limit
    limit = max(1, min(500, limit))

    # Calculate time range for readings
    now = datetime.now(timezone.utc)
    end_time = now.isoformat().replace("+00:00", "Z")
    start_time = (now - timedelta(hours=hours_back)).isoformat().replace("+00:00", "Z")

    # Decode cursor if provided
    exclusive_start_key = None
    if cursor:
        cursor_data = _decode_cursor(cursor)
        if cursor_data:
            exclusive_start_key = cursor_data

    # Batch size: fetch more than needed since we filter for active status
    batch_size = max(limit * 3, 100)

    active_stations = []
    last_station_key = None
    has_more_data = True

    session = aioboto3.Session()

    async with session.resource("dynamodb", region_name=DYNAMODB_REGION) as dynamodb:
        table = await dynamodb.Table(TABLE_NAME)

        # Keep querying until we have enough active stations or no more data
        while len(active_stations) < limit and has_more_data:
            query_kwargs = {
                "IndexName": GSI_NAME,
                "KeyConditionExpression": Key("entityType").eq("StationMetadata"),
                "FilterExpression": Attr("status").eq("active"),
                "Limit": batch_size,
            }

            if exclusive_start_key:
                query_kwargs["ExclusiveStartKey"] = exclusive_start_key

            try:
                response = await table.query(**query_kwargs)
            except ClientError as e:
                logger.error(f"Error querying stations: {e}")
                break

            items = response.get("Items", [])

            for item in items:
                if len(active_stations) >= limit:
                    break

                station_id = _extract_station_id(item.get("PK", ""))

                station = {
                    "station_id": station_id,
                    "name": item.get("name", ""),
                    "provider": item.get("provider", ""),
                    "provider_id": item.get("providerId", ""),
                    # Store key info for cursor construction
                    "_cursor_key": {
                        "entityType": item.get("entityType"),
                        "nextSyncAt": item.get("nextSyncAt"),
                        "PK": item.get("PK"),
                        "SK": item.get("SK"),
                    }
                }

                active_stations.append(station)
                last_station_key = station["_cursor_key"]

            # Check if there's more data
            exclusive_start_key = response.get("LastEvaluatedKey")
            if not exclusive_start_key:
                has_more_data = False

        # Determine next cursor
        # Use the last station's key, not DynamoDB's LastEvaluatedKey
        next_cursor = None
        if has_more_data and last_station_key:
            next_cursor = _encode_cursor(last_station_key)

        # Fetch readings for all stations concurrently
        semaphore = asyncio.Semaphore(concurrency_limit)

        async def fetch_station_readings(station: dict) -> dict:
            station_id = station["station_id"]
            readings = await _fetch_readings_for_station(
                table, station_id, start_time, end_time, semaphore
            )
            # Remove internal cursor key before returning
            result = {k: v for k, v in station.items() if not k.startswith("_")}
            result["readings"] = readings
            return result

        # Fetch all readings in parallel
        stations_with_readings = await asyncio.gather(
            *[fetch_station_readings(s) for s in active_stations]
        )

    return {
        "stations": stations_with_readings,
        "next_cursor": next_cursor,
        "stations_in_page": len(stations_with_readings),
    }


def get_stations_with_readings_cursor(
    limit: int = 100,
    cursor: Optional[str] = None,
    hours_back: int = 48,
    concurrency_limit: int = 10
) -> dict:
    """
    Synchronous wrapper for get_stations_with_readings_cursor_async.

    Fetch stations with their recent readings using cursor-based pagination.
    Connects directly to DynamoDB.

    Args:
        limit: Number of stations per page (1-500)
        cursor: Base64-encoded cursor string or None for first page
        hours_back: Hours of readings to fetch (default: 48)
        concurrency_limit: Max parallel reading queries (default: 10)

    Returns:
        Dict with structure:
        {
            "stations": [
                {
                    "station_id": str,
                    "name": str,
                    "provider": str,
                    "provider_id": str,
                    "readings": [
                        {
                            "timestamp": str,
                            "water_flow_cfs": float | None,
                            "river_depth_ft": float | None,
                            "water_temp_c": float | None
                        }
                    ]
                }
            ],
            "next_cursor": str | None,
            "stations_in_page": int
        }
    """
    return asyncio.run(
        get_stations_with_readings_cursor_async(
            limit=limit,
            cursor=cursor,
            hours_back=hours_back,
            concurrency_limit=concurrency_limit
        )
    )


def fetch_all_stations_from_dynamodb(
    limit_per_page: int = 100,
    hours_back: int = 48,
    concurrency_limit: int = 10
) -> list[dict]:
    """
    Fetch all stations using cursor-based pagination from DynamoDB.

    This is the DynamoDB equivalent of the existing fetch_all_stations()
    function that uses the REST API.

    Args:
        limit_per_page: Number of stations per page (default 100)
        hours_back: Hours of readings to fetch (default: 48)
        concurrency_limit: Max parallel reading queries (default: 10)

    Returns:
        List of all station objects with readings.
    """
    all_stations = []
    cursor = None
    page_count = 0

    while True:
        page_count += 1

        try:
            result = get_stations_with_readings_cursor(
                limit=limit_per_page,
                cursor=cursor,
                hours_back=hours_back,
                concurrency_limit=concurrency_limit
            )

            stations = result.get("stations", [])
            all_stations.extend(stations)

            stations_in_page = result.get("stations_in_page", len(stations))
            logger.info(f"Fetched page {page_count}: {stations_in_page} stations from DynamoDB")

            cursor = result.get("next_cursor")
            if cursor is None:
                logger.info(f"Pagination complete. Total stations: {len(all_stations)}")
                break

        except Exception as e:
            logger.error(f"Error fetching stations (page {page_count}): {e}")
            if all_stations:
                logger.warning(f"Returning {len(all_stations)} stations fetched before error")
            break

    return all_stations
