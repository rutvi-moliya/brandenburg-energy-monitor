import requests
import logging
import time
from datetime import datetime
from typing import Optional
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SMARD_BASE_URL = "https://www.smard.de/app/chart_data"
SMARD_FILTERS = {
    "wind_onshore": 4067,
    "solar": 4068,
    "consumption": 410,
}

def get_smard_index(filter_id: int) -> list[int]:
    """
    Fetch the list of available timestamps for a given SMARD filter.

    Parameters:
        filter_id: The SMARD filter code (e.g. 4067 for wind onshore)

    Returns:
        A list of Unix timestamps in milliseconds
    """
    url = f"{SMARD_BASE_URL}/{filter_id}/DE/index_hour.json"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        timestamps = response.json()["timestamps"]
        logger.info(f"Fetched {len(timestamps)} timestamps for filter {filter_id}")
        return timestamps
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching index for filter {filter_id}")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for filter {filter_id}: {e}")
        return []


def get_smard_data(filter_id: int, timestamp: int) -> Optional[pd.DataFrame]:
    """
    Fetch hourly energy data for a specific filter and timestamp.

    Parameters:
        filter_id: The SMARD filter code
        timestamp: Unix timestamp in milliseconds (from the index)

    Returns:
        A pandas DataFrame with columns [timestamp, datetime, value_mw]
        or None if the request fails
    """
    url = f"{SMARD_BASE_URL}/{filter_id}/DE/{filter_id}_DE_hour_{timestamp}.json"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        series = response.json()["series"]

        records = []
        for point in series:
            if point[1] is not None:
                records.append({
                    "timestamp": point[0],
                    "datetime": datetime.fromtimestamp(point[0] / 1000),
                    "value_mw": point[1],
                })

        df = pd.DataFrame(records)
        logger.info(f"Fetched {len(df)} data points for filter {filter_id}")
        return df

    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching data for filter {filter_id}, timestamp {timestamp}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for filter {filter_id}: {e}")
        return None


def fetch_latest(metric_name: str) -> Optional[pd.DataFrame]:
    """
    Fetch the most recent week of data for a named energy metric.

    Parameters:
        metric_name: One of 'wind_onshore', 'solar', 'consumption'

    Returns:
        A pandas DataFrame with the latest data, or None if it fails
    """
    if metric_name not in SMARD_FILTERS:
        logger.error(f"Unknown metric: {metric_name}. Choose from {list(SMARD_FILTERS.keys())}")
        return None

    filter_id = SMARD_FILTERS[metric_name]

    timestamps = get_smard_index(filter_id)
    if not timestamps:
        return None

    latest_timestamp = timestamps[-1]
    logger.info(f"Latest timestamp for {metric_name}: {latest_timestamp}")

    time.sleep(1)

    df = get_smard_data(filter_id, latest_timestamp)
    if df is not None:
        df["metric"] = metric_name
    return df


def fetch_all_metrics() -> dict[str, pd.DataFrame]:
    """
    Fetch the latest data for all three metrics: wind, solar, consumption.

    Returns:
        A dictionary mapping metric name to its DataFrame
    """
    results = {}
    for metric in SMARD_FILTERS.keys():
        logger.info(f"Fetching {metric}...")
        df = fetch_latest(metric)
        if df is not None:
            results[metric] = df
        time.sleep(1)
    return results


if __name__ == "__main__":
    print("Testing fetch_all_metrics...\n")
    data = fetch_all_metrics()
    for metric, df in data.items():
        print(f"\n{metric.upper()}")
        print(f"  Rows: {len(df)}")
        print(f"  Date range: {df['datetime'].min()} → {df['datetime'].max()}")
        print(f"  Latest value: {df['value_mw'].iloc[-1]:.1f} MW")
