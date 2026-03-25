
import logging
import sqlite3
from typing import Optional

import pandas as pd

from database import DB_PATH, save_anomaly

# Logging
logger = logging.getLogger(__name__)


# Constants
SIGMA_THRESHOLD = 2.0  # Number of standard deviations to flag as anomaly
MIN_HISTORICAL_ROWS = 30  # Minimum rows needed to compute a meaningful baseline


# Baseline computation
def build_hourly_baseline(metric: str) -> Optional[pd.DataFrame]:
    """
    Build a per-hour baseline (mean and std) from the historical_data table.

    Groups all historical readings by hour-of-day (0-23) and computes
    the mean and standard deviation for each hour. This gives us 24
    reference points representing what's "normal" for each hour.

    Args:
        metric: One of 'wind_onshore_mw', 'solar_mw', 'load_mw'

    Returns:
        A DataFrame with columns [hour, mean_mw, std_mw] indexed by hour,
        or None if insufficient data exists.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(
            """
            SELECT timestamp, wind_onshore_mw, solar_mw, load_mw
            FROM historical_data
            WHERE wind_onshore_mw IS NOT NULL
              AND solar_mw IS NOT NULL
              AND load_mw IS NOT NULL
            ORDER BY timestamp ASC
            """,
            conn,
        )
        conn.close()
    except sqlite3.Error as e:
        logger.error(f"Failed to load historical data for baseline: {e}")
        return None

    if len(df) < MIN_HISTORICAL_ROWS:
        logger.warning(
            f"Only {len(df)} historical rows found. "
            f"Need at least {MIN_HISTORICAL_ROWS} to build a reliable baseline."
        )
        return None

    if metric not in df.columns:
        logger.error(f"Metric '{metric}' not found in historical_data columns.")
        return None

    # Parse timestamp and extract hour-of-day
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour"] = df["timestamp"].dt.hour

    # Group by hour and compute mean and std for the target metric
    baseline = (
        df.groupby("hour")[metric]
        .agg(mean_mw="mean", std_mw="std")
        .reset_index()
    )

    # Fill any zero std values with a small number to avoid division issues
    baseline["std_mw"] = baseline["std_mw"].fillna(0).clip(lower=1.0)

    logger.info(
        f"Baseline built for '{metric}' across {len(df):,} historical rows "
        f"({len(baseline)} hour buckets)."
    )
    return baseline


# Anomaly detection
def detect_anomalies(
    live_df: pd.DataFrame,
    metric_col: str,
    db_metric_name: str,
) -> pd.DataFrame:
    """
    Compare live energy readings against the hourly historical baseline
    and flag values that fall outside mean ± 2σ.

    Args:
        live_df:        DataFrame from load_energy_data(). Must have columns
                        [timestamp, datetime, value_mw, metric].
        metric_col:     The historical_data column name to build baseline from.
                        One of 'wind_onshore_mw', 'solar_mw', 'load_mw'.
        db_metric_name: The metric name as stored in energy_data table.
                        One of 'wind_onshore', 'solar', 'consumption'.

    Returns:
        A DataFrame of anomalous rows with added columns:
        [mean_mw, std_mw, upper_bound, lower_bound, is_anomaly].
        Empty DataFrame if no anomalies found or if baseline unavailable.
    """
    if live_df is None or live_df.empty:
        logger.warning(f"No live data provided for {db_metric_name}.")
        return pd.DataFrame()

    # Build the hourly baseline from historical data
    baseline = build_hourly_baseline(metric_col)
    if baseline is None:
        logger.warning(f"Cannot detect anomalies for {db_metric_name} — no baseline.")
        return pd.DataFrame()

    # Parse datetime and extract hour-of-day from live data
    live_df = live_df.copy()
    live_df["datetime_parsed"] = pd.to_datetime(live_df["datetime"], format="mixed")
    live_df["hour"] = live_df["datetime_parsed"].dt.hour

    # Merge live data with baseline on hour
    merged = live_df.merge(baseline, on="hour", how="left")

    # Calculate upper and lower bounds
    merged["upper_bound"] = merged["mean_mw"] + SIGMA_THRESHOLD * merged["std_mw"]
    merged["lower_bound"] = merged["mean_mw"] - SIGMA_THRESHOLD * merged["std_mw"]
    merged["lower_bound"] = merged["lower_bound"].clip(lower=0)  # MW can't be negative

    # Flag anomalies
    merged["is_anomaly"] = (
        (merged["value_mw"] > merged["upper_bound"]) |
        (merged["value_mw"] < merged["lower_bound"])
    )

    anomalies = merged[merged["is_anomaly"]].copy()

    logger.info(
        f"{db_metric_name}: {len(anomalies)} anomalies detected "
        f"out of {len(live_df)} live readings."
    )
    return anomalies


# Save anomalies to database
def run_anomaly_detection(live_data: dict) -> dict:
    """
    Run anomaly detection for all three metrics and save results to database.

    This is the main entry point called by scheduler.py and app.py.
    It detects anomalies and saves new ones to the anomalies table,
    then returns the anomaly DataFrames for use in the dashboard.

    Args:
        live_data: Dictionary mapping metric name to its DataFrame.
                   Keys must be 'wind_onshore', 'solar', 'consumption'.
                   This is the same dict returned by load_energy_data().

    Returns:
        Dictionary mapping metric name to its anomalies DataFrame.
        Example: {'wind_onshore': df, 'solar': df, 'consumption': df}
    """
    # Maps: live metric name → historical column name
    metric_map: dict[str, str] = {
        "wind_onshore": "wind_onshore_mw",
        "solar":        "solar_mw",
        "consumption":  "load_mw",
    }

    results: dict[str, pd.DataFrame] = {}

    for db_metric, hist_col in metric_map.items():
        if db_metric not in live_data or live_data[db_metric] is None:
            logger.warning(f"No live data available for {db_metric}, skipping.")
            continue

        anomalies = detect_anomalies(
            live_df=live_data[db_metric],
            metric_col=hist_col,
            db_metric_name=db_metric,
        )

        if anomalies.empty:
            results[db_metric] = pd.DataFrame()
            continue

        # Save each anomaly to the database
        saved_count = 0
        for _, row in anomalies.iterrows():
            try:
                save_anomaly(
                    timestamp=int(row["timestamp"]),
                    dt=str(row["datetime"]),
                    value_mw=float(row["value_mw"]),
                    metric=db_metric,
                    mean_mw=float(row["mean_mw"]),
                    std_mw=float(row["std_mw"]),
                )
                saved_count += 1
            except Exception as e:
                logger.error(f"Failed to save anomaly row for {db_metric}: {e}")

        logger.info(f"Saved {saved_count} anomalies for {db_metric}.")
        results[db_metric] = anomalies

    return results


# Quick test - run this file directly to verify detection works
if __name__ == "__main__":
    import logging
    from database import load_energy_data

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    print("Loading live data from database...")
    live_data = {}
    for metric in ["wind_onshore", "solar", "consumption"]:
        df = load_energy_data(metric)
        if df is not None:
            live_data[metric] = df
            print(f"  {metric}: {len(df)} rows loaded")

    print("\nRunning anomaly detection...")
    results = run_anomaly_detection(live_data)

    print("\n--- Results ---")
    for metric, anomalies in results.items():
        if anomalies.empty:
            print(f"  {metric}: no anomalies detected")
        else:
            print(f"  {metric}: {len(anomalies)} anomalies")
            print(anomalies[["datetime", "value_mw", "mean_mw", "upper_bound", "lower_bound"]].to_string(index=False))