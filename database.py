import sqlite3
import logging
import pandas as pd
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = "energy.db"

def init_db() -> None:
    """
    Create the database tables if they don't already exist.
    This function is safe to call multiple times — it never
    deletes existing data.

    Parameters:
        None

    Returns:
        None
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS energy_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                datetime TEXT NOT NULL,
                value_mw REAL NOT NULL,
                metric TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS anomalies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                datetime TEXT NOT NULL,
                value_mw REAL NOT NULL,
                metric TEXT NOT NULL,
                mean_mw REAL NOT NULL,
                std_mw REAL NOT NULL,
                detected_at TEXT NOT NULL
                UNIQUE(timestamp, metric)
            )
        """)

        conn.commit()
        conn.close()
        logger.info("Database initialised successfully")

    except sqlite3.Error as e:
        logger.error(f"Database initialisation failed: {e}")

def save_energy_data(df: pd.DataFrame) -> int:
    """
    Save a DataFrame of energy readings to the database.
    Skips rows that already exist to avoid duplicates.

    Parameters:
        df: DataFrame with columns [timestamp, datetime, value_mw, metric]

    Returns:
        Number of new rows saved
    """
    if df is None or df.empty:
        logger.warning("No data to save — DataFrame is empty or None")
        return 0

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        rows_saved = 0
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for _, row in df.iterrows():
            cursor.execute("""
                SELECT id FROM energy_data
                WHERE timestamp = ? AND metric = ?
            """, (int(row["timestamp"]), row["metric"]))

            exists = cursor.fetchone()

            if exists is None:
                cursor.execute("""
                    INSERT INTO energy_data
                    (timestamp, datetime, value_mw, metric, fetched_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    int(row["timestamp"]),
                    str(row["datetime"]),
                    float(row["value_mw"]),
                    row["metric"],
                    fetched_at
                ))
                rows_saved += 1

        conn.commit()
        conn.close()
        logger.info(f"Saved {rows_saved} new rows to database")
        return rows_saved

    except sqlite3.Error as e:
        logger.error(f"Failed to save energy data: {e}")
        return 0

def load_energy_data(metric: str, limit: int = 1000) -> Optional[pd.DataFrame]:
    """
    Load the most recent energy readings for a given metric.

    Parameters:
        metric: One of 'wind_onshore', 'solar', 'consumption'
        limit: Maximum number of rows to return (default 500)

    Returns:
        A pandas DataFrame or None if the query fails
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("""
            SELECT timestamp, datetime, value_mw, metric
            FROM energy_data
            WHERE metric = ?
            ORDER BY timestamp ASC
            LIMIT ?
        """, conn, params=(metric, limit))
        conn.close()
        logger.info(f"Loaded {len(df)} rows for {metric}")
        return df

    except sqlite3.Error as e:
        logger.error(f"Failed to load data for {metric}: {e}")
        return None


def save_anomaly(
    timestamp: int,
    dt: str,
    value_mw: float,
    metric: str,
    mean_mw: float,
    std_mw: float
) -> None:
    """
    Save a detected anomaly to the anomalies table.

    Parameters:
        timestamp: Unix timestamp in milliseconds
        dt: Human readable datetime string
        value_mw: The anomalous energy value
        metric: Which energy metric triggered the anomaly
        mean_mw: The rolling mean at the time of detection
        std_mw: The rolling standard deviation at the time of detection

    Returns:
        None
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cursor = conn.cursor()
        detected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute("""
            INSERT OR IGNORE INTO anomalies
            (timestamp, datetime, value_mw, metric, mean_mw, std_mw, detected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (timestamp, dt, value_mw, metric, mean_mw, std_mw, detected_at))

        conn.commit()
        conn.close()
        logger.info(f"Anomaly saved for {metric} at {dt}: {value_mw:.1f} MW")

    except sqlite3.Error as e:
        logger.error(f"Failed to save anomaly: {e}")

def load_same_hour_last_week(metric: str, current_timestamp: int) -> Optional[float]:
    """
    Fetch the energy value from exactly 7 days ago at the same hour.
    Used to calculate the metric card delta vs same hour last week.

    Parameters:
        metric: One of 'wind_onshore', 'solar', 'consumption'
        current_timestamp: Unix timestamp in milliseconds of the current reading

    Returns:
        The value_mw from 7 days ago, or None if not found
    """
    seven_days_ms = 7 * 24 * 60 * 60 * 1000
    target_timestamp = current_timestamp - seven_days_ms

    # Allow ±30 minutes tolerance in case that exact hour is missing
    tolerance_ms = 30 * 60 * 1000

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT value_mw FROM energy_data
            WHERE metric = ?
              AND timestamp BETWEEN ? AND ?
            ORDER BY ABS(timestamp - ?) ASC
            LIMIT 1
        """, (
            metric,
            target_timestamp - tolerance_ms,
            target_timestamp + tolerance_ms,
            target_timestamp
        ))
        row = cursor.fetchone()
        conn.close()

        if row:
            logger.info(f"Found same-hour-last-week value for {metric}: {row[0]:.1f} MW")
            return float(row[0])
        else:
            logger.warning(f"No same-hour-last-week data found for {metric}")
            return None

    except sqlite3.Error as e:
        logger.error(f"Failed to load same-hour-last-week for {metric}: {e}")
        return None

def load_anomalies(metric: Optional[str] = None, limit: int = 100) -> Optional[pd.DataFrame]:
    """
    Load detected anomalies from the database.

    Parameters:
        metric: If provided, filter to this metric only. If None, load all metrics.
        limit: Maximum number of rows to return (default 100)

    Returns:
        A pandas DataFrame or None if the query fails
    """
    try:
        conn = sqlite3.connect(DB_PATH)

        if metric:
            df = pd.read_sql_query("""
                SELECT datetime, value_mw, metric, mean_mw, std_mw, detected_at
                FROM anomalies
                WHERE metric = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, conn, params=(metric, limit))
        else:
            df = pd.read_sql_query("""
                SELECT datetime, value_mw, metric, mean_mw, std_mw, detected_at
                FROM anomalies
                ORDER BY timestamp DESC
                LIMIT ?
            """, conn, params=(limit,))

        conn.close()
        logger.info(f"Loaded {len(df)} anomalies from database")
        return df

    except sqlite3.Error as e:
        logger.error(f"Failed to load anomalies: {e}")
        return None

if __name__ == "__main__":
    from fetcher import fetch_all_metrics

    print("Initialising database...")
    init_db()

    print("Fetching data...")
    data = fetch_all_metrics()

    for metric, df in data.items():
        saved = save_energy_data(df)
        print(f"{metric}: saved {saved} new rows")

    print("\nLoading back from database to verify...")
    for metric in ["wind_onshore", "solar", "consumption"]:
        df = load_energy_data(metric)
        if df is not None:
            print(f"{metric}: {len(df)} rows in database, latest: {df['value_mw'].iloc[-1]:.1f} MW")