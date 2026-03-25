
import sqlite3
import logging
from pathlib import Path

import pandas as pd

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
GENERATION_CSV  = Path("data/historical/smard_generation_2025.csv")
CONSUMPTION_CSV = Path("data/historical/smard_consumption_2025.csv")
DB_PATH         = Path("energy.db")

# SMARD generation column names → our internal names
GENERATION_COLS: dict[str, str] = {
    "Wind Onshore [MWh] Berechnete Auflösungen": "wind_onshore_mw",
    "Photovoltaik [MWh] Berechnete Auflösungen": "solar_mw",
}

# SMARD consumption column name → our internal name
CONSUMPTION_COL = "Netzlast [MWh] Berechnete Auflösungen"

# SMARD uses German number format: 32.875,75 means 32875.75
# Period = thousands separator, comma = decimal point
THOUSANDS_SEP = "."
DECIMAL_SEP   = ","


# Database setup
def create_historical_table(conn: sqlite3.Connection) -> None:
    """
    Creates the historical_data table if it does not already exist.

    Args:
        conn: An open SQLite connection.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS historical_data (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp        TEXT NOT NULL,
            wind_onshore_mw  REAL,
            solar_mw         REAL,
            load_mw          REAL,
            source           TEXT NOT NULL DEFAULT 'smard',
            UNIQUE(timestamp)
        )
    """)
    conn.commit()
    logger.info("historical_data table ready.")


# CSV parsing helpers
def parse_german_number(value: str) -> float:
    """
    Convert a German-formatted number string to a Python float.

    German format uses period as thousands separator and comma as decimal.
    Example: '32.875,75' → 32875.75
             '9,50'      → 9.5
             '-'         → NaN (missing value marker in SMARD files)

    Args:
        value: A string number in German format.

    Returns:
        A Python float, or NaN if the value is missing/unparseable.
    """
    if not isinstance(value, str):
        return float("nan")
    value = value.strip()
    if value in ("-", "", "–"):
        return float("nan")
    # Remove thousands separator, replace decimal comma with point
    value = value.replace(THOUSANDS_SEP, "").replace(DECIMAL_SEP, ".")
    try:
        return float(value)
    except ValueError:
        return float("nan")


def parse_smard_timestamp(value: str) -> str:
    """
    Convert a SMARD timestamp string to ISO format.

    SMARD format: '01.01.2025 00:00'
    Output format: '2025-01-01T00:00:00'

    Args:
        value: Timestamp string from SMARD CSV.

    Returns:
        ISO format timestamp string.
    """
    dt = pd.to_datetime(value, format="%d.%m.%Y %H:%M")
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


# CSV loading
def load_generation_csv(csv_path: Path) -> pd.DataFrame:
    """
    Load wind onshore and solar data from the SMARD generation CSV.

    Args:
        csv_path: Path to smard_generation_2025.csv

    Returns:
        DataFrame with columns [timestamp, wind_onshore_mw, solar_mw]

    Raises:
        FileNotFoundError: If the CSV does not exist.
    """
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Generation CSV not found at {csv_path}. "
            "Please download from SMARD and save to data/historical/"
        )

    logger.info(f"Reading generation CSV: {csv_path}...")

    df = pd.read_csv(
        csv_path,
        sep=";",
        encoding="utf-8-sig",  # handles BOM character SMARD sometimes adds
        dtype=str,             # read everything as string first — we parse numbers manually
    )

    # Keep only the columns we need
    cols_needed = ["Datum von"] + list(GENERATION_COLS.keys())
    missing = [c for c in cols_needed if c not in df.columns]
    if missing:
        raise KeyError(
            f"Expected columns not found in generation CSV: {missing}\n"
            f"Available columns: {list(df.columns)}"
        )

    df = df[cols_needed].copy()

    # Parse timestamp
    df["timestamp"] = df["Datum von"].apply(parse_smard_timestamp)

    # Parse generation values
    for smard_col, internal_col in GENERATION_COLS.items():
        df[internal_col] = df[smard_col].apply(parse_german_number)

    df = df[["timestamp", "wind_onshore_mw", "solar_mw"]]

    # Drop rows where both generation values are NaN
    df = df.dropna(subset=["wind_onshore_mw", "solar_mw"], how="all")

    logger.info(f"Generation CSV loaded: {len(df):,} rows.")
    return df


def load_consumption_csv(csv_path: Path) -> pd.DataFrame:
    """
    Load grid load (consumption) data from the SMARD consumption CSV.

    Args:
        csv_path: Path to smard_consumption_2025.csv

    Returns:
        DataFrame with columns [timestamp, load_mw]

    Raises:
        FileNotFoundError: If the CSV does not exist.
    """
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Consumption CSV not found at {csv_path}. "
            "Please download from SMARD and save to data/historical/"
        )

    logger.info(f"Reading consumption CSV: {csv_path}...")

    df = pd.read_csv(
        csv_path,
        sep=";",
        encoding="utf-8-sig",
        dtype=str,
    )

    if CONSUMPTION_COL not in df.columns:
        raise KeyError(
            f"Expected column '{CONSUMPTION_COL}' not found in consumption CSV.\n"
            f"Available columns: {list(df.columns)}"
        )

    df = df[["Datum von", CONSUMPTION_COL]].copy()

    df["timestamp"] = df["Datum von"].apply(parse_smard_timestamp)
    df["load_mw"]   = df[CONSUMPTION_COL].apply(parse_german_number)
    df = df[["timestamp", "load_mw"]]
    df = df.dropna(subset=["load_mw"])

    logger.info(f"Consumption CSV loaded: {len(df):,} rows.")
    return df


# Merge and insert
def merge_and_insert(
    conn: sqlite3.Connection,
    gen_df: pd.DataFrame,
    con_df: pd.DataFrame,
) -> int:
    """
    Merge generation and consumption DataFrames on timestamp,
    then insert into historical_data table using INSERT OR IGNORE.

    Args:
        conn:   An open SQLite connection.
        gen_df: Generation DataFrame [timestamp, wind_onshore_mw, solar_mw]
        con_df: Consumption DataFrame [timestamp, load_mw]

    Returns:
        Number of rows actually inserted.
    """
    # Merge on timestamp — inner join keeps only rows present in both files
    merged = gen_df.merge(con_df, on="timestamp", how="inner")
    merged["source"] = "smard"

    logger.info(f"Merged dataset: {len(merged):,} rows.")

    before = conn.execute("SELECT COUNT(*) FROM historical_data").fetchone()[0]

    conn.executemany(
        """
        INSERT OR IGNORE INTO historical_data
            (timestamp, wind_onshore_mw, solar_mw, load_mw, source)
        VALUES (?, ?, ?, ?, ?)
        """,
        merged[["timestamp", "wind_onshore_mw", "solar_mw", "load_mw", "source"]]
        .itertuples(index=False, name=None),
    )
    conn.commit()

    after = conn.execute("SELECT COUNT(*) FROM historical_data").fetchone()[0]
    inserted = after - before
    logger.info(f"Inserted {inserted:,} new rows ({before:,} → {after:,} total).")
    return inserted

# Main
def main() -> None:
    """
    Load SMARD generation and consumption CSVs into historical_data table.
    """
    try:
        gen_df = load_generation_csv(GENERATION_CSV)
        con_df = load_consumption_csv(CONSUMPTION_CSV)
    except (FileNotFoundError, KeyError) as e:
        logger.error(str(e))
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        create_historical_table(conn)
        inserted = merge_and_insert(conn, gen_df, con_df)
        conn.close()
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return

    logger.info(
        f"Done. Verify with: python -c \"import sqlite3; "
        f"c=sqlite3.connect('energy.db'); "
        f"print(c.execute('SELECT COUNT(*) FROM historical_data').fetchone())\""
    )


if __name__ == "__main__":
    main()