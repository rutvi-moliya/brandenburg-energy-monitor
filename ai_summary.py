import logging
import os
import sqlite3
from datetime import datetime, date
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

from config_loader import get_config
from database import DB_PATH, load_energy_data, load_anomalies

load_dotenv()

logger = logging.getLogger(__name__)


# Database setup - ai_summaries table
def create_summaries_table(conn: sqlite3.Connection) -> None:
    """
    Create the ai_summaries table if it does not already exist.

    Args:
        conn: An open SQLite connection.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ai_summaries (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            summary_date TEXT NOT NULL,
            summary_text TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            UNIQUE(summary_date)
        )
    """)
    conn.commit()
    logger.info("ai_summaries table ready.")


def save_summary(summary_date: str, summary_text: str) -> None:
    """
    Save a generated summary to the ai_summaries table.
    Uses INSERT OR REPLACE so re-running on the same day updates the summary.

    Args:
        summary_date: Date string in YYYY-MM-DD format.
        summary_text: The generated summary text from GPT-4o-mini.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        create_summaries_table(conn)
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        conn.execute("""
            INSERT OR REPLACE INTO ai_summaries
                (summary_date, summary_text, generated_at)
            VALUES (?, ?, ?)
        """, (summary_date, summary_text, generated_at))

        conn.commit()
        conn.close()
        logger.info(f"Summary saved for {summary_date}.")

    except sqlite3.Error as e:
        logger.error(f"Failed to save summary: {e}")


def load_latest_summary() -> Optional[dict]:
    """
    Load the most recently generated AI summary from the database.

    Returns:
        Dictionary with keys [summary_date, summary_text, generated_at],
        or None if no summary exists yet.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        create_summaries_table(conn)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT summary_date, summary_text, generated_at
            FROM ai_summaries
            ORDER BY summary_date DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                "summary_date": row[0],
                "summary_text": row[1],
                "generated_at": row[2],
            }
        return None

    except sqlite3.Error as e:
        logger.error(f"Failed to load latest summary: {e}")
        return None


# Statistics calculation
def calculate_daily_stats(target_date: date) -> dict:
    """
    Calculate min, max, and average for each metric for a given date.
    Only includes readings from that specific calendar date.

    Args:
        target_date: The date to calculate statistics for.

    Returns:
        Dictionary mapping metric name to its stats dict,
        e.g. {'wind_onshore': {'avg': 8450, 'max': 12300, 'min': 4200}, ...}
    """
    date_str = target_date.strftime("%Y-%m-%d")
    stats = {}

    for metric in ["wind_onshore", "solar", "consumption"]:
        df = load_energy_data(metric, limit=500)

        if df is None or df.empty:
            logger.warning(f"No data available for {metric}.")
            continue

        # Filter to target date only
        df["date"] = pd.to_datetime(df["datetime"]).dt.date
        day_df = df[df["date"] == target_date]

        if day_df.empty:
            logger.warning(f"No data for {metric} on {date_str}.")
            continue

        stats[metric] = {
            "avg": round(day_df["value_mw"].mean(), 1),
            "max": round(day_df["value_mw"].max(), 1),
            "min": round(day_df["value_mw"].min(), 1),
            "readings": len(day_df),
        }

    return stats


# Prompt builder
def build_prompt(stats: dict, anomalies_today: pd.DataFrame) -> str:
    """
    Build the user prompt sent to GPT-4o-mini.
    Only contains aggregated statistics — no raw data rows.

    Args:
        stats:          Daily statistics per metric from calculate_daily_stats().
        anomalies_today: DataFrame of anomalies detected today.

    Returns:
        A formatted prompt string ready to send to the OpenAI API.
    """
    today_str = date.today().strftime("%d %B %Y")

    lines = [f"Here is today's ({today_str}) energy data for Germany:\n"]

    metric_labels = {
        "wind_onshore": "Wind Onshore",
        "solar":        "Solar (Photovoltaic)",
        "consumption":  "Total Grid Consumption",
    }

    for metric, label in metric_labels.items():
        if metric in stats:
            s = stats[metric]
            lines.append(
                f"{label}:\n"
                f"  Average: {s['avg']:,.0f} MW\n"
                f"  Peak:    {s['max']:,.0f} MW\n"
                f"  Minimum: {s['min']:,.0f} MW\n"
                f"  Hourly readings: {s['readings']}\n"
            )
        else:
            lines.append(f"{label}: No data available today.\n")

    # Add anomaly information if any exist
    if anomalies_today is not None and not anomalies_today.empty:
        lines.append(f"\nAnomalies detected today: {len(anomalies_today)}")
        for _, row in anomalies_today.iterrows():
            lines.append(
                f"  - {metric_labels.get(row['metric'], row['metric'])} "
                f"at {row['datetime']}: {row['value_mw']:,.0f} MW"
            )
    else:
        lines.append("\nNo anomalies detected today.")

    lines.append(
        "\nWrite a concise plain-English summary (max 150 words) of today's "
        "energy situation. Mention notable patterns, the balance between "
        "renewable generation and consumption, and any anomalies. "
        "Be specific with numbers."
    )

    return "\n".join(lines)



# OpenAI API call
def call_openai(prompt: str) -> Optional[str]:
    """
    Send the prompt to GPT-4o-mini and return the response text.

    Args:
        prompt: The user prompt built by build_prompt().

    Returns:
        The summary text from GPT-4o-mini, or None if the call fails.
    """
    config = get_config()
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        logger.error(
            "OPENAI_API_KEY not found in environment. "
            "Add it to your .env file."
        )
        return None

    client = OpenAI(api_key=api_key)

    system_message = (
        "You are an energy analyst assistant. "
        "Write concise, factual summaries of German electricity generation "
        "and consumption data. Be specific with numbers. "
        "Keep summaries under 150 words. "
        "Do not use bullet points — write in flowing prose."
    )

    # Log the prompt before sending so it can always be audited
    logger.info("Sending prompt to OpenAI (aggregated stats only):")
    logger.info(f"\n{prompt}")

    try:
        response = client.chat.completions.create(
            model=config["ai"]["model"],
            max_tokens=config["ai"]["max_tokens"],
            temperature=config["ai"]["temperature"],
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user",   "content": prompt},
            ],
        )

        summary = response.choices[0].message.content.strip()
        logger.info(f"OpenAI response received. Tokens used: {response.usage.total_tokens}")
        return summary

    except Exception as e:
        logger.error(f"OpenAI API call failed: {e}")
        return None


# Main entry point
def create_and_save_summary(target_date: Optional[date] = None) -> Optional[str]:
    """
    Full pipeline: calculate stats → build prompt → call OpenAI → save result.

    Args:
        target_date: Date to summarise. Defaults to today if not provided.

    Returns:
        The generated summary text, or None if generation failed.
    """
    if target_date is None:
        target_date = date.today()

    date_str = target_date.strftime("%Y-%m-%d")
    logger.info(f"Generating AI summary for {date_str}...")

    # Calculate today's statistics
    stats = calculate_daily_stats(target_date)

    if not stats:
        logger.warning(f"No statistics available for {date_str}. Skipping summary.")
        return None

    # Load today's anomalies
    anomalies_df = load_anomalies(limit=50)
    if anomalies_df is not None and not anomalies_df.empty:
        anomalies_df["date"] = pd.to_datetime(anomalies_df["datetime"]).dt.date
        anomalies_today = anomalies_df[anomalies_df["date"] == target_date]
    else:
        anomalies_today = pd.DataFrame()

    # Build and send prompt
    prompt = build_prompt(stats, anomalies_today)
    summary = call_openai(prompt)

    if summary is None:
        logger.error("Summary generation failed.")
        return None

    # Save to database
    save_summary(date_str, summary)

    return summary


# Manual test
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    print("Generating AI summary for today...\n")
    summary = create_and_save_summary()

    if summary:
        print("\n--- Generated Summary ---")
        print(summary)
        print("\nSummary saved to database.")
    else:
        print("Summary generation failed. Check logs above.")