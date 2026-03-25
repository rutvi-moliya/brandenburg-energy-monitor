import logging
import time
from datetime import datetime, date
from ai_summary import load_latest_summary


from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from config_loader import get_config
from database import init_db, save_energy_data, load_energy_data
from fetcher import fetch_all_metrics
from anomaly import run_anomaly_detection

logger = logging.getLogger(__name__)

# Module-level scheduler instance — only one should ever exist
_scheduler: BackgroundScheduler | None = None

# Job 1 - Hourly fetch and anomaly detection
def fetch_and_store() -> None:
    """
    Fetch live energy data from SMARD, save to database, and run
    anomaly detection. This is the core hourly pipeline.

    Called automatically by APScheduler every N minutes.
    Also safe to call manually for testing.

    Returns:
        None
    """
    logger.info("Scheduled fetch started.")
    start_time = time.time()

    try:
        # Step 1 - fetch live data from SMARD API
        data = fetch_all_metrics()

        if not data:
            logger.warning("fetch_all_metrics() returned no data. Skipping this run.")
            return

        # Step 2 - save to database
        total_saved = 0
        for metric, df in data.items():
            if df is not None and not df.empty:
                saved = save_energy_data(df)
                total_saved += saved

        logger.info(f"Fetch complete. Saved {total_saved} new rows total.")

        # Step 3 - run anomaly detection on freshly saved data
        # Reload from database so anomaly detection sees the full history
        live_data = {}
        for metric in ["wind_onshore", "solar", "consumption"]:
            df = load_energy_data(metric)
            if df is not None and not df.empty:
                live_data[metric] = df

        if live_data:
            anomaly_results = run_anomaly_detection(live_data)
            total_anomalies = sum(
                len(df) for df in anomaly_results.values() if not df.empty
            )
            if total_anomalies > 0:
                logger.info(f"Anomaly detection complete. {total_anomalies} new anomalies found.")
            else:
                logger.info("Anomaly detection complete. No anomalies this run.")

    except Exception as e:
        # Broad exception catch here is intentional — we never want a scheduler
        # job to crash silently. We log the error and let the next run try again.
        logger.error(f"fetch_and_store() failed: {e}", exc_info=True)

    elapsed = time.time() - start_time
    logger.info(f"Scheduled fetch finished in {elapsed:.1f}s.")

# Job 2 - Daily AI summary (imported lazily to avoid circular imports)
def generate_daily_summary() -> None:
    """
    Generate an AI-powered summary of today's energy data using GPT-4o-mini.
    Saves the result to the database.

    Called automatically by APScheduler once per day at the configured hour.
    Also safe to call manually for testing.

    Returns:
        None
    """
    logger.info("Daily AI summary job started.")

    try:
        # Import here to avoid circular import issues at module load time
        from ai_summary import create_and_save_summary
        create_and_save_summary()
    except Exception as e:
        logger.error(f"generate_daily_summary() failed: {e}", exc_info=True)


# Scheduler lifecycle
def start_scheduler() -> BackgroundScheduler:
    """
    Initialise and start the APScheduler BackgroundScheduler.

    Creates two jobs:
        1. fetch_and_store()       — runs every fetch_interval_minutes
        2. generate_daily_summary() — runs daily at scheduler.summary_hour

    Safe to call multiple times — will not create duplicate schedulers.
    The scheduler runs as a daemon thread and stops when the app exits.

    Returns:
        The running BackgroundScheduler instance.
    """
    global _scheduler

    # Guard against starting multiple schedulers
    if _scheduler is not None and _scheduler.running:
        logger.info("Scheduler already running. Skipping start.")
        return _scheduler

    config = get_config()
    interval_minutes = config["scheduler"]["fetch_interval_minutes"]
    summary_hour     = config["scheduler"]["summary_hour"]

    # Initialise database before starting jobs
    init_db()

    _scheduler = BackgroundScheduler(
        job_defaults={
            "coalesce": True,       # if a job is missed, run it once — not multiple times
            "max_instances": 1,     # never run more than one instance of the same job at once
        }
    )

    # Job 1 - hourly fetch
    _scheduler.add_job(
        func=fetch_and_store,
        trigger=IntervalTrigger(minutes=interval_minutes),
        id="fetch_and_store",
        name="Hourly SMARD fetch + anomaly detection",
        replace_existing=True,
        next_run_time=datetime.now(),   # run immediately on startup, then every hour
    )

    # Job 2 - daily AI summary
    _scheduler.add_job(
        func=generate_daily_summary,
        trigger=CronTrigger(hour=summary_hour, minute=0),
        id="daily_summary",
        name="Daily AI summary generation",
        replace_existing=True,
    )
    # Generate today's summary on startup if it hasn't been done yet

    latest = load_latest_summary()
    today = date.today().strftime("%Y-%m-%d")
    if latest is None or latest["summary_date"] != today:
        logger.info("No summary for today found on startup — generating now.")
        _scheduler.add_job(
            func=generate_daily_summary,
            trigger="date",  # run once immediately
            id="startup_summary",
            name="Startup summary catch-up",
    )
    _scheduler.start()

    logger.info(
        f"Scheduler started. "
        f"Fetch every {interval_minutes} min. "
        f"AI summary daily at {summary_hour:02d}:00."
    )

    return _scheduler


def stop_scheduler() -> None:
    """
    Gracefully stop the scheduler if it is running.
    Called automatically when the process exits, but can be called manually.

    Returns:
        None
    """
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")

# Manual test - run this file directly to test one fetch cycle
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    print("Running one manual fetch cycle to test the pipeline...")
    print("(This is not the scheduler — just a single test run)\n")

    fetch_and_store()

    print("\nPipeline test complete. Check logs above for results.")