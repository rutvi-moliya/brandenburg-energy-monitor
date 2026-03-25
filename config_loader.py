import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = Path("config.yaml")

# Cache the config after first load — no need to re-read the file every call
_config_cache: dict | None = None


def get_config() -> dict:
    """
    Load config.yaml and return it as a dictionary.
    Uses a module-level cache so the file is only read once per session.

    Returns:
        Dictionary of all configuration values.

    Raises:
        FileNotFoundError: If config.yaml does not exist.
        ValueError: If any required value is missing or invalid.
    """
    global _config_cache

    if _config_cache is not None:
        return _config_cache

    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            "config.yaml not found. "
            "Make sure it exists in the project root directory."
        )

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    _validate_config(config)

    _config_cache = config
    logger.info("Configuration loaded and validated successfully.")
    return config


def _validate_config(config: dict) -> None:
    """
    Validate that all required config values are present and have correct types.

    Args:
        config: The raw dictionary loaded from config.yaml.

    Raises:
        ValueError: If any value is missing, wrong type, or out of range.
    """
    errors = []

    # Helper to safely get nested values
    def get(keys: str, expected_type: type) -> Any:
        parts = keys.split(".")
        val = config
        try:
            for part in parts:
                val = val[part]
        except (KeyError, TypeError):
            errors.append(f"Missing required config value: {keys}")
            return None

        if not isinstance(val, expected_type):
            errors.append(
                f"Config value '{keys}' must be {expected_type.__name__}, "
                f"got {type(val).__name__}: {val!r}"
            )
            return None
        return val

    # Validate each required value
    get("database.path",                    str)
    get("smard.base_url",                   str)
    get("smard.request_timeout",            int)
    get("smard.sleep_between_requests",     int)
    get("anomaly.sigma_threshold",          float)
    get("anomaly.min_historical_rows",      int)
    get("scheduler.fetch_interval_minutes", int)
    get("scheduler.summary_hour",           int)
    get("ai.model",                         str)
    get("ai.max_tokens",                    int)
    get("ai.temperature",                   float)

    # Range checks
    sigma = config.get("anomaly", {}).get("sigma_threshold")
    if sigma is not None and not (0.5 <= sigma <= 5.0):
        errors.append(
            f"anomaly.sigma_threshold must be between 0.5 and 5.0, got {sigma}"
        )

    interval = config.get("scheduler", {}).get("fetch_interval_minutes")
    if interval is not None and not (1 <= interval <= 1440):
        errors.append(
            f"scheduler.fetch_interval_minutes must be between 1 and 1440, got {interval}"
        )

    summary_hour = config.get("scheduler", {}).get("summary_hour")
    if summary_hour is not None and not (0 <= summary_hour <= 23):
        errors.append(
            f"scheduler.summary_hour must be between 0 and 23, got {summary_hour}"
        )

    if errors:
        raise ValueError(
            "Configuration errors found in config.yaml:\n" +
            "\n".join(f"  - {e}" for e in errors)
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = get_config()
    print("Config loaded successfully.")
    print(f"  Database path:     {config['database']['path']}")
    print(f"  Sigma threshold:   {config['anomaly']['sigma_threshold']}")
    print(f"  Fetch interval:    {config['scheduler']['fetch_interval_minutes']} minutes")
    print(f"  AI model:          {config['ai']['model']}")