"""
config.py — single source of truth for all runtime constants.

Every value can be overridden with an env var prefixed BETCLIC_:
  BETCLIC_PORT=8080 python server/app.py
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BETCLIC_")

    # Server
    port: int = 5003

    # Database
    db_path: str = "betclic_prices.db"

    # Feed
    locale: str = "fr"
    reconnect_delay_s: float = 5.0
    max_match_age_h: float = 48.0
    markets: list[str] = ["ca_ftb_rslt", "ca_ftb_goa"]
    market_names: dict[str, str] = {
        "ca_ftb_rslt": "1X2",
        "ca_ftb_goa":  "O/U",
    }

    # Connection pool
    max_streams_per_host: int = 80
    feed_queue_maxsize: int = 20_000

    # Circuit breaker
    cb_max_failures: int = 5
    cb_reset_after_s: float = 300.0

    # Logging
    log_level: str = "INFO"


settings = Settings()
