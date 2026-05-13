"""
config.py — All runtime configuration via environment variables.

Prefix: BETCLIC_
File:   .env (auto-loaded if present)

Usage:
    from config import settings
    print(settings.port)      # 5001
    print(settings.db_path)   # "betclic.db"
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="BETCLIC_",
        env_file=".env",
        extra="ignore",
    )

    # ── Server ──────────────────────────────────────────────────────────────
    port: int = 5001

    # ── Database ────────────────────────────────────────────────────────────
    db_path: str = "betclic.db"

    # ── Feed ────────────────────────────────────────────────────────────────
    locale: str = "fr"
    reconnect_delay_s: float = 5.0
    max_match_age_h: float = 48.0
    feed_queue_maxsize: int = 20_000
    match_refresh_s: float = 300.0   # 5 minutes

    # ── Circuit breaker ──────────────────────────────────────────────────────
    cb_max_failures: int = 5
    cb_reset_after_s: float = 300.0

    # ── Storage ──────────────────────────────────────────────────────────────
    db_flush_interval_s: float = 0.5
    db_batch_size: int = 100

    # ── SSE ──────────────────────────────────────────────────────────────────
    sse_queue_maxsize: int = 500
    sse_keepalive_s: float = 25.0

    # ── Connection pool ───────────────────────────────────────────────────────
    connection_pool_size: int = 20

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = "INFO"


settings = Settings()
