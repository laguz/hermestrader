from __future__ import annotations

import os
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from hermes.common import VALID_MODES


class HermesSettings(BaseSettings):
    hermes_mode: str = Field(default="paper")
    hermes_dsn: str = Field(default="postgresql+psycopg://hermes:hermes@db:5432/hermes")
    hermes_redis_dsn: str = Field(default="redis://localhost:6379/0")
    hermes_tick_interval: int = Field(default=3600)
    hermes_watchlist: str = Field(default="AAPL,SPY,QQQ,NVDA,AMD,KO")
    hermes_dry_run: bool = Field(default=True)
    hermes_use_mcp_broker: bool = Field(default=True)
    hermes_version: str = Field(default="dev")

    # Tradier generic credentials
    tradier_access_token: Optional[str] = None
    tradier_account_id: Optional[str] = None
    tradier_base_url: Optional[str] = None

    # Mode-specific Tradier overrides
    tradier_paper_token: Optional[str] = None
    tradier_paper_account_id: Optional[str] = None
    tradier_paper_base_url: str = "https://sandbox.tradier.com/v1"

    tradier_live_token: Optional[str] = None
    tradier_live_account_id: Optional[str] = None
    tradier_live_base_url: str = "https://api.tradier.com/v1"

    # LLM Settings fallback defaults
    llm_base_url: str = ""

    model_config = SettingsConfigDict(
        env_file=os.environ.get("HERMES_ENV_FILE", ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @field_validator("hermes_mode")
    @classmethod
    def validate_mode(cls, v: str) -> str:
        v_clean = v.lower().strip()
        if v_clean not in VALID_MODES:
            raise ValueError(f"mode must be one of {VALID_MODES}")
        return v_clean


    @property
    def watchlist_list(self) -> list[str]:
        return [s.strip() for s in self.hermes_watchlist.split(",") if s.strip()]

    def get_tradier_credentials(self) -> tuple[str, str, str]:
        """Resolve token, account_id, and base_url for the active hermes_mode."""
        mode = self.hermes_mode
        if mode == "paper":
            token = self.tradier_paper_token or self.tradier_access_token
            account = self.tradier_paper_account_id or self.tradier_account_id
            url = self.tradier_paper_base_url or self.tradier_base_url or "https://sandbox.tradier.com/v1"
        else:
            token = self.tradier_live_token or self.tradier_access_token
            account = self.tradier_live_account_id or self.tradier_account_id
            url = self.tradier_live_base_url or self.tradier_base_url or "https://api.tradier.com/v1"

        if not token or not account:
            # Fall back to checking raw environment if not populated via pydantic (e.g. dynamic changes)
            token = token or os.environ.get("TRADIER_ACCESS_TOKEN") or os.environ.get("TRADIER_API_KEY")
            account = account or os.environ.get("TRADIER_ACCOUNT_ID")
            if not token or not account:
                raise RuntimeError(
                    f"Missing Tradier credentials for mode={mode!r}. Set TRADIER_{mode.upper()}_TOKEN "
                    f"and TRADIER_{mode.upper()}_ACCOUNT_ID, or TRADIER_ACCESS_TOKEN and TRADIER_ACCOUNT_ID."
                )
        return token, account, url


# Export a global singleton settings instance
settings = HermesSettings()
