from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any, Dict
from hermes.common import (
    DEFAULT_OVERSEER_MODE,
    STRATEGY_PRIORITIES,
    normalize_overseer_mode,
)
from hermes.db.events import (
    BaseEvent,
    WatchlistChangedEvent,
    ModeChangedEvent,
    StrategyToggledEvent,
    AutonomyChangedEvent,
    PauseChangedEvent,
    ApprovalDecidedEvent,
    DoctrineUpdatedEvent,
    SystemSettingChangedEvent,
)

logger = logging.getLogger("hermes.service1_agent.control_state")

# How stale (seconds) control state may get before the clock-tick backstop
# re-hydrates it from the DB. Settings normally arrive as events; this is the
# safety net for a dropped Redis pub/sub message, so it runs on the slow clock cadence
# rather than on every reactive tick.
CONTROL_STATE_BACKSTOP_S = 60


class ControlState:
    def __init__(self):
        self.mode = "paper"
        self.paused = False
        self.autonomy = "advisory"
        self.soul = ""
        self.approval_mode = True
        # Default-OFF gate for the no-human-in-the-loop autonomous HermesAlpha
        # live path. Even at autonomy=='autonomous', Alpha entries only skip the
        # human approval queue when this is explicitly ON (see CLAUDE.md rule #2).
        self.alpha_autonomous_live = False
        self.llm_out_of_loop = True
        self.overseer_mode = DEFAULT_OVERSEER_MODE
        self.strategy_enabled = {sid: True for sid in STRATEGY_PRIORITIES}
        self.watchlist = {sid: [] for sid in STRATEGY_PRIORITIES}
        self.llm_config = {
            "provider": "mock",
            "base_url": "",
            "model": "",
            "temperature": 0.2,
            "timeout_s": 30.0,
            "vision": True,
            "api_key": "",
        }
        self.max_daily_loss = None
        self.lot_settings = {
            "cs75_target_lots": 10, "cs75_max_lots": 10,
            "cs7_target_lots":  10, "cs7_max_lots":  10,
            "tt45_target_lots":  5, "tt45_max_lots":  5,
            "wheel_max_lots":    5,
        }
        self.pending_order_ttl_s = 3600
        self.last_sync_ts = None

    def update_with_event(self, event: BaseEvent) -> None:
        if isinstance(event, ModeChangedEvent):
            self.mode = event.mode.lower()
            logger.info("[ControlState] Mode updated reactively to: %s", self.mode)
        elif isinstance(event, PauseChangedEvent):
            self.paused = event.paused
            logger.info("[ControlState] Paused status updated reactively to: %s", self.paused)
        elif isinstance(event, AutonomyChangedEvent):
            self.autonomy = event.autonomy.lower()
            logger.info("[ControlState] Autonomy updated reactively to: %s", self.autonomy)
        elif isinstance(event, DoctrineUpdatedEvent):
            self.soul = event.doctrine_text
            logger.info("[ControlState] Soul doctrine updated reactively (%d bytes)", len(self.soul.encode()))
        elif isinstance(event, StrategyToggledEvent):
            self.strategy_enabled[event.strategy_id] = event.enabled
            logger.info("[ControlState] Strategy %s enabled status: %s", event.strategy_id, event.enabled)
        elif isinstance(event, WatchlistChangedEvent):
            self.watchlist[event.strategy_id] = event.symbols
            logger.info("[ControlState] Watchlist updated reactively for %s: %s", event.strategy_id, event.symbols)
        elif isinstance(event, ApprovalDecidedEvent):
            logger.info("[ControlState] Approval decided: id=%d status=%s", event.approval_id, event.status)
        elif isinstance(event, SystemSettingChangedEvent):
            self._update_setting(event.key, event.value)

    def _update_setting(self, key: str, value: str) -> None:
        if key == "hermes_mode":
            self.mode = value.lower()
        elif key == "agent_paused":
            self.paused = (value.lower() == "true")
        elif key == "agent_autonomy":
            self.autonomy = value.lower()
        elif key == "approval_mode":
            self.approval_mode = (value.lower() == "true")
        elif key == "alpha_autonomous_live":
            self.alpha_autonomous_live = (value.lower() == "true")
        elif key == "llm_out_of_loop":
            self.llm_out_of_loop = (value.lower() == "true")
        elif key == "overseer_mode":
            self.overseer_mode = normalize_overseer_mode(value)
        elif key == "max_daily_loss":
            try:
                self.max_daily_loss = float(value) if value else None
            except ValueError:
                self.max_daily_loss = None
        elif key == "pending_order_ttl_s":
            try:
                self.pending_order_ttl_s = int(value)
            except ValueError:
                self.pending_order_ttl_s = 3600
        elif key in self.lot_settings:
            try:
                self.lot_settings[key] = int(value)
            except ValueError:
                pass
        elif key.startswith("llm_"):
            field = key.replace("llm_", "")
            if field in self.llm_config:
                if field in ("temperature", "timeout_s"):
                    try:
                        self.llm_config[field] = float(value)
                    except ValueError:
                        pass
                elif field == "vision":
                    self.llm_config[field] = (value.lower() == "true")
                else:
                    self.llm_config[field] = value

    async def load_from_db(self, db, conf: Dict[str, Any]) -> None:
        settings = await db.settings.get_settings(
            ["hermes_mode", "agent_paused", "agent_autonomy", "approval_mode",
             "alpha_autonomous_live",
             "llm_out_of_loop", "overseer_mode", "max_daily_loss", "pending_order_ttl_s"]
            + list(self.lot_settings.keys())
            + [f"strategy_{sid.lower()}_enabled" for sid in STRATEGY_PRIORITIES]
        )
        self.mode = (settings.get("hermes_mode") or conf.get("hermes_mode") or "paper").lower()
        self.paused = settings.get("agent_paused", "false").lower() == "true"
        self.autonomy = settings.get("agent_autonomy", "advisory").lower()
        self.approval_mode = settings.get("approval_mode", "true").lower() == "true"
        self.alpha_autonomous_live = settings.get("alpha_autonomous_live", "false").lower() == "true"
        self.llm_out_of_loop = settings.get("llm_out_of_loop", "true").lower() == "true"
        self.overseer_mode = normalize_overseer_mode(settings.get("overseer_mode"))
        
        try:
            self.max_daily_loss = float(settings["max_daily_loss"]) if settings.get("max_daily_loss") else None
        except (KeyError, ValueError):
            self.max_daily_loss = None
            
        try:
            self.pending_order_ttl_s = int(settings["pending_order_ttl_s"]) if settings.get("pending_order_ttl_s") else 3600
        except (KeyError, ValueError):
            self.pending_order_ttl_s = 3600
            
        for k in self.lot_settings:
            if k in settings:
                try:
                    self.lot_settings[k] = int(settings[k])
                except ValueError:
                    pass
                    
        for sid in STRATEGY_PRIORITIES:
            enabled_key = f"strategy_{sid.lower()}_enabled"
            self.strategy_enabled[sid] = settings.get(enabled_key, "true").lower() != "false"
            
        # Load soul
        self.soul = await db.settings.get_setting("soul_md") or ""
        
        # Load LLM config
        llm_keys = ["llm_provider", "llm_base_url", "llm_model", "llm_api_key", "llm_temperature", "llm_vision", "llm_timeout_s"]
        llm_settings = await db.settings.get_settings(llm_keys)
        self.llm_config["provider"] = (llm_settings.get("llm_provider") or "mock").lower()
        self.llm_config["base_url"] = (llm_settings.get("llm_base_url") or "").strip()
        self.llm_config["model"] = (llm_settings.get("llm_model") or "").strip()
        from hermes.utils import decrypt_value
        self.llm_config["api_key"] = decrypt_value((llm_settings.get("llm_api_key") or "").strip())
        temp_val = llm_settings.get("llm_temperature")
        if temp_val is not None and str(temp_val).strip() != "":
            try:
                self.llm_config["temperature"] = float(temp_val)
            except ValueError:
                self.llm_config["temperature"] = 0.2
        else:
            self.llm_config["temperature"] = 0.2

        timeout_val = llm_settings.get("llm_timeout_s")
        if timeout_val is not None and str(timeout_val).strip() != "":
            try:
                self.llm_config["timeout_s"] = float(timeout_val)
            except ValueError:
                self.llm_config["timeout_s"] = 30.0
        else:
            self.llm_config["timeout_s"] = 30.0
        self.llm_config["vision"] = llm_settings.get("llm_vision", "true").lower() != "false"
        
        # Load watchlists
        self.watchlist = await db.watchlist.list_all_watchlists()

        self.last_sync_ts = datetime.now(timezone.utc)
        logger.info("[ControlState] Loaded settings and watchlists from DB successfully.")
