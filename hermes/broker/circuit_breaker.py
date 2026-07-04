from __future__ import annotations

import time
import logging
from typing import Any

logger = logging.getLogger("hermes.broker.circuit_breaker")


class CircuitBreakerError(Exception):
    """Exception raised when the circuit breaker blocks operations or trips."""
    pass


class CircuitBreaker:
    """Stateful Circuit Breaker for order placement protection.
    
    States: CLOSED, OPEN, HALF-OPEN.
    Automatically trips to OPEN and pauses the trading agent if the threshold
    (3 consecutive failures) is reached.
    """
    def __init__(self, failure_threshold: int = 3, cooldown_s: float = 60.0):
        self.failure_threshold = failure_threshold
        self.cooldown_s = cooldown_s
        self.state = "CLOSED"
        self.failure_count = 0
        self.last_state_change = time.time()

    def check_state(self) -> str:
        """Evaluate state and handle OPEN -> HALF-OPEN transition after cooldown."""
        if self.state == "OPEN":
            if time.time() - self.last_state_change >= self.cooldown_s:
                logger.info("Circuit breaker cooldown expired. Transitioning from OPEN to HALF-OPEN.")
                self.state = "HALF-OPEN"
                self.last_state_change = time.time()
        return self.state

    def record_success(self):
        """Record a successful operation. Recovers the circuit from HALF-OPEN."""
        if self.state == "HALF-OPEN":
            logger.info("Circuit breaker recovered! Transitioning to CLOSED.")
            self.state = "CLOSED"
            self.last_state_change = time.time()
        self.failure_count = 0

    async def record_failure(self, db: Any, error_msg: str = ""):
        """Record a failed operation. Trips the circuit if threshold is reached."""
        self.failure_count += 1
        logger.warning(
            "Circuit breaker recorded failure #%d in state %s. Error: %s",
            self.failure_count, self.state, error_msg
        )
        if self.state == "CLOSED" and self.failure_count >= self.failure_threshold:
            await self._trip(db)
        elif self.state == "HALF-OPEN":
            await self._trip(db)

    async def _trip(self, db: Any):
        logger.critical("Circuit breaker tripped! Transitioning to OPEN.")
        self.state = "OPEN"
        self.last_state_change = time.time()
        
        if db is not None:
            try:
                # Set agent_paused to true in DB settings and write a critical log
                await db.settings.set_setting("agent_paused", "true")
                await db.logs.write_log("ENGINE", "[CRITICAL] Circuit Breaker tripped: Agent automatically PAUSED", level="ERROR")
            except Exception as e:
                logger.error("Failed to automatically pause agent in database: %s", e)
        else:
            logger.warning(
                "Circuit breaker tripped with no db handle — agent was NOT automatically paused."
            )
