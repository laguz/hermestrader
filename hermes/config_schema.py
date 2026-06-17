from __future__ import annotations

from typing import Literal
from pydantic import BaseModel, Field, field_validator


class RuntimeConfig(BaseModel):
    obp_reserve: float = Field(
        default=0.0,
        description="Options buying power reserve (dollar amount of BP that must be kept free)"
    )
    tick_interval: int = Field(
        default=300,
        description="Core engine execution loop tick interval in seconds"
    )
    bandit_tuner_mode: Literal["off", "shadow", "active"] = Field(
        default="off",
        description="Contextual bandit tuner mode"
    )
    exit_policy_mode: Literal["off", "shadow", "active"] = Field(
        default="off",
        description="Exit policy reinforcement learning / rule mode"
    )

    @field_validator("obp_reserve")
    @classmethod
    def validate_obp_reserve(cls, v: float) -> float:
        if v < 0:
            raise ValueError("obp_reserve must be non-negative")
        return v

    @field_validator("tick_interval")
    @classmethod
    def validate_tick_interval(cls, v: int) -> int:
        if v < 1:
            raise ValueError("tick_interval must be at least 1 second")
        return v
