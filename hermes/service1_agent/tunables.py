"""
[Service-1: Hermes-Agent-Core]
Strategy tunables — one declarative catalog for every per-strategy parameter.

Before this module, tunables were scattered across three sources with
duplicated boilerplate: ``db.settings.get_setting()`` (DTE windows), ``self.config``
/ env (widths, deltas, lots) and bare hardcoded literals (POP targets,
every TP/SL/time-exit threshold, the delta bounds). That made it impossible
to see — let alone tune — the full parameter surface without reading four
strategy files.

Now every parameter is declared once in :data:`TUNABLES` with its spec
default, type and operator-facing metadata. :func:`resolve` reads overrides
from ``system_settings`` so a value can be retuned without a deploy, falling
back to env config and finally the spec default. The spec
defaults are exactly the literals the strategies used before, so behaviour
is unchanged until someone deliberately overrides a value.

Precedence (highest wins):  ``system_settings`` > ``env_config`` > default.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


# ---------------------------------------------------------------------------
# Catalog primitives
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Tunable:
    """One operator-tunable strategy parameter.

    ``cast`` coerces the stored string (settings values are TEXT) into the
    runtime type; a cast failure falls back to ``default`` rather than
    raising, so a malformed settings row can never crash a tick.
    ``min``/``max`` bound the value for API-side validation (advisory only —
    ``resolve`` does not clamp, it only type-coerces).
    """
    key: str
    default: Any
    cast: Callable[[Any], Any]
    group: str
    label: str
    min: Optional[float] = None
    max: Optional[float] = None
    help: str = ""

    def coerce(self, raw: Any) -> Any:
        try:
            return self.cast(raw)
        except (ValueError, TypeError):
            return self.default


def _f(key, default, group, label, *, min=None, max=None, help=""):
    return Tunable(key, default, float, group, label, min, max, help)


def _i(key, default, group, label, *, min=None, max=None, help=""):
    return Tunable(key, default, int, group, label, min, max, help)


def _s(key, default, group, label, *, help=""):
    return Tunable(key, default, str, group, label, None, None, help)


# ---------------------------------------------------------------------------
# THE CATALOG — defaults are exactly the literals the strategies used before.
# Grouped by strategy NAME so ``resolve(group="CS75")`` loads only one slice.
# ---------------------------------------------------------------------------
_CATALOG: List[Tunable] = [
    # ── CS75 (priority 1; 39–45 DTE iron condors) ──────────────────────────
    _f("cs75_width", 5.0, "CS75", "Spread width ($)", min=0.5, max=50,
       help="Strike distance between short and long legs."),
    _i("cs75_min_dte", 39, "CS75", "Entry DTE min", min=1, max=400,
       help="Earliest expiry (days) for a new Mode-A iron condor."),
    _i("cs75_max_dte", 45, "CS75", "Entry DTE max", min=1, max=400,
       help="Latest expiry (days) for entry / Mode-B completion."),
    _i("cs75_completion_min_dte", 14, "CS75", "Completion DTE floor", min=1, max=400,
       help="Below this DTE an incomplete IC is no longer completed."),
    _f("cs75_short_delta_min", 0.05, "CS75", "Short Δ floor", min=0.0, max=1.0,
       help="Reject short strikes with |Δ| below this (no premium)."),
    _f("cs75_short_delta_max", 0.40, "CS75", "Short Δ cap", min=0.0, max=1.0,
       help="Reject short strikes with |Δ| above this (assignment risk)."),
    _f("cs75_pop_target", 0.75, "CS75", "POP target", min=0.5, max=0.99,
       help="Pick the S/R level whose POP is closest to (and ≥) this."),
    _f("cs75_min_credit_pct_far", 0.25, "CS75", "Min credit % (30–45 DTE)", min=0.01, max=1.0,
       help="Required net credit as a fraction of width for 30–45 DTE."),
    _f("cs75_min_credit_pct_near", 0.20, "CS75", "Min credit % (<30 DTE)", min=0.01, max=1.0,
       help="Required net credit as a fraction of width for <30 DTE."),
    _f("cs75_tp_pct_far", 0.50, "CS75", "Take-profit % (21–45 DTE)", min=0.01, max=1.0,
       help="Close when debit ≤ this fraction of entry credit, 21–45 DTE."),
    _f("cs75_tp_pct_near", 0.25, "CS75", "Take-profit % (<21 DTE)", min=0.01, max=1.0,
       help="Close when debit ≤ this fraction of entry credit, <21 DTE."),
    _f("cs75_sl_mult", 2.5, "CS75", "Stop-loss multiple", min=1.0, max=10.0,
       help="Close when debit ≥ entry credit × this."),
    _i("cs75_time_exit_dte", 8, "CS75", "Time-exit DTE", min=0, max=60,
       help="Force-close at or below this many days to expiry."),
    _i("cs75_event_blackout_days", 7, "CS75", "Earnings blackout days", min=0, max=30,
       help="Days to look ahead for the symbol's earnings to block new entries."),
    _i("cs75_macro_blackout_days", 1, "CS75", "Macro blackout days", min=0, max=30,
       help="Days to look ahead for FOMC/CPI dates to block new entries."),
    _f("cs75_min_ivr", 0.0, "CS75", "Minimum IV Rank", min=0.0, max=100.0,
       help="Minimum implied volatility rank (0-100) required to open entries. Default 0 is off."),
    _i("cs75_throttle_window", 0, "CS75", "Throttle Window", min=0, max=100,
       help="Number of past closed predictions to monitor for underperformance (0 is OFF)."),
    _f("cs75_throttle_drift_threshold", 0.05, "CS75", "Throttle Drift Threshold", min=0.0, max=1.0,
       help="Win rate drift below predicted POP before throttle engages (e.g. 0.05 is 5%)."),
    _f("cs75_throttle_floor_mult", 1.0, "CS75", "Throttle Floor Multiplier", min=0.0, max=1.0,
       help="Sizing multiplier when the throttle engages. 1.0 = no reduction; "
            "fractional values shrink but never below 1 lot; 0.0 halts entries."),

    # ── CS7 (priority 2; ~7 DTE short-cycle spreads) ───────────────────────
    _f("cs7_width", 1.0, "CS7", "Spread width ($)", min=0.5, max=50,
       help="Strike distance between short and long legs."),
    _i("cs7_dte", 7, "CS7", "Entry DTE", min=1, max=60,
       help="Exact target DTE for a new Mode-A entry."),
    _i("cs7_completion_window", 3, "CS7", "Completion window (days)", min=1, max=30,
       help="Mode-B completes only within [dte-this, dte]."),
    _f("cs7_min_credit_pct", 0.12, "CS7", "Min credit %", min=0.01, max=1.0,
       help="Required net credit as a fraction of width."),
    _f("cs7_short_delta_min", 0.05, "CS7", "Short Δ floor", min=0.0, max=1.0,
       help="Reject short strikes with |Δ| below this."),
    _f("cs7_short_delta_max", 0.45, "CS7", "Short Δ cap", min=0.0, max=1.0,
       help="Reject short strikes with |Δ| above this (looser than CS75)."),
    _f("cs7_pop_target", 0.75, "CS7", "POP target", min=0.5, max=0.99,
       help="Pick the S/R level whose POP is closest to (and ≥) this."),
    _f("cs7_tp_pct_width", 0.02, "CS7", "Take-profit % of width", min=0.001, max=1.0,
       help="Close when debit ≤ this fraction of spread width."),
    _f("cs7_sl_mult", 3.0, "CS7", "Stop-loss multiple", min=1.0, max=10.0,
       help="Close when debit ≥ entry credit × this."),
    _i("cs7_event_blackout_days", 7, "CS7", "Earnings blackout days", min=0, max=30,
       help="Days to look ahead for the symbol's earnings to block new entries."),
    _i("cs7_macro_blackout_days", 1, "CS7", "Macro blackout days", min=0, max=30,
       help="Days to look ahead for FOMC/CPI dates to block new entries."),
    _f("cs7_min_ivr", 0.0, "CS7", "Minimum IV Rank", min=0.0, max=100.0,
       help="Minimum implied volatility rank (0-100) required to open entries. Default 0 is off."),
    _i("cs7_throttle_window", 0, "CS7", "Throttle Window", min=0, max=100,
       help="Number of past closed predictions to monitor for underperformance (0 is OFF)."),
    _f("cs7_throttle_drift_threshold", 0.05, "CS7", "Throttle Drift Threshold", min=0.0, max=1.0,
       help="Win rate drift below predicted POP before throttle engages (e.g. 0.05 is 5%)."),
    _f("cs7_throttle_floor_mult", 1.0, "CS7", "Throttle Floor Multiplier", min=0.0, max=1.0,
       help="Sizing multiplier when the throttle engages. 1.0 = no reduction; "
            "fractional values shrink but never below 1 lot; 0.0 halts entries."),

    # ── TT45 (priority 3; 16Δ verticals, 30–60 DTE) ────────────────────────
    _f("tt45_width", 5.0, "TT45", "Spread width ($)", min=0.5, max=50,
       help="Strike distance between short and long legs."),
    _i("tt45_min_dte", 30, "TT45", "Entry DTE min", min=1, max=400,
       help="Earliest expiry (days) for entry."),
    _i("tt45_max_dte", 60, "TT45", "Entry DTE max", min=1, max=400,
       help="Latest expiry (days) for entry."),
    _f("tt45_delta", 0.16, "TT45", "Short Δ target", min=0.0, max=1.0,
       help="Pick the short strike whose |Δ| is closest to this."),
    _f("tt45_delta_tol", 0.05, "TT45", "Short Δ tolerance", min=0.0, max=0.5,
       help="Accept strikes within ±this of the target delta."),
    _i("tt45_hard_exit_dte", 21, "TT45", "Hard-exit DTE", min=0, max=120,
       help="Force-close at or below this many days to expiry."),
    _f("tt45_challenged_delta", 0.30, "TT45", "Challenged Δ", min=0.0, max=1.0,
       help="Neutralise the side when the short's |Δ| exceeds this."),
    _i("tt45_event_blackout_days", 7, "TT45", "Earnings blackout days", min=0, max=30,
       help="Days to look ahead for the symbol's earnings to block new entries."),
    _i("tt45_macro_blackout_days", 1, "TT45", "Macro blackout days", min=0, max=30,
       help="Days to look ahead for FOMC/CPI dates to block new entries."),
    _f("tt45_min_ivr", 0.0, "TT45", "Minimum IV Rank", min=0.0, max=100.0,
       help="Minimum implied volatility rank (0-100) required to open entries. Default 0 is off."),
    _i("tt45_throttle_window", 0, "TT45", "Throttle Window", min=0, max=100,
       help="Number of past closed predictions to monitor for underperformance (0 is OFF)."),
    _f("tt45_throttle_drift_threshold", 0.05, "TT45", "Throttle Drift Threshold", min=0.0, max=1.0,
       help="Win rate drift below predicted POP before throttle engages (e.g. 0.05 is 5%)."),
    _f("tt45_throttle_floor_mult", 1.0, "TT45", "Throttle Floor Multiplier", min=0.0, max=1.0,
       help="Sizing multiplier when the throttle engages. 1.0 = no reduction; "
            "fractional values shrink but never below 1 lot; 0.0 halts entries."),

    # ── WHEEL (priority 4; CSP → assignment → covered call) ────────────────
    _f("wheel_delta", 0.30, "WHEEL", "Short Δ target", min=0.0, max=1.0,
       help="Anchor delta for wheel short strikes."),
    _f("wheel_delta_tol", 0.05, "WHEEL", "Short Δ tolerance", min=0.0, max=0.5,
       help="Accept strikes within ±this of the target delta."),
    _f("wheel_min_pop", 0.50, "WHEEL", "POP floor", min=0.0, max=0.99,
       help="Skip the entry when the chosen strike's 6M POP is below this; 0 disables."),
    _i("wheel_min_dte", 30, "WHEEL", "Entry DTE min", min=1, max=400,
       help="Earliest expiry (days) for wheel legs."),
    _i("wheel_max_dte", 45, "WHEEL", "Entry DTE max", min=1, max=400,
       help="Latest expiry (days) for wheel legs."),
    _i("wheel_roll_dte", 7, "WHEEL", "Roll DTE", min=0, max=60,
       help="Roll an ITM short below this many days to expiry."),
    _i("wheel_event_blackout_days", 0, "WHEEL", "Earnings blackout days", min=0, max=30,
       help="Days to look ahead for the symbol's earnings to block new entries."),
    _i("wheel_macro_blackout_days", 0, "WHEEL", "Macro blackout days", min=0, max=30,
       help="Days to look ahead for FOMC/CPI dates to block new entries."),
    _f("wheel_min_ivr", 0.0, "WHEEL", "Minimum IV Rank", min=0.0, max=100.0,
       help="Minimum implied volatility rank (0-100) required to open entries. Default 0 is off."),
    _i("wheel_throttle_window", 0, "WHEEL", "Throttle Window", min=0, max=100,
       help="Number of past closed predictions to monitor for underperformance (0 is OFF)."),
    _f("wheel_throttle_drift_threshold", 0.05, "WHEEL", "Throttle Drift Threshold", min=0.0, max=1.0,
       help="Win rate drift below predicted POP before throttle engages (e.g. 0.05 is 5%)."),
    _f("wheel_throttle_floor_mult", 1.0, "WHEEL", "Throttle Floor Multiplier", min=0.0, max=1.0,
       help="Sizing multiplier when the throttle engages. 1.0 = no reduction; "
            "fractional values shrink but never below 1 lot; 0.0 halts entries."),

    # ── Lot sizing (catalog visibility only) ───────────────────────────────
    # These keep their existing read path (routes/strategies.py _LOT_SPECS +
    # main.py lot refresh); listed here so the tunables API can surface them
    # read-alongside the rest. resolve() still honours them if asked.
    _i("cs75_target_lots", 1, "LOTS", "CS75 target lots", min=1, max=100),
    _i("cs75_max_lots", 1, "LOTS", "CS75 max lots", min=1, max=100),
    _i("cs7_target_lots", 1, "LOTS", "CS7 target lots", min=1, max=100),
    _i("cs7_max_lots", 1, "LOTS", "CS7 max lots", min=1, max=100),
    _i("tt45_target_lots", 5, "LOTS", "TT45 target lots", min=1, max=100),
    _i("tt45_max_lots", 5, "LOTS", "TT45 max lots", min=1, max=100),
    _i("wheel_max_lots", 5, "LOTS", "WHEEL max lots", min=1, max=100),
    _i("ds0_target_lots", 1, "LOTS", "DS0 target lots", min=1, max=100),
    _i("ds0_max_lots", 1, "LOTS", "DS0 max lots", min=0, max=100),

    # ── HERMESALPHA (priority 5; LLM-originated credit spreads) ────────────
    _i("hermesalpha_width", 5, "HERMESALPHA", "Spread width ($)", min=1, max=50,
       help="Dollar-width of the credit spread."),
    _i("hermesalpha_target_lots", 1, "HERMESALPHA", "Target lots", min=1, max=100),
    _i("hermesalpha_max_lots", 1, "HERMESALPHA", "Max lots", min=0, max=100),
    _f("hermesalpha_min_credit_pct", 0.20, "HERMESALPHA", "Min credit %", min=0.0, max=1.0,
       help="Reject spreads collecting less than this fraction of width."),
    _i("hermesalpha_time_exit_dte", 2, "HERMESALPHA", "Time-exit DTE", min=0, max=30,
       help="Close the position when DTE falls to this."),
    _f("hermesalpha_sl_mult", 2.5, "HERMESALPHA", "Stop-loss multiplier", min=1.0, max=10.0,
       help="Close when debit exceeds credit times this."),
    _i("hermesalpha_event_blackout_days", 7, "HERMESALPHA", "Earnings blackout days", min=0, max=30,
       help="Days to look ahead for the symbol's earnings to block new entries."),
    _i("hermesalpha_macro_blackout_days", 1, "HERMESALPHA", "Macro blackout days", min=0, max=30,
       help="Days to look ahead for FOMC/CPI dates to block new entries."),
    _f("hermesalpha_min_ivr", 0.0, "HERMESALPHA", "Minimum IV Rank", min=0.0, max=100.0,
       help="Minimum implied volatility rank (0-100) required to open entries. Default 0 is off."),
    _i("hermesalpha_throttle_window", 0, "HERMESALPHA", "Throttle Window", min=0, max=100,
       help="Number of past closed predictions to monitor for underperformance (0 is OFF)."),
    _f("hermesalpha_throttle_drift_threshold", 0.05, "HERMESALPHA", "Throttle Drift Threshold", min=0.0, max=1.0,
       help="Win rate drift below predicted POP before throttle engages (e.g. 0.05 is 5%)."),
    _f("hermesalpha_throttle_floor_mult", 1.0, "HERMESALPHA", "Throttle Floor Multiplier", min=0.0, max=1.0,
       help="Sizing multiplier when the throttle engages. 1.0 = no reduction; "
            "fractional values shrink but never below 1 lot; 0.0 halts entries."),

    # ── DS0 (priority 6; 0 DTE S/R-reversion debit spreads, docs/ds0_spec.md) ─
    _f("ds0_open_price", 0.10, "DS0", "Max entry debit ($)", min=0.01, max=5.0,
       help="Day-limit price for the entry; never repriced or chased."),
    _f("ds0_close_price", 0.40, "DS0", "Close limit ($)", min=0.01, max=10.0,
       help="Resting take-profit credit placed as soon as the entry fills."),
    _f("ds0_pop_target", 0.75, "DS0", "POP floor", min=0.5, max=0.99,
       help="Min 3m POP that the qualifying S/R level holds."),
    _f("ds0_width", 1.0, "DS0", "Spread width ($)", min=0.5, max=50,
       help="Strike distance between long and short legs."),
    _i("ds0_atr_period", 14, "DS0", "ATR period (days)", min=2, max=60,
       help="Wilder ATR over this many completed daily bars sets the "
            "open±ATR range an S/R level must sit in to qualify."),
    _f("ds0_sweep_min", 0.13, "DS0", "Sweep floor ($)", min=0.01, max=10.0,
       help="The 15:01 sweep closes marks at/above this; below it the "
            "spread rides to expiration as the accepted loss."),
    _f("ds0_guard_band", 0.005, "DS0", "Guard band (fraction)", min=0.0, max=0.05,
       help="Assignment guard fires when spot is within this of the near strike."),
    _i("ds0_assignment_guard", 1, "DS0", "Assignment guard (0/1)", min=0, max=1,
       help="3:50 PM force-close of near/in-the-money spreads (pin/assignment risk)."),
    _i("ds0_approval_ttl_s", 900, "DS0", "Entry approval TTL (s)", min=0, max=86400,
       help="A queued DS0 entry approved after this window is expired, not executed."),
    _s("ds0_entry_cutoff", "14:00", "DS0", "Entry cutoff (ET HH:MM)",
       help="No new entries at or after this time — the reversion needs runway."),
    _s("ds0_sweep_time", "15:01", "DS0", "Sweep time (ET HH:MM)",
       help="Close anything marked at/above the sweep floor; below rides to expiry."),
    _s("ds0_guard_time", "15:50", "DS0", "Guard time (ET HH:MM)",
       help="When the assignment guard starts checking spot vs strikes."),
]

TUNABLES: Dict[str, Tunable] = {t.key: t for t in _CATALOG}


# ---------------------------------------------------------------------------
# Resolved-values wrapper
# ---------------------------------------------------------------------------
class Tunables:
    """Resolved tunable values with attribute / item / ``.get`` access."""

    __slots__ = ("_values",)

    def __init__(self, values: Dict[str, Any]):
        self._values = values

    def __getattr__(self, name: str) -> Any:
        try:
            return self._values[name]
        except KeyError as exc:
            raise AttributeError(
                f"unknown tunable {name!r}; not loaded for this group"
            ) from exc

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._values.get(key, default)

    def as_dict(self) -> Dict[str, Any]:
        return dict(self._values)


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------
def _specs_for(group: Optional[str]) -> List[Tunable]:
    if group is None:
        return list(TUNABLES.values())
    return [t for t in TUNABLES.values() if t.group == group]


def _resolve_one(spec: Tunable, raw: Optional[str], env_config: Dict[str, Any]) -> Any:
    """settings > env_config > default, all type-coerced via ``spec.coerce``."""
    if raw is not None:
        return spec.coerce(raw)
    if spec.key in env_config:
        return spec.coerce(env_config[spec.key])
    return spec.default


async def resolve(db, env_config: Optional[Dict[str, Any]] = None,
                  group: Optional[str] = None) -> Tunables:
    """Load the tunables for ``group`` (or all) with override precedence.

    One DB round-trip via ``db.get_settings`` when available; falls back to
    per-key ``db.get_setting`` for stub DBs that don't implement the bulk
    reader (keeps test doubles simple).
    """
    env_config = env_config or {}
    specs = _specs_for(group)
    keys = [s.key for s in specs]

    overrides: Dict[str, Optional[str]] = {}
    bulk = getattr(db.settings, "get_settings", None)
    if callable(bulk):
        overrides = await bulk(keys) or {}
    else:                                                    # stub-DB fallback
        for k in keys:
            overrides[k] = await db.settings.get_setting(k)

    values = {s.key: _resolve_one(s, overrides.get(s.key), env_config) for s in specs}
    return Tunables(values)
