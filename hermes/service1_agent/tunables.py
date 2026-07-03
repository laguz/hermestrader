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
from ``system_settings`` so the operator can retune from the panel without a
deploy, falling back to env config and finally the spec default. The spec
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


def catalog(group: Optional[str] = None) -> List[Dict[str, Any]]:
    """Catalog metadata (no current values) — for the tunables API."""
    out: List[Dict[str, Any]] = []
    for t in _specs_for(group):
        out.append({
            "key": t.key,
            "group": t.group,
            "label": t.label,
            "default": t.default,
            "type": "int" if t.cast is int else "float",
            "min": t.min,
            "max": t.max,
            "help": t.help,
        })
    return out


def groups() -> List[str]:
    """Distinct group names in catalog declaration order."""
    seen: List[str] = []
    for t in TUNABLES.values():
        if t.group not in seen:
            seen.append(t.group)
    return seen
