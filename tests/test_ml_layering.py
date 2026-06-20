"""ML-layering guardrail.

The ``hermes/ml`` package mixes two very different kinds of code:

- **Decision-path modules** that the order-placing agent (Service-1) imports,
  directly or transitively, because they gate or size real trades:
  ``pop_engine`` (the POP entry gate), the ``xgb_features`` predictor stack and
  its internals (``feature_engineer`` / ``predictor_*`` / ``feature_catalog`` /
  ``persistence`` / ``calibration`` / ``drift`` / ``meta_learner`` /
  ``ledger``), ``regime_weights``, and the operator-gated tuners ``bandit`` /
  ``exit_policy``.
- **Pure-analysis modules** that exist only to populate the read-only operator
  panel (Service-2): ``attribution`` (P&L attribution for ``/api/analytics``)
  and ``backtester`` (walk-forward POP reality check for ``/api/ml/backtest``).
  These are the heaviest modules in the package (~630 LOC combined) and **must
  stay out of the order-placing process** — they read history and render
  diagnostics; they have no business on the path that touches the broker.

ARCHITECTURE.md states the rule generally ("A given file should never reach more
than one layer up or down"); this test makes the specific, easy-to-violate case
executable. If someone wires ``backtester``/``attribution`` into a strategy, the
engine, or the overseer, the heavy analysis surface silently leaks into the hot
money path — this guard fails first and forces a conscious decision (move the
call to the watcher, or accept the new dependency by editing the allowlist).

The boundary is computed from the actual import graph via AST, not a hand-kept
list, so it cannot drift: we walk every ``hermes.ml`` import reachable from
``hermes/service1_agent`` (following ml→ml edges transitively) and assert the
diagnostics-only modules never appear in that set.
"""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ML_DIR = ROOT / "hermes" / "ml"
AGENT_DIR = ROOT / "hermes" / "service1_agent"

# Pure-analysis modules that must never be reachable from the order-placing
# agent. Adding a module here is a deliberate statement that it is watcher-only.
WATCHER_ONLY_ML_MODULES = {"attribution", "backtester"}


def _ml_imports(path: Path) -> set[str]:
    """Top-level ``hermes.ml`` submodule names imported by ``path``."""
    out: set[str] = set()
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "hermes.ml":
                out |= {alias.name for alias in node.names}  # from hermes.ml import x
            elif node.module.startswith("hermes.ml."):
                out.add(node.module.split(".")[2])           # from hermes.ml.x import ...
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("hermes.ml."):
                    out.add(alias.name.split(".")[2])
    return out


def _ml_module_names() -> set[str]:
    return {p.stem for p in ML_DIR.glob("*.py") if p.stem != "__init__"}


def _reachable_from_agent() -> set[str]:
    """ml submodules the agent imports directly + their transitive ml deps."""
    ml_graph = {name: _ml_imports(ML_DIR / f"{name}.py") for name in _ml_module_names()}

    seeds: set[str] = set()
    for path in AGENT_DIR.rglob("*.py"):
        seeds |= _ml_imports(path)

    seen: set[str] = set()
    stack = list(seeds)
    while stack:
        mod = stack.pop()
        if mod in seen:
            continue
        seen.add(mod)
        stack.extend(ml_graph.get(mod, ()))
    return seen & _ml_module_names()


def test_watcher_only_ml_modules_are_named_real_modules():
    """Guard against a stale allowlist: every name we forbid must still exist."""
    assert WATCHER_ONLY_ML_MODULES <= _ml_module_names()


def test_agent_never_imports_diagnostics_only_ml():
    """Service-1 (order placement) must not reach the watcher-only analysis
    modules, directly or transitively."""
    reachable = _reachable_from_agent()
    leaked = WATCHER_ONLY_ML_MODULES & reachable
    assert not leaked, (
        f"hermes/service1_agent now imports diagnostics-only ml modules {sorted(leaked)}; "
        "these belong to the read-only watcher (Service-2). Move the call to the "
        "watcher, or — if the dependency is intentional — drop the module from "
        "WATCHER_ONLY_ML_MODULES in this test and update tests/test_ml_layering.py's docstring."
    )
