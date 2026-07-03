"""
[ML-Persistence]
Joblib-based model persistence with feature-schema-hash gating.

Why this exists
---------------
The previous predictor used ``pickle.load`` on warm boot, with no check
that the persisted model was trained against the same feature catalog
the live FeatureEngineer produces today. A renamed feature could let a
stale model score live data with shifted columns and the predictor
would happily emit garbage probabilities.

This module:

1. Stores models with a metadata sidecar (model_hash, schema_hash,
   trained_at, sample_size, target, training_metrics).
2. Refuses to load a model whose schema_hash differs from the live
   catalog. The caller treats that as "no model available" and the
   stale checkpoint is moved aside instead of discarded silently.
3. Uses joblib (which falls back to pickle but is the sklearn / XGBoost
   recommended pickler) so warm-start times stay reasonable on the
   ~/.hermes durable volume.

The on-disk layout under ~/.hermes/models/ is:

    AAPL/
      xgb_q50_45dte.joblib
      xgb_q50_45dte.meta.json
      ...
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import os
import shutil
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

try:                                                  # pragma: no cover - optional
    import joblib                                     # type: ignore
    _HAS_JOBLIB = True
except ImportError:                                   # pragma: no cover
    import pickle as joblib                           # type: ignore
    _HAS_JOBLIB = False

from hermes.ml.feature_catalog import schema_hash

logger = logging.getLogger("hermes.ml.persistence")


# ---------------------------------------------------------------------------
# Default storage root.  ``~/.hermes`` survives Docker container restarts
# (mounted as a host volume in docker-compose.yml). We intentionally do not
# fall back to /tmp here — silent /tmp use was one of the bugs this module
# was created to fix.
# ---------------------------------------------------------------------------
DEFAULT_MODEL_ROOT = Path(os.environ.get(
    "HERMES_MODEL_ROOT",
    str(Path.home() / ".hermes" / "models"),
))


@dataclass
class ModelMeta:
    """Sidecar metadata stored next to every model artefact."""

    model_hash: str
    schema_hash: str
    schema_stage: str
    trained_at: float                  # POSIX timestamp
    target: str
    sample_size: int
    horizon_dte: Optional[int] = None
    quantile: Optional[float] = None   # e.g. 0.5 for the median head
    metrics: Dict[str, float] = field(default_factory=dict)
    notes: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, indent=2)

    @classmethod
    def from_json(cls, raw: str) -> "ModelMeta":
        data = json.loads(raw)
        # Drop any unknown keys so older artefacts written before a new
        # field was added still load successfully.
        valid = {f.name for f in dataclasses.fields(cls)}
        clean = {k: v for k, v in data.items() if k in valid}
        return cls(**clean)


def _model_dir(root: Path, symbol: str) -> Path:
    return root / symbol.upper()


def _paths(root: Path, symbol: str, model_name: str) -> tuple[Path, Path]:
    base = _model_dir(root, symbol) / model_name
    return base.with_suffix(".joblib"), base.with_suffix(".meta.json")


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def save_model(
    model: Any,
    *,
    symbol: str,
    model_name: str,
    target: str,
    sample_size: int,
    schema_stage: str = "raw",
    horizon_dte: Optional[int] = None,
    quantile: Optional[float] = None,
    metrics: Optional[Mapping[str, float]] = None,
    notes: str = "",
    root: Path = DEFAULT_MODEL_ROOT,
) -> ModelMeta:
    """Persist ``model`` plus its sidecar metadata.

    Returns the freshly written ModelMeta — useful for the prediction
    ledger so it can stamp predictions with the model_hash.
    """
    root.mkdir(parents=True, exist_ok=True)
    _model_dir(root, symbol).mkdir(parents=True, exist_ok=True)
    artefact_path, meta_path = _paths(root, symbol, model_name)

    # Atomic write: dump to a temp path then rename so an aborted
    # checkpoint never leaves a half-written .joblib in place.
    tmp_artefact = artefact_path.with_suffix(".joblib.tmp")
    with tmp_artefact.open("wb") as fh:
        joblib.dump(model, fh)
    body = tmp_artefact.read_bytes()
    tmp_artefact.replace(artefact_path)

    meta = ModelMeta(
        model_hash=_hash_bytes(body),
        schema_hash=schema_hash(schema_stage),
        schema_stage=schema_stage,
        trained_at=time.time(),
        target=target,
        sample_size=int(sample_size),
        horizon_dte=horizon_dte,
        quantile=quantile,
        metrics=dict(metrics or {}),
        notes=notes,
    )
    tmp_meta = meta_path.with_suffix(".meta.json.tmp")
    tmp_meta.write_text(meta.to_json())
    tmp_meta.replace(meta_path)

    logger.info(
        "persisted %s/%s  hash=%s schema=%s rows=%d",
        symbol, model_name, meta.model_hash[:12], meta.schema_hash[:12],
        meta.sample_size,
    )
    return meta


def load_model(
    *,
    symbol: str,
    model_name: str,
    schema_stage: str = "raw",
    root: Path = DEFAULT_MODEL_ROOT,
    quarantine: bool = True,
) -> tuple[Optional[Any], Optional[ModelMeta]]:
    """Return ``(model, meta)`` or ``(None, None)`` if unavailable.

    Returns ``(None, None)`` and quarantines the artefact in three cases:

    - Files missing.
    - Sidecar JSON unreadable.
    - ``schema_hash`` mismatch with the live catalog.

    Quarantining (rather than deleting) preserves forensic value: the
    operator can inspect the stale model to decide whether to retrain
    or to investigate a feature regression.
    """
    artefact_path, meta_path = _paths(root, symbol, model_name)
    if not artefact_path.exists() or not meta_path.exists():
        return None, None

    try:
        meta = ModelMeta.from_json(meta_path.read_text())
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        logger.warning("meta unreadable for %s/%s: %s", symbol, model_name, exc)
        if quarantine:
            _quarantine(artefact_path, meta_path, reason="meta-unreadable")
        return None, None

    expected = schema_hash(schema_stage)
    if meta.schema_hash != expected:
        logger.warning(
            "schema-hash mismatch for %s/%s: stored=%s expected=%s; quarantining",
            symbol, model_name, meta.schema_hash[:12], expected[:12],
        )
        if quarantine:
            _quarantine(artefact_path, meta_path, reason="schema-mismatch")
        return None, None

    try:
        with artefact_path.open("rb") as fh:
            model = joblib.load(fh)
    except Exception as exc:                          # noqa: BLE001
        logger.warning("artefact load failed for %s/%s: %s",
                       symbol, model_name, exc)
        if quarantine:
            _quarantine(artefact_path, meta_path, reason="load-failure")
        return None, None

    return model, meta



def _quarantine(artefact: Path, meta: Path, *, reason: str) -> None:
    """Move a bad pair into ``<root>/_quarantine/<ts>-<reason>/`` so a
    future audit can recover what was there."""
    base = artefact.parent.parent / "_quarantine" / f"{int(time.time())}-{reason}"
    base.mkdir(parents=True, exist_ok=True)
    for src in (artefact, meta):
        if src.exists():
            try:
                shutil.move(str(src), str(base / src.name))
            except OSError as exc:                    # pragma: no cover
                logger.warning("could not quarantine %s: %s", src, exc)


__all__ = [
    "DEFAULT_MODEL_ROOT",
    "ModelMeta",
    "save_model",
    "load_model",
]
