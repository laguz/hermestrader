"""Tunables catalog + loader: precedence, coercion, and the DB bulk reader."""
from __future__ import annotations

import pytest

from hermes.service1_agent.tunables import (
    TUNABLES,
    Tunables,
    catalog,
    groups,
    resolve,
)
from ._stubs import RepoNamespaceMixin


# ---------------------------------------------------------------------------
# Pure-catalog tests (no DB)
# ---------------------------------------------------------------------------
def test_catalog_covers_every_tunable():
    cat_keys = {e["key"] for e in catalog()}
    assert cat_keys == set(TUNABLES.keys())
    # Every entry carries the metadata the API relies on.
    for e in catalog():
        assert e["type"] in ("int", "float")
        assert e["group"] in groups()
        assert e["label"]


def test_defaults_match_documented_spec_literals():
    # A spot-check that the spec defaults equal the literals the strategies
    # used before centralisation — the guarantee that behaviour is unchanged.
    expected = {
        "cs75_width": 5.0, "cs75_min_dte": 39, "cs75_max_dte": 45,
        "cs75_short_delta_max": 0.40, "cs75_pop_target": 0.75,
        "cs75_sl_mult": 2.5, "cs75_time_exit_dte": 8,
        "cs7_width": 1.0, "cs7_dte": 7, "cs7_tp_pct_width": 0.02, "cs7_sl_mult": 3.0,
        "tt45_delta": 0.16, "tt45_hard_exit_dte": 21, "tt45_challenged_delta": 0.30,
        "wheel_delta": 0.30, "wheel_min_pop": 0.50, "wheel_roll_dte": 7,
    }
    for key, val in expected.items():
        assert TUNABLES[key].default == val, key


def test_coerce_falls_back_to_default_on_garbage():
    spec = TUNABLES["cs75_sl_mult"]
    assert spec.coerce("not-a-number") == spec.default
    assert spec.coerce("3.0") == 3.0
    int_spec = TUNABLES["cs75_time_exit_dte"]
    assert int_spec.coerce("12") == 12
    assert int_spec.coerce("oops") == int_spec.default


# ---------------------------------------------------------------------------
# resolve() precedence with a lightweight stub DB
# ---------------------------------------------------------------------------
class _StubDB(RepoNamespaceMixin):
    """Minimal DB double exposing the bulk + single settings readers."""

    def __init__(self, settings):
        self._settings = dict(settings)

    async def get_settings(self, keys):
        return {k: self._settings[k] for k in keys if k in self._settings}

    async def get_setting(self, key, default=None):
        return self._settings.get(key, default)


async def test_resolve_returns_defaults_when_unset():
    t = await resolve(_StubDB({}), group="CS75")
    assert isinstance(t, Tunables)
    assert t.cs75_sl_mult == 2.5
    assert t.cs75_width == 5.0


async def test_setting_overrides_default():
    t = await resolve(_StubDB({"cs75_sl_mult": "3.5"}), group="CS75")
    assert t.cs75_sl_mult == 3.5


async def test_env_config_beats_default_but_loses_to_setting():
    # env_config supplies a value; with no setting it wins over the default.
    t = await resolve(_StubDB({}), env_config={"cs75_width": "7.5"}, group="CS75")
    assert t.cs75_width == 7.5
    # With a setting present, the setting wins over env_config.
    t2 = await resolve(
        _StubDB({"cs75_width": "9.0"}), env_config={"cs75_width": "7.5"}, group="CS75")
    assert t2.cs75_width == 9.0


async def test_bad_setting_value_falls_back_to_default():
    t = await resolve(_StubDB({"cs75_time_exit_dte": "not-int"}), group="CS75")
    assert t.cs75_time_exit_dte == 8


async def test_group_filter_scopes_keys():
    t = await resolve(_StubDB({}), group="WHEEL")
    assert t.wheel_roll_dte == 7
    with pytest.raises(AttributeError):
        _ = t.cs75_sl_mult  # not loaded for the WHEEL group


async def test_resolve_uses_per_key_getter_when_no_bulk_reader():
    class _NoBulk(RepoNamespaceMixin):
        def __init__(self, s):
            self._s = s

        async def get_setting(self, key, default=None):
            return self._s.get(key, default)

    t = await resolve(_NoBulk({"tt45_delta": "0.20"}), group="TT45")
    assert t.tt45_delta == 0.20
    assert t.tt45_hard_exit_dte == 21


# ---------------------------------------------------------------------------
# Integration: real Timescale HermesDB get_settings + end-to-end override.
# The ``db`` fixture (fresh throwaway Timescale DB) comes from tests/conftest.py.
# ---------------------------------------------------------------------------
async def test_get_settings_bulk_reads_only_existing_keys(db):
    await db.settings.set_setting("cs75_sl_mult", "2.8")
    await db.settings.set_setting("cs7_sl_mult", "3.3")
    got = await db.settings.get_settings(["cs75_sl_mult", "cs7_sl_mult", "does_not_exist"])
    assert got == {"cs75_sl_mult": "2.8", "cs7_sl_mult": "3.3"}
    assert await db.settings.get_settings([]) == {}


async def test_setting_flows_through_resolve_against_real_db(db):
    # Default first.
    t = await resolve(db, group="CS75")
    assert t.cs75_sl_mult == 2.5
    # Operator override lands in system_settings and is picked up.
    await db.settings.set_setting("cs75_sl_mult", "3.0")
    t2 = await resolve(db, group="CS75")
    assert t2.cs75_sl_mult == 3.0
