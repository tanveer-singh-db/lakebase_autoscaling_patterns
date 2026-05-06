"""Unit tests for the Data API exposed-schema reconciler."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from lakebase_utils.provisioning.reconcilers.exposed_schemas import (
    ExposedSchemaReconciler,
)
from lakebase_utils.provisioning.state import data_api


def _engine(config: dict) -> SimpleNamespace:
    return SimpleNamespace(pg=MagicMock(), ws=MagicMock(), config=config)


CFG = {
    "target": {"project": "cust360", "branch": "production",
               "endpoint": "primary", "database": "d", "host": "h"},
    "exposed_schemas": ["public", "analytics"],
    "refresh_schema_cache": True,
}


def test_plan_no_op_when_lists_match():
    engine = _engine(CFG)
    with patch.object(data_api, "get_exposed_schemas",
                      return_value=["public", "analytics"]):
        actions = ExposedSchemaReconciler(engine).plan(None)
    assert actions == []


def test_plan_emits_set_and_refresh_when_diverged():
    engine = _engine(CFG)
    with patch.object(data_api, "get_exposed_schemas", return_value=["public"]):
        actions = ExposedSchemaReconciler(engine).plan(None)
    kinds = [a.kind for a in actions]
    assert "set_exposed_schemas" in kinds
    assert "refresh_cache" in kinds


def test_plan_skips_refresh_when_disabled():
    cfg = {**CFG, "refresh_schema_cache": False}
    engine = _engine(cfg)
    with patch.object(data_api, "get_exposed_schemas", return_value=["public"]):
        actions = ExposedSchemaReconciler(engine).plan(None)
    assert [a.kind for a in actions] == ["set_exposed_schemas"]


def test_plan_emits_manual_action_when_sdk_unsupported():
    engine = _engine(CFG)
    with patch.object(data_api, "get_exposed_schemas",
                      side_effect=data_api.UnsupportedOperation("nope")):
        actions = ExposedSchemaReconciler(engine).plan(None)
    assert len(actions) == 1
    assert actions[0].kind == "manual"


def test_apply_calls_set_and_refresh():
    engine = _engine(CFG)
    with patch.object(data_api, "set_exposed_schemas") as set_mock, \
         patch.object(data_api, "refresh_schema_cache") as refresh_mock, \
         patch.object(data_api, "get_exposed_schemas", return_value=["public"]):
        rec = ExposedSchemaReconciler(engine)
        actions = rec.plan(None)
        rec.apply(actions)
    set_mock.assert_called_once()
    refresh_mock.assert_called_once()
