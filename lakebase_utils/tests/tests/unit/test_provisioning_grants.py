"""Unit tests for the grants reconciler."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from lakebase_utils.provisioning.reconcilers.grants import GrantReconciler


def _engine(side_effect):
    pg = MagicMock()
    pg.fetch.side_effect = side_effect
    return SimpleNamespace(pg=pg, ws=MagicMock(), config={})


def test_plan_grants_usage_and_select_when_missing():
    # pg_state.tables_in_schema, schema_usage_grants, table_grants — order
    # depends on plan() call sequence; use whatever lookups consume.
    engine = SimpleNamespace(pg=MagicMock(), ws=MagicMock(), config={})
    # schema_usage_grants -> empty; table_grants -> empty; tables_in_schema not called.
    engine.pg.fetch.side_effect = [
        [],   # schema_usage_grants
        [],   # table_grants
    ]
    rec = GrantReconciler(engine)
    actions = rec.plan([{
        "to": "u@x.com",
        "usage_on_schemas": ["public"],
        "select_on_tables": ["public.widgets"],
    }])
    kinds = [a.kind for a in actions]
    targets = [a.target for a in actions]
    assert "grant" in kinds
    assert any("usage:public/u@x.com" in t for t in targets)
    assert any("table:public.widgets/u@x.com" in t for t in targets)
    table_action = next(a for a in actions if a.target.startswith("table:"))
    assert "SELECT" in table_action.sql


def test_plan_no_op_when_grants_already_present():
    engine = SimpleNamespace(pg=MagicMock(), ws=MagicMock(), config={})
    engine.pg.fetch.side_effect = [
        [("public", "u@x.com")],                            # schema_usage_grants
        {("public", "widgets", "u@x.com", "SELECT")},       # table_grants (set)
    ]
    rec = GrantReconciler(engine)
    actions = rec.plan([{
        "to": "u@x.com",
        "usage_on_schemas": ["public"],
        "select_on_tables": ["public.widgets"],
    }])
    assert actions == []


def test_wildcard_table_expands_via_pg_tables():
    engine = SimpleNamespace(pg=MagicMock(), ws=MagicMock(), config={})
    engine.pg.fetch.side_effect = [
        [],                              # schema_usage_grants
        set(),                           # table_grants
        [("widgets",), ("orders",)],     # tables_in_schema('public')
    ]
    rec = GrantReconciler(engine)
    actions = rec.plan([{
        "to": "u@x.com",
        "select_on_tables": ["public.*"],
    }])
    table_targets = sorted(a.target for a in actions if a.target.startswith("table:"))
    assert "table:public.orders/u@x.com" in table_targets
    assert "table:public.widgets/u@x.com" in table_targets


def test_revoke_existing_emits_revokes_for_drift():
    engine = SimpleNamespace(pg=MagicMock(), ws=MagicMock(), config={})
    # current state has an extra SELECT on public.legacy that's not in config
    engine.pg.fetch.side_effect = [
        [],
        {
            ("public", "widgets", "u@x.com", "SELECT"),
            ("public", "legacy",  "u@x.com", "SELECT"),
        },
    ]
    rec = GrantReconciler(engine)
    actions = rec.plan([{
        "to": "u@x.com",
        "usage_on_schemas": ["public"],
        "select_on_tables": ["public.widgets"],
        "revoke_existing": True,
    }])
    revokes = [a for a in actions if a.kind == "revoke"]
    assert len(revokes) == 1
    assert "legacy" in revokes[0].sql


def test_extra_privileges_grants_insert_alongside_select():
    engine = SimpleNamespace(pg=MagicMock(), ws=MagicMock(), config={})
    engine.pg.fetch.side_effect = [[], set()]
    rec = GrantReconciler(engine)
    actions = rec.plan([{
        "to": "u@x.com",
        "select_on_tables": ["public.widgets"],
        "extra_privileges_on_tables": ["INSERT"],
    }])
    table_action = next(a for a in actions if a.target.startswith("table:"))
    assert "SELECT" in table_action.sql
    assert "INSERT" in table_action.sql
