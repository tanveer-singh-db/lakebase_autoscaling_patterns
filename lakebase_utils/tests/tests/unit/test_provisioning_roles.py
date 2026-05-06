"""Unit tests for the roles reconciler — pg state mocked."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lakebase_utils.provisioning.reconcilers.roles import RoleReconciler


def _make_engine(*, fetch_rows: list[tuple]) -> SimpleNamespace:
    pg = MagicMock()
    pg.fetch.return_value = fetch_rows
    return SimpleNamespace(pg=pg, ws=MagicMock(), config={})


def test_plan_creates_missing_role():
    # Both pg_roles and pg_auth_members reads return empty -> identity is missing
    engine = _make_engine(fetch_rows=[])
    rec = RoleReconciler(engine)
    actions = rec.plan([{"identity": "api-user@example.com", "type": "USER"}])
    assert len(actions) == 1
    a = actions[0]
    assert a.kind == "create_role"
    assert a.target == "role:api-user@example.com"
    assert "databricks_create_role('api-user@example.com', 'USER')" in a.sql
    assert 'GRANT "api-user@example.com" TO authenticator' in a.sql


def test_plan_skips_existing_role():
    # First fetch (existing_db_roles) and second fetch (members) both return the identity.
    engine = SimpleNamespace(pg=MagicMock(), ws=MagicMock(), config={})
    engine.pg.fetch.side_effect = [
        [("api-user@example.com",)],   # existing_db_roles
        [("api-user@example.com",)],   # members_of_authenticator
    ]
    rec = RoleReconciler(engine)
    actions = rec.plan([{"identity": "api-user@example.com", "type": "USER"}])
    assert actions == []


def test_plan_recreates_when_role_exists_but_not_member():
    engine = SimpleNamespace(pg=MagicMock(), ws=MagicMock(), config={})
    engine.pg.fetch.side_effect = [
        [("sp",)],   # exists in databricks_group
        [],          # but not member of authenticator
    ]
    rec = RoleReconciler(engine)
    actions = rec.plan([{"identity": "sp", "type": "SERVICE_PRINCIPAL"}])
    assert len(actions) == 1
    assert "SERVICE_PRINCIPAL" in actions[0].sql


def test_apply_executes_sql():
    engine = _make_engine(fetch_rows=[])
    rec = RoleReconciler(engine)
    actions = rec.plan([{"identity": "x@y.z", "type": "USER"}])
    rec.apply(actions)
    engine.pg.execute.assert_called_once()
    assert "databricks_create_role" in engine.pg.execute.call_args.args[0]


def test_sql_quotes_identity_with_apostrophe():
    engine = _make_engine(fetch_rows=[])
    rec = RoleReconciler(engine)
    actions = rec.plan([{"identity": "o'malley@example.com", "type": "USER"}])
    # Single quotes inside the SELECT-arg literal must be doubled.
    assert "'o''malley@example.com'" in actions[0].sql
