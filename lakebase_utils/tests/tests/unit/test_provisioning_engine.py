"""Unit tests for the engine orchestrator and CLI plan output."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lakebase_utils.provisioning.engine import ProvisioningEngine, format_plan
from lakebase_utils.provisioning.reconcilers import Action, Plan

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures"


def _engine_with_mocked_clients(config: dict) -> ProvisioningEngine:
    pg = MagicMock()
    pg.fetch.return_value = []   # no existing roles, grants, or schemas
    ws = MagicMock()
    return ProvisioningEngine(config, ws=ws, pg=pg)


def test_plan_aggregates_all_reconcilers(monkeypatch):
    cfg = {
        "target": {"project": "p", "branch": "b", "endpoint": "e",
                   "database": "d", "host": "h"},
        "roles": [{"identity": "u@x.com", "type": "USER"}],
    }
    engine = _engine_with_mocked_clients(cfg)
    # Stub the Data API state read so it doesn't attempt SDK calls.
    with patch("lakebase_utils.provisioning.state.data_api.get_exposed_schemas",
               return_value=[]):
        plan = engine.plan()
    assert "roles" in plan.actions_by_section
    assert plan.actions_by_section["roles"][0].kind == "create_role"


def test_apply_dry_run_calls_no_writes(monkeypatch):
    cfg = {
        "target": {"project": "p", "branch": "b", "endpoint": "e",
                   "database": "d", "host": "h"},
        "roles": [{"identity": "u@x.com", "type": "USER"}],
    }
    engine = _engine_with_mocked_clients(cfg)
    with patch("lakebase_utils.provisioning.state.data_api.get_exposed_schemas",
               return_value=[]):
        result = engine.apply(dry_run=True)
    assert not result.applied
    engine.pg.execute.assert_not_called()


def test_apply_without_auto_approve_raises():
    cfg = {
        "target": {"project": "p", "branch": "b", "endpoint": "e",
                   "database": "d", "host": "h"},
        "roles": [{"identity": "u@x.com", "type": "USER"}],
    }
    engine = _engine_with_mocked_clients(cfg)
    with patch("lakebase_utils.provisioning.state.data_api.get_exposed_schemas",
               return_value=[]):
        with pytest.raises(RuntimeError, match="auto_approve"):
            engine.apply(dry_run=False, auto_approve=False)


def test_apply_with_auto_approve_writes(monkeypatch):
    cfg = {
        "target": {"project": "p", "branch": "b", "endpoint": "e",
                   "database": "d", "host": "h"},
        "roles": [{"identity": "u@x.com", "type": "USER"}],
    }
    engine = _engine_with_mocked_clients(cfg)
    with patch("lakebase_utils.provisioning.state.data_api.get_exposed_schemas",
               return_value=[]):
        result = engine.apply(dry_run=False, auto_approve=True)
    assert result.applied
    engine.pg.execute.assert_called()  # role creation SQL


def test_format_plan_empty():
    assert "No changes required" in format_plan(Plan())


def test_format_plan_renders_sql():
    plan = Plan()
    plan.add("roles", [Action(kind="create_role", target="role:u",
                              sql="SELECT 1;", metadata={})])
    out = format_plan(plan)
    assert "[create_role] role:u" in out
    assert "SELECT 1;" in out
    assert "Total actions: 1" in out


def test_cli_plan_subcommand_exits_zero(monkeypatch, capsys):
    """CLI smoke: validate -> plan path with mocked engine."""
    from lakebase_utils.provisioning import cli

    fake_engine = MagicMock()
    fake_engine.plan.return_value = Plan()  # no actions
    with patch.object(cli, "ProvisioningEngine", return_value=fake_engine):
        rc = cli.main(["plan", "-f", str(FIXTURES / "provisioning_minimal.yaml")])
    assert rc == 0
    assert "No changes required" in capsys.readouterr().out


def test_cli_validate_subcommand(capsys):
    from lakebase_utils.provisioning import cli
    rc = cli.main(["validate", "-f", str(FIXTURES / "provisioning_minimal.yaml")])
    assert rc == 0
    assert "valid" in capsys.readouterr().out.lower()


def test_cli_invalid_config_exits_two(capsys):
    from lakebase_utils.provisioning import cli
    rc = cli.main(["validate", "-f", str(FIXTURES / "provisioning_invalid.yaml")])
    assert rc == 2
    err = capsys.readouterr().err
    assert "invalid" in err.lower()
