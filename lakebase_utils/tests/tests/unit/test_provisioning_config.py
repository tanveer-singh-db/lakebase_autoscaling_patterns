"""Unit tests for the YAML loader, env-var substitution, and JSON Schema validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakebase_utils.provisioning.config import (
    ConfigError,
    load_config,
    validate_config,
)

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures"


def test_load_minimal_passes(clean_env):
    cfg = load_config(FIXTURES / "provisioning_minimal.yaml")
    assert cfg["target"]["project"] == "cust360"
    assert "roles" not in cfg


def test_load_full_substitutes_env(monkeypatch, clean_env):
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "demo-profile")
    cfg = load_config(FIXTURES / "provisioning_full.yaml")
    assert cfg["target"]["profile"] == "demo-profile"
    assert len(cfg["roles"]) == 2


def test_env_default_is_used_when_var_unset(clean_env):
    cfg = load_config(FIXTURES / "provisioning_full.yaml")
    assert cfg["target"]["profile"] == "DEFAULT"


def test_invalid_fixture_raises_with_pointers(clean_env):
    with pytest.raises(ConfigError) as exc:
        load_config(FIXTURES / "provisioning_invalid.yaml")
    msg = str(exc.value)
    # All three independent violations should be reported in one error.
    assert "/target" in msg
    assert "/roles/0/identity" in msg or "/roles/0" in msg
    assert "ROOT" in msg


def test_missing_file():
    with pytest.raises(ConfigError, match="not found"):
        load_config("/nope/does-not-exist.yaml")


def test_validate_config_direct_passes_minimal():
    validate_config({
        "target": {
            "project": "p", "branch": "b", "endpoint": "e",
            "database": "d", "host": "h",
        }
    })


def test_unset_env_no_default_raises(monkeypatch, tmp_path, clean_env):
    cfg = tmp_path / "c.yaml"
    cfg.write_text(
        "target:\n"
        "  project: ${env:UNSET_VAR_LAKEBASE_TEST}\n"
        "  branch: b\n  endpoint: e\n  database: d\n  host: h\n"
    )
    with pytest.raises(ConfigError, match="UNSET_VAR_LAKEBASE_TEST"):
        load_config(cfg)
