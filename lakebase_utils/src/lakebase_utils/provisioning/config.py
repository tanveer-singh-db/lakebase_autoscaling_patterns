"""YAML config loader: env-var substitution + JSON Schema validation."""

from __future__ import annotations

import json
import os
import re
from importlib import resources
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft7Validator

_ENV_PATTERN = re.compile(
    r"\$\{env:([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}"
)


class ConfigError(ValueError):
    """Raised when config is missing, unreadable, or schema-invalid."""


def _substitute_env(value: Any) -> Any:
    if isinstance(value, str):
        def repl(m: re.Match) -> str:
            name, default = m.group(1), m.group(2)
            if name in os.environ:
                return os.environ[name]
            if default is not None:
                return default
            raise ConfigError(
                f"environment variable ${{env:{name}}} is not set and has no default"
            )
        return _ENV_PATTERN.sub(repl, value)
    if isinstance(value, list):
        return [_substitute_env(v) for v in value]
    if isinstance(value, dict):
        return {k: _substitute_env(v) for k, v in value.items()}
    return value


def _load_schema() -> dict:
    pkg = resources.files(__package__) / "schemas" / "lakebase_provisioning_spec.json"
    return json.loads(pkg.read_text(encoding="utf-8"))


def validate_config(cfg: dict) -> None:
    """Validate ``cfg`` against the packaged JSON Schema. Raises ``ConfigError``
    listing every violation with json-pointer paths."""
    validator = Draft7Validator(_load_schema())
    errors = sorted(validator.iter_errors(cfg), key=lambda e: list(e.absolute_path))
    if not errors:
        return
    lines = []
    for err in errors:
        ptr = "/" + "/".join(str(p) for p in err.absolute_path) if err.absolute_path else "/"
        lines.append(f"  at {ptr}: {err.message}")
    raise ConfigError("invalid provisioning config:\n" + "\n".join(lines))


def load_config(path: str | Path) -> dict:
    """Load YAML, substitute ``${env:VAR}`` / ``${env:VAR:-default}`` placeholders,
    validate against the schema, and return a dict."""
    p = Path(path)
    if not p.is_file():
        raise ConfigError(f"config file not found: {path}")
    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ConfigError(f"invalid YAML in {path}: {e}") from e
    if not isinstance(raw, dict):
        raise ConfigError(f"top-level config must be a mapping, got {type(raw).__name__}")
    cfg = _substitute_env(raw)
    validate_config(cfg)
    return cfg
