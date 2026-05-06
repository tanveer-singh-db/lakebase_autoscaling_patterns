"""Config-driven data-layer provisioning for Lakebase Autoscaling.

This module fills the gap that Databricks Asset Bundles can't:
Postgres roles, GRANTs, and Data API exposed schemas. Platform-layer
resources (projects, branches, endpoints, synced tables) belong in
``databricks.yml`` — see ``docs/lakebase_provisioning.md``.
"""

from .config import ConfigError, load_config, validate_config
from .engine import ProvisioningEngine

__all__ = ["ConfigError", "ProvisioningEngine", "load_config", "validate_config"]
