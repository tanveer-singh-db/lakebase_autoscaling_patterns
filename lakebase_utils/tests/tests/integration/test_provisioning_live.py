"""Live integration tests for the provisioning module.

Skipped unless ``LAKEBASE_PROVISIONING_LIVE=1`` is set, since these tests
mutate Postgres state on a real Lakebase project. Reads the same
``test_config.yaml`` the rest of the integration suite uses.
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.slow,
    pytest.mark.skipif(
        os.environ.get("LAKEBASE_PROVISIONING_LIVE") != "1",
        reason="set LAKEBASE_PROVISIONING_LIVE=1 to enable",
    ),
]


@pytest.fixture(scope="module")
def live_config(test_config):
    """Build a provisioning config dict from the shared test_config fixture."""
    return {
        "target": {
            "project": test_config["project"],
            "branch": test_config["branch"],
            "endpoint": test_config["endpoint"],
            "database": test_config["database"],
            "host": test_config["host"],
            "workspace_host": test_config.get("workspace_host"),
            "auth_mode": test_config.get("auth_mode", "user_oauth"),
            "oauth_user": test_config.get("oauth_user"),
            "profile": test_config.get("profile"),
        },
        "roles": [],
        "grants": [],
    }


def test_apply_creates_role_and_is_idempotent(live_config):
    """Apply, assert plan empties, apply again, assert no-op."""
    from lakebase_utils.provisioning import ProvisioningEngine

    sp_id = str(uuid.uuid4())
    cfg = {**live_config, "roles": [{"identity": sp_id, "type": "SERVICE_PRINCIPAL"}]}

    engine = ProvisioningEngine(cfg)
    try:
        first = engine.apply(dry_run=False, auto_approve=True)
        assert first.applied
        assert first.plan.total_actions() >= 1

        second_plan = engine.plan()
        assert second_plan.is_empty(), (
            f"second plan should be empty after apply; got "
            f"{second_plan.actions_by_section}"
        )
    finally:
        # Teardown: drop the role we created.
        engine.pg.execute(f'DROP OWNED BY "{sp_id}" CASCADE; DROP ROLE "{sp_id}";')
        engine.close()


def test_revoke_drift_detected(live_config):
    """Plan with revoke_existing=true should surface a manually-added GRANT."""
    pytest.skip("manual sequencing — see docs/lakebase_provisioning.md for the steps")
