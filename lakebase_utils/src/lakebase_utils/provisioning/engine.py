"""Provisioning engine: builds clients, dispatches reconcilers, prints plans."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from .reconcilers import REGISTRY, Action, Plan, Reconciler

log = logging.getLogger(__name__)


@dataclass
class ApplyResult:
    plan: Plan
    applied: bool


def _build_clients(target: dict) -> tuple[Any, Any]:
    """Construct ``(WorkspaceClient, LakebaseAutoscalingClient)`` from config.

    Imports are lazy so unit tests can monkey-patch the engine without paying
    the SDK / psycopg2 import cost.
    """
    from .._common import _make_ws
    from ..lakebase_connect import LakebaseAutoscalingClient

    ws_kwargs = {
        "host": target.get("workspace_host"),
        "profile": target.get("profile"),
        "client_id": target.get("client_id"),
        "client_secret": target.get("client_secret"),
    }
    ws = _make_ws(**ws_kwargs)

    auth_mode = target.get("auth_mode", "user_oauth")
    endpoint_path = (
        f"projects/{target['project']}/branches/{target['branch']}/"
        f"endpoints/{target['endpoint']}"
    )
    pg_kwargs: dict[str, Any] = {
        "host": target["host"],
        "database": target["database"],
        "auth_mode": auth_mode,
        "endpoint_path": endpoint_path,
        "workspace_host": target.get("workspace_host"),
    }
    if auth_mode == "user_oauth":
        pg_kwargs["oauth_user"] = target.get("oauth_user") or target.get("profile")
        pg_kwargs["profile"] = target.get("profile")
    elif auth_mode == "sp_oauth":
        pg_kwargs["client_id"] = target["client_id"]
        pg_kwargs["client_secret"] = target["client_secret"]
    elif auth_mode == "oauth_token":
        pg_kwargs["oauth_user"] = target["oauth_user"]
        pg_kwargs["token"] = target.get("client_secret")  # repurposed for token

    pg = LakebaseAutoscalingClient(**{k: v for k, v in pg_kwargs.items() if v is not None})
    return ws, pg


class ProvisioningEngine:
    """Holds the SDK + Postgres clients and orchestrates reconcilers."""

    def __init__(self, config: dict, *, ws: Any = None, pg: Any = None):
        self.config = config
        if ws is None or pg is None:
            built_ws, built_pg = _build_clients(config["target"])
            self.ws = ws or built_ws
            self.pg = pg or built_pg
        else:
            self.ws = ws
            self.pg = pg
        self.reconcilers: list[Reconciler] = [cls(self) for cls in REGISTRY]

    def plan(self) -> Plan:
        plan = Plan()
        for r in self.reconcilers:
            section_cfg = self.config.get(r.section, [])
            actions = r.plan(section_cfg)
            plan.add(r.section, actions)
        return plan

    def apply(self, *, dry_run: bool = False, auto_approve: bool = False) -> ApplyResult:
        plan = self.plan()
        if dry_run or plan.is_empty():
            return ApplyResult(plan=plan, applied=False)
        if not auto_approve:
            raise RuntimeError(
                "apply() called without --auto-approve and not in dry-run; "
                "the CLI handles interactive confirmation, library callers "
                "must pass auto_approve=True explicitly."
            )
        for r in self.reconcilers:
            actions = plan.for_section(r.section)
            if actions:
                r.apply(actions)
        return ApplyResult(plan=plan, applied=True)

    def close(self) -> None:
        close = getattr(self.pg, "close", None)
        if callable(close):
            close()


def format_plan(plan: Plan) -> str:
    """Human-readable plan output for the CLI."""
    if plan.is_empty():
        return "No changes required. Lakebase data layer matches config."

    lines: list[str] = []
    for section, actions in plan.actions_by_section.items():
        if not actions:
            continue
        lines.append(f"\n# {section} ({len(actions)} action(s))")
        for a in actions:
            lines.append(f"  [{a.kind}] {a.target}")
            if a.sql:
                for sql_line in a.sql.splitlines():
                    lines.append(f"      {sql_line}")
            elif a.metadata:
                for k, v in a.metadata.items():
                    lines.append(f"      {k}: {v}")
    lines.append(f"\nTotal actions: {plan.total_actions()}")
    return "\n".join(lines)


def _render_action(a: Action) -> str:
    """Used by tests; same shape as one block in format_plan."""
    head = f"[{a.kind}] {a.target}"
    if a.sql:
        return head + "\n" + a.sql
    return head
