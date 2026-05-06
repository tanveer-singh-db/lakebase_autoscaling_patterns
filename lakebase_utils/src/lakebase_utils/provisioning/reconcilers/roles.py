"""Reconciler: Postgres roles for Databricks identities."""

from __future__ import annotations

from .base import Action, Reconciler
from ..state import postgres as pg_state


def _create_role_sql(identity: str, identity_type: str) -> str:
    safe = identity.replace("'", "''")
    return (
        "CREATE EXTENSION IF NOT EXISTS databricks_auth;\n"
        f"SELECT databricks_create_role('{safe}', '{identity_type}');\n"
        f'GRANT "{identity}" TO authenticator;'
    )


class RoleReconciler(Reconciler):
    section = "roles"

    def plan(self, desired: list[dict]) -> list[Action]:
        existing = pg_state.existing_db_roles(self.engine.pg)
        members = pg_state.members_of_authenticator(self.engine.pg)

        actions: list[Action] = []
        for r in desired or []:
            identity = r["identity"]
            if identity in existing and identity in members:
                continue
            actions.append(Action(
                kind="create_role",
                target=f"role:{identity}",
                sql=_create_role_sql(identity, r["type"]),
                metadata={"identity": identity, "type": r["type"]},
            ))
        return actions

    def apply(self, actions: list[Action]) -> None:
        for a in actions:
            if a.sql:
                self.engine.pg.execute(a.sql)
