"""Reconciler: schema USAGE + table-level GRANTs (with optional drift REVOKE)."""

from __future__ import annotations

from .base import Action, Reconciler
from ..state import postgres as pg_state


def _expand_table_targets(pg, grant: dict) -> set[tuple[str, str]]:
    """Resolve a grant block to a concrete set of ``(schema, table)`` pairs."""
    out: set[tuple[str, str]] = set()
    for ref in grant.get("select_on_tables") or []:
        schema, table = ref.split(".", 1)
        if table == "*":
            for t in pg_state.tables_in_schema(pg, schema):
                out.add((schema, t))
        else:
            out.add((schema, table))
    for schema in grant.get("select_on_all_tables_in_schemas") or []:
        for t in pg_state.tables_in_schema(pg, schema):
            out.add((schema, t))
    return out


def _grant_table_sql(schema: str, table: str, grantee: str, privileges: list[str]) -> str:
    privs = ", ".join(privileges)
    return f'GRANT {privs} ON "{schema}"."{table}" TO "{grantee}";'


def _revoke_table_sql(schema: str, table: str, grantee: str, privilege: str) -> str:
    return f'REVOKE {privilege} ON "{schema}"."{table}" FROM "{grantee}";'


def _grant_usage_sql(schema: str, grantee: str) -> str:
    return f'GRANT USAGE ON SCHEMA "{schema}" TO "{grantee}";'


class GrantReconciler(Reconciler):
    section = "grants"

    def plan(self, desired: list[dict]) -> list[Action]:
        actions: list[Action] = []
        if not desired:
            return actions

        # 1. Collect every schema mentioned across all grant blocks so we can
        #    read current state in one shot.
        all_schemas: set[str] = set()
        for g in desired:
            all_schemas.update(g.get("usage_on_schemas") or [])
            all_schemas.update(g.get("select_on_all_tables_in_schemas") or [])
            for ref in g.get("select_on_tables") or []:
                all_schemas.add(ref.split(".", 1)[0])
        schemas_list = sorted(all_schemas)

        current_usage = pg_state.schema_usage_grants(self.engine.pg, schemas_list)
        current_grants = pg_state.table_grants(self.engine.pg, schemas_list)

        for g in desired:
            grantee = g["to"]

            # USAGE on schemas.
            for schema in g.get("usage_on_schemas") or []:
                if (schema, grantee) not in current_usage:
                    actions.append(Action(
                        kind="grant",
                        target=f"usage:{schema}/{grantee}",
                        sql=_grant_usage_sql(schema, grantee),
                        metadata={"schema": schema, "grantee": grantee, "privilege": "USAGE"},
                    ))

            # Table privileges.
            privileges = ["SELECT", *(g.get("extra_privileges_on_tables") or [])]
            tables = _expand_table_targets(self.engine.pg, g)
            desired_grants: set[tuple[str, str, str, str]] = set()
            for schema, table in sorted(tables):
                missing = [p for p in privileges
                           if (schema, table, grantee, p) not in current_grants]
                if missing:
                    actions.append(Action(
                        kind="grant",
                        target=f"table:{schema}.{table}/{grantee}",
                        sql=_grant_table_sql(schema, table, grantee, missing),
                        metadata={"schema": schema, "table": table,
                                  "grantee": grantee, "privileges": missing},
                    ))
                for p in privileges:
                    desired_grants.add((schema, table, grantee, p))

            # Drift REVOKE — only when explicitly opted in.
            if g.get("revoke_existing"):
                grant_schemas = {s for s, _, gg, _ in current_grants if gg == grantee}
                for schema, table, gg, priv in current_grants:
                    if gg != grantee:
                        continue
                    if (schema, table, gg, priv) in desired_grants:
                        continue
                    if schema not in (set(g.get("usage_on_schemas") or []) | grant_schemas):
                        # Only revoke privileges in schemas this block manages.
                        continue
                    actions.append(Action(
                        kind="revoke",
                        target=f"table:{schema}.{table}/{grantee}/{priv}",
                        sql=_revoke_table_sql(schema, table, grantee, priv),
                        metadata={"schema": schema, "table": table,
                                  "grantee": grantee, "privilege": priv},
                    ))

        return actions

    def apply(self, actions: list[Action]) -> None:
        for a in actions:
            if a.sql:
                self.engine.pg.execute(a.sql)
