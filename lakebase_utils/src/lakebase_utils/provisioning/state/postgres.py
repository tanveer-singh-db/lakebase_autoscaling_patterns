"""Read current state from Lakebase Postgres for diffing."""

from __future__ import annotations

from typing import Protocol


class _PgReader(Protocol):
    def fetch(self, sql: str) -> list[tuple]: ...


def existing_db_roles(pg: _PgReader) -> set[str]:
    """All Postgres roles that look like Databricks identities (members of
    ``databricks_group``). Excludes service roles like ``authenticator`` and
    superusers."""
    rows = pg.fetch(
        "SELECT r.rolname "
        "FROM pg_catalog.pg_roles r "
        "JOIN pg_catalog.pg_auth_members m ON m.member = r.oid "
        "JOIN pg_catalog.pg_roles g ON g.oid = m.roleid "
        "WHERE g.rolname = 'databricks_group'"
    )
    return {row[0] for row in rows}


def members_of_authenticator(pg: _PgReader) -> set[str]:
    """Roles GRANTed to ``authenticator`` (i.e. usable as Data API identities)."""
    rows = pg.fetch(
        "SELECT r.rolname "
        "FROM pg_catalog.pg_auth_members m "
        "JOIN pg_catalog.pg_roles r ON r.oid = m.member "
        "JOIN pg_catalog.pg_roles a ON a.oid = m.roleid "
        "WHERE a.rolname = 'authenticator'"
    )
    return {row[0] for row in rows}


def schema_usage_grants(pg: _PgReader, schemas: list[str]) -> dict[tuple[str, str], bool]:
    """Map ``(schema, grantee) -> True`` for every USAGE grant currently held.
    Empty schemas list short-circuits to an empty dict."""
    if not schemas:
        return {}
    placeholders = ",".join(f"'{s}'" for s in schemas)
    rows = pg.fetch(
        "SELECT n.nspname, r.rolname "
        "FROM pg_catalog.pg_namespace n "
        "JOIN pg_catalog.pg_roles r ON has_schema_privilege(r.oid, n.oid, 'USAGE') "
        f"WHERE n.nspname IN ({placeholders})"
    )
    return {(row[0], row[1]): True for row in rows}


def table_grants(pg: _PgReader, schemas: list[str]) -> set[tuple[str, str, str, str]]:
    """Set of ``(schema, table, grantee, privilege)`` tuples held in the given
    schemas. Privilege is one of ``SELECT/INSERT/UPDATE/DELETE``."""
    if not schemas:
        return set()
    placeholders = ",".join(f"'{s}'" for s in schemas)
    rows = pg.fetch(
        "SELECT table_schema, table_name, grantee, privilege_type "
        "FROM information_schema.role_table_grants "
        f"WHERE table_schema IN ({placeholders}) "
        "AND privilege_type IN ('SELECT','INSERT','UPDATE','DELETE')"
    )
    return {(s, t, g, p) for s, t, g, p in rows}


def tables_in_schema(pg: _PgReader, schema: str) -> list[str]:
    """All non-system tables in a schema. Uses ``pg_catalog.pg_tables`` to
    bypass ``information_schema``'s privilege filtering."""
    rows = pg.fetch(
        "SELECT tablename FROM pg_catalog.pg_tables "
        f"WHERE schemaname = '{schema}' "
        "ORDER BY tablename"
    )
    return [row[0] for row in rows]
