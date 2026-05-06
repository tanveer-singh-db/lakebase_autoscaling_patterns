"""Read/update Data API project settings (exposed schemas + cache refresh).

The exposed-schemas list is a project-level Lakebase setting. As of the
SDK we test against, the SDK's typed surface for this varies; we read
defensively and surface a clear ``UnsupportedOperation`` if the field
isn't present, so the reconciler can advise the user to flip it in the
UI manually rather than silently no-oping.
"""

from __future__ import annotations

from typing import Any


class UnsupportedOperation(RuntimeError):
    """The current SDK does not expose Data API project settings via a typed
    method, so we can't programmatically read or update them."""


def _project_resource_name(project: str) -> str:
    return project if project.startswith("projects/") else f"projects/{project}"


def get_exposed_schemas(ws: Any, project: str) -> list[str]:
    """Return the schemas the Data API currently exposes for ``project``."""
    name = _project_resource_name(project)
    try:
        proj = ws.postgres.get_project(name=name)
    except AttributeError as e:
        raise UnsupportedOperation(
            "WorkspaceClient.postgres.get_project is not available in this "
            "SDK version; configure exposed schemas via the Lakebase UI."
        ) from e

    spec = getattr(proj, "spec", None) or proj
    data_api = getattr(spec, "data_api", None)
    if data_api is None:
        raise UnsupportedOperation(
            "Project resource has no `data_api` field in this SDK version; "
            "configure exposed schemas via the Lakebase UI."
        )
    schemas = getattr(data_api, "exposed_schemas", None)
    if schemas is None:
        return []
    return list(schemas)


def set_exposed_schemas(ws: Any, project: str, schemas: list[str]) -> None:
    """Replace the exposed-schemas list for ``project``."""
    name = _project_resource_name(project)
    try:
        from databricks.sdk.service.postgres import FieldMask  # type: ignore
    except ImportError as e:
        raise UnsupportedOperation(
            "databricks.sdk.service.postgres.FieldMask not importable; cannot "
            "update Data API exposed schemas."
        ) from e

    try:
        proj = ws.postgres.get_project(name=name)
    except AttributeError as e:
        raise UnsupportedOperation(
            "WorkspaceClient.postgres.get_project is not available."
        ) from e

    spec = getattr(proj, "spec", None) or proj
    data_api = getattr(spec, "data_api", None)
    if data_api is None:
        raise UnsupportedOperation(
            "Project resource has no `data_api` field; cannot update."
        )
    data_api.exposed_schemas = list(schemas)

    ws.postgres.update_project(
        name=name,
        project=proj,
        update_mask=FieldMask(field_mask=["spec.data_api.exposed_schemas"]),
    )


def refresh_schema_cache(ws: Any, project: str) -> None:
    """Bust the PostgREST schema cache so newly-exposed schemas become
    queryable. Falls back to ``UnsupportedOperation`` when the SDK lacks the
    method — the user will need to click *Refresh schema cache* in the UI."""
    name = _project_resource_name(project)
    refresh = getattr(ws.postgres, "refresh_data_api_schema_cache", None)
    if refresh is None:
        raise UnsupportedOperation(
            "WorkspaceClient.postgres.refresh_data_api_schema_cache is not "
            "available; click *Refresh schema cache* in the Lakebase UI."
        )
    refresh(name=name)
