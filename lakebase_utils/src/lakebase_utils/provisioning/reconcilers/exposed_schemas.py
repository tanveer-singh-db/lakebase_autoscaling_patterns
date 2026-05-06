"""Reconciler: Data API exposed-schemas list + cache refresh."""

from __future__ import annotations

import logging

from .base import Action, Reconciler
from ..state import data_api

log = logging.getLogger(__name__)


class ExposedSchemaReconciler(Reconciler):
    section = "exposed_schemas"

    def plan(self, desired) -> list[Action]:
        # The CLI passes the full config slice; pull the section + cache flag.
        cfg = self.engine.config
        schemas_desired = list(cfg.get("exposed_schemas") or [])
        refresh_after = bool(cfg.get("refresh_schema_cache", True))
        project = cfg["target"]["project"]

        try:
            current = set(data_api.get_exposed_schemas(self.engine.ws, project))
        except data_api.UnsupportedOperation as e:
            if not schemas_desired:
                return []
            return [Action(
                kind="manual",
                target=f"data_api:{project}",
                metadata={"reason": str(e), "schemas": schemas_desired},
            )]

        want = set(schemas_desired)
        if want == current:
            return []

        actions: list[Action] = [Action(
            kind="set_exposed_schemas",
            target=f"data_api:{project}",
            metadata={"current": sorted(current), "desired": sorted(want)},
        )]
        if refresh_after:
            actions.append(Action(
                kind="refresh_cache",
                target=f"data_api:{project}",
            ))
        return actions

    def apply(self, actions: list[Action]) -> None:
        project = self.engine.config["target"]["project"]
        for a in actions:
            if a.kind == "set_exposed_schemas":
                data_api.set_exposed_schemas(
                    self.engine.ws, project, a.metadata["desired"]
                )
            elif a.kind == "refresh_cache":
                data_api.refresh_schema_cache(self.engine.ws, project)
            elif a.kind == "manual":
                log.warning(
                    "exposed_schemas: skipped (%s). Configure %s manually in the Lakebase UI.",
                    a.metadata.get("reason"), a.metadata.get("schemas"),
                )
