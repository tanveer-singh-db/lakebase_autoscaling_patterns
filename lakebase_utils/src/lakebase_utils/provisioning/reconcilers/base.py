"""Reconciler ABC + Action / Plan dataclasses."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar


@dataclass
class Action:
    """A single planned change. ``sql`` is populated for SQL-emitting actions
    so ``--dry-run`` can echo the exact statement."""
    kind: str
    target: str
    sql: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Plan:
    actions_by_section: dict[str, list[Action]] = field(default_factory=dict)

    def add(self, section: str, actions: list[Action]) -> None:
        self.actions_by_section.setdefault(section, []).extend(actions)

    def for_section(self, section: str) -> list[Action]:
        return self.actions_by_section.get(section, [])

    def is_empty(self) -> bool:
        return all(not v for v in self.actions_by_section.values())

    def total_actions(self) -> int:
        return sum(len(v) for v in self.actions_by_section.values())


class Reconciler(ABC):
    """Subclasses implement plan() (read state, diff vs desired) and apply()."""

    section: ClassVar[str]

    def __init__(self, engine: Any):
        self.engine = engine

    @abstractmethod
    def plan(self, desired: Any) -> list[Action]: ...

    @abstractmethod
    def apply(self, actions: list[Action]) -> None: ...
