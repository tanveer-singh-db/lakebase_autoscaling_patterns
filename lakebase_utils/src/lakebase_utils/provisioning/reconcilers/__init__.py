"""Reconciler registry. Order matters: roles → grants → exposed_schemas."""

from __future__ import annotations

from .base import Action, Plan, Reconciler
from .exposed_schemas import ExposedSchemaReconciler
from .grants import GrantReconciler
from .roles import RoleReconciler

REGISTRY: list[type[Reconciler]] = [
    RoleReconciler,
    GrantReconciler,
    ExposedSchemaReconciler,
]

__all__ = [
    "Action",
    "ExposedSchemaReconciler",
    "GrantReconciler",
    "Plan",
    "REGISTRY",
    "Reconciler",
    "RoleReconciler",
]
