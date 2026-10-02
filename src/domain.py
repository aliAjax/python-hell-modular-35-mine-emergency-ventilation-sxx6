from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class DomainError(Exception):
    """Base error for domain failures."""


class ValidationError(DomainError):
    """Input does not satisfy a domain rule."""


class PermissionDenied(DomainError):
    """Actor is not allowed to perform the action."""


class NotFoundError(DomainError):
    """Requested record does not exist."""


class ConflictError(DomainError):
    """A version or uniqueness constraint was violated."""


class OfflineMergeError(ConflictError):
    """A batch of offline records could not be merged.

    ``conflicts`` lists records that press against current center state;
    ``rejected_capacity`` describes the refuge whose capacity would be exceeded.
    The whole batch is rejected, so nothing is written.
    """

    def __init__(self, message, conflicts=None, rejected_capacity=None):
        super().__init__(message)
        self.conflicts = conflicts or []
        self.rejected_capacity = rejected_capacity
        details = {}
        if self.conflicts:
            details["conflicts"] = self.conflicts
        if self.rejected_capacity:
            details["rejected_capacity"] = self.rejected_capacity
        self.details = details


class InvalidTransition(DomainError):
    """The requested state transition is not valid."""


class Role(str, Enum):
    viewer = "viewer"
    admin = "admin"
    dispatcher = "dispatcher"
    safety = "safety"
    field = "field"


@dataclass
class Actor:
    user_id: str
    role: str

    @classmethod
    def from_headers(cls, headers):
        user_id = headers.get("X-User-Id", "anonymous")
        role = headers.get("X-Role", "viewer")
        if role not in {item.value for item in Role}:
            raise PermissionDenied("unknown role: " + role)
        return cls(user_id=user_id, role=role)


@dataclass
class Entity:
    id: str
    kind: str
    status: str
    version: int
    data: Dict[str, Any]
    created_by: str
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row):
        return cls(
            id=row["id"],
            kind=row["kind"],
            status=row["status"],
            version=row["version"],
            data=row["data"],
            created_by=row["created_by"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
