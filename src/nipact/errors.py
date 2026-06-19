"""Small NIPACT error types for internal validation contracts."""

from __future__ import annotations


class NipactError(Exception):
    """Base class for NIPACT-specific exceptions."""


class ValidationError(ValueError, NipactError):
    """Raised when a user-facing identifier or manifest fails validation."""
