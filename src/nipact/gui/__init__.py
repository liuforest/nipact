"""Local read-only provenance GUI backend."""

from __future__ import annotations

__all__ = ["create_gui_app"]


def create_gui_app(*args: object, **kwargs: object) -> object:
    from .app import create_gui_app as _create_gui_app

    return _create_gui_app(*args, **kwargs)
