"""Helper functions for Ather Electric integration."""

from __future__ import annotations

from typing import Any, Mapping


def normalize_api_token(data: Mapping[str, Any] | None) -> str | None:
    """Return the canonical Ather API token from legacy and current config keys."""
    if not data:
        return None

    for key in ("ather_token", "api_token", "token"):
        value = data.get(key)
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned:
                return cleaned

    return None


def safe_bool(value) -> bool:
    """Safely convert to bool."""
    if value in [1, "1", True, "True", "true", "On", "on"]:
        return True
    return False


def is_binary_value(value) -> bool:
    """Check if a value is effectively binary (0/1/True/False)."""
    if isinstance(value, bool):
        return True
    if str(value).lower() in ["true", "false", "on", "off"]:
        return True
    if str(value) in ["0", "1"]:
        return True
    return False
