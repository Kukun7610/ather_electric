import importlib.util
from pathlib import Path

_MODULE_PATH = Path(__file__).resolve().parents[1] / "custom_components" / "ather_electric" / "helpers.py"
_SPEC = importlib.util.spec_from_file_location("ather_helpers", _MODULE_PATH)
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
_SPEC.loader.exec_module(_MODULE)
normalize_api_token = _MODULE.normalize_api_token


def test_normalize_api_token_prefers_canonical_value():
    assert normalize_api_token({"api_token": "legacy", "ather_token": "new"}) == "new"
    assert normalize_api_token({"api_token": "legacy"}) == "legacy"
    assert normalize_api_token({}) is None


def test_normalize_api_token_ignores_blank_values():
    assert normalize_api_token({"api_token": "", "ather_token": "  "}) is None
    assert normalize_api_token({"api_token": "   ", "ather_token": "token"}) == "token"
