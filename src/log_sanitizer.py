"""Helpers to keep secrets out of application logs.

Standard library imports only: ``views.py`` and ``connectors.py`` both need this
module, and ``utils.py`` (which already imports ``connectors``) needs it too.
Keeping this module dependency-free avoids any risk of an import cycle.

These functions are called from logging paths, including public request
handling, so they must never raise, whatever they are given.
"""

from typing import Any, Dict
from urllib.parse import urlsplit

UNPARSEABLE_PLACEHOLDER = "<unparseable-uri>"
MASK = "***"

_SENSITIVE_KEYS = {
    "password",
    "passwd",
    "pwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "authorization",
    "auth",
}


def mask_uri(uri: Any) -> str:
    """Return ``uri`` with its password (if any) replaced by a fixed placeholder.

    Scheme, username, host, port and path are kept intact so operators can still
    identify which connector failed. Idempotent: masking an already-masked or
    credential-free URI returns it unchanged in substance. Never raises - on any
    unparseable input it returns a safe placeholder instead of echoing the raw
    value.
    """
    if not isinstance(uri, str) or not uri:
        return UNPARSEABLE_PLACEHOLDER

    try:
        parts = urlsplit(uri)
    except Exception:  # noqa: BLE001 - logging helper must never raise
        return UNPARSEABLE_PLACEHOLDER

    netloc = parts.netloc
    if not netloc:
        # No authority component (e.g. "sqlite:///file.db" or a bare path):
        # nothing that could carry credentials, so it is already safe.
        return uri

    if "@" not in netloc:
        # No userinfo section, nothing to mask.
        return uri

    userinfo, _, hostinfo = netloc.rpartition("@")
    if ":" not in userinfo:
        # Username with no password: nothing to mask.
        return uri

    username, _, _password = userinfo.partition(":")
    new_netloc = f"{username}:{MASK}@{hostinfo}"

    rebuilt = parts.scheme + "://" if parts.scheme else ""
    rebuilt += new_netloc
    rebuilt += parts.path or ""
    if parts.query:
        rebuilt += "?" + parts.query
    if parts.fragment:
        rebuilt += "#" + parts.fragment
    return rebuilt


def redact_query_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of ``params`` safe to log.

    The ``uri`` key is masked with :func:`mask_uri`. Other sensitive keys
    (matched case-insensitively against a fixed set) are replaced with a fixed
    placeholder. Every other key is left untouched. Never raises.
    """
    if not isinstance(params, dict):
        return {}

    redacted: Dict[str, Any] = {}
    for key, value in params.items():
        try:
            if isinstance(key, str) and key.lower() == "uri":
                redacted[key] = mask_uri(value)
            elif isinstance(key, str) and key.lower() in _SENSITIVE_KEYS:
                redacted[key] = MASK
            else:
                redacted[key] = value
        except Exception:  # noqa: BLE001 - logging helper must never raise
            redacted[key] = MASK
    return redacted
