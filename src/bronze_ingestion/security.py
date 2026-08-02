from __future__ import annotations

import re

_SENSITIVE_QUERY_PARAMETER = re.compile(
    r"(?i)([?&](?:access_key|api_key|apikey|appid|client_secret|password|token)=)[^&#\s]+"
)
_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)\b(access_key|api_key|apikey|appid|client_secret|password|token)"
    r"(\s*[:=]\s*)([^\s,;?&#]+)"
)
_BEARER_TOKEN = re.compile(r"(?i)(\bBearer\s+)[A-Za-z0-9._~+/=-]+")


def redact_sensitive_text(value: object, max_length: int = 2_000) -> str:
    """Return a bounded log message with common credential forms redacted."""
    message = str(value)
    message = _SENSITIVE_QUERY_PARAMETER.sub(r"\1<redacted>", message)
    message = _SENSITIVE_ASSIGNMENT.sub(r"\1\2<redacted>", message)
    message = _BEARER_TOKEN.sub(r"\1<redacted>", message)

    if len(message) > max_length:
        return f"{message[:max_length]}...<truncated>"
    return message


def sanitize_exception(exc: BaseException, max_length: int = 2_000) -> str:
    """Convert an exception to a bounded message that is safe for logs and audit tables."""
    return redact_sensitive_text(exc, max_length=max_length)
