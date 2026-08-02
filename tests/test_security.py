from bronze_ingestion.security import redact_sensitive_text, sanitize_exception


def test_sensitive_query_parameters_are_redacted() -> None:
    message = (
        "401 for https://example.test/data?appid=weather-secret"
        "&access_key=aviation-secret&units=metric"
    )

    redacted = redact_sensitive_text(message)

    assert "weather-secret" not in redacted
    assert "aviation-secret" not in redacted
    assert "appid=<redacted>" in redacted
    assert "access_key=<redacted>" in redacted
    assert "units=metric" in redacted


def test_assignments_and_bearer_tokens_are_redacted() -> None:
    message = "token=plain-secret Authorization: Bearer header.secret.value"

    redacted = redact_sensitive_text(message)

    assert "plain-secret" not in redacted
    assert "header.secret.value" not in redacted
    assert "token=<redacted>" in redacted
    assert "Bearer <redacted>" in redacted


def test_sanitized_exception_is_bounded() -> None:
    exc = RuntimeError("password=secret-value " + "x" * 100)

    redacted = sanitize_exception(exc, max_length=32)

    assert "secret-value" not in redacted
    assert redacted.endswith("...<truncated>")
