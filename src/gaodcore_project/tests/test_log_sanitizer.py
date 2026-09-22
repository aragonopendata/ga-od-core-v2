"""Pure unit tests for log_sanitizer - no DB needed."""

from log_sanitizer import mask_uri, redact_query_params


def test_mask_uri_hides_password_keeps_the_rest():
    uri = "postgresql://user:s3cr3t@host:5432/db"
    masked = mask_uri(uri)

    assert "s3cr3t" not in masked
    assert masked.startswith("postgresql://user:***@host:5432/db")
    assert masked == "postgresql://user:***@host:5432/db"


def test_mask_uri_idempotent():
    uri = "postgresql://user:s3cr3t@host:5432/db"
    once = mask_uri(uri)
    twice = mask_uri(once)

    assert once == twice
    assert "s3cr3t" not in twice


def test_mask_uri_idempotent_on_credential_free_input():
    uri = "https://example.org/data.json"
    assert mask_uri(uri) == uri
    assert mask_uri(mask_uri(uri)) == uri


def test_mask_uri_passes_through_credential_free_uris():
    assert mask_uri("https://example.org/data.json") == "https://example.org/data.json"
    assert mask_uri("sqlite:///file.db") == "sqlite:///file.db"


def test_mask_uri_never_raises_on_none():
    result = mask_uri(None)
    assert isinstance(result, str)


def test_mask_uri_never_raises_on_empty_string():
    result = mask_uri("")
    assert isinstance(result, str)


def test_mask_uri_never_raises_on_non_string():
    result = mask_uri(12345)
    assert isinstance(result, str)
    assert "12345" not in result


def test_mask_uri_never_raises_on_malformed_string():
    result = mask_uri("not a uri at all !! ///:::")
    assert isinstance(result, str)


def test_mask_uri_handles_password_with_at_and_colon():
    uri = "postgresql://user:pa@ss:word@host:5432/db"
    masked = mask_uri(uri)

    assert "pa@ss:word" not in masked
    assert "host:5432/db" in masked
    assert "user:***@" in masked


def test_redact_query_params_masks_uri():
    params = {"uri": "postgresql://user:s3cr3t@host:5432/db"}
    redacted = redact_query_params(params)

    assert "s3cr3t" not in str(redacted)
    assert redacted["uri"] == mask_uri(params["uri"])


def test_redact_query_params_redacts_sensitive_keys_case_insensitively():
    params = {
        "Password": "hunter2",
        "API_KEY": "abc123",
        "token": "xyz",
        "secret": "shh",
    }
    redacted = redact_query_params(params)

    for key in params:
        assert redacted[key] == "***"


def test_redact_query_params_preserves_ordinary_keys():
    params = {"resource_id": 42, "limit": 10, "filters": "a=1"}
    redacted = redact_query_params(params)

    assert redacted == params


def test_redact_query_params_returns_a_copy():
    params = {"uri": "postgresql://user:s3cr3t@host:5432/db", "resource_id": 1}
    redacted = redact_query_params(params)

    assert redacted is not params
    assert params["uri"] == "postgresql://user:s3cr3t@host:5432/db"


def test_redact_query_params_never_raises_on_non_dict():
    assert redact_query_params(None) == {}
