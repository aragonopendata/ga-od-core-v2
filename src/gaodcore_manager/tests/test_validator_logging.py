"""Regression tests: the validator endpoint must not leak URI credentials to logs."""

import logging

import pytest
from _pytest.logging import LogCaptureFixture

SECRET = "s3cr3t"
# Single-label host "test" mirrors test_validator_invalid_uri_error in
# test_validator.py: it fails DNS resolution fast, so the request cannot hang
# (pytest.ini enforces a 10s global timeout). GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS
# is not consulted on this code path (ValidatorView always probes the origin),
# so an unroutable host - not that setting - is what keeps this test fast.
LEAKY_URI = f"postgresql://user:{SECRET}@test/adsf"


@pytest.mark.django_db
def test_get_validator_does_not_leak_password_to_logs(
    auth_client, caplog: LogCaptureFixture
):
    caplog.set_level(logging.INFO, logger="views")

    auth_client.get(
        "/admin/GA_OD_Core_admin/manager/validator",
        {"uri": LEAKY_URI, "object_location": "fail"},
    )

    # The request was recorded ...
    assert any("validator" in record.message for record in caplog.records)
    # ... but the secret never made it into any record.
    for record in caplog.records:
        assert SECRET not in record.getMessage()
    # The masked form should show up, proving redaction (not simple absence of logging).
    assert any("***" in record.getMessage() for record in caplog.records)


@pytest.mark.django_db
def test_post_validator_json_body_reaches_same_validation_path(
    auth_client, caplog: LogCaptureFixture
):
    caplog.set_level(logging.INFO, logger="views")

    response = auth_client.post(
        "/admin/GA_OD_Core_admin/manager/validator",
        data={"uri": LEAKY_URI, "object_location": "fail"},
        content_type="application/json",
    )

    for record in caplog.records:
        assert SECRET not in record.getMessage()
    assert response.status_code != 415


@pytest.mark.django_db
def test_post_validator_form_encoded_body_also_works(
    auth_client, caplog: LogCaptureFixture
):
    caplog.set_level(logging.INFO, logger="views")

    response = auth_client.post(
        "/admin/GA_OD_Core_admin/manager/validator",
        data={"uri": LEAKY_URI, "object_location": "fail"},
    )

    for record in caplog.records:
        assert SECRET not in record.getMessage()
    assert response.status_code != 415


@pytest.mark.django_db
def test_post_validator_missing_uri_returns_400(auth_client):
    response = auth_client.post(
        "/admin/GA_OD_Core_admin/manager/validator",
        data={"object_location": "fail"},
        content_type="application/json",
    )

    assert response.status_code == 400


@pytest.mark.django_db
def test_post_validator_rejects_non_staff_user(non_staff_client):
    response = non_staff_client.post(
        "/admin/GA_OD_Core_admin/manager/validator",
        data={"uri": LEAKY_URI, "object_location": "fail"},
        content_type="application/json",
    )

    assert response.status_code == 403


@pytest.mark.django_db
def test_get_validator_existing_contract_still_holds(auth_client, settings):
    """The GET path keeps working unchanged: an unreachable origin still yields 503.

    Mirrors test_validator_invalid_uri_error in test_validator.py - same
    unroutable-host approach, just confirming the redaction change did not alter
    the response contract.
    """
    settings.GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS = True

    response = auth_client.get(
        "/admin/GA_OD_Core_admin/manager/validator",
        {"object_location": "fail", "uri": "postgresql://test/adsf"},
    )

    assert response.status_code == 503
