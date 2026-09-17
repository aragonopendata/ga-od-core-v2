"""Tests for the custom exception classes and the problem-document exception handler.

The API answers errors with an RFC 9457-style problem document served as
``application/problem+json``. ``error_code`` is a deliberate project-specific extension
of RFC 9457, and validation problems carry an extra ``errors`` member; neither member is
part of the RFC, so this suite pins them explicitly.
"""

import pytest
from rest_framework import serializers
from rest_framework.exceptions import APIException, NotFound, ValidationError
from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.request import Request
from rest_framework.test import APIRequestFactory

from custom_renderers import ProblemJSONRenderer, SCSVRenderer
from exceptions import (
    PROBLEM_CONTENT_TYPE,
    PROBLEM_TYPE_BASE,
    VALIDATION_ERROR_DETAIL,
    BadGateway,
    ErrorCodes,
    ServiceUnavailable,
    normalize_error_code,
    problem_title,
    problem_type,
)
from gaodcore.exception_handlers import custom_exception_handler

ENVELOPE_KEYS = {"type", "title", "status", "detail", "error_code"}


def handle(exc, request=None):
    """Run the handler and return the problem document."""
    context = {"request": request} if request is not None else {}
    response = custom_exception_handler(exc, context)
    assert response is not None
    return response


def drf_request(accept="application/json"):
    """Build a DRF request with an already negotiated renderer, as a view would have."""
    request = Request(APIRequestFactory().get("/GA_OD_Core/download", HTTP_ACCEPT=accept))
    request.accepted_renderer = SCSVRenderer()
    request.accepted_media_type = SCSVRenderer.media_type
    return request


class TestErrorCodes:
    """Tests for ErrorCodes constants."""

    def test_connection_unavailable_code(self):
        assert ErrorCodes.CONNECTION_UNAVAILABLE == "CONNECTION_UNAVAILABLE"

    def test_object_unavailable_code(self):
        assert ErrorCodes.OBJECT_UNAVAILABLE == "OBJECT_UNAVAILABLE"

    def test_query_error_code(self):
        assert ErrorCodes.QUERY_ERROR == "QUERY_ERROR"

    def test_schema_not_implemented_code(self):
        assert ErrorCodes.SCHEMA_NOT_IMPLEMENTED == "SCHEMA_NOT_IMPLEMENTED"

    def test_bad_gateway_code(self):
        assert ErrorCodes.BAD_GATEWAY == "BAD_GATEWAY"

    def test_validation_error_code(self):
        assert ErrorCodes.VALIDATION_ERROR == "VALIDATION_ERROR"


class TestProblemVocabulary:
    """Tests for the stable type/title derivation."""

    @pytest.mark.parametrize(
        "code,expected_type,expected_title",
        [
            (
                "CONNECTION_UNAVAILABLE",
                PROBLEM_TYPE_BASE + "connection-unavailable",
                "Connection unavailable",
            ),
            ("VALIDATION_ERROR", PROBLEM_TYPE_BASE + "validation-error", "Validation error"),
            ("BAD_GATEWAY", PROBLEM_TYPE_BASE + "bad-gateway", "Bad gateway"),
        ],
    )
    def test_type_and_title(self, code, expected_type, expected_title):
        assert problem_type(code) == expected_type
        assert problem_title(code) == expected_title

    @pytest.mark.parametrize("raw", ["connection_unavailable", "Connection-Unavailable"])
    def test_codes_are_normalized_to_upper_snake_case(self, raw):
        assert normalize_error_code(raw) == "CONNECTION_UNAVAILABLE"

    @pytest.mark.parametrize(
        "raw,status_code,expected",
        [
            # An HTTP status passed as a DRF code is meaningless; fall back on the status.
            ("400", 400, "BAD_REQUEST"),
            ("503", 503, "SERVICE_UNAVAILABLE"),
            (None, 502, "BAD_GATEWAY"),
            (None, None, "ERROR"),
        ],
    )
    def test_unusable_codes_fall_back_to_the_status(self, raw, status_code, expected):
        assert normalize_error_code(raw, status_code) == expected


class TestServiceUnavailable:
    """Tests for ServiceUnavailable exception."""

    def test_default_status_code(self):
        assert ServiceUnavailable().status_code == 503

    def test_default_detail(self):
        assert ServiceUnavailable().detail == "Service temporarily unavailable."

    def test_custom_detail(self):
        assert ServiceUnavailable("Database connection failed.").detail == (
            "Database connection failed."
        )

    def test_custom_code(self):
        exc = ServiceUnavailable(
            "Connection failed.", code=ErrorCodes.CONNECTION_UNAVAILABLE
        )
        assert exc.get_codes() == ErrorCodes.CONNECTION_UNAVAILABLE

    def test_inherits_from_api_exception(self):
        assert issubclass(ServiceUnavailable, APIException)


class TestBadGateway:
    """Tests for BadGateway exception."""

    def test_default_status_code(self):
        assert BadGateway().status_code == 502

    def test_default_code(self):
        assert BadGateway().default_code == ErrorCodes.BAD_GATEWAY


class TestProblemDocument:
    """Tests for the shape of the problem document."""

    def test_general_error_document(self):
        """The reference document of the contract."""
        response = handle(
            ServiceUnavailable(
                "Connection is not available.", code=ErrorCodes.CONNECTION_UNAVAILABLE
            )
        )
        assert response.status_code == 503
        assert response.data == {
            "type": "https://opendata.aragon.es/problems/connection-unavailable",
            "title": "Connection unavailable",
            "status": 503,
            "detail": "Connection is not available.",
            "error_code": "CONNECTION_UNAVAILABLE",
        }

    def test_status_matches_the_http_status(self):
        for exc, expected in (
            (ValidationError("Invalid input."), 400),
            (BadGateway(), 502),
            (ServiceUnavailable("Nope."), 503),
            (NotFound(), 404),
        ):
            response = handle(exc)
            assert response.data["status"] == response.status_code == expected

    def test_general_error_has_no_errors_member(self):
        assert "errors" not in handle(BadGateway()).data

    def test_detail_is_always_a_string(self):
        for exc in (
            ValidationError({"uri": ["Bad."]}),
            ValidationError(["Bad."]),
            BadGateway(),
        ):
            assert isinstance(handle(exc).data["detail"], str)

    def test_error_code_comes_from_the_exception_default_code(self):
        """A bare APIException only carries DRF's generic "error" code."""
        problem = handle(APIException()).data
        assert problem["status"] == 500
        assert problem["error_code"] == "ERROR"
        assert problem["type"] == PROBLEM_TYPE_BASE + "error"

    def test_error_code_falls_back_to_the_status_when_unusable(self):
        """``ValidationError(msg, 400)`` passes an HTTP status where a code belongs."""
        exc = ValidationError("Bad.", 400)
        assert handle(exc).data["errors"]["non_field_errors"] == [
            {"message": "Bad.", "code": "BAD_REQUEST"}
        ]

    def test_exceptions_translated_by_drf_are_handled(self):
        """``Http404`` has no ``detail`` attribute of its own."""
        from django.http import Http404

        problem = handle(Http404()).data
        assert problem["status"] == 404
        assert problem["error_code"] == "NOT_FOUND"


class TestValidationErrors:
    """Tests for the ``errors`` member of validation problems."""

    def test_field_error_document(self):
        """The reference validation document of the contract."""
        exc = ValidationError(
            {
                "uri": [
                    serializers.ErrorDetail(
                        "Connection is not available.",
                        code=ErrorCodes.CONNECTION_UNAVAILABLE,
                    )
                ]
            }
        )
        assert handle(exc).data == {
            "type": "https://opendata.aragon.es/problems/validation-error",
            "title": "Validation error",
            "status": 400,
            "detail": VALIDATION_ERROR_DETAIL,
            "error_code": "VALIDATION_ERROR",
            "errors": {
                "uri": [
                    {
                        "message": "Connection is not available.",
                        "code": "CONNECTION_UNAVAILABLE",
                    }
                ]
            },
        }

    def test_top_level_error_code_is_never_a_field_code(self):
        """The top level code is not the first entry of ``exc.get_codes()``."""
        exc = ValidationError({"uri": ["Bad."], "name": ["Worse."]})
        assert handle(exc).data["error_code"] == ErrorCodes.VALIDATION_ERROR

    def test_non_field_errors(self):
        errors = handle(ValidationError("Invalid JSON.")).data["errors"]
        assert errors == {
            "non_field_errors": [{"message": "Invalid JSON.", "code": "INVALID"}]
        }

    def test_multiple_messages_per_field_are_all_kept(self):
        exc = ValidationError(
            {
                "uri": [
                    serializers.ErrorDetail("Too short.", code="min_length"),
                    serializers.ErrorDetail("Not a URI.", code="invalid"),
                ]
            }
        )
        assert handle(exc).data["errors"]["uri"] == [
            {"message": "Too short.", "code": "MIN_LENGTH"},
            {"message": "Not a URI.", "code": "INVALID"},
        ]

    def test_multiple_fields_are_all_kept(self):
        exc = ValidationError({"uri": ["Bad."], "name": ["Required."]})
        errors = handle(exc).data["errors"]
        assert set(errors) == {"uri", "name"}

    def test_nested_serializer_errors_keep_their_shape(self):
        exc = ValidationError(
            {"connector": {"uri": [serializers.ErrorDetail("Bad.", code="invalid")]}}
        )
        assert handle(exc).data["errors"] == {
            "connector": {"uri": [{"message": "Bad.", "code": "INVALID"}]}
        }

    def test_list_serializer_errors_keep_their_index(self):
        """``many=True`` produces one entry per item, empty for the valid ones."""
        exc = ValidationError(
            {"resources": [{}, {"name": [serializers.ErrorDetail("Bad.", code="invalid")]}]}
        )
        assert handle(exc).data["errors"] == {
            "resources": [{}, {"name": [{"message": "Bad.", "code": "INVALID"}]}]
        }

    @pytest.mark.parametrize("field", ["status", "detail", "error_code", "errors", "type", "title"])
    def test_collision_prone_field_names_are_not_overwritten(self, field):
        """A field named like an envelope member keeps its own messages."""
        exc = ValidationError({field: [serializers.ErrorDetail("Bad.", code="invalid")]})
        problem = handle(exc).data

        assert problem["errors"][field] == [{"message": "Bad.", "code": "INVALID"}]
        # ...and the envelope is intact.
        assert problem["status"] == 400
        assert problem["detail"] == VALIDATION_ERROR_DETAIL
        assert problem["error_code"] == "VALIDATION_ERROR"
        assert problem["type"] == PROBLEM_TYPE_BASE + "validation-error"
        assert problem["title"] == "Validation error"


class TestProblemRenderer:
    """Tests that errors never go through the data renderers."""

    def test_negotiated_data_renderer_is_replaced(self):
        """Changing ``response.data`` is not enough: the renderer must change too."""
        request = drf_request(accept="text/scsv")
        response = handle(ServiceUnavailable("Nope."), request=request)

        assert isinstance(response.accepted_renderer, ProblemJSONRenderer)
        assert response.accepted_media_type == PROBLEM_CONTENT_TYPE
        # ``APIView.finalize_response`` copies the renderer back from the request.
        assert isinstance(request.accepted_renderer, ProblemJSONRenderer)
        assert request.accepted_media_type == PROBLEM_CONTENT_TYPE

    def test_content_type_is_exactly_problem_json(self):
        request = drf_request(accept="text/csv")
        response = handle(ServiceUnavailable("Nope."), request=request)
        response.renderer_context = {"request": request, "response": response}

        assert response.content_type == PROBLEM_CONTENT_TYPE
        response.render()
        assert response["Content-Type"] == PROBLEM_CONTENT_TYPE

    def test_browsable_api_keeps_rendering_html(self):
        request = drf_request()
        request.accepted_renderer = BrowsableAPIRenderer()
        request.accepted_media_type = BrowsableAPIRenderer.media_type

        response = handle(ServiceUnavailable("Nope."), request=request)

        assert isinstance(request.accepted_renderer, BrowsableAPIRenderer)
        assert not isinstance(
            getattr(response, "accepted_renderer", None), ProblemJSONRenderer
        )

    def test_problem_renderer_media_type(self):
        assert ProblemJSONRenderer.media_type == "application/problem+json"


class CollisionProneSerializer(serializers.Serializer):
    """A serializer whose field names clash with every envelope member."""

    type = serializers.IntegerField()
    title = serializers.IntegerField()
    status = serializers.IntegerField()
    detail = serializers.IntegerField()
    error_code = serializers.IntegerField()
    errors = serializers.IntegerField()


class TestSerializerCollisions:
    """End-to-end check that a real serializer cannot clobber the envelope."""

    def test_every_envelope_member_survives(self):
        serializer = CollisionProneSerializer(data={})
        assert not serializer.is_valid()

        with pytest.raises(ValidationError) as exc_info:
            serializer.is_valid(raise_exception=True)
        problem = handle(exc_info.value).data

        assert problem["type"] == PROBLEM_TYPE_BASE + "validation-error"
        assert problem["title"] == "Validation error"
        assert problem["status"] == 400
        assert problem["detail"] == VALIDATION_ERROR_DETAIL
        assert problem["error_code"] == "VALIDATION_ERROR"

        # Every declared field reported its own "required" message, under "errors".
        assert set(problem["errors"]) == {
            "type",
            "title",
            "status",
            "detail",
            "error_code",
            "errors",
        }
        # The message itself is localized, so pin the code and that a message exists.
        for messages in problem["errors"].values():
            assert len(messages) == 1
            assert messages[0]["code"] == "REQUIRED"
            assert messages[0]["message"]


def problem_code(exc):
    """Return the top level ``error_code`` of the problem document for ``exc``."""
    return handle(exc).data["error_code"]


def non_field_code(exc):
    """Return the semantic code DRF validation errors carry inside ``errors``."""
    problem = handle(exc).data
    assert problem["error_code"] == ErrorCodes.VALIDATION_ERROR
    codes = [item["code"] for item in problem["errors"]["non_field_errors"]]
    assert len(codes) == 1
    return codes[0]


class TestPublicDataErrorCodes:
    """``_get_data_public_error`` translates every connector failure into a stable code.

    Reaching these branches through ``/GA_OD_Core/download`` needs a live database, so
    the translation is exercised directly: the raise sites, their messages and their
    codes are the public contract and must not silently regress to ``INVALID``.
    """

    @staticmethod
    def call(error):
        """Run ``_get_data_public_error`` over a callable that fails with ``error``."""
        from gaodcore.views import _get_data_public_error

        def failing():
            raise error

        with pytest.raises(Exception) as exc_info:
            _get_data_public_error(failing)
        return exc_info.value

    def test_unknown_field_is_a_validation_error(self):
        from connectors import FieldNoExistsError

        exc = self.call(FieldNoExistsError("Field: nope not exists."))
        assert isinstance(exc, ValidationError)
        assert non_field_code(exc) == ErrorCodes.INVALID_FIELD

    def test_unknown_sort_field_is_a_validation_error(self):
        from connectors import SortFieldNoExistsError

        exc = self.call(SortFieldNoExistsError("Sort field: nope not exists."))
        assert isinstance(exc, ValidationError)
        assert non_field_code(exc) == ErrorCodes.INVALID_SORT

    @pytest.mark.parametrize(
        "error_name,message,expected_code",
        [
            ("NoObjectError", "Object is not available.", ErrorCodes.OBJECT_UNAVAILABLE),
            (
                "DriverConnectionError",
                "Connection is not available.",
                ErrorCodes.CONNECTION_UNAVAILABLE,
            ),
            (
                "NotImplementedSchemaError",
                "Unexpected error: schema is not implemented.",
                ErrorCodes.SCHEMA_NOT_IMPLEMENTED,
            ),
            (
                "MimeTypeError",
                "Unexpected error: mimetype of input file is not implemented.",
                ErrorCodes.SCHEMA_NOT_IMPLEMENTED,
            ),
        ],
    )
    def test_server_side_failures_carry_their_code_at_the_top_level(
        self, error_name, message, expected_code
    ):
        """503 problems are not validation errors: no ``errors``, code at the top."""
        import connectors

        exc = self.call(getattr(connectors, error_name)("boom"))
        problem = handle(exc).data

        assert problem["status"] == 503
        assert problem["detail"] == message
        assert problem["error_code"] == expected_code
        assert "errors" not in problem


class TestConnectorQueryErrorCodes:
    """``get_session_data`` translates SQLAlchemy failures into stable codes.

    The query itself is made to fail without a database: what is pinned is the mapping
    from the driver error to the ``error_code`` the client finally sees.
    """

    @staticmethod
    def run_failing_query(mocker, error):
        import sqlalchemy
        from sqlalchemy import Column, Integer, MetaData, Table

        import connectors

        model = Table("resource", MetaData(), Column("id", Integer))

        class FailingSession:
            def query(self, *_args, **_kwargs):
                raise error

            def close(self):
                pass

        engine = sqlalchemy.create_engine("sqlite://")
        mocker.patch.object(connectors, "_get_engine", return_value=engine)
        mocker.patch.object(connectors, "_get_model", return_value=model)
        mocker.patch.object(
            connectors, "sessionmaker", return_value=lambda: FailingSession()
        )

        with pytest.raises(Exception) as exc_info:
            connectors.get_session_data(
                uri="postgresql://user:password@localhost:5432/db",
                object_location="resource",
                object_location_schema=None,
                filters={},
                like={},
                fields=[],
                sort=[],
            )
        return exc_info.value

    def test_missing_object_is_object_unavailable(self, mocker):
        import sqlalchemy

        error = sqlalchemy.exc.ProgrammingError("SELECT 1", {}, Exception("no table"))
        problem = handle(self.run_failing_query(mocker, error)).data

        assert problem["status"] == 503
        assert problem["detail"] == "Object not available."
        assert problem["error_code"] == ErrorCodes.OBJECT_UNAVAILABLE

    def test_invalid_request_is_a_query_error(self, mocker):
        import sqlalchemy

        error = sqlalchemy.exc.InvalidRequestError("bad query")
        problem = handle(self.run_failing_query(mocker, error)).data

        assert problem["status"] == 503
        assert problem["detail"] == "Invalid Request Error."
        assert problem["error_code"] == ErrorCodes.QUERY_ERROR

    def test_unexpected_failure_is_a_query_error(self, mocker):
        """The catch-all branch must not degrade into a generic code either."""
        problem = handle(self.run_failing_query(mocker, RuntimeError("boom"))).data

        assert problem["status"] == 503
        assert problem["detail"] == "Query error"
        assert problem["error_code"] == ErrorCodes.QUERY_ERROR

    def test_sort_field_failure_is_a_validation_error(self, mocker):
        from connectors import SortFieldNoExistsError

        error = SortFieldNoExistsError("Sort field: nope not exists.")
        exc = self.run_failing_query(mocker, error)

        assert isinstance(exc, ValidationError)
        assert non_field_code(exc) == ErrorCodes.INVALID_SORT


class TestUpstreamAndColumnErrorCodes:
    """Codes produced while post-processing a download."""

    def test_failed_upstream_response_is_a_bad_gateway(self):
        """``download_check`` guards every API connector response."""
        from utils import download_check

        class NotOkResponse:
            ok = False

        with pytest.raises(BadGateway) as exc_info:
            download_check(NotOkResponse())

        problem = handle(exc_info.value).data
        assert problem["status"] == 502
        assert problem["error_code"] == ErrorCodes.BAD_GATEWAY
        assert "errors" not in problem

    def test_columns_count_mismatch_is_invalid_columns(self):
        """"columns" has to match "fields"; the mismatch has its own code."""
        from utils import modify_header

        with pytest.raises(ValidationError) as exc_info:
            modify_header([{"id": 1, "name": "a"}], ["only_one"])

        assert non_field_code(exc_info.value) == ErrorCodes.INVALID_COLUMNS
