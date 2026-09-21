import io
import json
import os
import warnings
import zipfile
from types import SimpleNamespace

import pytest
from django.test.client import Client

from conftest import validate_error, compare_files, problem_of, field_messages, field_codes
from gaodcore import views as gaodcore_views


def test_xlsx_hyperlink_limit_logs_resource_context(monkeypatch):
    original_workbook = gaodcore_views.xlsxwriter.Workbook

    def workbook_with_exhausted_hyperlink_limit(*args, **kwargs):
        workbook = original_workbook(*args, **kwargs)
        original_add_worksheet = workbook.add_worksheet

        def add_worksheet(*worksheet_args, **worksheet_kwargs):
            worksheet = original_add_worksheet(*worksheet_args, **worksheet_kwargs)
            worksheet.hlink_count = 65530
            return worksheet

        workbook.add_worksheet = add_worksheet
        return workbook

    monkeypatch.setattr(
        gaodcore_views.xlsxwriter,
        "Workbook",
        workbook_with_exhausted_hyperlink_limit,
    )
    resource = SimpleNamespace(
        id=42,
        name="test resource",
        object_location="public_view",
        object_location_schema="public",
    )
    logged_warnings = []
    monkeypatch.setattr(
        gaodcore_views.logger,
        "warning",
        lambda message, *args: logged_warnings.append(message % args),
    )

    with warnings.catch_warnings(record=True) as caught_warnings:
        response = gaodcore_views.get_response_xlsx(
            [
                {"source_url": "https://example.com/one"},
                {"source_url": "https://example.com/two"},
            ],
            resource,
        )

    assert response.status_code == 200
    assert caught_warnings == []
    with zipfile.ZipFile(io.BytesIO(response.content)) as workbook:
        shared_strings = workbook.read("xl/sharedStrings.xml")
    assert b"https://example.com/one" in shared_strings
    assert b"https://example.com/two" in shared_strings
    assert len(logged_warnings) == 1
    warning_message = logged_warnings[0]
    assert "XLSX hyperlink limit reached; URLs remain as text" in warning_message
    assert "resource_id=42" in warning_message
    assert "resource_name='test resource'" in warning_message
    assert "object_location='public_view'" in warning_message
    assert "schema='public'" in warning_message
    assert "skipped_hyperlinks=2" in warning_message
    assert "first_excel_row=2" in warning_message
    assert "first_excel_column=1" in warning_message
    assert "column_name='source_url'" in warning_message


@pytest.fixture(params=["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def endpoint(request):
    return request.param


@pytest.mark.django_db
def test_download_view_id(endpoint: str, client: Client, full_example):
    download_response = client.get(
        endpoint, {"view_id": full_example.resources.table.id, "fields": ["id"]}
    )
    response = download_response.json()
    response.sort(key=lambda item: item["id"])
    assert response == [{"id": 1}, {"id": 2}]


@pytest.mark.django_db
def test_download_field(endpoint: str, client: Client, full_example):
    download_response = client.get(
        endpoint, {"resource_id": full_example.resources.table.id, "fields": ["id"]}
    )

    response = download_response.json()
    response.sort(key=lambda item: item["id"])
    assert response == [{"id": 1}, {"id": 2}]


@pytest.mark.django_db
def test_download_field_columns(endpoint: str, client: Client, full_example):
    """
    Test the download with columns filter. Columns filter updates the shown field names in the response with the
    columns specified in the filter.
    """
    download_response = client.get(
        endpoint,
        {
            "resource_id": full_example.resources.table.id,
            "fields": ["id", "name"],
            "columns": ["identifier", "full_name"],
        },
    )

    response = download_response.json()
    assert len(response) == 2
    for key_name in ["identifier", "full_name"]:
        for item in response:
            assert key_name in item.keys()


@pytest.mark.django_db
def test_download_fields(endpoint: str, client: Client, full_example):
    download_response = client.get(
        endpoint,
        {"resource_id": full_example.resources.table.id, "fields": ["id", "name"]},
    )

    response = download_response.json()
    response.sort(key=lambda item: item["id"])
    assert response == [
        {"id": 1, "name": "RX-78-2 Gundam"},
        {"id": 2, "name": "Half Gundam"},
    ]


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_non_existent_field_error(
    endpoint: str, accept_error, client: Client, full_example
):
    download_response = client.get(
        endpoint,
        {
            "resource_id": full_example.resources.table.id,
            "fields": ["non_existent_field"],
        },
        HTTP_ACCEPT=accept_error,
    )
    assert download_response.status_code == 400
    validate_error(
        download_response,
        "Field: non_existent_field not exists.",
        accept_error,
        field_error_code="INVALID_FIELD",
    )


@pytest.mark.django_db
def test_download_name(endpoint: str, client: Client, full_example):
    download_response = client.get(
        "/GA_OD_Core/download",
        {"resource_id": full_example.resources.table.id, "nameRes": "download_name"},
    )
    assert (
        'attachment; filename="download_name.json"'
        == download_response["content-disposition"]
    )


@pytest.mark.django_db
def test_download_name_res(endpoint: str, client: Client, full_example):
    download_response = client.get(
        "/GA_OD_Core/download",
        {
            "resource_id": full_example.resources.table.id,
            "nameRes": "download_name_res",
        },
    )
    assert (
        'attachment; filename="download_name_res.json"'
        == download_response["content-disposition"]
    )


@pytest.mark.django_db
def test_download_format(
    endpoint: str, client: Client, full_example, accept_download: str
):
    download_response = client.get(
        endpoint,
        {
            "resource_id": full_example.resources.table.id,
            "formato": accept_download.split("/")[1],
            "sort": ["id"],
        },
    )

    compare_files(
        os.path.dirname(__file__),
        f"download_{full_example.scheme}",
        accept_download,
        download_response.content,
    )


@pytest.mark.django_db
def test_download_format_error(endpoint: str, client: Client, full_example):
    download_response = client.get(
        endpoint, {"resource_id": full_example.resources.table.id, "formato": "dj"}
    )
    assert download_response.status_code == 400
    # "formato" replaces the Accept header, so an invalid one fails content negotiation
    # before any renderer is chosen. The problem document is served as problem+json.
    problem = problem_of(download_response)
    assert problem["error_code"] == "VALIDATION_ERROR"
    message = field_messages(problem)[0]
    assert message.startswith('Formato: "dj" is not allowed. Allowed values: ')
    assert field_codes(problem) == ["INVALID_FORMAT"]


@pytest.mark.django_db
def test_download_offset(endpoint: str, client: Client, full_example):
    download_response = client.get(
        endpoint, {"resource_id": full_example.resources.table.id, "offset": "1"}
    )

    with open(
        os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r"rb"
    ) as file:
        assert json.loads(file.read())[1:] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_offset_error(
    endpoint: str, accept_error, client: Client, full_example
):
    download_response = client.get(
        endpoint,
        {"resource_id": full_example.resources.table.id, "offset": "a"},
        HTTP_ACCEPT=accept_error,
    )

    assert download_response.status_code == 400
    validate_error(
        download_response, "Value of offset is not a number.", accept_error
    )


@pytest.mark.django_db
def test_download_limit(endpoint: str, client: Client, full_example):
    download_response = client.get(
        endpoint, {"resource_id": full_example.resources.table.id, "limit": "1"}
    )

    with open(
        os.path.join(os.path.dirname(__file__), "download_postgresql.json"), "rb"
    ) as file:
        data = file.read()
        assert json.loads(data)[:1] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_limit_error(
    endpoint: str, accept_error, client: Client, full_example
):
    download_response = client.get(
        endpoint,
        {"resource_id": full_example.resources.table.id, "limit": "a"},
        HTTP_ACCEPT=accept_error,
    )

    assert download_response.status_code == 400
    validate_error(
        download_response, "Value of limit is not a number.", accept_error
    )


@pytest.mark.django_db
def test_download_pagination(endpoint: str, client: Client, full_example):
    download_response = client.get(
        endpoint,
        {
            "resource_id": full_example.resources.table.id,
            "_page": "2",
            "_pageSize": "1",
        },
    )

    with open(
        os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r"rb"
    ) as file:
        data = json.loads(file.read())
        response_data = json.loads(download_response.content)
        assert data[1:2] == response_data


@pytest.mark.django_db
def test_download_pagination_overflow(endpoint: str, client: Client, full_example):
    download_response = client.get(
        endpoint,
        {
            "resource_id": full_example.resources.table.id,
            "_page": "3",
            "_pageSize": "1",
        },
    )

    assert [] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_pagination_page_error(
    endpoint: str, accept_error, client: Client, full_example
):
    download_response = client.get(
        endpoint,
        {
            "resource_id": full_example.resources.table.id,
            "_page": "a",
            "_pageSize": "1",
        },
        HTTP_ACCEPT=accept_error,
    )

    assert download_response.status_code == 400
    validate_error(
        download_response, "Value of _page is not a number.", accept_error
    )


@pytest.mark.django_db
def test_download_pagination_page_size_error(
    endpoint: str, accept_error, client: Client, full_example
):
    download_response = client.get(
        endpoint,
        {
            "resource_id": full_example.resources.table.id,
            "_page": "1",
            "_pageSize": "a",
        },
        HTTP_ACCEPT=accept_error,
    )

    assert download_response.status_code == 400
    validate_error(
        download_response, "Value of _pageSize is not a number.", accept_error
    )


@pytest.mark.django_db
def test_download_filters(endpoint: str, client: Client, full_example):
    download_response = client.get(
        endpoint,
        {
            "resource_id": full_example.resources.table.id,
            "filters": '{"description": null}',
        },
    )

    with open(
        os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r"rb"
    ) as file:
        assert json.loads(file.read())[1:2] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_filters_json_error(endpoint: str, accept_error, client: Client):
    download_response = client.get(
        endpoint, {"resource_id": 1, "filters": "a"}, HTTP_ACCEPT=accept_error
    )

    assert download_response.status_code == 400
    validate_error(
        download_response, "Invalid JSON.", accept_error, field_error_code="INVALID_JSON"
    )


# TODO:  Fix this test
# ['Resource not exists or is not available'] != ['Value [] is not a String, Integer, Float, Bool, Null or None']
@pytest.mark.skip(
    reason="Test is not valid. With operators a filter can have a value of type list."
)
@pytest.mark.django_db
def test_download_filters_value_error(endpoint: str, accept_error, client: Client):
    download_response = client.get(
        endpoint, {"resource_id": 1, "filters": '{"a": []}'}, HTTP_ACCEPT=accept_error
    )
    assert download_response.status_code == 400
    validate_error(
        download_response,
        "Value [] is not a String, Integer, Float, Bool, Null or None",
        accept_error,
    )


@pytest.mark.django_db
def test_download_sort_asc(endpoint: str, client: Client, full_example):
    download_response = client.get(
        endpoint, {"resource_id": full_example.resources.table.id, "sort": "name asc"}
    )

    with open(
        os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r"rb"
    ) as file:
        data = json.loads(file.read())
        data.sort(key=lambda x: x["name"])
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort_desc(endpoint: str, client: Client, full_example):
    download_response = client.get(
        endpoint, {"resource_id": full_example.resources.table.id, "sort": "name desc"}
    )

    with open(
        os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r"rb"
    ) as file:
        data = json.loads(file.read())
        data.sort(key=lambda x: x["name"], reverse=True)
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort_n(endpoint: str, client: Client, full_example):
    download_response = client.get(
        endpoint,
        {
            "resource_id": full_example.resources.table.id,
            "sort": "name asc, description",
        },
    )

    with open(
        os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r"rb"
    ) as file:
        data = json.loads(file.read())
        data.sort(key=lambda x: [x["name"], x["description"]])
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort_non_existent_field_error(
    endpoint: str, accept_error, client: Client, full_example
):
    download_response = client.get(
        endpoint,
        {
            "resource_id": full_example.resources.table.id,
            "sort": "name desc, description, acceleration asc",
        },
        HTTP_ACCEPT=accept_error,
    )

    assert download_response.status_code == 400
    validate_error(
        download_response,
        "Sort field: acceleration not exists.",
        accept_error,
        field_error_code="INVALID_SORT",
    )


@pytest.mark.django_db
def test_download_sort_mode_error(
    endpoint: str, accept_error, client: Client, full_example
):
    download_response = client.get(
        endpoint,
        {"resource_id": full_example.resources.table.id, "sort": "name none"},
        HTTP_ACCEPT=accept_error,
    )
    assert download_response.status_code == 400
    validate_error(
        download_response,
        "Sort value name none is not allowed. Ej: fieldname1 asc, fieldname2 desc.",
        accept_error,
    )


@pytest.mark.django_db
def test_download_sort_too_many_arguments_error(
    endpoint: str, accept_error, client: Client, full_example
):
    download_response = client.get(
        endpoint,
        {"resource_id": full_example.resources.table.id, "sort": "name none asd"},
        HTTP_ACCEPT=accept_error,
    )
    assert download_response.status_code == 400
    validate_error(
        download_response,
        "Sort value name none asd is not allowed. Too many arguments.",
        accept_error,
    )


@pytest.mark.django_db
def test_download_resource_not_exists(endpoint: str, accept_error, client: Client):
    download_response = client.get(
        endpoint,
        {"resource_id": 96, "fields": ["id", "name"]},
        HTTP_ACCEPT=accept_error,
    )
    assert download_response.status_code == 400
    validate_error(
        download_response,
        "Resource not exists or is not available",
        accept_error,
        field_error_code="RESOURCE_UNAVAILABLE",
    )


# Every semantic code a bad query string can produce. They are all raised while parsing
# the query string, before the resource is looked up, so no database is involved.
#
# For a DRF ValidationError the top level "error_code" is always VALIDATION_ERROR: the
# code that clients branch on lives inside "errors". Pinning it here makes the suite
# fail if a raise site regresses to the generic "INVALID" code or to a bare HTTP status.
QUERY_STRING_ERROR_CODES = [
    ({}, "It is required to specify resource_id in the query string.", "REQUIRED"),
    ({"resource_id": "a"}, "Resource_id is not a number.", "NOT_A_NUMBER"),
    ({"resource_id": 1, "offset": "a"}, "Value of offset is not a number.", "NOT_A_NUMBER"),
    ({"resource_id": 1, "limit": "a"}, "Value of limit is not a number.", "NOT_A_NUMBER"),
    ({"resource_id": 1, "_page": "a"}, "Value of _page is not a number.", "NOT_A_NUMBER"),
    (
        {"resource_id": 1, "_pageSize": "a"},
        "Value of _pageSize is not a number.",
        "NOT_A_NUMBER",
    ),
    ({"resource_id": 1, "filters": "a"}, "Invalid JSON.", "INVALID_JSON"),
    ({"resource_id": 1, "like": "a"}, "Invalid JSON.", "INVALID_JSON"),
    (
        {"resource_id": 1, "filters": "[]"},
        "Invalid format: eg. {\u201ckey1\u201d: \u201ca\u201d, \u201ckey2\u201d: \u201cb\u201d}",
        "INVALID_FILTER",
    ),
    (
        {"resource_id": 1, "like": "[]"},
        "Invalid format: eg. {\u201ckey1\u201d: \u201ca\u201d, \u201ckey2\u201d: \u201cb\u201d}",
        "INVALID_FILTER",
    ),
    (
        {"resource_id": 1, "sort": "name none"},
        "Sort value name none is not allowed. Ej: fieldname1 asc, fieldname2 desc.",
        "INVALID_SORT",
    ),
    (
        {"resource_id": 1, "sort": "name none asd"},
        "Sort value name none asd is not allowed. Too many arguments.",
        "INVALID_SORT",
    ),
    ({"resource_id": 96}, "Resource not exists or is not available", "RESOURCE_UNAVAILABLE"),
]


@pytest.mark.django_db
@pytest.mark.parametrize(
    "query,message,expected_code",
    QUERY_STRING_ERROR_CODES,
    ids=[f"{item[2]}-{index}" for index, item in enumerate(QUERY_STRING_ERROR_CODES)],
)
def test_download_query_string_error_codes(
    endpoint: str, query: dict, message: str, expected_code: str, client: Client
):
    """Each bad query string reports its own stable semantic code inside "errors"."""
    response = client.get(endpoint, query, HTTP_ACCEPT="application/json")

    assert response.status_code == 400
    validate_error(
        response,
        message,
        "application/json",
        error_code="VALIDATION_ERROR",
        field_error_code=expected_code,
    )


@pytest.mark.django_db
def test_download_invalid_formato_error_code(endpoint: str, client: Client):
    """``formato`` replaces the Accept header, so a bad one fails content negotiation."""
    response = client.get(endpoint, {"resource_id": 1, "formato": "dj"})

    assert response.status_code == 400
    problem = problem_of(response)
    assert problem["error_code"] == "VALIDATION_ERROR"
    assert field_codes(problem) == ["INVALID_FORMAT"]
    assert field_messages(problem)[0].startswith(
        'Formato: "dj" is not allowed. Allowed values: '
    )


@pytest.mark.django_db
def test_download_extension(
    endpoint: str, accept_download: str, client: Client, full_example
):
    download_response = client.get(
        "/GA_OD_Core/download",
        {"resource_id": full_example.resources.table.id},
        HTTP_ACCEPT=accept_download,
    )
    compare_files(
        os.path.dirname(__file__),
        f"download_{full_example.scheme}",
        accept_download,
        download_response.content,
    )


@pytest.mark.django_db
def test_download_error_is_never_serialized_as_data(accept_error, client: Client):
    """Errors must not go through the data renderers, whatever the client asked for."""
    response = client.get(
        "/GA_OD_Core/download", {"resource_id": -1}, HTTP_ACCEPT=accept_error
    )
    assert response.status_code == 400

    if accept_error == "text/html":
        # The browsable API is the one consumer that still gets HTML.
        assert response["Content-Type"].startswith("text/html")
        return

    assert response["Content-Type"] == "application/problem+json"

    body = response.content
    # Not XLSX (zip magic), not XML, and not a CSV/SCSV table.
    assert not body.startswith(b"PK")
    assert not body.lstrip().startswith(b"<")
    assert b"\r\n" not in body

    problem = problem_of(response)
    assert problem["error_code"] == "VALIDATION_ERROR"
    assert field_codes(problem) == ["RESOURCE_UNAVAILABLE"]


@pytest.mark.django_db
def test_download_body_status_matches_http_status(accept_error, client: Client):
    """The "status" member never drifts from the real HTTP status."""
    response = client.get(
        "/GA_OD_Core/download", {"resource_id": 1, "filters": "a"}, HTTP_ACCEPT=accept_error
    )
    if accept_error == "text/html":
        return
    assert problem_of(response)["status"] == response.status_code == 400
