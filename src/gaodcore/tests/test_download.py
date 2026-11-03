import io
import json
import os
import warnings
import zipfile
from openpyxl import load_workbook
from types import SimpleNamespace

import pytest
from django.test.client import Client

from conftest import validate_error as _validate_error, compare_files, problem_of, field_messages, field_codes
from gaodcore import views as gaodcore_views


def validate_error(response, error_description, mime_type, **kwargs):
    """Shared error contract; the Browsable API is not offered here, so text/html gets the default problem JSON."""
    if mime_type == "text/html":
        mime_type = "application/json"
    return _validate_error(response, error_description, mime_type, **kwargs)



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
    sheet = load_workbook(io.BytesIO(b"".join(response.streaming_content))).active
    url_cells = [sheet.cell(row=row, column=1) for row in (2, 3)]
    assert [cell.value for cell in url_cells] == ["https://example.com/one", "https://example.com/two"]
    assert all(cell.hyperlink is None for cell in url_cells)
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


def _xlsx_body(response) -> bytes:
    return b"".join(response.streaming_content)


def _xlsx_rows(response):
    from openpyxl import load_workbook

    sheet = load_workbook(io.BytesIO(_xlsx_body(response))).active
    return [[(cell.value, cell.data_type) for cell in row] for row in sheet.iter_rows()]


@pytest.fixture
def xlsx_temp_files(monkeypatch):
    """Record every temporary file get_response_xlsx creates, to check none survives."""
    names = []
    original = gaodcore_views.tempfile.NamedTemporaryFile

    def recording(*args, **kwargs):
        handle = original(*args, **kwargs)
        names.append(handle.name)
        return handle

    monkeypatch.setattr(gaodcore_views.tempfile, "NamedTemporaryFile", recording)
    return names


@pytest.mark.django_db  # closing a response fires request_finished, which touches the DB connection
def test_xlsx_is_streamed_from_a_temporary_file_that_is_deleted_on_close(xlsx_temp_files):
    response = gaodcore_views.get_response_xlsx([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])

    assert response.streaming
    assert len(xlsx_temp_files) == 1
    assert os.path.exists(xlsx_temp_files[0])
    assert response["Content-Length"] == str(os.path.getsize(xlsx_temp_files[0]))
    assert response["Content-Type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert "Content-Disposition" not in response  # the view sets the public download name
    assert _xlsx_rows(response) == [[("a", "s"), ("b", "s")], [(1, "n"), ("x", "s")], [(2, "n"), ("y", "s")]]
    response.close()
    assert not os.path.exists(xlsx_temp_files[0])


def test_xlsx_temporary_files_are_deleted_when_writing_fails(xlsx_temp_files, monkeypatch):
    spill_dirs = []
    original_workbook = gaodcore_views.xlsxwriter.Workbook

    def recording_workbook(filename, options=None):
        spill_dirs.append(options["tmpdir"])
        return original_workbook(filename, options)

    monkeypatch.setattr(gaodcore_views.xlsxwriter, "Workbook", recording_workbook)

    def rows_then_error():
        yield {"a": 1}
        yield {"a": 2}
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        gaodcore_views.get_response_xlsx(rows_then_error())

    assert len(xlsx_temp_files) == 1
    assert not os.path.exists(xlsx_temp_files[0])
    assert len(spill_dirs) == 1
    assert not os.path.exists(spill_dirs[0])


@pytest.mark.django_db  # closing a response fires request_finished, which touches the DB connection
@pytest.mark.parametrize(
    "rows",
    [
        [{"id": 1, "name": "a", "n": None}],
        [{"id": i, "name": f"name {i}", "n": i * 0.5} for i in range(1, 50)],
    ],
    ids=["one-row", "many-rows"],
)
def test_xlsx_constant_memory_keeps_header_and_rows(rows):
    """With constant_memory a header written after its row 1 would be lost: it must come out first and complete."""
    response = gaodcore_views.get_response_xlsx(rows)
    cells = _xlsx_rows(response)
    response.close()

    assert [value for value, _ in cells[0]] == ["id", "name", "n"]
    assert len(cells) == len(rows) + 1
    assert [value for value, _ in cells[1]] == [rows[0]["id"], rows[0]["name"], rows[0]["n"]]
    assert [value for value, _ in cells[-1]] == [rows[-1]["id"], rows[-1]["name"], rows[-1]["n"]]


@pytest.mark.django_db  # closing a response fires request_finished, which touches the DB connection
def test_xlsx_empty_data_has_no_header():
    response = gaodcore_views.get_response_xlsx([])
    assert _xlsx_rows(response) == []
    response.close()


@pytest.mark.django_db  # closing a response fires request_finished, which touches the DB connection
def test_xlsx_constant_memory_cell_values():
    data = [{"cr": "a\rb", "empty": "", "none": None, "flag": True, "no": False, "text": "ñ ✓"}]
    response = gaodcore_views.get_response_xlsx(data)
    sheet_xml = zipfile.ZipFile(io.BytesIO(_xlsx_body(response))).read("xl/worksheets/sheet1.xml")
    response.close()
    assert b"a_x000D_b" in sheet_xml  # a carriage return is stored escaped

    response = gaodcore_views.get_response_xlsx(data)
    row = _xlsx_rows(response)[1]
    response.close()
    assert row[0] == ("a_x000D_b", "s")  # read back literally, as without constant_memory
    assert row[1][0] is None  # "" is an empty cell
    assert row[2][0] is None  # None is a blank cell
    assert row[3] == (True, "b")
    assert row[4] == (False, "b")
    assert row[5] == ("ñ ✓", "s")


@pytest.mark.django_db  # closing a response fires request_finished, which touches the DB connection
def test_xlsx_hyperlink_over_the_limit_stays_as_text_cell(monkeypatch):
    original_workbook = gaodcore_views.xlsxwriter.Workbook

    def exhausted(*args, **kwargs):
        workbook = original_workbook(*args, **kwargs)
        add_worksheet = workbook.add_worksheet

        def patched(*a, **k):
            worksheet = add_worksheet(*a, **k)
            worksheet.hlink_count = 65530
            return worksheet

        workbook.add_worksheet = patched
        return workbook

    monkeypatch.setattr(gaodcore_views.xlsxwriter, "Workbook", exhausted)
    response = gaodcore_views.get_response_xlsx([{"u": "https://example.com/a", "t": "plain"}])
    assert _xlsx_rows(response)[1] == [("https://example.com/a", "s"), ("plain", "s")]
    response.close()


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
@pytest.mark.parametrize(
    "accept",
    ["application/json", "text/csv", "application/xlsx"],
)
def test_download_field_columns_empty_result(
    endpoint: str, client: Client, full_example, accept: str
):
    """
    A ``columns`` rename request whose filters match zero rows must not crash
    (regression for the IndexError previously raised by modify_header on an
    empty result set), for every export format.
    """
    download_response = client.get(
        endpoint,
        {
            "resource_id": full_example.resources.table.id,
            "fields": ["id", "name"],
            "columns": ["identifier", "full_name"],
            "filters": '{"id": -999}',
        },
        HTTP_ACCEPT=accept,
    )

    assert download_response.status_code == 200
    if accept == "application/json":
        assert download_response.json() == []
    elif accept == "text/csv":
        assert download_response.content == b""
    elif accept == "application/xlsx":
        assert b"".join(download_response.streaming_content) != b""


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
        _xlsx_body(download_response) if download_response.streaming else download_response.content,
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
        _xlsx_body(download_response) if download_response.streaming else download_response.content,
    )


@pytest.mark.django_db
def test_download_error_is_never_serialized_as_data(accept_error, client: Client):
    """Errors must not go through the data renderers, whatever the client asked for."""
    response = client.get(
        "/GA_OD_Core/download", {"resource_id": -1}, HTTP_ACCEPT=accept_error
    )
    assert response.status_code == 400

    if accept_error == "text/html":
        # The Browsable API is not offered on public data views: no HTML, default renderer problem document.
        assert response["Content-Type"] == "application/problem+json"
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
def test_download_delimiter_multi_char_error(endpoint: str, client: Client):
    """A multi-character delimiter on a CSV download is a 400, not the 500 crash
    that ``csv.writer`` used to raise (regression)."""
    download_response = client.get(
        endpoint,
        {"resource_id": 1, "delimiter": "||"},
        HTTP_ACCEPT="text/csv",
    )

    assert download_response.status_code == 400
    validate_error(
        download_response,
        "Invalid delimiter: it must be a single character (and not a line break).",
        "text/csv",
        field_error_code="INVALID_DELIMITER",
    )


@pytest.mark.django_db
def test_download_delimiter_newline_error(endpoint: str, client: Client):
    """``\\n`` is a single character but corrupts the row structure, so it is rejected."""
    download_response = client.get(
        endpoint,
        {"resource_id": 1, "delimiter": "\n"},
        HTTP_ACCEPT="text/csv",
    )

    assert download_response.status_code == 400
    validate_error(
        download_response,
        "Invalid delimiter: it must be a single character (and not a line break).",
        "text/csv",
        field_error_code="INVALID_DELIMITER",
    )


@pytest.mark.django_db
def test_download_delimiter_validated_before_data_access(endpoint: str, client: Client):
    """The delimiter is validated while parsing the query string, before the resource
    is even looked up: an invalid delimiter on a non-existent resource still reports
    INVALID_DELIMITER, not RESOURCE_UNAVAILABLE."""
    download_response = client.get(
        endpoint,
        {"resource_id": 96, "delimiter": "||"},
        HTTP_ACCEPT="text/csv",
    )

    assert download_response.status_code == 400
    validate_error(
        download_response,
        "Invalid delimiter: it must be a single character (and not a line break).",
        "text/csv",
        field_error_code="INVALID_DELIMITER",
    )


@pytest.mark.django_db
@pytest.mark.parametrize("bad_format", ["application/json", "application/xlsx"])
def test_download_delimiter_multi_char_ignored_for_non_csv(
    endpoint: str, client: Client, full_example, bad_format: str
):
    """A multi-character delimiter is only meaningful for csv/scsv: it must not break
    json or xlsx downloads, which today silently ignore the parameter."""
    download_response = client.get(
        endpoint,
        {"resource_id": full_example.resources.table.id, "delimiter": "||"},
        HTTP_ACCEPT=bad_format,
    )

    assert download_response.status_code == 200


@pytest.mark.django_db
@pytest.mark.parametrize("delimiter", [";", "|"])
def test_download_delimiter_single_char(
    endpoint: str, client: Client, full_example, delimiter: str
):
    download_response = client.get(
        endpoint,
        {"resource_id": full_example.resources.table.id, "delimiter": delimiter},
        HTTP_ACCEPT="text/csv",
    )

    assert download_response.status_code == 200
    content = download_response.content.decode()
    header_line = content.splitlines()[0]
    assert delimiter in header_line


@pytest.mark.django_db
@pytest.mark.parametrize("accept,expected_default", [("text/csv", ","), ("text/scsv", ";")])
def test_download_delimiter_default(
    endpoint: str, client: Client, full_example, accept: str, expected_default: str
):
    """Without an explicit delimiter, csv keeps using ',' and scsv keeps using ';'."""
    download_response = client.get(
        endpoint,
        {"resource_id": full_example.resources.table.id},
        HTTP_ACCEPT=accept,
    )

    assert download_response.status_code == 200
    header_line = download_response.content.decode().splitlines()[0]
    assert expected_default in header_line


@pytest.mark.django_db
def test_download_delimiter_empty_falls_back_to_default(
    endpoint: str, client: Client, full_example
):
    """``?delimiter=`` (empty string) preserves today's behavior: fall back to the
    default rather than erroring."""
    download_response = client.get(
        endpoint,
        {"resource_id": full_example.resources.table.id, "delimiter": ""},
        HTTP_ACCEPT="text/csv",
    )

    assert download_response.status_code == 200
    header_line = download_response.content.decode().splitlines()[0]
    assert "," in header_line


@pytest.mark.django_db
def test_download_body_status_matches_http_status(accept_error, client: Client):
    """The "status" member never drifts from the real HTTP status."""
    response = client.get(
        "/GA_OD_Core/download", {"resource_id": 1, "filters": "a"}, HTTP_ACCEPT=accept_error
    )
    if accept_error == "text/html":
        return
    assert problem_of(response)["status"] == response.status_code == 400


@pytest.fixture
def timing_caplog():
    """Capture the timing logger, which does not propagate to root (so the stock caplog cannot see it)."""
    import logging

    class _ListHandler(logging.Handler):
        def __init__(self):
            super().__init__(logging.INFO)
            self.records = []

        def emit(self, record):
            self.records.append(record)

    handler = _ListHandler()
    timing_logger = gaodcore_views.timing_logger
    timing_logger.addHandler(handler)
    yield handler
    timing_logger.removeHandler(handler)


def _timing_lines(caplog):
    return [r.getMessage() for r in caplog.records if r.getMessage().startswith("download_timing ")]


def _timing_fields(line):
    return dict(part.split("=", 1) for part in line.split()[1:])


@pytest.mark.django_db
@pytest.mark.parametrize("fmt", ["json", "csv", "xlsx"])
def test_download_timing_log(endpoint: str, fmt: str, client: Client, full_example, timing_caplog):
    caplog = timing_caplog
    response = client.get(
        endpoint,
        {"resource_id": full_example.resources.table.id, "formato": fmt, "sort": ["id"]},
    )
    assert response.status_code == 200

    lines = _timing_lines(caplog)
    assert len(lines) == 1
    fields = _timing_fields(lines[0])
    assert fields["resource_id"] == str(full_example.resources.table.id)
    assert fields["endpoint"] == endpoint.rsplit("/", 1)[1]
    assert fields["format"] == fmt
    assert fields["status"] == "200"
    assert fields["rows"] == "2"
    assert int(fields["connects"]) == 1
    for key in ("t_connect", "t_query", "t_normalize", "t_render", "t_total"):
        assert float(fields[key]) >= 0
    # XLSX is a streaming FileResponse: its size comes from Content-Length, not from ``.content``.
    body = b"".join(response.streaming_content) if response.streaming else response.content
    assert fields["bytes"] == str(len(body))
    assert int(fields["maxrss_kb"]) > 0


@pytest.mark.django_db
def test_download_timing_log_on_error(client: Client, full_example, timing_caplog):
    caplog = timing_caplog
    response = client.get("/GA_OD_Core/download", {"resource_id": full_example.resources.table.id, "formato": "dj"})
    assert response.status_code == 400

    lines = _timing_lines(caplog)
    assert len(lines) == 1
    fields = _timing_fields(lines[0])
    assert fields["status"] == "400"
    assert fields["rows"] == "-1"
    assert fields["bytes"] == str(len(response.content))


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_timing_log_disabled(endpoint: str, client: Client, full_example, timing_caplog, settings):
    params = {"resource_id": full_example.resources.table.id, "formato": "json", "sort": ["id"]}
    enabled = client.get(endpoint, params)
    assert enabled.status_code == 200
    assert len(_timing_lines(timing_caplog)) == 1
    timing_caplog.records.clear()

    settings.DOWNLOAD_TIMING_LOG = False
    disabled = client.get(endpoint, params)
    assert disabled.status_code == 200
    assert disabled.content == enabled.content
    assert _timing_lines(timing_caplog) == []
