"""XML output (``xml_output``): same bytes as ``rest_framework_xml``'s renderer wherever that one gave valid XML with
the exact data, and valid XML with the exact data everywhere else (or an error when XML cannot carry a character).

The reference is always the old renderer itself, not the new one, so a mistake shared by every new code path cannot
hide.
"""

import decimal
import xml.etree.ElementTree as ET
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from django.utils.translation import gettext_lazy
from rest_framework.exceptions import ErrorDetail
from rest_framework.settings import api_settings
from rest_framework.utils.serializer_helpers import ReturnDict, ReturnList
from rest_framework_xml.renderers import XMLRenderer
from sqlalchemy import create_engine, text

import xml_output
from gaodcore import views as gaodcore_views
from xml_output import GaodXMLRenderer, XmlUnrepresentableError, encode_items, is_xml_name, render_document

OLD = XMLRenderer()


def _row(i):
    return {
        "id": i, "name": f"Nombre <{i}> & \"co\" 'x'", "d": "2024-01-01", "v": i * 1.5, "n": None, "e": "",
        "big": 10**20, "neg": -3, "flag": i % 2 == 0, "uni": "Ñandú ✓ 日本 😀", "tab": "a\tb\nc", "c1": "\x7f\x85",
    }  # fmt: skip


SAME_AS_OLD = [
    None,
    [],
    {},
    "text",
    5,
    1.5,
    True,
    [None],
    [{}],
    [[]],
    {"detail": "x<y & z>"},
    {"detail": ErrorDetail("No encontrado", code="not_found")},
    {"errors": [ErrorDetail("a", code="x"), ErrorDetail("b", code="y")]},
    ReturnDict({"a": 1, "b": [1, 2]}, serializer=None),
    ReturnList([{"a": 1}, {"a": 2}], serializer=None),
    {"a": {"b": [1, {"c": None}, (2, 3)]}},
    [[1, 2], (3,), "s"],
    [{"a": 1}, {"b": 2, "a": 3}, {"a": 4, "b": 5}, {"b": 6, "a": 7}],  # keys change and reorder between rows
    [{"a": ""}, {"a": {}}, {"a": []}, {"a": None}],  # all empty, as today
    [{"a": decimal.Decimal("1.50"), "b": b"caf\xc3\xa9", "c": gettext_lazy("Yes")}],
    [{"xmlish": 1, "año": 2, "_x-y.z": 3, "Ñ·": 4, "field": 5}],
    [{"a": "&#13; &amp; already escaped"}],
    [{"a": {"inner": [{"x": 1}, {"x": None}]}}],
    [_row(i) for i in range(3)],
]


@pytest.mark.parametrize("data", SAME_AS_OLD, ids=range(len(SAME_AS_OLD)))
def test_same_bytes_as_the_old_renderer(data):
    assert render_document(data) == OLD.render(data)


@pytest.mark.parametrize("count", [0, 1, 999, 1000, 1001, 2500])
def test_same_bytes_as_the_old_renderer_for_many_rows_and_in_blocks(count):
    rows = [_row(i) for i in range(count)]
    expected = OLD.render(rows)
    assert render_document(rows) == expected
    blocks = [encode_items(rows[start:start + 1000]) for start in range(0, count, 1000)]
    assert xml_output.XML_DECLARATION + xml_output.ROOT_OPEN + "".join(blocks) + xml_output.ROOT_CLOSE == expected


def test_renderer_keeps_the_drf_contract():
    renderer = GaodXMLRenderer()
    assert (renderer.media_type, renderer.format, renderer.charset) == ("application/xml", "xml", "utf-8")
    assert renderer.render(None) == ""
    assert renderer.render([{"a": 1}], "application/xml", {}) == OLD.render([{"a": 1}])


def test_renderer_is_the_xml_renderer_of_every_view():
    renderers = list(api_settings.DEFAULT_RENDERER_CLASSES)
    assert GaodXMLRenderer in renderers and XMLRenderer not in renderers
    assert GaodXMLRenderer in gaodcore_views.PUBLIC_DATA_RENDERER_CLASSES


def _parse(document):
    return ET.fromstring(document.encode("utf-8"))


@pytest.mark.parametrize(
    "key",
    ["a b", "1x", "TOTAL$", "a:b", "", "-a", ".a", "a/b", "a<b>", 'a"b', "a&b", "a\tb", "a\nb", "a\rb", "Ĳ", "a😀"],
)
def test_a_key_that_is_not_an_xml_name_goes_into_a_field_element(key):
    document = render_document([{key: "v", "ok": 1}])
    item = _parse(document)[0]
    assert [(e.tag, e.attrib, e.text) for e in item] == [("field", {"name": key}, "v"), ("ok", {}, "1")]


def test_field_names_are_exact_and_never_collide():
    rows = [{"a b": 1, "a_b": 2, "field": 3, "x": {"c d": None, "e": [1]}}]
    item = _parse(render_document(rows))[0]
    assert [(e.tag, e.attrib, e.text) for e in item][:3] == [
        ("field", {"name": "a b"}, "1"),
        ("a_b", {}, "2"),
        ("field", {}, "3"),
    ]
    nested = item[3]
    assert [(e.tag, e.attrib) for e in nested] == [("field", {"name": "c d"}), ("e", {})]
    assert nested[1][0].tag == "list-item" and nested[1][0].text == "1"


def test_non_string_keys_are_written_as_text():
    item = _parse(render_document([{1: "a", None: "b"}]))[0]
    assert [(e.tag, e.attrib, e.text) for e in item] == [("field", {"name": "1"}, "a"), ("None", {}, "b")]


@pytest.mark.parametrize("key,valid", [("a", True), ("año", True), ("xmlfoo", True), ("Ĳ", False), ("a:b", False)])
def test_xml_names_are_the_ones_expat_also_reads(key, valid):
    assert is_xml_name(key) is valid


@pytest.mark.parametrize("value", ["a\rb", "a\r\nb", "\r", "&#13;", "a & b < c > d", "\t\n"])
def test_text_comes_back_exactly(value):
    assert _parse(render_document([{"a": value}]))[0][0].text == value


@pytest.mark.parametrize("char", ["\x00", "\x01", "\x08", "\x0b", "\x0c", "\x1f", "￾", "￿", "\ud800", "\udfff"])
def test_a_character_xml_cannot_carry_raises_naming_the_key_not_the_value(char):
    with pytest.raises(XmlUnrepresentableError) as error:
        render_document([{"ok": "x"}, {"ok": "y", "bad": f"secret{char}"}])
    assert error.value.key == "bad"
    assert f"U+{ord(char):04X}" in str(error.value) and "secret" not in str(error.value)
    with pytest.raises(XmlUnrepresentableError):
        encode_items([{"bad": f"secret{char}"}])


def test_a_character_xml_cannot_carry_in_a_key_raises():
    with pytest.raises(XmlUnrepresentableError) as error:
        render_document([{"a\x01": 1}])
    assert error.value.key == "a\x01"


def test_the_error_is_still_the_one_django_raised_before():
    from django.utils.xmlutils import UnserializableContentError

    assert issubclass(XmlUnrepresentableError, UnserializableContentError)


def test_render_is_faster_than_the_old_renderer():
    import time

    rows = [_row(i) for i in range(20000)]
    started = time.perf_counter()
    OLD.render(rows)
    old = time.perf_counter() - started
    started = time.perf_counter()
    render_document(rows)
    new = time.perf_counter() - started
    assert new < old / 2


# --------------------------------------------------------------------------------------------------------------------
# Through the views (download and preview), with a real SQLite table whose column names are not XML names.
# --------------------------------------------------------------------------------------------------------------------


@pytest.fixture
def odd_names_resource(tmp_path):
    uri = f"sqlite:///{tmp_path / 'data.db'}"
    engine = create_engine(uri)
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE things (id INTEGER PRIMARY KEY, "a b" TEXT, "TOTAL$" INTEGER, "1x" TEXT)'))
        conn.execute(text("INSERT INTO things VALUES (1, 'x' || char(13) || 'y', 5, NULL)"))
        conn.execute(text("INSERT INTO things VALUES (2, '<&>', NULL, 'z')"))
    engine.dispose()
    config = SimpleNamespace(
        id=1, name="things", object_location="things", object_location_schema=None,
        connector_config=SimpleNamespace(uri=uri),
    )  # fmt: skip
    with (
        patch.object(gaodcore_views, "_get_resource", return_value=config),
        patch.object(gaodcore_views, "update_resource_size"),
    ):
        yield


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_views_return_valid_xml_with_the_exact_data_and_names(client, odd_names_resource, endpoint):
    response = client.get(endpoint, {"resource_id": 1, "format": "xml", "sort": "id"})
    assert response.status_code == 200
    assert response["Content-Type"] == "application/xml; charset=utf-8"
    root = _parse(response.content.decode("utf-8"))
    rows = [[(e.tag, e.attrib.get("name"), e.text) for e in item] for item in root]
    assert rows == [
        [("id", None, "1"), ("field", "a b", "x\ry"), ("field", "TOTAL$", "5"), ("field", "1x", None)],
        [("id", None, "2"), ("field", "a b", "<&>"), ("field", "TOTAL$", None), ("field", "1x", "z")],
    ]


@pytest.mark.django_db
def test_columns_that_are_not_xml_names_go_into_field_elements(client, odd_names_resource):
    response = client.get(
        "/GA_OD_Core/download",
        {"resource_id": 1, "format": "xml", "sort": "id", "fields": "id,TOTAL$", "columns": "Id,Total €"},
    )
    assert response.status_code == 200
    root = _parse(response.content.decode("utf-8"))
    assert [(e.tag, e.attrib.get("name"), e.text) for e in root[0]] == [("Id", None, "1"), ("field", "Total €", "5")]
