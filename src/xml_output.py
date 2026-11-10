"""XML output of the API: the bytes of ``rest_framework_xml``'s ``XMLRenderer``, built faster and always valid XML.

The document is ``<?xml version="1.0" encoding="utf-8"?>\\n<root>…</root>``. A list or tuple becomes one
``<list-item>`` per element, a dict one element per key, ``None`` nothing and anything else its text (``force_str``),
escaped. For every input the old renderer turned into valid XML with the exact data, the output here is the same,
byte for byte (``test_xml_output.py`` compares both). The differences are where the old output was not valid XML or
did not give the data back:

* A key that is not an XML name, or that expat does not read as one (``a b``, ``1x``, ``TOTAL$``, ``a:b``, ``""``,
  ``Ĳ``…), is written as
  ``<field name="the key">…</field>`` instead of ``<the key>`` (which no parser can read). The original key is in the
  attribute, so nothing is lost and two such keys never collide; a valid key ``field`` keeps ``<field>`` with no
  attribute.
* ``\\r`` in a text is written as ``&#13;``: parsers turn a literal ``\\r`` into ``\\n``.
* A character XML 1.0 cannot carry (control characters other than tab, line feed and carriage return, U+FFFE,
  U+FFFF, lone surrogates) raises ``XmlUnrepresentableError``, as the old renderer did for control characters; it
  used to emit U+FFFE/U+FFFF into an unreadable document.

Rendering goes through plain string building instead of a SAX generator call per element, about 7 times faster.
"""

import re
from functools import lru_cache
from typing import Any, Callable, Iterable, List, Optional, Tuple
from xml.parsers import expat

from django.utils.encoding import force_str
from django.utils.xmlutils import UnserializableContentError
from rest_framework_xml.renderers import XMLRenderer

XML_DECLARATION = '<?xml version="1.0" encoding="utf-8"?>\n'
ROOT_TAG = "root"
ITEM_TAG = "list-item"
ROOT_OPEN = f"<{ROOT_TAG}>"
ROOT_CLOSE = f"</{ROOT_TAG}>"
_ITEM_OPEN = f"<{ITEM_TAG}>"
_ITEM_CLOSE = f"</{ITEM_TAG}>"

# Characters XML 1.0 allows (section 2.2, "Char"): anything else cannot be written, not even as a reference.
_NOT_XML_CHAR = re.compile("[^\t\n\r -퟿-�\U00010000-\U0010ffff]")

# XML 1.0 fifth edition "Name" (section 2.3) without ":", which would need a namespace declaration.
_NAME_START = (
    "A-Z_a-zÀ-ÖØ-öø-˿Ͱ-ͽͿ-῿‌‍⁰-↏"
    "Ⰰ-⿯、-퟿豈-﷏ﷰ-�\U00010000-\U000effff"
)
_XML_NAME = re.compile(f"[{_NAME_START}][{_NAME_START}\\-.0-9·̀-ͯ‿⁀]*")


class XmlUnrepresentableError(UnserializableContentError):
    """A text or key holds a character that XML 1.0 cannot represent. The message names the character, not the text."""

    def __init__(self, char: str, key: Optional[str] = None):
        self.char = char
        self.key = key
        where = "" if key is None else f" in {ascii(key)}"
        super().__init__(f"Character U+{ord(char):04X}{where} cannot be represented in XML 1.0")


def _check_chars(text: str, key: Optional[str] = None) -> None:
    match = _NOT_XML_CHAR.search(text)
    if match is not None:
        raise XmlUnrepresentableError(match.group(), key)


def escape_text(text: str) -> str:
    """Element content: ``&``, ``<`` and ``>`` as ``xml.sax.saxutils.escape`` does, then ``\\r`` as ``&#13;``."""
    _check_chars(text)
    if "&" in text:
        text = text.replace("&", "&amp;")
    if ">" in text:
        text = text.replace(">", "&gt;")
    if "<" in text:
        text = text.replace("<", "&lt;")
    if "\r" in text:
        text = text.replace("\r", "&#13;")
    return text


def _escape_attribute(text: str) -> str:
    # Tab, line feed and carriage return as references: attribute-value normalization would turn them into spaces.
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace('"', "&quot;")
        .replace("\t", "&#9;")
        .replace("\n", "&#10;")
        .replace("\r", "&#13;")
    )


def is_xml_name(key: str) -> bool:
    """Whether ``key`` can be an element name as it is: an XML 1.0 name without ``:`` that expat also reads.

    Expat (Python's parser) still checks names with the older, narrower XML 1.0 tables: it rejects many names the
    fifth edition allows (``Ĳ``, most supplementary characters). Both must accept the name.
    """
    if _XML_NAME.fullmatch(key) is None:
        return False
    if key.isascii():
        return True
    parser = expat.ParserCreate()
    try:
        parser.Parse(f"<{key}/>", True)
    except expat.ExpatError:
        return False
    return True


@lru_cache(maxsize=4096)
def element_tags(key: str) -> Tuple[str, str]:
    """Opening and closing tag for ``key``: ``<key>``/``</key>``, or ``<field name="key">``/``</field>``."""
    if is_xml_name(key):
        return f"<{key}>", f"</{key}>"
    _check_chars(key, key)
    return f'<field name="{_escape_attribute(key)}">', "</field>"


def _tags(key: Any) -> Tuple[str, str]:
    return element_tags(force_str(key))


def _write(out: List[str], data: Any) -> None:
    """Append the XML of ``data`` to ``out``, with the structure of ``XMLRenderer._to_xml``."""
    if isinstance(data, (list, tuple)):
        _write_items(out, data)
    elif isinstance(data, dict):
        for key, value in data.items():
            opening, closing = _tags(key)
            out.append(opening)
            _write(out, value)
            out.append(closing)
    elif data is None:
        pass
    else:
        out.append(escape_text(force_str(data)))


def _write_items(out: List[str], items: Iterable[Any]) -> None:
    """One ``<list-item>`` per element. Rows (dicts) with the same keys in the same order reuse their tags."""
    keys: Optional[Tuple[Any, ...]] = None
    tags: List[Tuple[str, str]] = []
    append = out.append
    for item in items:
        if type(item) is not dict:  # noqa: E721 - subclasses (OrderedDict, ReturnDict) take the generic path
            append(_ITEM_OPEN)
            _write(out, item)
            append(_ITEM_CLOSE)
            continue
        row_keys = tuple(item)
        if row_keys != keys:
            keys = row_keys
            tags = [_tags(key) for key in row_keys]
        append(_ITEM_OPEN)
        for (opening, closing), value in zip(tags, item.values()):
            append(opening)
            value_type = type(value)
            if value_type is str:
                append(escape_text(value))
            elif value_type is int or value_type is float or value_type is bool:
                append(str(value))  # what force_str gives; digits, signs, letters and dots need no escaping
            elif value is not None:
                _write(out, value)
            append(closing)
        append(_ITEM_CLOSE)


def encode_items(items: Iterable[Any]) -> str:
    """The ``<list-item>`` elements of ``items``, without declaration or root (one block of a streamed download)."""
    out: List[str] = []
    try:
        _write_items(out, items)
    except XmlUnrepresentableError as error:
        raise _with_key(error, items) from None
    return "".join(out)


def render_document(data: Any) -> str:
    """The whole document for ``data``; ``""`` for ``None``, as ``XMLRenderer.render``."""
    if data is None:
        return ""
    out: List[str] = [XML_DECLARATION, ROOT_OPEN]
    try:
        _write(out, data)
    except XmlUnrepresentableError as error:
        raise _with_key(error, data) from None
    out.append(ROOT_CLOSE)
    return "".join(out)


def _with_key(error: XmlUnrepresentableError, data: Any) -> XmlUnrepresentableError:
    """Name the key whose value holds the character (only on the error path: the hot loop does not track it)."""
    if error.key is not None:
        return error
    key = _find_key(data, lambda text: error.char in text)
    return XmlUnrepresentableError(error.char, key) if key is not None else error


def _find_key(data: Any, has_char: Callable[[str], bool], key: Optional[str] = None) -> Optional[str]:
    if isinstance(data, (list, tuple)):
        for item in data:
            found = _find_key(item, has_char, key)
            if found is not None:
                return found
    elif isinstance(data, dict):
        for item_key, value in data.items():
            found = _find_key(value, has_char, force_str(item_key))
            if found is not None:
                return found
    elif data is not None and key is not None and has_char(force_str(data)):
        return key
    return None


class GaodXMLRenderer(XMLRenderer):
    """``XMLRenderer`` with the output of ``render_document``: same media type, format, charset and tags.

    It is the XML renderer of every DRF view (``DEFAULT_RENDERER_CLASSES``), so it keeps the general contract: any
    mix of dicts, lists and scalars, and ``""`` for ``None``.
    """

    def render(self, data, accepted_media_type=None, renderer_context=None):  # noqa: ARG002 - DRF signature
        return render_document(data)
