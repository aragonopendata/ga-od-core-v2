from src.connectors import _get_engine, TypeDocumentError


def test_get_engine():
    try:
        _get_engine('https://support.oneskyapp.com/hc/en-us/article_attachments/202761627/example_1.json')
    except TypeDocumentError as err:
        assert 'Type of document is not csv or excel.' == err