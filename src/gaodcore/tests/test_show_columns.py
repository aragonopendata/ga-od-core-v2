import os

from django.test import Client
import pytest

from conftest import validate_error, compare_files, ConnectorData


@pytest.mark.django_db
def test_show_columns(accept_download: str, client: Client, full_example):
    download_response = client.get(
        f'/GA_OD_Core/show_columns',
        {'resource_id': full_example.resources.table.id}, HTTP_ACCEPT=accept_download)

    compare_files(os.path.dirname(__file__), 'show_columns', accept_download, download_response.content)


@pytest.mark.django_db
def test_show_columns_resource_not_exists(accept_error, client: Client):
    download_response = client.get(f'/GA_OD_Core/show_columns', {'resource_id': -1}, HTTP_ACCEPT=accept_error)
    assert download_response.status_code == 400
    validate_error(download_response.content, "Resource not exists or is not available", accept_error)
