import pytest

from django.test.client import Client

from gaodcore_manager.models import ResourceConfig


@pytest.mark.django_db
def test_resources(client: Client, full_example):
    test_resources_id = ResourceConfig.objects.get(name="test_resources").id
    download_response = client.get("/GA_OD_Core/views.json")
    if full_example.scheme in ["http", "https"]:
        assert download_response.json() == [
            {"available": True, "id": test_resources_id, "name": "test_resources"}
        ]
    else:
        test_resources_view_id = ResourceConfig.objects.get(
            name="test_resources_view"
        ).id
        assert download_response.json() == [
            {
                "available": True,
                "id": test_resources_view_id,
                "name": "test_resources_view",
            },
            {"available": True, "id": test_resources_id, "name": "test_resources"},
        ]
