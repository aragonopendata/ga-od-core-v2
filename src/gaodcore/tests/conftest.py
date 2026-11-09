from itertools import count

import pytest
from django.http import HttpResponse
from django.test import Client

from download_stream import StreamedDownloadResponse

import gaodcore.operators as _operators_module


@pytest.fixture(autouse=True)
def reset_bind_counter():
    _operators_module._bind_counter = count(1)


class _BufferingClient(Client):
    """Test client that reads a streamed download to the end, so tests can use ``response.content`` on it.

    The body of a ``StreamedDownloadResponse`` is generated while it is read; here it is read at once (which also
    closes the response) and returned as a plain ``HttpResponse`` with the same status, headers and test attributes.
    Tests that need to control the reading (cut it half way, read it block by block) use ``streaming_client``.
    """

    _COPIED = ("client", "request", "wsgi_request", "templates", "context", "resolver_match", "exc_info", "json")

    def request(self, **request):
        response = super().request(**request)
        if not isinstance(response, StreamedDownloadResponse):
            return response
        buffered = HttpResponse(b"".join(response.streaming_content), status=response.status_code)
        buffered.headers = response.headers
        buffered.cookies = response.cookies
        for name in self._COPIED:
            if name in response.__dict__:
                setattr(buffered, name, response.__dict__[name])
        return buffered


@pytest.fixture
def client():
    return _BufferingClient()


@pytest.fixture
def streaming_client():
    """The plain Django test client: a streamed download comes back unread."""
    return Client()
