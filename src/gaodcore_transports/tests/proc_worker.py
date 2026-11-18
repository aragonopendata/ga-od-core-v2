"""Child process for the multi-process tests: one simulated Gunicorn worker with its own Django and Redis client.

Runs with ``gaodcore_project.test_settings`` (``CONFIG_PATH`` from the environment), replaces ``CACHES`` with
``build_caches(REDIS_URL)`` and points the sources at a local stand-in of the Consorcio API. It prints ``READY``, waits
for a line on stdin (the common start instant of all the workers) and prints one JSON line with what happened.

Usage: python proc_worker.py REDIS_URL BASE_URL INTERNAL_JSON MODE [ARG]
  stops          open resource 2187
  arrivals CODE  open resource 2190 for the stop CODE
  aggregate      open resource 2190 without a stop
"""

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "gaodcore_project.test_settings")

import django  # noqa: E402

django.setup()

from django.conf import settings  # noqa: E402
from django.test.utils import override_settings  # noqa: E402
from rest_framework.response import Response  # noqa: E402
from rest_framework.test import APIRequestFactory  # noqa: E402
from rest_framework.views import APIView  # noqa: E402

from gaodcore_project.cache_backends import build_caches  # noqa: E402
from gaodcore_project.config import ZaragozaInternalSources, ZaragozaTransport  # noqa: E402
from gaodcore_transports import sources  # noqa: E402

STOPS_URI = "internal://transports/zaragoza/stops"
ARRIVALS_URI = "internal://transports/zaragoza/arrival_time"


def main():
    redis_url, base_url, internal_json, mode = sys.argv[1:5]
    argument = sys.argv[5] if len(sys.argv) > 5 else None
    internal = ZaragozaInternalSources(**json.loads(internal_json))
    real = settings.CONFIG.projects.transport.zaragoza
    zaragoza = ZaragozaTransport(base_url=base_url, max_concurrency=5, endpoints=real.endpoints)
    sources.internal_config = lambda: internal
    sources.zaragoza_config = lambda: zaragoza
    override_settings(CACHES=build_caches(redis_url)).enable()

    if mode == "stops":
        uri, filters = STOPS_URI, {}
    elif mode == "arrivals":
        uri, filters = ARRIVALS_URI, {"stop_id": argument}
    else:
        uri, filters = ARRIVALS_URI, {}

    class SourceView(APIView):
        """What the public routes do: open the source and let the DRF exception handler answer."""

        authentication_classes = []
        permission_classes = []

        def get(self, request):
            opened = sources.open_for_request(uri, filters, {}, [], [])
            try:
                with opened.resource.engine.connect() as conn:
                    rows = len(list(conn.execute(opened.resource.model.select())))
            finally:
                opened.resource.engine.dispose()
            return Response({"state": opened.description, "rows": rows})

    view = SourceView.as_view()
    factory = APIRequestFactory()
    # Warm up (Redis connection, imports) with a request that cannot touch the keys under test.
    sources.transports_cache().get("warmup")
    print("READY", flush=True)
    sys.stdin.readline()
    response = view(factory.get("/source"))
    data = response.data if isinstance(response.data, dict) else {}
    print(json.dumps({
        "status": response.status_code, "error_code": data.get("error_code"), "state": data.get("state"),
        "rows": data.get("rows"), "retry_after": response.get("Retry-After"),
    }), flush=True)


if __name__ == "__main__":
    main()
