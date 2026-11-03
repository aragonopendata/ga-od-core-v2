"""Gunicorn configuration loaded with ``gunicorn -c gunicorn.conf.py`` (see the Dockerfile).

Command line options (bind, workers, timeout, max-requests) stay in the Dockerfile and take precedence over this
file; it only adds the ``post_request`` hook below.

Why: a download of a large resource keeps the whole result in memory, and CPython does not give that memory back to
the operating system after the response. Each worker that served one keeps 2-3 GB of RSS until it is recycled, so a
few of them in a row fill the container's memory limit and the kernel OOM-kills workers, including ones serving
unrelated requests. Measured on 2 November 2026: 14 sequential downloads of resource 3146 (300k-600k rows each)
took the container from 1.4 to 10.7 GB and caused 6 OOM kills.

The hook recycles a worker once its resident memory passes a threshold. It runs after the response has been sent,
so no request is cut short: the worker exits gracefully and the arbiter starts a fresh one, as with
``--max-requests``.
"""

import os
from typing import Optional

# Resident memory, in MiB, above which a worker is recycled after finishing its current request. An idle worker uses
# around 150-250 MiB. 0 disables the check.
_MAX_WORKER_RSS_MB_DEFAULT = 1024
_MAX_WORKER_RSS_MB_ENV = "GUNICORN_MAX_WORKER_RSS_MB"


def _max_worker_rss_bytes() -> int:
    """Threshold in bytes, from ``GUNICORN_MAX_WORKER_RSS_MB`` or the default. 0 (or less) disables the check."""
    raw = os.environ.get(_MAX_WORKER_RSS_MB_ENV, "").strip()
    try:
        megabytes = int(raw) if raw else _MAX_WORKER_RSS_MB_DEFAULT
    except ValueError:
        megabytes = _MAX_WORKER_RSS_MB_DEFAULT
    return max(megabytes, 0) * 1024 * 1024


def _current_rss_bytes() -> Optional[int]:
    """Current resident set size of this process, read from ``/proc/self/statm``. None where it is unavailable.

    ``resource.getrusage().ru_maxrss`` is not used on purpose: it is the peak, which never goes down.
    """
    try:
        with open("/proc/self/statm", encoding="ascii") as statm:
            resident_pages = int(statm.read().split()[1])
    except (OSError, ValueError, IndexError):
        return None
    return resident_pages * os.sysconf("SC_PAGE_SIZE")


def post_request(worker, req, environ, resp):  # pylint: disable=unused-argument
    """Recycle the worker after this request if its resident memory is above the threshold."""
    limit = _max_worker_rss_bytes()
    if not limit:
        return
    rss = _current_rss_bytes()
    if rss is None or rss <= limit:
        return
    worker.log.warning(
        "Recycling worker (pid:%s): RSS %d MiB above the %d MiB limit after %s %s",
        worker.pid,
        rss // (1024 * 1024),
        limit // (1024 * 1024),
        getattr(req, "method", "-"),
        getattr(req, "path", "-"),
    )
    worker.alive = False
