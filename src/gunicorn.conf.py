"""Gunicorn configuration loaded with ``gunicorn -c gunicorn.conf.py`` (see the Dockerfile).

Everything is configurable through environment variables, so a deployment can change it without rebuilding the image
(variable: default):

* ``GUNICORN_WORKERS``: 9. Worker processes.
* ``GUNICORN_THREADS``: 4. Threads per worker (``gthread`` worker class, here ``DrainingThreadWorker``), so each
  worker serves several requests at once and one slow download no longer blocks a whole worker.
* ``GUNICORN_TIMEOUT``: 240. Seconds without a heartbeat before the arbiter kills a worker (see below).
* ``GUNICORN_GRACEFUL_TIMEOUT``: 1800. Seconds a worker that is being recycled or stopped waits for the requests in
  progress to finish (see below).
* ``GUNICORN_MAX_REQUESTS`` / ``GUNICORN_MAX_REQUESTS_JITTER``: 1000 / 100. Requests served before a worker is
  recycled (0 disables it).
* ``GUNICORN_WORKER_CONNECTIONS``: threads + 2. Connections a worker accepts at once (see below).
* ``GUNICORN_MAX_WORKER_RSS_MB``: 1024. Memory threshold for the ``post_request`` recycling hook (see below).

An invalid or empty value falls back to the default. Command line options (``--bind``, ``--workers``...) take
precedence over this file.

What ``--timeout`` means with ``gthread``
-----------------------------------------
Unlike the ``sync`` worker, the ``gthread`` main thread calls ``worker.notify()`` (the heartbeat) once per second
while it runs the accept loop, no matter how long the request threads take (gunicorn/workers/gthread.py, ``run``).
So ``--timeout`` does not kill a worker for serving a long request, whether it is slow or streams data for
minutes; it only detects a worker whose main thread is hung (or the whole process is stuck, for example in a C
extension holding the GIL for longer than the timeout). It is still the limit for a worker that is draining, see
below.

Why the worker is recycled
--------------------------
A download of a large resource keeps the whole result in memory, and CPython does not give that memory back to the
operating system after the response. Each worker that served one keeps 2-3 GB of RSS until it is recycled, so a few
of them in a row fill the container's memory limit and the kernel OOM-kills workers, including ones serving
unrelated requests. Measured on 2 November 2026: 14 sequential downloads of resource 3146 (300k-600k rows each)
took the container from 1.4 to 10.7 GB and caused 6 OOM kills.

``post_request`` recycles a worker once its resident memory passes a threshold. It sets ``worker.alive = False``,
the same thing ``--max-requests`` does: the worker stops accepting connections (the other workers take them) and
exits when it is done.

Recycling with threads
----------------------
* ``post_request`` runs in the thread that served the request, but the RSS it reads belongs to the whole process,
  so it includes the memory of the requests the other threads are serving right now. The default threshold is
  deliberately not scaled with the number of threads: the preview and the other bounded responses use little
  memory, and the memory that matters (a large download that is not given back) is what the threshold has to
  catch, whichever thread finished last. If a worker is recycled because another thread's big download is in
  progress, it costs one early restart, nothing is cut.
* The stock gunicorn 21.2 ``gthread`` worker misbehaves when ``alive`` becomes False (by this hook,
  ``--max-requests`` or SIGTERM). Measured with a small test application (``--timeout 20 --graceful-timeout 5``), a
  60 second request in a second thread was killed after 21 seconds by ``WORKER TIMEOUT`` because the worker stops
  sending heartbeats once its accept loop ends, not after ``graceful_timeout``. Worse, the connections the worker had
  accepted but not yet given to a thread are dropped, so their clients see a closed connection (with a single
  upstream behind nginx, a 502). A ``sync`` worker never loses an accepted request.
* ``gaodcore_project.gunicorn_worker.DrainingThreadWorker`` fixes both: once ``alive`` is False it stops accepting
  (the other workers take new connections), keeps the heartbeat and keeps serving every connection already accepted
  until they are all done or ``graceful_timeout`` seconds have passed, and only then exits. Idle keep-alive
  connections are closed. SIGQUIT/SIGINT still stop at once. A request longer than ``GUNICORN_GRACEFUL_TIMEOUT`` is
  cut at that point; the large default also delays the arbiter's graceful stop of a server in the middle of a long
  download (Docker's own stop timeout, 10 s by default, comes first).
* ``worker_connections`` defaults to ``threads + 2`` (the 2 are slots for keep-alive connections): a ``gthread``
  worker accepts up to ``worker_connections`` connections and queues the ones that do not have a thread yet, so a
  quick request could wait behind long downloads of its own worker while other workers are idle. Keep that queue
  short.
"""

import os
from typing import Optional

_WORKERS_DEFAULT = 9
_THREADS_DEFAULT = 4
_TIMEOUT_DEFAULT = 240
_GRACEFUL_TIMEOUT_DEFAULT = 1800
_MAX_REQUESTS_DEFAULT = 1000
_MAX_REQUESTS_JITTER_DEFAULT = 100
_KEEPALIVE_SLOTS = 2

# Resident memory, in MiB, above which a worker is recycled once it has finished a request. An idle worker uses
# around 150-250 MiB. 0 disables the check.
_MAX_WORKER_RSS_MB_DEFAULT = 1024
_MAX_WORKER_RSS_MB_ENV = "GUNICORN_MAX_WORKER_RSS_MB"


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    """Integer from the environment variable ``name``; ``default`` if it is unset, empty or not a number.

    Values below ``minimum`` are raised to it.
    """
    raw = os.environ.get(name, "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return max(value, minimum)


worker_class = "gaodcore_project.gunicorn_worker.DrainingThreadWorker"
workers = _env_int("GUNICORN_WORKERS", _WORKERS_DEFAULT, minimum=1)
threads = _env_int("GUNICORN_THREADS", _THREADS_DEFAULT, minimum=1)
timeout = _env_int("GUNICORN_TIMEOUT", _TIMEOUT_DEFAULT)
graceful_timeout = _env_int("GUNICORN_GRACEFUL_TIMEOUT", _GRACEFUL_TIMEOUT_DEFAULT)
max_requests = _env_int("GUNICORN_MAX_REQUESTS", _MAX_REQUESTS_DEFAULT)
max_requests_jitter = _env_int("GUNICORN_MAX_REQUESTS_JITTER", _MAX_REQUESTS_JITTER_DEFAULT)
worker_connections = _env_int("GUNICORN_WORKER_CONNECTIONS", threads + _KEEPALIVE_SLOTS, minimum=1)


def _max_worker_rss_bytes() -> int:
    """Threshold in bytes, from ``GUNICORN_MAX_WORKER_RSS_MB`` or the default. 0 (or less) disables the check."""
    return _env_int(_MAX_WORKER_RSS_MB_ENV, _MAX_WORKER_RSS_MB_DEFAULT) * 1024 * 1024


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
    """Recycle the worker if its resident memory is above the threshold.

    Only sets ``worker.alive``; ``DrainingThreadWorker`` then finishes the requests in the other threads and the
    connections already accepted before the process exits.
    """
    if not worker.alive:
        return
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
