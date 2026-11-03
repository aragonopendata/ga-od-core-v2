"""Gunicorn ``gthread`` worker that serves every connection it has already accepted before it exits.

Why: with the stock ``gunicorn.workers.gthread.ThreadWorker`` (gunicorn 21.2), when ``alive`` becomes False (memory
recycling in ``gunicorn.conf.py``, ``--max-requests`` or a graceful stop signal) the accept loop ends and the poller is
closed. The connections the worker had accepted but not yet handed to a thread are dropped, and their clients get a
closed connection without a response. The ``sync`` worker never loses an accepted request, so with a single upstream
(no other server for nginx to retry on) that would be a regression. The stock worker also stops calling ``notify()``
after the loop, so the arbiter kills it ``--timeout`` seconds later even when long requests are still running in
other threads.

What this worker does when ``alive`` is False:

1. Stops accepting: the listening sockets are removed from the poller. Connections still waiting in the kernel queue
   are taken by the other workers.
2. Keeps running the poller and calling ``notify()`` until every accepted connection has been served, or
   ``graceful_timeout`` seconds have passed (it is ``timeout`` after the last heartbeat that the arbiter kills a
   worker, so the heartbeat keeps long downloads in other threads alive).
   * Connections that already have data to read are handed to a thread.
   * Idle keep-alive connections without a pending request are closed.
   * Connections accepted but whose request has not arrived yet are waited for at most ``_FRESH_CONNECTION_GRACE``
     seconds.
3. Exits.

SIGQUIT and SIGINT (fast stop) keep the stock behavior (``handle_quit`` exits at once); SIGTERM (graceful stop) sets
``alive`` and goes through the drain above.

This class relies on internals of gunicorn 21.2 (``run``, ``poller``, ``_keep``, ``_lock``, ``futures``,
``nr_conns``, ``on_client_socket_readable``, ``murder_keepalived``, ``is_parent_alive``, ``tpool``, ``sockets``).
``test_gunicorn_worker.py`` fails if the gunicorn version or any of them changes, so that nobody upgrades gunicorn
without reviewing this file.
"""

import errno
import selectors
import time
from concurrent import futures

from gunicorn.workers.gthread import ThreadWorker

# Seconds an accepted connection whose request has not arrived yet is waited for once the worker is draining.
_FRESH_CONNECTION_GRACE = 5.0
_DRAIN_POLL_SECONDS = 0.5


class DrainingThreadWorker(ThreadWorker):
    """``ThreadWorker`` that finishes the connections it accepted before exiting, see the module docstring."""

    # Same loop as gunicorn 21.2 ThreadWorker.run (workers/gthread.py lines 194-240); only the ending changes.
    def run(self):
        for sock in self.sockets:
            sock.setblocking(False)
            server = sock.getsockname()
            self.poller.register(sock, selectors.EVENT_READ, lambda listener, server=server: self.accept(server, listener))

        parent_alive = True
        while self.alive:
            self.notify()

            if self.nr_conns < self.worker_connections:
                events = self.poller.select(1.0)
                for key, _ in events:
                    callback = key.data
                    callback(key.fileobj)

                result = futures.wait(self.futures, timeout=0, return_when=futures.FIRST_COMPLETED)
            else:
                result = futures.wait(self.futures, timeout=1.0, return_when=futures.FIRST_COMPLETED)

            for fut in result.done:
                self.futures.remove(fut)

            if not self.is_parent_alive():
                parent_alive = False
                break

            self.murder_keepalived()

        deadline = time.monotonic() + self.cfg.graceful_timeout
        if parent_alive:
            self._drain(deadline)

        self.tpool.shutdown(False)
        self.poller.close()
        for sock in self.sockets:
            sock.close()
        futures.wait(self.futures, timeout=max(deadline - time.monotonic(), 0))

    def _stop_accepting(self):
        with self._lock:
            for sock in self.sockets:
                try:
                    self.poller.unregister(sock)
                except (KeyError, ValueError, OSError):
                    pass

    def _close_idle_keepalived(self):
        """Close the keep-alive connections that are waiting for a next request that has not arrived."""
        while True:
            with self._lock:
                try:
                    conn = self._keep.popleft()
                except IndexError:
                    return
                try:
                    self.poller.unregister(conn.sock)
                except (KeyError, ValueError):
                    pass
                except OSError as err:
                    if err.errno != errno.EBADF:
                        raise
            self.nr_conns -= 1
            conn.close()

    def _drain(self, deadline):
        self._stop_accepting()
        fresh_deadline = time.monotonic() + _FRESH_CONNECTION_GRACE
        while time.monotonic() < deadline:
            self.notify()
            for key, _ in self.poller.select(0 if self._keep else _DRAIN_POLL_SECONDS):
                key.data(key.fileobj)
            self._close_idle_keepalived()

            for fut in [fut for fut in self.futures if fut.done()]:
                self.futures.remove(fut)

            waiting_connections = len(self.poller.get_map())
            if waiting_connections and time.monotonic() >= fresh_deadline:
                self._close_waiting_connections()
                waiting_connections = 0
            if not self.futures and not waiting_connections:
                return
            if self.futures and not waiting_connections:
                futures.wait(self.futures, timeout=_DRAIN_POLL_SECONDS, return_when=futures.FIRST_COMPLETED)

    def _close_waiting_connections(self):
        """Give up on connections accepted whose request never arrived (they are still registered in the poller)."""
        with self._lock:
            keys = list(self.poller.get_map().values())
            for key in keys:
                try:
                    self.poller.unregister(key.fileobj)
                except (KeyError, ValueError, OSError):
                    pass
        for key in keys:
            self.nr_conns -= 1
            try:
                key.fileobj.close()
            except OSError:
                pass
