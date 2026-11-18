"""A local stand-in for the Zaragoza transport API (``/stops`` and ``/arrival_time/{code}``), for tests and the lab.

Counts the calls per path and the maximum number of calls in progress at once. Behaviour per path:

* ``stops`` (list of stop objects) or ``arrivals[code]`` (the JSON object the API returns for that stop);
* ``delay`` seconds before answering (global, or per path in ``delays``);
* ``failures[path]``: ``"500"`` (HTTP 500), ``"invalid"`` (a body that is not JSON), ``"cut"`` (headers then the
  connection closes mid-body), ``"hang"`` (headers then nothing until ``hang_seconds``).

Unknown codes answer as the real API does: ``{"nombre": "Parada Desconocida", "arrival_time": []}``.
Run standalone (lab): ``python consorcio_mock.py --port 8099 --delay 2``.
"""

import argparse
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional

UNKNOWN = {"nombre": "Parada Desconocida", "arrival_time": []}


def arrival(line: str, remaining: str, **extra) -> Dict[str, Any]:
    """One arrival object with the fields of the real API."""
    return {
        "id": "0", "id_linea": line, "name": f"Línea {line}", "route": "1", "direction": "0", "bus": "0",
        "departure_time": "0", "remaining_time": remaining, "TR": "0", "inc": "", "url": "", **extra,
    }


def default_data():
    """Stops and arrivals modelled on what the API answered in November 2026.

    ``/stops`` numbers stops differently from ``arrival_time``: 40 is the same place in both, 1011 is a different
    place in each (collision), and 510 has arrivals but is absent from ``/stops``.
    """
    stops = [
        {"stop_id": "40", "stop_name": "Pº Mª Agustín, 59", "lat": 41.65, "lon": -0.89},
        {"stop_id": "55", "stop_name": "Pº Mª Agustín, 50", "lat": 41.65, "lon": -0.89},
        {"stop_id": "1011", "stop_name": "Aeropuerto - Carga Aérea", "lat": 41.66, "lon": -1.02},
    ]
    arrivals = {
        "40": {"nombre": "Pº Mª Agustín, 59", "arrival_time": [arrival("111", "00:07:12"), arrival("112", "00:21:40")]},
        "55": {"nombre": "Pº Mª Agustín, 50", "arrival_time": []},
        "510": {"nombre": "Fue - Pol. La Corona", "arrival_time": [arrival("210", "00:03:05")]},
        "1011": {"nombre": "Lugarico de Cerdán", "arrival_time": [arrival("601", "00:12:00")]},
    }
    return stops, arrivals


class ConsorcioMock:
    def __init__(self, port: int = 0, delay: float = 0.0, host: str = "127.0.0.1"):
        self.stops, self.arrivals = default_data()
        self.delay = delay
        self.delays: Dict[str, float] = {}
        self.failures: Dict[str, str] = {}
        self.hang_seconds = 30.0
        self.hits: Dict[str, int] = {}
        self.in_flight = 0
        self.max_in_flight = 0
        self.log = []
        self._lock = threading.Lock()
        self.on_request = None  # optional callback(path), called when a request arrives
        self.server = ThreadingHTTPServer((host, port), self._handler())
        self.server.daemon_threads = True
        self.thread: Optional[threading.Thread] = None

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}/api/1/"

    def start(self):
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def stop(self):
        self.server.shutdown()
        self.server.server_close()

    def reset_counters(self):
        with self._lock:
            self.hits.clear()
            self.max_in_flight = 0
            self.log.clear()

    def total_hits(self, prefix: str = "") -> int:
        return sum(count for path, count in self.hits.items() if path.startswith(prefix))

    def _handler(self):
        mock = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # silence
                pass

            def do_GET(self):  # noqa: N802
                path = self.path.split("/api/1/", 1)[-1].strip("/")
                started = time.monotonic()
                with mock._lock:
                    mock.hits[path] = mock.hits.get(path, 0) + 1
                    mock.in_flight += 1
                    mock.max_in_flight = max(mock.max_in_flight, mock.in_flight)
                try:
                    if mock.on_request is not None:
                        mock.on_request(path)
                    time.sleep(mock.delays.get(path, mock.delay))
                    self._answer(path)
                finally:
                    with mock._lock:
                        mock.in_flight -= 1
                        mock.log.append((path, started, time.monotonic()))

            def _answer(self, path):
                failure = mock.failures.get(path)
                if failure == "500":
                    self._send(500, b'{"error": "boom"}')
                    return
                if path == "stops":
                    body = json.dumps({"stops": mock.stops}).encode()
                elif path.startswith("arrival_time/"):
                    code = path.split("/", 1)[1]
                    body = json.dumps(mock.arrivals.get(code, UNKNOWN)).encode()
                else:
                    self._send(404, b"{}")
                    return
                if failure == "invalid":
                    body = b"<html>not json</html>"
                if failure in ("cut", "hang"):
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body[: len(body) // 2])
                    self.wfile.flush()
                    if failure == "hang":
                        time.sleep(mock.hang_seconds)
                    self.close_connection = True
                    return
                self._send(200, body)

            def _send(self, status, body):
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8099)
    parser.add_argument("--delay", type=float, default=0.0)
    parser.add_argument("--stops", type=int, default=0, help="pad /stops to this many stops (the real one has 189)")
    args = parser.parse_args()
    mock = ConsorcioMock(args.port, args.delay, args.host)
    for index in range(len(mock.stops), args.stops):
        mock.stops.append({"stop_id": str(2000 + index), "stop_name": f"Parada {index}", "lat": 41.6, "lon": -0.9})
    # /__stats and /__reset on the same server, for the lab client.
    handler = mock.server.RequestHandlerClass
    original = handler.do_GET

    def do_get(self):
        if self.path.startswith("/__stats"):
            with mock._lock:
                body = json.dumps({"hits": mock.hits, "max_in_flight": mock.max_in_flight,
                                   "total": sum(mock.hits.values())}).encode()
            self._send(200, body)
            return
        if self.path.startswith("/__reset"):
            mock.reset_counters()
            self._send(200, b"{}")
            return
        if self.path.startswith("/__delay/"):
            mock.delay = float(self.path.rsplit("/", 1)[1])
            self._send(200, b"{}")
            return
        original(self)

    handler.do_GET = do_get
    print(f"consorcio mock on :{args.port} delay={args.delay}", flush=True)
    mock.server.serve_forever()


if __name__ == "__main__":
    main()
