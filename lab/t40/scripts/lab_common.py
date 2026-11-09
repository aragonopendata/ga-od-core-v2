"""Utilidades compartidas por los scripts del laboratorio T40 (solo biblioteca estandar + docker + curl)."""
import csv
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
LAB_DIR = os.path.dirname(HERE)
COMPOSE = ["docker", "compose", "-p", "gaod-t40-lab", "-f", os.path.join(LAB_DIR, "docker-compose.yml")]
RESULTS_DIR = os.path.join(LAB_DIR, "results")
HOPS = {"front": 18080, "back": 18081, "app": 18082}
HOP_ORDER = ["app", "back", "front"]
ORACLE_SERVICE = os.environ.get("LAB_ORACLE_SERVICE", "FREEPDB1")


def dc(*args, check=True, capture=True, timeout=600, stdin=None):
    res = subprocess.run(COMPOSE + list(args), capture_output=capture, text=True, check=check, timeout=timeout,
                         input=stdin)
    return res.stdout if capture else None


def lab_sh(*args):
    subprocess.run([os.path.join(HERE, "lab.sh")] + list(args), check=True)


def log(msg):
    print(msg, flush=True)


# ---------------------------------------------------------------------------------------------------------------------
# Recursos
# ---------------------------------------------------------------------------------------------------------------------
_RES_CACHE = {}


def resource_ids():
    if not _RES_CACHE:
        for attempt in range(10):   # la app puede estar arrancando tras un app-env
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{HOPS['app']}/GA_OD_Core/views?format=json",
                                            timeout=30) as r:
                    for item in json.load(r):
                        _RES_CACHE[item["name"]] = item["id"]
                break
            except OSError:
                time.sleep(3)
    return _RES_CACHE


def resource_id(name):
    ids = resource_ids()
    if name not in ids:
        sys.exit(f"recurso {name!r} no sembrado (scripts/lab.sh seed). Hay: {sorted(ids)}")
    return ids[name]


def resource_family(name, n):
    """n recursos distintos de la misma tabla (name, name_b, name_c...) para no chocar con los limites T53; si no hay
    copias (recursos pequenos, no limitados) se repite el mismo."""
    ids = resource_ids()
    out = [name]
    for suffix in "bcd"[: max(0, n - 1)]:
        out.append(f"{name}_{suffix}" if f"{name}_{suffix}" in ids else name)
    return out


def download_path(name, fmt="csv", nonce=None, extra=""):
    """URL de descarga. ``_lab`` (nonce) hace unica la URL (bloqueo L2 de T53) y permite localizarla en los logs."""
    nonce = nonce or uuid.uuid4().hex[:10]
    return f"/GA_OD_Core/download?resource_id={resource_id(name)}&format={fmt}&_lab={nonce}{extra}", nonce


# ---------------------------------------------------------------------------------------------------------------------
# Configuracion de la cadena
# ---------------------------------------------------------------------------------------------------------------------
def app_env(name, default=None):
    out = dc("exec", "-T", "app", "printenv", name, check=False).strip()
    return out or default


def set_app_env(**kv):
    """Recrea app (y reinicia los Apache) con variables LAB_*. Ej.: set_app_env(LAB_GUNICORN_TIMEOUT=20)."""
    env = dict(os.environ)
    env.update({k: str(v) for k, v in kv.items()})
    subprocess.run([os.path.join(HERE, "lab.sh"), "_recreate_app"], check=True, env=env)


def set_apache(mode):
    lab_sh("apache", mode)


# ---------------------------------------------------------------------------------------------------------------------
# Observador (corre dentro del contenedor app)
# ---------------------------------------------------------------------------------------------------------------------
class Observer:
    """Lanza scripts/observer.py en el contenedor y guarda las muestras. Uso: ``with Observer() as obs: ...``."""

    def __init__(self, interval=0.25, oracle=True):
        cmd = COMPOSE + ["exec", "-T", "app", "python", "/lab/scripts/observer.py", "--interval", str(interval)]
        if not oracle:
            cmd.append("--no-oracle")
        self.cmd = cmd
        self.samples = []
        self.proc = None
        self._thread = None

    def __enter__(self):
        self.proc = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self._thread = threading.Thread(target=self._read, daemon=True)
        self._thread.start()
        deadline = time.time() + 30
        while not self.samples and time.time() < deadline:
            time.sleep(0.1)
        if not self.samples:
            raise RuntimeError("el observador no produce muestras")
        return self

    def __exit__(self, *exc):
        self.proc.terminate()
        try:
            self.proc.wait(5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        # El proceso dentro del contenedor muere al cerrarse su stdout (BrokenPipe); por si acaso:
        subprocess.run(COMPOSE + ["exec", "-T", "app", "pkill", "-f", "observer.py"], capture_output=True)

    def _read(self):
        for line in self.proc.stdout:
            try:
                self.samples.append(json.loads(line))
            except ValueError:
                pass

    def window(self, t0, t1=None):
        t1 = t1 or time.time()
        return [s for s in self.samples if t0 <= s["t"] <= t1]

    def latest(self):
        return self.samples[-1]

    def wait_quiet(self, since, timeout=60, need_oracle=True, settle=2):
        """Espera a que no haya conexion ocupada, ni bloqueos T53, ni sesiones Oracle de LAB_DATA durante ``settle``
        muestras seguidas. Devuelve (t_quiet o None, ultima muestra)."""
        deadline = time.time() + timeout
        streak = 0
        first = None
        seen = 0
        while time.time() < deadline:
            time.sleep(0.15)
            new = [s for s in self.samples[seen:] if s["t"] >= since]
            seen = len(self.samples)
            for s in new:
                ora = s.get("ora", {})
                quiet = s["busy"] == 0 and s["locks"] in (0, "0") and (
                    not need_oracle or ora.get("sessions", 0) == 0)
                if quiet:
                    streak += 1
                    first = first if streak > 1 else s["t"]
                    if streak >= settle:
                        return first, s
                else:
                    streak, first = 0, None
        return None, self.latest()

    def first_free(self, t_after, timeout=300, field="busy", need_oracle=False, settle=2):
        """Primer instante >= t_after con busy == 0 (y opcionalmente 0 sesiones Oracle), durante ``settle`` muestras."""
        deadline = time.time() + timeout
        streak, first, seen = 0, None, 0
        while time.time() < deadline:
            time.sleep(0.15)
            new = [s for s in self.samples[seen:] if s["t"] >= t_after]
            seen = len(self.samples)
            for s in new:
                ok = s["busy"] == 0 and (not need_oracle or s.get("ora", {}).get("sessions", 0) == 0)
                if ok:
                    streak += 1
                    first = first if streak > 1 else s["t"]
                    if streak >= settle:
                        return first
                else:
                    streak, first = 0, None
        return None

    def first_time(self, pred, t_after, timeout=120):
        deadline = time.time() + timeout
        seen = 0
        while time.time() < deadline:
            time.sleep(0.1)
            new = self.samples[seen:]
            seen = len(self.samples)
            for s in new:
                if s["t"] >= t_after and pred(s):
                    return s
        return None


def max_locks(samples):
    vals = [s["locks"] for s in samples if isinstance(s["locks"], int)]
    return max(vals) if vals else None


def total_rss_kb(sample):
    return sum(sample["workers"].values())


# ---------------------------------------------------------------------------------------------------------------------
# Cliente HTTP en bruto
# ---------------------------------------------------------------------------------------------------------------------
def http_get(port, path, *, accept_gzip=False, rate=None, cut_after=None, stall_after=None, stall_seconds=0,
             stall_then_close=True, timeout=600, on_headers=None):
    """GET HTTP/1.1 con un socket. Devuelve un dict con:
    status, t_headers (s hasta la linea de estado = primer byte), t_body (s hasta el primer byte del cuerpo), total (s),
    body_bytes, complete (True si llego el bloque final chunked / todo el Content-Length), chunked, error, how_ended.

    rate        bytes/s maximos de lectura (cliente lento)
    cut_after   cierra la conexion tras recibir tantos bytes de cuerpo (corte del cliente)
    stall_after + stall_seconds   deja de leer al llegar a esos bytes durante stall_seconds sin cerrar el socket
    """
    t0 = time.time()
    res = {"t_start": t0, "status": None, "t_headers": None, "t_body": None, "body_bytes": 0, "complete": False,
           "chunked": False, "error": None, "how_ended": None, "headers": {}}
    sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    sock.settimeout(timeout)
    req = (f"GET {path} HTTP/1.1\r\nHost: lab\r\nConnection: close\r\n"
           f"{'Accept-Encoding: gzip' if accept_gzip else 'Accept-Encoding: identity'}\r\n\r\n")
    sock.sendall(req.encode())
    buf = b""
    header_done = False
    content_length = None
    chunk_state = "size"   # size | data | crlf | trailer | done
    chunk_left = 0
    pending = b""
    stalled = False
    try:
        while True:
            try:
                data = sock.recv(65536 if rate is None else max(1024, int(rate / 20)))
            except socket.timeout:
                res["error"] = "timeout de lectura"
                res["how_ended"] = "timeout"
                break
            now = time.time()
            if not data:
                res["how_ended"] = "eof"
                break
            if res["t_headers"] is None:
                res["t_headers"] = now - t0
            if not header_done:
                buf += data
                if b"\r\n\r\n" not in buf:
                    continue
                head, _, data = buf.partition(b"\r\n\r\n")
                lines = head.decode("latin-1").split("\r\n")
                res["status"] = int(lines[0].split()[1])
                for ln in lines[1:]:
                    k, _, v = ln.partition(":")
                    res["headers"][k.strip().lower()] = v.strip()
                res["chunked"] = "chunked" in res["headers"].get("transfer-encoding", "").lower()
                content_length = int(res["headers"]["content-length"]) if "content-length" in res["headers"] else None
                header_done = True
                if on_headers:
                    on_headers(res)
                if not data:
                    continue
            if res["t_body"] is None:
                res["t_body"] = now - t0
            # Analisis del cuerpo
            if res["chunked"]:
                pending += data
                while pending and chunk_state != "done":
                    if chunk_state == "size":
                        if b"\r\n" not in pending:
                            break
                        line, _, pending = pending.partition(b"\r\n")
                        chunk_left = int(line.split(b";")[0] or b"0", 16)
                        chunk_state = "data" if chunk_left else "trailer"
                    elif chunk_state == "data":
                        take = min(chunk_left, len(pending))
                        res["body_bytes"] += take
                        pending = pending[take:]
                        chunk_left -= take
                        if chunk_left == 0:
                            chunk_state = "crlf"
                        else:
                            break
                    elif chunk_state == "crlf":
                        if len(pending) < 2:
                            break
                        pending = pending[2:]
                        chunk_state = "size"
                    elif chunk_state == "trailer":
                        if b"\r\n" not in pending:
                            break
                        line, _, pending = pending.partition(b"\r\n")
                        if line == b"":
                            chunk_state = "done"
                if chunk_state == "done":
                    res["complete"] = True
                    res["how_ended"] = "final_chunk"
                    break
            else:
                res["body_bytes"] += len(data)
                if content_length is not None and res["body_bytes"] >= content_length:
                    res["complete"] = True
                    res["how_ended"] = "content_length"
                    break
            if cut_after is not None and res["body_bytes"] >= cut_after:
                res["how_ended"] = "client_cut"
                res["t_cut"] = time.time()
                break
            if stall_after is not None and not stalled and res["body_bytes"] >= stall_after:
                stalled = True
                res["t_stall_start"] = time.time()
                time.sleep(stall_seconds)
                res["t_stall_end"] = time.time()
                if stall_then_close:
                    res["how_ended"] = "closed_after_stall"
                    res["t_cut"] = time.time()
                    break
            if rate is not None:
                time.sleep(len(data) / rate)
    except (ConnectionError, OSError) as err:
        res["error"] = f"{type(err).__name__}: {err}"
        res["how_ended"] = "connection_error"
    finally:
        res.setdefault("t_cut", time.time())
        sock.close()
    res["total"] = time.time() - t0
    res["t_end"] = time.time()
    return res


def verdict(res):
    """Como ve el cliente la transferencia."""
    if res["how_ended"] in ("client_cut", "closed_after_stall"):
        return "cortada por el cliente"
    if res["status"] is None:
        return f"FALLIDA (sin respuesta: {res['error'] or res['how_ended']})"
    if res["status"] >= 400:
        return f"error HTTP {res['status']} (respuesta completa, no es un corte)"
    if res["complete"]:
        return "COMPLETA"
    return f"FALLIDA (truncada: {res['how_ended']}, sin bloque final)"


def curl_download(port, path, *, limit_rate=None, accept_gzip=False, max_time=None, extra=None):
    """Descarga con curl; devuelve exit code, http_code, bytes, tiempos (curl -w)."""
    fmt = "%{http_code} %{size_download} %{time_starttransfer} %{time_total}"
    cmd = ["curl", "-s", "-o", "/dev/null", "-w", fmt, f"http://127.0.0.1:{port}{path}"]
    if limit_rate:
        cmd += ["--limit-rate", str(limit_rate)]
    if accept_gzip:
        cmd += ["-H", "Accept-Encoding: gzip"]
    if max_time:
        cmd += ["--max-time", str(max_time)]
    cmd += extra or []
    t0 = time.time()
    p = subprocess.run(cmd, capture_output=True, text=True)
    parts = (p.stdout.split() + ["0", "0", "0", "0"])[:4]
    return {"exit": p.returncode, "http": int(parts[0]), "bytes": int(parts[1]), "ttfb": float(parts[2]),
            "total": float(parts[3]), "t_start": t0, "t_end": time.time(), "stderr": p.stderr.strip()}


CURL_EXIT = {0: "ok", 18: "transferencia parcial (18)", 28: "timeout de curl (28)", 52: "respuesta vacia (52)",
             56: "conexion reiniciada/cerrada (56)", 7: "sin conexion (7)", 22: "HTTP>=400 (22)"}


# ---------------------------------------------------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------------------------------------------------
_KV = re.compile(r"(\w+)=(\S+)")


def logs(service, since):
    out = subprocess.run(COMPOSE + ["logs", "--no-log-prefix", "--since", str(int(since)), service],
                         capture_output=True, text=True).stdout
    return out.splitlines()


def timing_lines(since, service="app"):
    """Lineas download_timing del log de app desde ``since`` (epoch) como dicts."""
    rows = []
    for line in logs(service, since):
        if "download_timing " not in line or "could not be logged" in line:
            continue
        kv = dict(_KV.findall(line.split("download_timing", 1)[1]))
        kv["_raw"] = line
        rows.append(kv)
    return rows


def wait_timing(since, resource_ident, timeout=120, count=1, nonce=None):
    """Espera hasta ``count`` lineas download_timing nuevas del recurso. Devuelve la lista (puede ser mas corta)."""
    deadline = time.time() + timeout
    rows = []
    while time.time() < deadline:
        rows = [r for r in timing_lines(since) if r.get("resource_id") == str(resource_ident)]
        if len(rows) >= count:
            return rows
        time.sleep(1)
    return rows


def count_worker_timeouts(since):
    return sum(1 for ln in logs("app", since) if "WORKER TIMEOUT" in ln)


def apache_lines(service, since, needle):
    return [ln for ln in logs(service, since) if needle in ln]


def conn_flag(line):
    m = re.search(r"conn=(\S)", line)
    return m.group(1) if m else "?"


def fmt_timing(row):
    keys = ["status", "rows", "bytes", "t_connect", "t_query", "t_normalize", "t_render", "t_total", "t_first",
            "result", "rss_kb", "maxrss_kb"]
    return " ".join(f"{k}={row[k]}" for k in keys if k in row)


# ---------------------------------------------------------------------------------------------------------------------
# Oracle (para observar y para provocar fallos)
# ---------------------------------------------------------------------------------------------------------------------
def sqlplus_admin(sql):
    out = subprocess.run(
        COMPOSE + ["exec", "-T", "oracle", "sqlplus", "-s", f"system/lab_admin_pw@localhost/{ORACLE_SERVICE}"],
        input="set heading off feedback off pagesize 0 linesize 300\n" + sql + "\nexit\n", capture_output=True,
        text=True).stdout
    return out.strip()


KILL_PLSQL = """set serveroutput on feedback off
declare n number := 0;
begin
  for r in (select sid, serial# from v$session where username = 'LAB_DATA') loop
    begin
      execute immediate 'alter system kill session ''' || r.sid || ',' || r.serial# || ''' immediate';
      n := n + 1;
    exception when others then null;
    end;
  end loop;
  dbms_output.put_line('KILLED=' || n);
end;
/
exit
"""


def kill_lab_sessions():
    """ALTER SYSTEM KILL SESSION de las sesiones LAB_DATA (mecanismo de fallo provocado a mitad de descarga).
    Un solo sqlplus con un bloque PL/SQL para que el kill llegue cuanto antes. Devuelve cuantas sesiones mato."""
    out = subprocess.run(
        COMPOSE + ["exec", "-T", "oracle", "sqlplus", "-s", f"system/lab_admin_pw@localhost/{ORACLE_SERVICE}"],
        input=KILL_PLSQL, capture_output=True, text=True).stdout
    m = re.search(r"KILLED=(\d+)", out)
    return int(m.group(1)) if m else 0


def kill_until_done(thread, delay):
    """Espera ``delay`` s y mata las sesiones LAB_DATA una y otra vez hasta que termina el hilo del cliente. Para recursos
    pequenos, cuya sesion Oracle dura solo 1-2 s, es mas fiable que un unico kill. Devuelve sesiones muertas en total."""
    time.sleep(delay)
    total = 0
    while thread.is_alive():
        total += kill_lab_sessions()
    return total


# ---------------------------------------------------------------------------------------------------------------------
# Salida
# ---------------------------------------------------------------------------------------------------------------------
def table(rows, headers):
    if not rows:
        print("(sin filas)")
        return
    cells = [[("" if r.get(h) is None else str(r.get(h))) for h in headers] for r in rows]
    widths = [max(len(h), *(len(c[i]) for c in cells)) for i, h in enumerate(headers)]
    print("  ".join(h.ljust(w) for h, w in zip(headers, widths)))
    print("  ".join("-" * w for w in widths))
    for c in cells:
        print("  ".join(v.ljust(w) for v, w in zip(c, widths)))


def write_csv(name, rows, headers):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, f"{name}_{time.strftime('%Y%m%d_%H%M%S')}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"\nCSV: {path}")
    return path


def f1(x, nd=2):
    return "-" if x is None else f"{x:.{nd}f}"


def mb(kb):
    return "-" if kb is None else f"{kb / 1024:.0f}"
