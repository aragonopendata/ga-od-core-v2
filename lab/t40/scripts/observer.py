#!/usr/bin/env python3
"""Observador del laboratorio. Corre DENTRO del contenedor app (``docker compose exec -T app python /lab/scripts/observer.py``).

Imprime una linea JSON por muestra (cada --interval segundos) con lo que ve desde fuera de la peticion:
  workers   {pid: rss_kb} de los procesos hijos de gunicorn, y master_pid
  busy      conexiones TCP al puerto 6001 de la app en ESTABLISHED o CLOSE_WAIT (un worker sync atiende una a la vez y la
            cierra al terminar: busy == 0 significa que ningun worker esta ocupado con un cliente/proxy)
  ora       {sessions, active, cursors} de LAB_DATA en Oracle (vista v$session / v$open_cursor, usuario SYSTEM)
  cg_kb     memoria del cgroup del contenedor (memory.current / memory.peak) y oom_kills acumulados
  locks     numero de bloqueos consultivos (pg_locks locktype='advisory') de la BD de configuracion, vistos desde otra
            conexion PostgreSQL
Solo biblioteca estandar + oracledb (modo thin) + psycopg2, que ya estan en la imagen. Con --once imprime una muestra.
"""
import argparse
import json
import os
import sys
import time

APP_PORT = 6001


def read_workers():
    """RSS (kB) de cada worker de gunicorn. El master es el proceso gunicorn cuyo padre no es gunicorn."""
    procs = {}
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmd = f.read().replace(b"\0", b" ").decode(errors="replace")
            if "gunicorn" not in cmd:
                continue
            with open(f"/proc/{pid}/status", encoding="ascii") as f:
                status = f.read()
            ppid = int(next(line for line in status.splitlines() if line.startswith("PPid:")).split()[1])
            rss = int(next((line for line in status.splitlines() if line.startswith("VmRSS:")), "VmRSS: 0 kB").split()[1])
            procs[int(pid)] = (ppid, rss)
        except (OSError, StopIteration, ValueError):
            continue
    masters = [p for p, (pp, _) in procs.items() if pp not in procs]
    master = masters[0] if masters else None
    return master, {str(p): rss for p, (pp, rss) in procs.items() if pp == master}


def busy_connections():
    """Conexiones TCP cuyo puerto local es el de la app, en ESTABLISHED (01) o CLOSE_WAIT (08)."""
    n = 0
    for path in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(path, encoding="ascii") as f:
                next(f)
                for line in f:
                    cols = line.split()
                    if int(cols[1].rsplit(":", 1)[1], 16) == APP_PORT and cols[3] in ("01", "08"):
                        n += 1
        except OSError:
            pass
    return n


def cgroup_kb(name):
    """Memoria del cgroup del contenedor (cgroup v2), en kB; None si no existe."""
    try:
        with open(f"/sys/fs/cgroup/{name}", encoding="ascii") as f:
            return int(f.read().strip()) // 1024
    except (OSError, ValueError):
        return None


def oom_kills():
    try:
        with open("/sys/fs/cgroup/memory.events", encoding="ascii") as f:
            return int(dict(line.split() for line in f)["oom_kill"])
    except (OSError, KeyError, ValueError):
        return None


class Probes:
    def __init__(self):
        self.ora = None
        self.pg = None
        self.service = os.environ.get("LAB_ORACLE_SERVICE", "FREEPDB1")

    def oracle(self):
        try:
            if self.ora is None:
                import oracledb  # modo thin: no toca la inicializacion thick de la app
                self.ora = oracledb.connect(user="system", password="lab_admin_pw", dsn=f"oracle:1521/{self.service}")
                self.ora.autocommit = True
            cur = self.ora.cursor()
            cur.execute("select count(*), nvl(sum(case status when 'ACTIVE' then 1 else 0 end),0) "
                        "from v$session where username = 'LAB_DATA'")
            sessions, active = cur.fetchone()
            cur.execute("select count(*) from v$open_cursor oc join v$session s on s.sid = oc.sid "
                        "where s.username = 'LAB_DATA'")
            cursors = cur.fetchone()[0]
            cur.execute("select nvl(max(st.value), 0) from v$sesstat st join v$statname n on n.statistic# = st.statistic# "
                        "join v$session s on s.sid = st.sid where s.username = 'LAB_DATA' "
                        "and n.name = 'SQL*Net roundtrips to/from client'")
            roundtrips = cur.fetchone()[0]
            cur.close()
            return {"sessions": sessions, "active": active, "cursors": cursors, "roundtrips": roundtrips}
        except Exception as err:  # noqa: BLE001
            self.ora = None
            return {"error": str(err)[:80]}

    def locks(self):
        try:
            if self.pg is None:
                import psycopg2
                self.pg = psycopg2.connect(host="configdb", dbname="gaodcore", user="postgres", password="lab_pg_pw")
                self.pg.autocommit = True
            cur = self.pg.cursor()
            cur.execute("select count(*) from pg_locks where locktype = 'advisory'")
            n = cur.fetchone()[0]
            cur.close()
            return n
        except Exception as err:  # noqa: BLE001
            self.pg = None
            return f"error: {str(err)[:60]}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=0.5)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--no-oracle", action="store_true", help="no consultar Oracle (muestras mas rapidas)")
    args = parser.parse_args()
    probes = Probes()
    while True:
        started = time.time()
        master, workers = read_workers()
        sample = {"t": round(started, 3), "master": master, "workers": workers, "busy": busy_connections(),
                  "locks": probes.locks(), "cg_kb": cgroup_kb("memory.current"), "cg_peak_kb": cgroup_kb("memory.peak"),
                  "oom_kills": oom_kills()}
        if not args.no_oracle:
            sample["ora"] = probes.oracle()
        print(json.dumps(sample), flush=True)
        if args.once:
            return
        time.sleep(max(0.0, args.interval - (time.time() - started)))


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, BrokenPipeError):
        sys.exit(0)
