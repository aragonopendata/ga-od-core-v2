#!/usr/bin/env python3
"""R4: Oracle. Efecto de arraysize y cierre de cursores/sesiones tras descargas completas, cortadas y con error.

  r4_oracle.py [--part arraysize|close|all] [--arraysizes 100,1000,5000,20000] [--resources ora_200k,ora_wide,ora_lob]
               [--hop app]

Parte 1, arraysize. Hoy se configura con common_config.oracle_arraysize (lab: LAB_ORACLE_ARRAYSIZE, por defecto 5000), que
_get_engine pasa como ``arraysize`` al dialecto, y que se aplica a cada cursor. El prefetchrows de oracledb NO es configurable
en el proyecto (se queda en el valor por defecto del driver, 2): este script no puede variarlo sin tocar src/. Para cada
valor recrea app con ese arraysize y descarga cada recurso por --hop; mide t_query/t_normalize/t_total del log, la duracion
vista por el cliente y los viajes de red del lado Oracle ("SQL*Net roundtrips to/from client", maximo visto en v$sesstat
de las sesiones LAB_DATA; es una cota inferior porque se muestrea cada 0,25 s) y el RSS maximo de workers.

Parte 2, cierre. Para cada recurso y para cada caso (completa / cortada tras 1 MB / error con KILL SESSION a mitad):
espera a que la peticion termine y comprueba, con un usuario administrador SOLO para observar (SYSTEM), cuantas sesiones y
cursores abiertos de LAB_DATA quedan (v$session, v$open_cursor) y cuanto tardan en llegar a 0. Si tras --wait segundos
siguen abiertos, es un fallo de cierre (fuga).
"""
import argparse
import sys
import threading
import time

import lab_common as lc


def run_download(hop, name, fmt="csv", **kw):
    path, nonce = lc.download_path(name, fmt)
    t0 = time.time()
    r = lc.http_get(lc.HOPS[hop], path, timeout=900, **kw)
    return r, t0, nonce


def arraysize_part(args, rows):
    for size in [int(x) for x in args.arraysizes.split(",")]:
        lc.log(f"== arraysize={size}: recreando app")
        lc.set_app_env(LAB_ORACLE_ARRAYSIZE=size)
        with lc.Observer(interval=0.25) as obs:
            for name in args.resources.split(","):
                lc.log(f"   descargando {name}")
                # una descarga de calentamiento por recurso (reflexion y modo thick ya inicializado)
                run_download(args.hop, name)
                obs.wait_quiet(time.time(), timeout=args.wait)
                r, t0, _ = run_download(args.hop, name)
                timing = lc.wait_timing(t0 - 1, lc.resource_id(name), timeout=60)
                t = timing[-1] if timing else {}
                obs.wait_quiet(r["t_end"], timeout=args.wait)
                window = obs.window(t0, time.time())
                trips = max((s.get("ora", {}).get("roundtrips", 0) or 0 for s in window), default=0)
                rows.append({
                    "arraysize": size, "resource": name, "http": r["status"], "bytes": r["body_bytes"],
                    "client_ttfb_s": lc.f1(r["t_body"]), "client_total_s": lc.f1(r["total"]),
                    "t_connect": t.get("t_connect", "-"), "t_query": t.get("t_query", "-"),
                    "t_normalize": t.get("t_normalize", "-"), "log_t_total": t.get("t_total", "-"),
                    "oracle_roundtrips": trips, "rss_peak_mb": lc.mb(max(lc.total_rss_kb(s) for s in window)),
                })
    # Deja el laboratorio con el valor por defecto
    lc.set_app_env(LAB_ORACLE_ARRAYSIZE=5000)


def close_part(args, rows):
    with lc.Observer(interval=0.25) as obs:
        for name in args.resources.split(","):
            for case in ("completa", "cortada", "error"):
                obs.wait_quiet(time.time(), timeout=args.wait)
                lc.log(f"== cierre: {name} / {case}")
                t0 = time.time()
                out = {}
                if case == "completa":
                    th = threading.Thread(target=lambda: out.update(r=run_download(args.hop, name)[0]))
                elif case == "cortada":
                    th = threading.Thread(target=lambda: out.update(r=run_download(args.hop, name, cut_after=1000000)[0]))
                else:
                    th = threading.Thread(target=lambda: out.update(r=run_download(args.hop, name)[0]))
                th.start()
                killed = 0
                if case == "error":
                    killed = lc.kill_until_done(th, args.kill_after)
                th.join()
                r = out["r"]
                t_end = r["t_end"]
                during = obs.window(t0, t_end)
                max_sessions = max((s.get("ora", {}).get("sessions", 0) for s in during), default=0)
                max_cursors = max((s.get("ora", {}).get("cursors", 0) for s in during), default=0)
                t_quiet, last = obs.wait_quiet(t_end - 0.5, timeout=args.wait)
                after = last.get("ora", {})
                rows.append({
                    "resource": name, "case": case, "killed": killed, "client": lc.verdict(r),
                    "max_sessions_during": max_sessions, "max_open_cursors_during": max_cursors,
                    "sessions_after": after.get("sessions"), "open_cursors_after": after.get("cursors"),
                    "t_to_zero_s": lc.f1(t_quiet - t_end) if t_quiet else f">{args.wait:.0f} (FUGA)",
                })


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--part", default="all", choices=["arraysize", "close", "all"])
    ap.add_argument("--arraysizes", default="100,1000,5000,20000")
    ap.add_argument("--resources", default="ora_200k,ora_wide,ora_lob")
    ap.add_argument("--hop", default="app", choices=list(lc.HOPS))
    ap.add_argument("--kill-after", type=float, default=0.4, help="segundos entre que Oracle ve una sesion ACTIVA y el kill")
    ap.add_argument("--wait", type=float, default=90)
    args = ap.parse_args()

    if args.part in ("arraysize", "all"):
        rows = []
        arraysize_part(args, rows)
        print("\nParte 1: arraysize (prefetchrows no configurable hoy; ver docstring)")
        cols = ["arraysize", "resource", "http", "bytes", "client_ttfb_s", "client_total_s", "t_connect", "t_query",
                "t_normalize", "log_t_total", "oracle_roundtrips", "rss_peak_mb"]
        lc.table(rows, cols)
        lc.write_csv("r4_arraysize", rows, cols)
    if args.part in ("close", "all"):
        rows = []
        close_part(args, rows)
        print("\nParte 2: cierre de sesiones y cursores en Oracle")
        cols = ["resource", "case", "killed", "client", "max_sessions_during", "max_open_cursors_during",
                "sessions_after", "open_cursors_after", "t_to_zero_s"]
        lc.table(rows, cols)
        lc.write_csv("r4_close", rows, cols)


if __name__ == "__main__":
    sys.exit(main())
