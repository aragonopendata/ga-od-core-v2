#!/usr/bin/env python3
"""R3: timeout de Gunicorn y contrapresion (cliente lento / que deja de leer).

  r3_timeout_backpressure.py [--hop front] [--resource ora_200k] [--timeout N] [--slow-rate 500000] [--restore]

Casos (todos con la URL de descarga CSV de --resource, que es "grande" para T53, asi que toma bloqueos consultivos):
  1 completa      cliente normal (curl): debe acabar COMPLETA, sin WORKER TIMEOUT, bloqueos liberados despues
  2 lento         curl --limit-rate: tarda mas que --timeout mientras salen datos
  3 se_para       el cliente abre la conexion, lee ~64 KB y deja de leer (sin cerrar) durante timeout+10 s
  4 timeout_flujo igual que 2 pero con --slow-rate mas bajo si hace falta: la transferencia dura > timeout
Por caso imprime: veredicto del cliente (codigo de salida de curl; para el caso 3, el cliente en bruto comprueba si
llego el bloque final chunked), numero de WORKER TIMEOUT en el log de app, workers reemplazados (pids antes/despues),
bloqueos T53 maximos durante el caso y los que quedan al asentarse (pg_locks locktype='advisory', otra conexion), sesiones
Oracle residuales y tiempo hasta quedar todo en calma.

--timeout N recrea app con GUNICORN_TIMEOUT=N (p. ej. 20 para probar rapido). Sin --timeout se usa el valor actual del
contenedor (240 en produccion: los casos 2-4 duran entonces mas de 4 minutos cada uno). --restore devuelve 240 al final.
"""
import argparse
import sys
import threading
import time

import lab_common as lc


def case(name, obs, hop, resource, fn, timeout_s, wait):
    obs.wait_quiet(time.time(), timeout=wait)
    pids0 = set(obs.latest()["workers"])
    t0 = time.time()
    out = {}
    th = threading.Thread(target=lambda: out.update(fn()))
    th.start()
    th.join()
    t_end = time.time()
    t_quiet, last = obs.wait_quiet(t_end - 0.5, timeout=wait)
    window = obs.window(t0, time.time())
    pids1 = set(obs.latest()["workers"])
    timeouts = lc.count_worker_timeouts(t0 - 1)
    return {
        "case": name, "client": out.get("verdict", "-"), "curl_exit": out.get("curl_exit", "-"),
        "http": out.get("http", "-"), "bytes": out.get("bytes", "-"), "dur_s": lc.f1(t_end - t0, 1),
        "worker_timeouts": timeouts, "workers_replaced": len(pids0 - pids1),
        "locks_max_during": lc.max_locks(window), "locks_after": last["locks"],
        "ora_sessions_after": last.get("ora", {}).get("sessions"),
        "t_quiet_s": lc.f1(max(0.0, t_quiet - t_end), 1) if t_quiet else f">{wait:.0f}",
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hop", default="front", choices=list(lc.HOPS))
    ap.add_argument("--resource", default="ora_200k")
    ap.add_argument("--timeout", type=int, help="GUNICORN_TIMEOUT para la prueba (recrea app)")
    ap.add_argument("--slow-rate", type=int, default=500000, help="bytes/s de curl --limit-rate")
    ap.add_argument("--wait", type=float, default=120, help="espera maxima a que todo quede en calma")
    ap.add_argument("--restore", action="store_true", help="vuelve a GUNICORN_TIMEOUT=240 al terminar")
    ap.add_argument("--cases", default="1,2,3,4")
    args = ap.parse_args()

    if args.timeout:
        lc.set_app_env(LAB_GUNICORN_TIMEOUT=args.timeout)
    timeout_s = int(lc.app_env("GUNICORN_TIMEOUT", "240"))
    port = lc.HOPS[args.hop]
    print(f"R3 salto={args.hop} recurso={args.resource} GUNICORN_TIMEOUT={timeout_s}s limit-rate={args.slow_rate} B/s")
    wanted = set(args.cases.split(","))
    rows = []

    def full():
        path, _ = lc.download_path(args.resource)
        r = lc.curl_download(port, path)
        return {"curl_exit": f"{r['exit']} {lc.CURL_EXIT.get(r['exit'], '')}".strip(), "http": r["http"],
                "bytes": r["bytes"], "verdict": "COMPLETA" if r["exit"] == 0 and r["http"] == 200 else "FALLIDA"}

    def slow():
        path, _ = lc.download_path(args.resource)
        r = lc.curl_download(port, path, limit_rate=args.slow_rate)
        return {"curl_exit": f"{r['exit']} {lc.CURL_EXIT.get(r['exit'], '')}".strip(), "http": r["http"],
                "bytes": r["bytes"], "verdict": "COMPLETA" if r["exit"] == 0 and r["http"] == 200 else
                "FALLIDA (curl exit != 0)"}

    def stalled():
        path, _ = lc.download_path(args.resource)
        r = lc.http_get(port, path, stall_after=65536, stall_seconds=timeout_s + 10, stall_then_close=False,
                        timeout=timeout_s + 120)
        # Tras la pausa sigue leyendo: si el worker fue reemplazado, el cuerpo llega truncado.
        return {"curl_exit": "n/a (cliente en bruto)", "http": r["status"], "bytes": r["body_bytes"],
                "verdict": lc.verdict(r) + f" [tras {timeout_s + 10}s parado]"}

    with lc.Observer() as obs:
        if "1" in wanted:
            lc.log("-- caso 1: descarga completa")
            rows.append(case("1 completa", obs, args.hop, args.resource, full, timeout_s, args.wait))
        if "2" in wanted:
            lc.log(f"-- caso 2: cliente lento ({args.slow_rate} B/s)")
            rows.append(case("2 lento", obs, args.hop, args.resource, slow, timeout_s, args.wait))
        if "3" in wanted:
            lc.log(f"-- caso 3: el cliente deja de leer {timeout_s + 10} s")
            rows.append(case("3 se_para", obs, args.hop, args.resource, stalled, timeout_s, args.wait))
        if "4" in wanted:
            rate = max(50000, args.slow_rate // 4)
            lc.log(f"-- caso 4: datos fluyendo a {rate} B/s durante mas que el timeout")

            def flowing():
                path, _ = lc.download_path(args.resource)
                r = lc.curl_download(port, path, limit_rate=rate)
                return {"curl_exit": f"{r['exit']} {lc.CURL_EXIT.get(r['exit'], '')}".strip(), "http": r["http"],
                        "bytes": r["bytes"], "verdict": "COMPLETA" if r["exit"] == 0 and r["http"] == 200 else
                        "FALLIDA (curl exit != 0)"}
            rows.append(case("4 timeout_flujo", obs, args.hop, args.resource, flowing, timeout_s, args.wait))

    print()
    cols = ["case", "client", "curl_exit", "http", "bytes", "dur_s", "worker_timeouts", "workers_replaced",
            "locks_max_during", "locks_after", "ora_sessions_after", "t_quiet_s"]
    lc.table(rows, cols)
    print("\nLectura: 'client' debe ser FALLIDA siempre que el worker sea cortado a mitad (WORKER TIMEOUT > 0), nunca "
          "COMPLETA con datos truncados; 'locks_after' y 'ora_sessions_after' deben ser 0. 'workers_replaced' > 0 con "
          "'worker_timeouts' > 0 indica que Gunicorn sustituyo el worker.")
    lc.write_csv("r3", rows, cols)
    if args.restore and args.timeout:
        lc.set_app_env(LAB_GUNICORN_TIMEOUT=240)


if __name__ == "__main__":
    sys.exit(main())
