#!/usr/bin/env python3
"""Condicion 7: corte del cliente.  ``curl -s URL | head -c 2000000 > /dev/null`` sobre un recurso grande, en cada salto.

  c7_client_cut.py [--resource ora_1500k] [--hops app,back,front] [--head-bytes 2000000]

Por salto mide (todo desde fuera de la peticion):
  t_cut        segundos que tarda el cliente en tener sus 2 MB y cortar (head sale, curl recibe SIGPIPE)
  t_worker     segundos desde el corte hasta que no queda conexion ocupada hacia Gunicorn (worker libre)
  t_ora        segundos desde el corte hasta que LAB_DATA no tiene sesiones en Oracle (v$session) = origen cerrado
  t_locks      segundos desde el corte hasta que no hay bloqueos consultivos T53 (pg_locks, otra conexion)
  download_timing: result (early_close cuando exista streaming), rows enviadas frente al total, t_total, bytes
  X            estado de conexion (%X) en el log de cada Apache: X = abortada antes de completar la respuesta
Un valor "-" en t_* significa que no ocurrio dentro de --wait segundos.
"""
import argparse
import subprocess
import sys
import time

import lab_common as lc

TOTAL_ROWS = {"ora_10k": 10000, "ora_200k": 200000, "ora_1500k": 1500000, "ora_wide": 20000, "ora_lob": 5000}


def run_hop(obs, hop, resource, head_bytes, wait):
    path, nonce = lc.download_path(resource)
    url = f"http://127.0.0.1:{lc.HOPS[hop]}{path}"
    t0 = time.time()
    p = subprocess.run(f"curl -s '{url}' | head -c {head_bytes} > /dev/null; echo ${{PIPESTATUS[0]}}", shell=True,
                       executable="/bin/bash", capture_output=True, text=True)
    t_cut = time.time()
    curl_exit = p.stdout.strip()
    t_worker = obs.first_free(t_cut, timeout=wait)
    t_quiet, last = obs.wait_quiet(t_cut, timeout=wait)
    window = obs.window(t_cut, time.time())
    t_ora = next((s["t"] for i, s in enumerate(window) if s.get("ora", {}).get("sessions", 1) == 0
                  and all(x.get("ora", {}).get("sessions", 1) == 0 for x in window[i:i + 2])), None)
    t_locks = next((s["t"] for i, s in enumerate(window) if s["locks"] == 0
                    and all(x["locks"] == 0 for x in window[i:i + 2])), None)
    timing = lc.wait_timing(t0 - 1, lc.resource_id(resource), timeout=wait, count=1)
    time.sleep(1.5)
    flags = {}
    for svc in ("apache-front", "apache-back"):
        if (svc == "apache-front" and hop != "front") or (svc == "apache-back" and hop == "app"):
            continue
        for _ in range(int(wait)):
            lines = lc.apache_lines(svc, t0 - 1, nonce)
            if lines:
                break
            time.sleep(1)
        flags[svc] = ",".join(f"{lc.conn_flag(line)}({line.split('dur_us=')[1].split()[0]}us)" for line in lines) or "sin linea"
    row = timing[-1] if timing else {}
    total = TOTAL_ROWS.get(resource)
    return {
        "hop": hop, "curl_exit": curl_exit, "t_cut": lc.f1(t_cut - t0),
        "t_worker": lc.f1(t_worker - t_cut if t_worker else None), "t_ora": lc.f1(t_ora - t_cut if t_ora else None),
        "t_locks": lc.f1(t_locks - t_cut if t_locks else None),
        "log_result": row.get("result", "-"), "log_rows": row.get("rows", "-"),
        "rows_pct": (f"{100 * int(row['rows']) / total:.0f}%" if row.get("rows", "-").lstrip("-").isdigit() and total else "-"),
        "log_t_total": row.get("t_total", "-"), "log_t_first": row.get("t_first", "-"), "log_bytes": row.get("bytes", "-"),
        "X_front": flags.get("apache-front", "n/a"), "X_back": flags.get("apache-back", "n/a"),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--resource", default="ora_1500k")
    ap.add_argument("--hops", default="app,back,front")
    ap.add_argument("--head-bytes", type=int, default=2000000)
    ap.add_argument("--wait", type=float, default=400, help="segundos maximos de espera por cada condicion")
    args = ap.parse_args()
    rows = []
    with lc.Observer() as obs:
        for hop in args.hops.split(","):
            lc.log(f"== salto {hop}: curl | head -c {args.head_bytes} sobre {args.resource} ...")
            obs.wait_quiet(time.time(), timeout=args.wait)
            rows.append(run_hop(obs, hop, args.resource, args.head_bytes, args.wait))
    print()
    cols = ["hop", "curl_exit", "t_cut", "t_worker", "t_ora", "t_locks", "log_result", "log_rows", "rows_pct",
            "log_t_first", "log_t_total", "log_bytes", "X_front", "X_back"]
    lc.table(rows, cols)
    print("\nLectura: con streaming, t_cut es de segundos (primer byte pronto), t_worker/t_ora/t_locks tras el corte son de "
          "pocos segundos, log_result=early_close y log_rows muy inferior al total (rows_pct). Sin streaming "
          "(codigo anterior) el primer byte llega al final del trabajo: t_cut ~ log_t_total, rows_pct=100% y t_* ~ 0 "
          "porque ya no queda nada por hacer cuando el cliente corta.")
    lc.write_csv("c7", rows, cols)


if __name__ == "__main__":
    sys.exit(main())
