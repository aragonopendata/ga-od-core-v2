#!/usr/bin/env python3
"""R7: efecto de los Apache (y de mod_deflate) salto a salto.

  r7_apache.py [--resource ora_200k] [--fail-resource ora_1500k] [--modes none,both] [--hops app,back,front]
               [--cut-bytes 1000000] [--kill-after 3] [--skip-fail]

Para cada combinacion (salto, modo de compresion) hace DOS pruebas con un cliente en bruto (Accept-Encoding: gzip):

  A. corte  El cliente lee --cut-bytes de cuerpo y cierra. Mide:
       ttfb_hdr / ttfb_body  segundos hasta la linea de estado y hasta el primer byte del cuerpo
       t_worker  corte -> ninguna conexion ocupada hacia Gunicorn (el worker vuelve a atender)
       t_ora     corte -> LAB_DATA sin sesiones en Oracle;  t_locks  corte -> sin bloqueos T53 en pg_locks
       log_result/rows/t_total  de la linea download_timing;  X  estado %X de los Apache para esa URL
  B. fallo  Con la descarga en marcha se mata la sesion Oracle (ALTER SYSTEM KILL SESSION ... IMMEDIATE, ver
       lab_common.kill_lab_sessions) --kill-after segundos despues de que aparezca la sesion. El cliente debe ver la
       transferencia como FALLIDA (sin bloque final chunked / Content-Length incompleto) y no como COMPLETA.
       Hoy (sin streaming) el fallo ocurre antes de enviar nada y el cliente ve un error HTTP: se distingue en 'client'.

Modos de compresion (--modes): none | front | back | both = en que Apache esta activo mod_deflate. Cada cambio
recrea los Apache (lab.sh apache MODO). Para el salto 'app' no aplica (se usa el modo actual, que no interviene).
Al terminar se deja el laboratorio en modo none.
"""
import argparse
import sys
import threading
import time

import lab_common as lc


def sync_modes(hop, mode):
    return mode if hop != "app" else "none"


def test_cut(obs, hop, mode, resource, cut_bytes, wait):
    obs.wait_quiet(time.time(), timeout=wait)
    path, nonce = lc.download_path(resource)
    t0 = time.time()
    r = lc.http_get(lc.HOPS[hop], path, accept_gzip=True, cut_after=cut_bytes, timeout=wait + 300)
    t_cut = r["t_cut"]
    obs.wait_quiet(t_cut, timeout=wait)
    window = obs.window(t_cut, time.time())

    def first_zero(pred):
        for i, s in enumerate(window):
            if pred(s) and all(pred(x) for x in window[i:i + 2]):
                return s["t"] - t_cut
        return None

    t_worker = first_zero(lambda s: s["busy"] == 0)
    t_ora = first_zero(lambda s: s.get("ora", {}).get("sessions", 1) == 0)
    t_locks = first_zero(lambda s: s["locks"] == 0)
    timing = lc.wait_timing(t0 - 1, lc.resource_id(resource), timeout=wait)
    time.sleep(1.5)
    flags = []
    for svc, needed in (("apache-front", hop == "front"), ("apache-back", hop in ("front", "back"))):
        if needed:
            lines = lc.apache_lines(svc, t0 - 1, nonce)
            flags.append(f"{svc.split('-')[1]}:{lc.conn_flag(lines[0]) if lines else 'sin linea'}")
    row = timing[-1] if timing else {}
    return {
        "hop": hop, "deflate": mode, "ttfb_hdr": lc.f1(r["t_headers"]), "ttfb_body": lc.f1(r["t_body"]),
        "enc": r["headers"].get("content-encoding", "-"), "t_worker": lc.f1(t_worker), "t_ora": lc.f1(t_ora),
        "t_locks": lc.f1(t_locks), "log_result": row.get("result", "-"), "log_rows": row.get("rows", "-"),
        "log_t_total": row.get("t_total", "-"), "X": " ".join(flags) or "n/a",
    }


def test_fail(obs, hop, mode, resource, kill_after, wait):
    obs.wait_quiet(time.time(), timeout=wait)
    path, _ = lc.download_path(resource)
    out = {}

    def client():
        out["r"] = lc.http_get(lc.HOPS[hop], path, accept_gzip=True, timeout=wait + 300)

    t0 = time.time()
    th = threading.Thread(target=client)
    th.start()
    s = obs.first_time(lambda x: x.get("ora", {}).get("sessions", 0) >= 1, t0, timeout=wait)
    killed = 0
    if s:
        time.sleep(kill_after)
        killed = lc.kill_lab_sessions()
    t_kill = time.time()
    th.join()
    r = out["r"]
    obs.wait_quiet(t_kill, timeout=wait)
    return {
        "hop": hop, "deflate": mode, "killed_sessions": killed, "client": lc.verdict(r), "http": r["status"],
        "chunked": r["chunked"], "body_bytes": r["body_bytes"], "kill_to_client_end_s": lc.f1(max(0.0, r["t_end"] - t_kill)),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--resource", default="ora_200k", help="recurso de las pruebas de corte")
    ap.add_argument("--fail-resource", default="ora_1500k", help="recurso de la prueba de fallo")
    ap.add_argument("--modes", default="none,both")
    ap.add_argument("--hops", default="app,back,front")
    ap.add_argument("--cut-bytes", type=int, default=1000000)
    ap.add_argument("--kill-after", type=float, default=3.0)
    ap.add_argument("--skip-fail", action="store_true")
    ap.add_argument("--wait", type=float, default=300)
    args = ap.parse_args()

    cut_rows, fail_rows = [], []
    done = set()
    with lc.Observer() as obs:
        for mode in args.modes.split(","):
            current = None
            for hop in args.hops.split(","):
                eff = sync_modes(hop, mode)
                if (hop, eff) in done:
                    continue
                done.add((hop, eff))
                if current != mode and hop != "app":
                    lc.set_apache(mode)
                    current = mode
                lc.log(f"== salto={hop} deflate={eff}")
                cut_rows.append(test_cut(obs, hop, eff, args.resource, args.cut_bytes, args.wait))
                if not args.skip_fail:
                    fail_rows.append(test_fail(obs, hop, eff, args.fail_resource, args.kill_after, args.wait))
    lc.set_apache("none")
    print("\nA. Corte del cliente")
    cols = ["hop", "deflate", "ttfb_hdr", "ttfb_body", "enc", "t_worker", "t_ora", "t_locks", "log_result",
            "log_rows", "log_t_total", "X"]
    lc.table(cut_rows, cols)
    lc.write_csv("r7_cut", cut_rows, cols)
    if fail_rows:
        print("\nB. Fallo provocado a mitad de descarga (kill de la sesion Oracle)")
        cols2 = ["hop", "deflate", "killed_sessions", "client", "http", "chunked", "body_bytes", "kill_to_client_end_s"]
        lc.table(fail_rows, cols2)
        lc.write_csv("r7_fail", fail_rows, cols2)
    print("\nLectura: ttfb_body es el tiempo hasta tener datos (hoy = tras construir todo el resultado). t_worker es la "
          "demora entre el corte y la liberacion del worker en cada salto. En B, 'client' debe ser FALLIDA para todos "
          "los saltos cuando el error ocurre con la cabecera ya enviada (streaming).")


if __name__ == "__main__":
    sys.exit(main())
