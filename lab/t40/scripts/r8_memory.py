#!/usr/bin/env python3
"""R8: memoria, tiempo hasta el primer byte y trabajo tras el corte, con 0,8 CPU.

  r8_memory.py [--hop front] [--resources ora_10k,ora_200k,ora_wide,ora_lob] [--repeat 3] [--concurrency 1,2,4]
               [--cut] [--format csv]

Para cada (recurso, concurrencia) lanza `repeat` rondas; en cada ronda arranca N descargas a la vez (recursos distintos
de la misma tabla para no chocar con los limites T53: ora_200k, ora_200k_b...; si no hay copias se repite el recurso) y mide:
  rss_peak_mb    maximo de la suma de RSS de los workers de Gunicorn durante la ronda (/proc, dentro del contenedor)
  rss_worker_mb  maximo RSS de un solo worker;  rss_after_mb  suma de RSS 3 s despues de terminar (memoria retenida)
  cg_peak_mb     maximo de memory.current del cgroup del contenedor;  oom_kills  muertes por OOM en la ronda
  ttfb_body_s    mediana de segundos hasta el primer byte del cuerpo (cliente);  total_s  mediana de la duracion
  log_*          mediana de los campos de download_timing de esas descargas (rows, bytes, t_first, t_total, rss_kb, maxrss_kb;
                 "-" si el codigo aun no los escribe)
  codes          codigos HTTP vistos (503/429 = limites T53)
Con --cut, el cliente corta tras 1 MB y se mide ademas t_work_after_cut_s = corte -> sin conexion ocupada (trabajo que
sigue tras el corte). Escribe lab/t40/results/r8_*.csv. NO impone umbrales: sirve para fijarlos despues.
"""
import argparse
import statistics
import sys
import threading
import time

import lab_common as lc


def med(vals):
    vals = [float(v) for v in vals if v not in (None, "-", "")]
    return statistics.median(vals) if vals else None


def one_round(obs, hop, names, fmt, cut_bytes, wait):
    obs.wait_quiet(time.time(), timeout=wait, need_oracle=False)
    time.sleep(1)
    base = obs.latest()
    oom0 = base.get("oom_kills") or 0
    jobs = [lc.download_path(n, fmt) for n in names]
    results = [None] * len(jobs)

    def run(i):
        results[i] = lc.http_get(lc.HOPS[hop], jobs[i][0], cut_after=cut_bytes, timeout=wait + 600)

    t0 = time.time()
    threads = [threading.Thread(target=run, args=(i,)) for i in range(len(jobs))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    t_end = time.time()
    t_cut = max(r["t_cut"] for r in results) if cut_bytes else None
    free = obs.first_free(t_cut or t_end, timeout=wait)
    time.sleep(3)
    window = obs.window(t0, time.time())
    after = obs.latest()
    ids = {str(lc.resource_id(n)) for n in names}
    deadline = time.time() + 60
    while len([r for r in lc.timing_lines(t0 - 1) if r.get("resource_id") in ids]) < len(jobs) and time.time() < deadline:
        time.sleep(1)
    timing = [r for r in lc.timing_lines(t0 - 1) if r.get("resource_id") in ids]
    return {
        "rss_peak_mb": lc.mb(max(lc.total_rss_kb(s) for s in window)),
        "rss_worker_mb": lc.mb(max((max(s["workers"].values(), default=0) for s in window), default=0)),
        "rss_after_mb": lc.mb(lc.total_rss_kb(after)),
        "cg_peak_mb": lc.mb(max((s.get("cg_kb") or 0) for s in window)),
        "oom_kills": (after.get("oom_kills") or 0) - oom0,
        "ttfb_body_s": lc.f1(med([r["t_body"] for r in results])),
        "total_s": lc.f1(med([r["total"] for r in results])),
        "t_work_after_cut_s": lc.f1(free - t_cut) if cut_bytes and free else ("-" if not cut_bytes else ">wait"),
        "log_rows": lc.f1(med([r.get("rows") for r in timing]), 0),
        "log_bytes": lc.f1(med([r.get("bytes") for r in timing]), 0),
        "log_t_first": lc.f1(med([r.get("t_first") for r in timing])),
        "log_t_total": lc.f1(med([r.get("t_total") for r in timing])),
        "log_rss_kb": lc.f1(med([r.get("rss_kb") for r in timing]), 0),
        "log_maxrss_kb": lc.f1(med([r.get("maxrss_kb") for r in timing]), 0),
        "log_result": ",".join(sorted({r.get("result", "-") for r in timing})) or "-",
        "codes": ",".join(sorted({str(r["status"]) for r in results})),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hop", default="front", choices=list(lc.HOPS))
    ap.add_argument("--resources", default="ora_10k,ora_200k,ora_wide,ora_lob")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--concurrency", default="1,2,4")
    ap.add_argument("--format", default="csv")
    ap.add_argument("--cut", action="store_true", help="el cliente corta tras --cut-bytes")
    ap.add_argument("--cut-bytes", type=int, default=1000000)
    ap.add_argument("--wait", type=float, default=300)
    args = ap.parse_args()

    rows = []
    cut = args.cut_bytes if args.cut else None
    with lc.Observer(interval=0.5, oracle=True) as obs:
        for res in args.resources.split(","):
            for conc in [int(c) for c in args.concurrency.split(",")]:
                names = lc.resource_family(res, conc)
                for rep in range(1, args.repeat + 1):
                    lc.log(f"== {res} concurrencia={conc} ronda={rep}/{args.repeat}")
                    row = {"resource": res, "concurrency": conc, "round": rep, "cut": bool(cut)}
                    row.update(one_round(obs, args.hop, names, args.format, cut, args.wait))
                    rows.append(row)
    cols = ["resource", "concurrency", "round", "cut", "rss_peak_mb", "rss_worker_mb", "rss_after_mb", "cg_peak_mb",
            "oom_kills", "ttfb_body_s", "total_s", "t_work_after_cut_s", "log_rows", "log_bytes", "log_t_first",
            "log_t_total", "log_rss_kb", "log_maxrss_kb", "log_result", "codes"]
    print()
    lc.table(rows, cols)
    lc.write_csv("r8", rows, cols)


if __name__ == "__main__":
    sys.exit(main())
