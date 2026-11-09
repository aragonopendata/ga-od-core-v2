#!/usr/bin/env python3
"""Extrae y resume las lineas ``download_timing`` del log del contenedor app.

  timing_summary.py [--since SEGUNDOS|--last N] [--resource NOMBRE] [--csv]

Tolera lineas antiguas (sin result/t_first/rss_kb): esas columnas salen como "-". Con streaming, ``rows`` y ``bytes``
son lo entregado a Gunicorn y ``result`` es complete|early_close|error.
"""
import argparse
import statistics
import sys
import time

import lab_common as lc

COLS = ["resource_id", "format", "status", "rows", "bytes", "t_connect", "t_query", "t_normalize", "t_render",
        "t_first", "t_total", "result", "rss_kb", "maxrss_kb", "limited"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--since", type=float, default=86400 * 30, help="segundos hacia atras (por defecto, todo)")
    ap.add_argument("--last", type=int, help="solo las ultimas N lineas")
    ap.add_argument("--resource", help="nombre de recurso sembrado (p. ej. ora_200k)")
    ap.add_argument("--csv", action="store_true", help="escribe tambien un CSV en lab/t40/results/")
    args = ap.parse_args()

    rows = lc.timing_lines(time.time() - args.since)
    if args.resource:
        rid = str(lc.resource_id(args.resource))
        rows = [r for r in rows if r.get("resource_id") == rid]
    if args.last:
        rows = rows[-args.last:]
    names = {str(v): k for k, v in lc.resource_ids().items()}
    for r in rows:
        r["resource"] = names.get(r.get("resource_id"), r.get("resource_id"))
    print(f"{len(rows)} lineas download_timing")
    lc.table(rows, ["resource"] + COLS[1:])

    groups = {}
    for r in rows:
        if r.get("endpoint") != "download":
            continue
        groups.setdefault((r["resource"], r.get("format"), r.get("result", "-")), []).append(r)
    summary = []
    for (res, fmt, result), items in sorted(groups.items()):
        def med(key):
            vals = []
            for i in items:
                try:
                    vals.append(float(i[key]))
                except (KeyError, ValueError):
                    pass
            return f"{statistics.median(vals):.2f}" if vals else "-"
        summary.append({"resource": res, "format": fmt, "result": result, "n": len(items), "rows_med": med("rows"),
                        "t_first_med": med("t_first"), "t_total_med": med("t_total"), "bytes_med": med("bytes"),
                        "rss_kb_med": med("rss_kb"), "maxrss_kb_med": med("maxrss_kb")})
    print("\nResumen por (recurso, formato, result):")
    lc.table(summary, ["resource", "format", "result", "n", "rows_med", "t_first_med", "t_total_med", "bytes_med",
                       "rss_kb_med", "maxrss_kb_med"])
    if args.csv:
        lc.write_csv("timing", rows, ["resource"] + COLS + ["endpoint"])


if __name__ == "__main__":
    sys.exit(main())
