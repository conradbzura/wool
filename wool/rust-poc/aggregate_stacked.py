"""Aggregate stacked_latency.jsonl: per (shape, g), median makespan across reps
for each config, the cumulative improvement vs baseline, and cross-rep spread so
small gains can be judged against noise. Robust to partial (in-progress) data.
"""

import json
import os
import statistics
import sys

RESULTS = "benchmarks/results/stacked_latency.jsonl"
CONFIGS = ["baseline", "+eager", "+eager+pickle", "+eager+pickle+proxy"]
SHORT = {
    "baseline": "base",
    "+eager": "+eager",
    "+eager+pickle": "+pickle",
    "+eager+pickle+proxy": "+proxy",
}
SHAPES = ["s1", "s2", "s3", "s4", "s5", "s6", "s7"]
NAME = {
    "s1": "point-to-point",
    "s2": "fan-out",
    "s3": "scatter-gather",
    "s4": "pipeline",
    "s5": "recursive-tree",
    "s6": "diamond",
    "s7": "streaming",
}


def load(path):
    rows = []
    for line in open(path):
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def main():
    rows = load(RESULTS)
    # (config, shape, g) -> [makespan_us per rep]
    agg = {}
    reps = set()
    for r in rows:
        if r["makespan_p50_s"] is None:
            continue
        reps.add(r["rep"])
        agg.setdefault((r["config"], r["shape"], r["granularity_s"]), []).append(
            r["makespan_p50_s"] * 1e6
        )
    gs = sorted({g for (_c, _s, g) in agg})
    print(f"reps present: {sorted(reps)}  (rows={len(rows)})\n")

    for g in gs:
        print(
            f"=== granularity g = {g:g} s  (median makespan µs across reps; "
            f"[min,max] spread) ==="
        )
        header = f"{'shape':<20}" + "".join(f"{SHORT[c]:>12}" for c in CONFIGS)
        header += f"{'stack Δ%':>10}"
        print(header)
        for s in SHAPES:
            cells = []
            meds = {}
            for c in CONFIGS:
                vals = agg.get((c, s, g))
                if not vals:
                    cells.append(f"{'—':>12}")
                    meds[c] = None
                else:
                    m = statistics.median(vals)
                    meds[c] = m
                    cells.append(f"{m:>12.0f}")
            base = meds.get("baseline")
            allc = meds.get("+eager+pickle+proxy")
            if base and allc:
                delta = 100 * (base - allc) / base
                dstr = f"{delta:>+9.1f}%"
            else:
                dstr = f"{'—':>10}"
            print(f"{s + ' ' + NAME[s]:<20}" + "".join(cells) + dstr)
        # spread line: worst-case cross-rep coefficient of variation at this g
        cvs = []
        for (c, s, gg), vals in agg.items():
            if gg == g and len(vals) >= 2 and statistics.mean(vals) > 0:
                cvs.append(statistics.pstdev(vals) / statistics.mean(vals))
        if cvs:
            print(
                f"  cross-rep CV: median={statistics.median(cvs) * 100:.1f}% "
                f"max={max(cvs) * 100:.1f}%"
            )
        print()

    # incremental attribution at g=0
    g0 = 0.0
    print("=== incremental latency attribution at g=0 (median µs) ===")
    print(
        f"{'shape':<20}{'base':>10}{'eager Δ':>10}{'pickle Δ':>10}{'proxy Δ':>10}{'total Δ':>10}"
    )
    for s in SHAPES:
        m = {
            c: (statistics.median(agg[(c, s, g0)]) if (c, s, g0) in agg else None)
            for c in CONFIGS
        }
        if any(v is None for v in m.values()):
            continue
        eager_d = m["baseline"] - m["+eager"]
        pickle_d = m["+eager"] - m["+eager+pickle"]
        proxy_d = m["+eager+pickle"] - m["+eager+pickle+proxy"]
        total_d = m["baseline"] - m["+eager+pickle+proxy"]
        print(
            f"{s + ' ' + NAME[s]:<20}{m['baseline']:>10.0f}{eager_d:>+10.0f}"
            f"{pickle_d:>+10.0f}{proxy_d:>+10.0f}{total_d:>+10.0f}"
        )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        RESULTS = sys.argv[1]
    if not os.path.exists(RESULTS):
        print(f"no results yet at {RESULTS}")
        sys.exit(0)
    main()
