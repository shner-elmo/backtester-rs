#!/usr/bin/env python3
"""Compare criterion `--output-format bencher` output against a committed baseline.

Usage:
    bench_compare.py <baseline.txt> <current.txt> [threshold]

Both files hold lines in libtest/bencher format, e.g.:

    test engine/read_all_bars ... bench:    12700000 ns/iter (+/- 223961)

For every benchmark present in both files, the current ns/iter is compared to
the baseline. Exit code is non-zero if any benchmark is more than `threshold`
times slower than its baseline (default 2.0 — deliberately loose to absorb the
hardware variance between GitHub runners; tighten once numbers prove stable).

If the baseline file does not exist, the current numbers are printed and the
script exits 0, so a first run can seed the baseline (commit those numbers into
the baseline file) without failing CI.

Stdlib only, no dependencies — auditable for a public repo.
"""
import re
import sys

_LINE = re.compile(r"^test\s+(\S+)\s+\.\.\.\s+bench:\s+([\d,]+)\s+ns/iter")


def parse(path):
    """Map benchmark name -> ns/iter from a bencher-format file."""
    results = {}
    with open(path) as handle:
        for line in handle:
            match = _LINE.match(line.strip())
            if match:
                results[match.group(1)] = int(match.group(2).replace(",", ""))
    return results


def main(argv):
    if len(argv) < 3:
        print("usage: bench_compare.py <baseline> <current> [threshold]", file=sys.stderr)
        return 2

    baseline_path, current_path = argv[1], argv[2]
    threshold = float(argv[3]) if len(argv) > 3 else 2.0

    current = parse(current_path)
    if not current:
        print(f"no benchmark results parsed from {current_path}", file=sys.stderr)
        return 2

    try:
        baseline = parse(baseline_path)
    except FileNotFoundError:
        print(f"no baseline at {baseline_path} — seed it by committing these numbers:")
        for name, ns in sorted(current.items()):
            print(f"  {name}: {ns} ns/iter")
        return 0

    regressed = False
    print(f"benchmark comparison (fail above {threshold:.2f}x slower):")
    for name in sorted(current):
        cur = current[name]
        base = baseline.get(name)
        if base is None or base == 0:
            print(f"  {name}: {cur} ns/iter (no comparable baseline — skipped)")
            continue
        ratio = cur / base
        flag = "   <-- REGRESSED" if ratio > threshold else ""
        print(f"  {name}: {cur} vs {base} ns/iter  ({ratio:.2f}x){flag}")
        if ratio > threshold:
            regressed = True

    if regressed:
        print("\nregression detected: a benchmark exceeded the threshold.", file=sys.stderr)
        return 1
    print("\nok: no benchmark exceeded the threshold.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
