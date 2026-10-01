#!/usr/bin/env python3
"""Profile *when* asynchronous replay messages arrive within a test cycle.

Answers the question "did the async fetch run out of time, or did delivery stop
early?" — i.e. whether an incomplete ``mqtt`` count is a **deadline truncation**
(messages still arriving when the 95% cutoff hit) or **genuine non-delivery**
(the stream went quiet well before the cutoff, so extra time would not help).

For each cycle it reports, relative to the cycle start: the first and last
arrival, how much idle time was left before the cutoff, and an ASCII histogram of
arrivals. A run of arrivals right up to the cutoff means truncation; a long idle
tail means the messages were never sent.

Reads the SCGRep log file (``Test period begins:`` for cycle starts and
``Replay message (asynchronous):`` for arrivals), so it works on any instance
without extra instrumentation. Standard library only; ``-h`` for usage.

Each profile covers one Global Replay service (``--centre``); when the log holds
replays from several, the centre must be chosen, as their arrivals are
independent and must not be pooled.
"""

from __future__ import annotations

import argparse
import glob
import gzip
import re
import sys
from collections import Counter

BEGIN_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}),(\d+) .*"
    r"Test period begins: window (\S+) \.\. (\S+)"
)
ASYNC_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}),(\d+) .*"
    r"Replay message \(asynchronous\): centre_id=(\S+) topic=(\S+) id=(\S+) "
)
RESULT_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}),(\d+) .*Result: centre_id=(\S+) "
    r"topic=(\S+) protocol=mqtt baseline=(\d+) fetched=(\d+) .*aborted=(\d)"
)


def secs(hms: str, ms: str) -> float:
    h, m, s = hms.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000


def fmt(t: float) -> str:
    t = int(t)
    return f"{t // 3600:02d}:{t % 3600 // 60:02d}:{t % 60:02d}"


def iter_lines(paths):
    for p in paths:
        opener = gzip.open if p.endswith(".gz") else open
        try:
            with opener(p, "rt", errors="replace") as fh:
                yield from fh
        except FileNotFoundError:
            print(f"warning: log file not found: {p}", file=sys.stderr)


def scan(paths, date, topic):
    """Return (cycle_starts, arrivals, results) for the date/topic.

    ``arrivals`` and ``results`` are keyed by replay centre-id, so independent
    Global Replay services are never pooled."""
    starts, arrivals, results = [], {}, {}
    for line in iter_lines(paths):
        m = BEGIN_RE.match(line)
        if m and m[1] == date:
            starts.append((secs(m[2], m[3]), m[4], m[5]))
            continue
        m = ASYNC_RE.match(line)
        if m and m[1] == date and topic in m[5]:
            arrivals.setdefault(m[4], []).append(secs(m[2], m[3]))
            continue
        m = RESULT_RE.match(line)
        if m and m[1] == date and topic in m[5]:
            results.setdefault(m[4], []).append(
                (secs(m[2], m[3]), int(m[6]), int(m[7]), int(m[8])))
    starts.sort()
    for times in arrivals.values():
        times.sort()
    return starts, arrivals, results


def resolve_centre(requested, seen):
    """Pick the replay service to profile: ``(centre, error)``."""
    seen = sorted(seen)
    if requested:
        if requested in seen:
            return requested, None
        found = ", ".join(seen) or "none"
        return None, (f"No asynchronous replay lines for centre '{requested}' "
                      f"(centres found: {found})")
    if len(seen) > 1:
        return None, ("Log contains replays from several Global Replay services: "
                      f"{', '.join(seen)}. Choose one with --centre.")
    return (seen[0] if seen else None), None


def profile(start, window, arrivals, results, deadline_s, buckets, bar):
    """Print the arrival profile for one cycle; return True if anything printed."""
    got = [a for a in arrivals if start <= a <= start + deadline_s]
    if not got:
        return False
    res = min(results, key=lambda r: abs(r[0] - (start + deadline_s)), default=None)
    ctx = ""
    if res and abs(res[0] - (start + deadline_s)) < 5:
        ctx = f"  baseline={res[1]} fetched={res[2]}" + ("  ABORTED" if res[3] else "")
    print(f"cycle {fmt(start)}  window {window[0]}..{window[1]}{ctx}")
    first, last = got[0] - start, got[-1] - start
    idle = deadline_s - last
    print(f"  arrivals={len(got)}  first=+{first:.1f}s  last=+{last:.1f}s  "
          f"cutoff=+{deadline_s:.1f}s  idle before cutoff={idle:.1f}s")
    width = deadline_s / buckets
    hist = Counter(min(int((a - start) / width), buckets - 1) for a in got)
    top = max(hist.values())
    for b in range(buckets):
        n = hist.get(b, 0)
        marker = "#" * round(bar * n / top) if n else ""
        print(f"    +{b * width:5.1f}-{(b + 1) * width:<5.1f}s {n:>7} {marker}")
    verdict = ("TRUNCATED — still arriving at the cutoff"
               if idle < width else
               f"NOT truncated — stream idle for the last {idle:.0f}s")
    print(f"  -> {verdict}\n")
    return True


def build_parser():
    p = argparse.ArgumentParser(
        prog="replay_arrival_profile.py",
        description="Profile when asynchronous replay messages arrive within a "
                    "test cycle, to tell deadline truncation from non-delivery.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  # busiest cycles for a topic today\n"
            "  replay_arrival_profile.py -t uk-metoffice-globalwave -d 2026-08-17\n\n"
            "  # only cycles with a big burst, from a specific log\n"
            "  replay_arrival_profile.py -t int-eumetsat --min-messages 1000 logs/scgrep.log\n\n"
            "Read the verdict line: arrivals continuing to the cutoff mean the 95% "
            "deadline truncated delivery (a longer TEST_INTERVAL would help); a long "
            "idle tail means the messages were never delivered (it would not)."
        ),
    )
    p.add_argument("log_files", nargs="*", default=["logs/scgrep.log"],
                   help="log file(s) to scan (.log or .gz); default: logs/scgrep.log")
    p.add_argument("-t", "--topic", required=True,
                   help="topic substring to profile (e.g. uk-metoffice-globalwave)")
    p.add_argument("-c", "--centre", metavar="CENTRE_ID",
                   help="Global Replay service to profile (its centre-id). Required "
                        "when the log holds replays from more than one; otherwise "
                        "the only one found is used.")
    p.add_argument("-d", "--date", required=True, metavar="YYYY-MM-DD",
                   help="date to analyse (the log spans multiple days)")
    p.add_argument("-n", "--cycles", type=int, default=5,
                   help="how many cycles to show, busiest first (default: 5)")
    p.add_argument("--min-messages", type=int, default=1, metavar="N",
                   help="only profile cycles with at least N arrivals (default: 1)")
    p.add_argument("-i", "--test-interval", type=float, default=60.0, metavar="SEC",
                   help="TEST_INTERVAL of the instance (default: 60)")
    p.add_argument("--buckets", type=int, default=12,
                   help="histogram buckets across the cycle (default: 12)")
    p.add_argument("--bar-width", type=int, default=40, metavar="N",
                   help="max histogram bar width (default: 40)")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    paths = []
    for pat in args.log_files:
        paths.extend(glob.glob(pat) or [pat])

    starts, by_centre, results_by_centre = scan(paths, args.date, args.topic)
    centre, error = resolve_centre(args.centre, by_centre)
    if error:
        print(error, file=sys.stderr)
        return 1
    arrivals = by_centre.get(centre, [])
    results = results_by_centre.get(centre, [])
    if not starts:
        print(f"No 'Test period begins' lines for {args.date} in: {', '.join(paths)}",
              file=sys.stderr)
        return 1
    if not arrivals:
        print(f"No asynchronous replay messages for topic '{args.topic}' on {args.date}.",
              file=sys.stderr)
        return 1

    deadline = 0.95 * args.test_interval
    counted = [(sum(1 for a in arrivals if s <= a <= s + deadline), s, w0, w1)
               for s, w0, w1 in starts]
    counted = [c for c in counted if c[0] >= args.min_messages]
    counted.sort(key=lambda c: -c[0])

    print(f"centre {centre}  topic '{args.topic}'  date {args.date}  cutoff = 95% of "
          f"{args.test_interval:g}s = {deadline:.1f}s\n")
    shown = 0
    for _n, s, w0, w1 in counted[:args.cycles]:
        if profile(s, (w0, w1), arrivals, results, deadline, args.buckets, args.bar_width):
            shown += 1
    if not shown:
        print("No cycles matched.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
