#!/usr/bin/env python3
"""Check whether a Global Replay (WIS2-GRep) service is deduplicating correctly.

HOW IT WORKS
------------
The script makes synchronous HTTP requests to the GRep OGC API Features
endpoint — no MQTT, no SCGRep logs, no external baseline required.

It queries ``origin/a/wis2/…`` topics for a given time window.  Using origin
topics (rather than cache topics) avoids the expected fan-out where each of
the six Global Caches publishes its own notification for the same resource.
On origin topics, each unique resource should produce exactly one notification.

For every item returned, the script extracts the ``data_id + pubtime``
fingerprint (the canonical identifier for a unique resource).  It then counts:

  * total items returned by the API
  * unique message ``id`` values
  * unique ``data_id + pubtime`` fingerprints  (unique resources)
  * duplication ratio  =  total items / unique fingerprints

WHAT A HEALTHY SERVICE LOOKS LIKE
----------------------------------
A correctly operating GRep service subscribes to multiple Global Brokers for
resilience, but deduplicates incoming messages by ``data_id + pubtime`` before
indexing.  Each unique resource is therefore stored exactly once, giving a
duplication ratio of **1.0**.

WHAT A BROKEN SERVICE LOOKS LIKE
----------------------------------
If cross-broker deduplication is not working, the same resource is stored once
per broker that delivered it.  With two brokers, the ratio is **~2.0** and the
same ``data_id + pubtime`` appears twice with different message ``id`` values.

Examples
--------
  # Check a recent 5-minute window (default topics):
  python grep_duplicate_check.py

  # Specify the window explicitly:
  python grep_duplicate_check.py --datetime 2026-09-30T20:10:00Z/2026-09-30T20:15:00Z

  # Check a single topic:
  python grep_duplicate_check.py --topic origin/a/wis2/us-noaa-nws \\
      --datetime 2026-09-30T20:10:00Z/2026-09-30T20:15:00Z

  # Use a different GRep endpoint:
  python grep_duplicate_check.py --url https://wis2-grep.example.org

Requires Python 3.9+, standard library only (+ optional certifi for TLS on macOS).
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone

DEFAULT_URL = "https://wis2-grep.weather.gc.ca"
DEFAULT_TOPICS = [
    "origin/a/wis2/us-noaa-nws",
    "origin/a/wis2/uk-metoffice",
    "origin/a/wis2/int-eumetsat",
    "origin/a/wis2/be-rmib",
    "origin/a/wis2/kg-kyrgyzhydromet",
]
COLLECTION = "wis2-notification-messages"
PAGE_SIZE = 1000

# Ratio threshold above which deduplication is considered broken.
RATIO_WARN = 1.5


# ---------------------------------------------------------------------------
# TLS
# ---------------------------------------------------------------------------
# On macOS with a python.org Python install, urllib does not find the system
# CA bundle.  Use certifi if available; otherwise fall back to the default
# context (works on Linux / Windows).  Pass --no-verify-tls as a last resort.

try:
    import certifi as _certifi
    _SSL_CTX = ssl.create_default_context(cafile=_certifi.where())
except ImportError:
    _certifi = None  # type: ignore[assignment]
    _SSL_CTX = ssl.create_default_context()

_NO_VERIFY_TLS = False


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _get(url: str) -> dict:
    ctx = ssl._create_unverified_context() if _NO_VERIFY_TLS else _SSL_CTX
    req = urllib.request.Request(url, headers={"Accept": "application/geo+json"})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as exc:
        sys.exit(f"HTTP {exc.code} from {url}")
    except ssl.SSLCertVerificationError:
        sys.exit(
            "TLS certificate verification failed.\n"
            "\n"
            "Fix (macOS python.org install — run once):\n"
            "  /Applications/Python\\ 3.x/Install\\ Certificates.command\n"
            "\n"
            "Or install certifi:\n"
            "  pip install certifi\n"
            "\n"
            "Or pass --no-verify-tls (insecure, for testing only)."
        )
    except Exception as exc:
        sys.exit(f"Request failed: {exc}")


def collect(base_url: str, topic: str, datetime_range: str) -> list[dict]:
    """Fetch all items for (topic, window), paging as needed."""
    features: list[dict] = []
    number_matched: int | None = None
    while True:
        params = urllib.parse.urlencode({
            "topic": topic,
            "datetime": datetime_range,
            "limit": PAGE_SIZE,
            "offset": len(features),
        })
        data = _get(f"{base_url}/collections/{COLLECTION}/items?{params}")
        if number_matched is None:
            number_matched = data.get("numberMatched", 0)
        page = data.get("features", [])
        features.extend(page)
        if not page or len(features) >= (number_matched or 0):
            break
    return features


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def analyse(features: list[dict]) -> dict:
    ids = [f.get("id", "") for f in features]
    props = [f.get("properties") or {} for f in features]
    fingerprints = [(p.get("data_id", ""), p.get("pubtime", "")) for p in props]

    fp_counts = Counter(fingerprints)
    total = len(ids)
    unique_ids = len(set(ids))
    unique_fps = len(set(fingerprints))
    ratio = total / unique_fps if unique_fps else 0.0

    # Examples: fingerprints that appear more than once.
    examples = sorted(
        ((fp, cnt) for fp, cnt in fp_counts.items() if cnt > 1),
        key=lambda x: -x[1],
    )[:3]

    return {
        "total": total,
        "unique_ids": unique_ids,
        "unique_fps": unique_fps,
        "ratio": ratio,
        "examples": examples,
    }


# ---------------------------------------------------------------------------
# Default window: 5-minute window ending 10 minutes ago
# ---------------------------------------------------------------------------

def default_window() -> str:
    now = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    end = now - timedelta(minutes=10)
    start = end - timedelta(minutes=5)
    return f"{start.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end.strftime('%Y-%m-%dT%H:%M:%SZ')}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        metavar="URL",
        help=f"Base URL of the GRep service (default: {DEFAULT_URL})",
    )
    parser.add_argument(
        "--topic",
        dest="topics",
        action="append",
        metavar="TOPIC",
        help=(
            "Topic prefix to check (no trailing /#).  May be repeated.  "
            "Defaults to five origin/a/wis2/… topics."
        ),
    )
    parser.add_argument(
        "--datetime",
        metavar="START/END",
        help=(
            "Time window as an ISO-8601 interval "
            "(e.g. 2026-10-01T14:00:00Z/2026-10-01T14:05:00Z).  "
            "Defaults to the 5-minute window ending 10 minutes ago."
        ),
    )
    parser.add_argument(
        "--no-verify-tls",
        action="store_true",
        help="Disable TLS certificate verification (insecure; use only for testing).",
    )
    args = parser.parse_args()

    global _NO_VERIFY_TLS
    _NO_VERIFY_TLS = args.no_verify_tls
    if _NO_VERIFY_TLS:
        print("WARNING: TLS certificate verification is disabled.", file=sys.stderr)

    topics = args.topics or DEFAULT_TOPICS
    window = args.datetime or default_window()

    print(f"Endpoint  : {args.url}")
    print(f"Window    : {window}")
    print(f"Topics    : {', '.join(t.split('/', 3)[-1] for t in topics)}")
    print()

    w = 40
    print(f"  {'Topic':{w}} {'Total':>7}  {'Uniq-ID':>7}  {'Uniq-res':>8}  {'Ratio':>6}")
    print(f"  {'-'*w} {'-------':>7}  {'-------':>7}  {'--------':>8}  {'------':>6}")

    any_problem = False
    for topic in topics:
        label = "/".join(topic.split("/")[3:]) or topic
        print(f"  {label:{w}}", end=" ", flush=True)
        features = collect(args.url, topic, window)
        r = analyse(features)

        flag = ""
        if r["total"] == 0:
            flag = "  (no data)"
        elif r["ratio"] >= RATIO_WARN:
            flag = "  *** DEDUPLICATION FAILURE ***"
            any_problem = True

        print(
            f"{r['total']:>7}  {r['unique_ids']:>7}  {r['unique_fps']:>8}  "
            f"{r['ratio']:>6.2f}{flag}"
        )
        for (data_id, pubtime), cnt in r["examples"]:
            short = data_id.split("/")[-1] if "/" in data_id else data_id
            print(f"      {cnt}x  {short}  ({pubtime})")

    print()
    if any_problem:
        print(
            "RESULT: Deduplication failure detected.\n"
            "\n"
            "One or more topics show a ratio significantly above 1.0, meaning\n"
            "the same resource (data_id + pubtime) has been stored more than once.\n"
            "\n"
            "Expected behaviour: the service subscribes to multiple Global Brokers\n"
            "for resilience and deduplicates incoming messages by data_id + pubtime\n"
            "before indexing, so each resource is stored exactly once (ratio = 1.0).\n"
            "\n"
            "A ratio of ~2.0 is consistent with cross-broker deduplication having\n"
            "stopped working: the same message is being stored once per broker\n"
            "rather than once per resource."
        )
        sys.exit(1)
    else:
        print("RESULT: No deduplication failure detected.")
        sys.exit(0)


if __name__ == "__main__":
    main()
