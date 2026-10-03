#!/usr/bin/env python3
"""Measure how many replay messages a mirror broker drops relative to the source GRep brokers.

HOW IT WORKS
------------
The script discovers the subscriber-id and replay-broker configuration from a
running SCGRep instance (via docker logs or the SCGRep log file).  It then
subscribes to the same replay channels that SCGRep is already using — one
subscription per broker — and passively observes what arrives.

For each GRep instance (centre-id):
  * "source" broker  — the GRep instance's own MQTT broker
  * "mirror" broker  — typically gb.wis2dev.io, which should republish every
                       message the source publishes

Because the mirror just forwards payloads unchanged, every source message
carries the same ``id`` on both brokers.  A message is counted as *dropped*
when it is received from the source but not from the mirror within the grace
period.

Results are bucketed by wall-clock time (configurable width), displayed as an
ASCII bar chart, and the raw drop records are saved to a JSONL file for later
analysis.

REQUIREMENTS
------------
  paho-mqtt >= 2.0  (pip install paho-mqtt)
  Running SCGRep container (named "scgrep") or a log file to read from.

EXAMPLES
--------
  # 30-minute run, 5-minute buckets (defaults):
  python broker_relay_check.py

  # 60-minute run, 10-minute buckets:
  python broker_relay_check.py --duration 60 --bucket 10

  # Use a specific log file instead of docker:
  python broker_relay_check.py --logfile logs/scgrep.log

  # Change the mirror broker:
  python broker_relay_check.py --mirror globalbroker.meteo.fr

  # Longer grace period for high-latency links:
  python broker_relay_check.py --grace 60
"""

from __future__ import annotations

import argparse
import json
import re
import ssl
import subprocess
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

try:
    import paho.mqtt.client as mqtt
    from paho.mqtt.client import CallbackAPIVersion
except ImportError:
    sys.exit(
        "paho-mqtt is required.  Install it with:\n"
        "  pip install paho-mqtt\n"
        "or use the project virtualenv:\n"
        "  .venv/bin/python scripts/broker_relay_check.py ..."
    )

DEFAULT_MIRROR = "gb.wis2dev.io"
DEFAULT_PORT = 8883
DEFAULT_DURATION = 30    # minutes
DEFAULT_BUCKET = 5       # minutes
DEFAULT_GRACE = 30       # seconds
BROKER_USER = "everyone"
BROKER_PASS = "everyone"
CHART_WIDTH = 50         # chars; each char represents 2% drop rate


# ---------------------------------------------------------------------------
# TLS
# ---------------------------------------------------------------------------

try:
    import certifi as _certifi
except ImportError:
    _certifi = None

_NO_VERIFY_TLS = False


def _tls_context() -> ssl.SSLContext:
    if _NO_VERIFY_TLS:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    if _certifi:
        return ssl.create_default_context(cafile=_certifi.where())
    return ssl.create_default_context()


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

@dataclass
class ScgrepConfig:
    subscriber_id: str
    # broker_host -> set of centre_ids it is subscribed to
    broker_subscriptions: dict[str, set[str]]


def _parse_config(lines: list[str]) -> ScgrepConfig:
    """Extract subscriber-id and broker/centre-id map from SCGRep log lines."""
    subscriber_id = None
    broker_subs: dict[str, set[str]] = {}

    # Walk in reverse so we pick up the most-recent startup
    for line in reversed(lines):
        # "Subscribed on <broker> to N topic(s): replay/a/wis2/<centre>/<sub>/#, ..."
        m = re.search(r"Subscribed on (\S+) to \d+ topic\(s\): (.+)", line)
        if not m:
            continue
        broker = m.group(1)
        topics_raw = m.group(2)
        for topic in topics_raw.split(","):
            topic = topic.strip().rstrip("#").rstrip("/")
            parts = topic.split("/")
            # replay/a/wis2/<centre-id>/<sub-id>
            if len(parts) >= 5 and parts[0] == "replay":
                centre_id = parts[3]
                sub_id = parts[4]
                if subscriber_id is None:
                    subscriber_id = sub_id
                broker_subs.setdefault(broker, set()).add(centre_id)

        # Stop once we have a full picture from one startup block
        if subscriber_id and len(broker_subs) >= 1:
            # Keep scanning until we've seen all brokers from this startup
            pass

    if not subscriber_id:
        raise RuntimeError(
            "Could not find subscriber-id in SCGRep logs.\n"
            "Make sure SCGRep is running and has logged its startup lines."
        )
    return ScgrepConfig(subscriber_id=subscriber_id, broker_subscriptions=broker_subs)


def discover_scgrep_config(logfile: str | None = None) -> ScgrepConfig:
    """Find SCGRep's subscriber-id and broker config from a log file or docker logs."""
    lines: list[str] = []

    if logfile:
        path = Path(logfile)
        if not path.exists():
            raise RuntimeError(f"Log file not found: {logfile}")
        lines = path.read_text().splitlines()
    else:
        # Prefer the SCGRep log file: it is trimmed to 24 h and always contains
        # the most-recent startup lines.
        candidates = [
            Path(__file__).parent.parent / "logs" / "scgrep.log",
            Path("logs/scgrep.log"),
        ]
        for p in candidates:
            if p.exists() and any("Subscribed on" in l for l in p.read_text().splitlines()):
                lines = p.read_text().splitlines()
                break

        if not any("Subscribed on" in l for l in lines):
            # Fall back to docker logs (last 2 days covers any startup)
            try:
                result = subprocess.run(
                    ["docker", "logs", "--since", "48h", "scgrep"],
                    capture_output=True, text=True, timeout=30,
                )
                lines = (result.stdout + result.stderr).splitlines()
            except Exception:
                pass

    if not any("Subscribed on" in l for l in lines):
        raise RuntimeError(
            "Cannot find SCGRep subscription lines in any log source.\n"
            "Make sure SCGRep is running, or pass --logfile <path>."
        )

    return _parse_config(lines)


# ---------------------------------------------------------------------------
# Message collection
# ---------------------------------------------------------------------------

@dataclass
class MsgRecord:
    id: str
    centre_id: str
    broker: str
    pubtime: str
    arrival: float      # time.time()


def _make_on_message(broker_host: str, store: list[MsgRecord], lock: threading.Lock):
    def on_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except (ValueError, TypeError):
            return
        mid = data.get("id", "")
        if not mid:
            return
        props = data.get("properties") or {}
        pubtime = props.get("pubtime", "")
        parts = msg.topic.split("/")
        if len(parts) < 5:
            return
        centre_id = parts[3]
        rec = MsgRecord(
            id=mid,
            centre_id=centre_id,
            broker=broker_host,
            pubtime=pubtime,
            arrival=time.time(),
        )
        with lock:
            store.append(rec)
    return on_message


def _make_on_connect(broker_host: str, subscriptions: list[str], ready: dict):
    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code != 0:
            print(f"\n  WARNING: connect to {broker_host} failed: {reason_code}", file=sys.stderr)
            return
        client.subscribe([(t, 0) for t in subscriptions])
        ready[broker_host] = True
    return on_connect


def _make_on_connect_fail(broker_host: str):
    def on_connect_fail(client, userdata):
        print(
            f"\n  WARNING: TLS/TCP connection to {broker_host} failed. "
            f"Try --no-verify-tls if certificate verification is the issue.",
            file=sys.stderr,
        )
    return on_connect_fail


def build_client(
    broker_host: str,
    port: int,
    subscriptions: list[str],
    store: list[MsgRecord],
    lock: threading.Lock,
    ready: dict,
) -> mqtt.Client:
    client_id = f"broker-cmp-{broker_host[:12]}-{int(time.time()) % 10000}"
    client = mqtt.Client(
        CallbackAPIVersion.VERSION2,
        client_id=client_id,
        clean_session=True,
    )
    client.username_pw_set(BROKER_USER, BROKER_PASS)
    if _NO_VERIFY_TLS:
        client.tls_set(cert_reqs=ssl.CERT_NONE)
        client.tls_insecure_set(True)
    elif _certifi:
        client.tls_set(ca_certs=_certifi.where())
    else:
        client.tls_set()
    client.on_connect = _make_on_connect(broker_host, subscriptions, ready)
    client.on_connect_fail = _make_on_connect_fail(broker_host)
    client.on_message = _make_on_message(broker_host, store, lock)
    return client


# ---------------------------------------------------------------------------
# Progress display
# ---------------------------------------------------------------------------

class ProgressPrinter:
    def __init__(self, duration_secs: int, store: list[MsgRecord], lock: threading.Lock, mirror: str):
        self._duration = duration_secs
        self._start = time.time()
        self._store = store
        self._lock = lock
        self._mirror = mirror
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        print(file=sys.stderr)

    def _run(self):
        while self._running:
            self._print()
            time.sleep(1)

    def _print(self):
        elapsed = time.time() - self._start
        pct = min(elapsed / self._duration, 1.0)
        filled = int(30 * pct)
        bar = "█" * filled + "░" * (30 - filled)
        rem = max(0, self._duration - elapsed)
        elapsed_s = f"{int(elapsed // 60):02d}:{int(elapsed % 60):02d}"
        rem_s = f"{int(rem // 60):02d}:{int(rem % 60):02d}"

        with self._lock:
            counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
            for rec in self._store:
                role = "mirror" if rec.broker == self._mirror else "source"
                counts[rec.centre_id][role] += 1

        parts = []
        for cid, c in sorted(counts.items()):
            short = cid.split("-")[0]
            src = c["source"]
            mir = c["mirror"]
            drop = max(0, src - mir)
            parts.append(f"{short}: src={src} mir={mir} drop={drop}")

        summary = "  ".join(parts) if parts else "waiting for messages…"
        line = f"\r[{bar}] {elapsed_s}/{elapsed_s[:-5]}{rem_s}  {summary}"
        # Simpler format:
        line = f"\r[{bar}] {elapsed_s} elapsed / {rem_s} remaining    {summary}"
        print(line + "   ", end="", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

@dataclass
class BucketResult:
    label: str
    start: float
    end: float
    is_partial: bool
    source_count: int
    mirror_count: int
    dropped_count: int
    mirror_only_count: int


def analyse(
    store: list[MsgRecord],
    mirror: str,
    centre_ids: list[str],
    source_brokers: dict[str, str],   # centre_id -> source broker host
    run_start: float,
    run_end: float,
    bucket_secs: int,
    grace_secs: int,
) -> dict[str, list[BucketResult]]:
    """Return {centre_id: [BucketResult, …]} for each centre-id."""

    results: dict[str, list[BucketResult]] = {}

    for cid in centre_ids:
        src_broker = source_brokers.get(cid)

        # All records for this centre-id
        src_recs = {r.id: r for r in store if r.centre_id == cid and r.broker != mirror}
        mir_ids  = {r.id for r in store if r.centre_id == cid and r.broker == mirror}

        buckets: list[BucketResult] = []
        t = run_start
        while t < run_end:
            b_end = min(t + bucket_secs, run_end)
            is_partial = (b_end - t) < bucket_secs * 0.95

            label = (
                datetime.fromtimestamp(t, tz=timezone.utc).strftime("%H:%M")
                + "–"
                + datetime.fromtimestamp(b_end, tz=timezone.utc).strftime("%H:%M")
            )

            # Source messages arriving in this bucket
            bucket_src = {
                mid: r for mid, r in src_recs.items()
                if t <= r.arrival < b_end
            }
            # Mirror messages for same ids (may arrive slightly later)
            bucket_mir_ids = {
                mid for mid, r in src_recs.items()
                if mid in mir_ids
            }
            # Mirror-only (arrived on mirror but not in source bucket)
            mir_in_bucket = sum(
                1 for r in store
                if r.centre_id == cid and r.broker == mirror and t <= r.arrival < b_end
            )

            dropped_ids = set(bucket_src) - bucket_mir_ids
            dropped = len(dropped_ids)

            buckets.append(BucketResult(
                label=label,
                start=t,
                end=b_end,
                is_partial=is_partial,
                source_count=len(bucket_src),
                mirror_count=len(bucket_src) - dropped,
                dropped_count=dropped,
                mirror_only_count=max(0, mir_in_bucket - (len(bucket_src) - dropped)),
            ))
            t += bucket_secs

        results[cid] = buckets

    return results


def source_broker_for(
    centre_id: str,
    store: list[MsgRecord],
    mirror: str,
) -> str | None:
    """Return the non-mirror broker that delivered the most messages for centre_id."""
    counts: dict[str, int] = defaultdict(int)
    for r in store:
        if r.centre_id == centre_id and r.broker != mirror:
            counts[r.broker] += 1
    return max(counts, key=lambda b: counts[b]) if counts else None


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _bar(drop_rate: float, width: int = CHART_WIDTH) -> str:
    """Horizontal bar: █ = dropped, ░ = delivered."""
    dropped_chars = round(drop_rate * width)
    return "█" * dropped_chars + "░" * (width - dropped_chars)


def print_chart(
    results: dict[str, list[BucketResult]],
    source_brokers: dict[str, str],
    mirror: str,
) -> None:
    print()
    for cid, buckets in sorted(results.items()):
        src = source_brokers.get(cid, "unknown")
        print(f"{'═' * 80}")
        print(f"  {cid}")
        print(f"  source: {src}  →  mirror: {mirror}")
        print(f"{'═' * 80}")

        if not buckets or all(b.source_count == 0 for b in buckets):
            print("  (no messages received for this centre-id)\n")
            continue

        print(f"  {'Bucket':<13}  {'Source':>7}  {'Mirror':>7}  {'Dropped':>8}  {'Drop%':>5}  Chart")
        print(f"  {'─'*13}  {'─'*7}  {'─'*7}  {'─'*8}  {'─'*5}  {'─'*CHART_WIDTH}")

        for b in buckets:
            partial_flag = " ⚠ partial" if b.is_partial else ""
            if b.source_count == 0:
                pct = 0.0
                bar = "░" * CHART_WIDTH
            else:
                pct = b.dropped_count / b.source_count
                bar = _bar(pct)
            print(
                f"  {b.label:<13}  {b.source_count:>7}  {b.mirror_count:>7}  "
                f"{b.dropped_count:>8}  {pct:>4.0%}  {bar}{partial_flag}"
            )

        total_src = sum(b.source_count for b in buckets)
        total_drop = sum(b.dropped_count for b in buckets)
        total_pct = total_drop / total_src if total_src else 0.0
        print(f"  {'─'*13}  {'─'*7}  {'─'*7}  {'─'*8}  {'─'*5}")
        print(f"  {'TOTAL':<13}  {total_src:>7}  {total_src - total_drop:>7}  {total_drop:>8}  {total_pct:>4.0%}")
        print()


def save_drops(
    store: list[MsgRecord],
    results: dict[str, list[BucketResult]],
    mirror: str,
    output_path: str,
) -> None:
    """Write one JSON record per dropped message to output_path."""
    mir_ids: set[str] = {r.id for r in store if r.broker == mirror}

    # Build a bucket-label lookup: for each source record, find its bucket label
    bucket_map: dict[tuple[str, float], str] = {}
    for cid, buckets in results.items():
        for b in buckets:
            bucket_map[(cid, b.start)] = b.label

    def find_bucket_label(cid: str, arrival: float) -> str:
        for (c, t_start), label in bucket_map.items():
            if c == cid:
                # find the bucket whose start <= arrival
                pass
        # simpler: find matching bucket
        for (c, t_start), label in bucket_map.items():
            if c != cid:
                continue
            # get end
            for b in results[cid]:
                if b.start <= arrival < b.end:
                    return b.label
        return "unknown"

    dropped_records = [
        r for r in store
        if r.broker != mirror and r.id not in mir_ids
    ]

    written = 0
    with open(output_path, "w") as f:
        for r in sorted(dropped_records, key=lambda x: x.arrival):
            rec = {
                "centre_id": r.centre_id,
                "id": r.id,
                "source_broker": r.broker,
                "pubtime": r.pubtime,
                "source_arrival_utc": datetime.fromtimestamp(r.arrival, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                "bucket": find_bucket_label(r.centre_id, r.arrival),
            }
            f.write(json.dumps(rec) + "\n")
            written += 1

    print(f"  Dropped message records written to: {output_path}  ({written} records)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION, metavar="MINUTES",
                        help=f"Total observation time in minutes (default: {DEFAULT_DURATION})")
    parser.add_argument("--bucket", type=int, default=DEFAULT_BUCKET, metavar="MINUTES",
                        help=f"Bucket width in minutes (default: {DEFAULT_BUCKET})")
    parser.add_argument("--grace", type=int, default=DEFAULT_GRACE, metavar="SECONDS",
                        help=f"Grace period in seconds to wait for late mirror arrivals (default: {DEFAULT_GRACE})")
    parser.add_argument("--mirror", default=DEFAULT_MIRROR, metavar="HOST",
                        help=f"Mirror broker hostname (default: {DEFAULT_MIRROR})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, metavar="PORT",
                        help=f"MQTT broker port (default: {DEFAULT_PORT})")
    parser.add_argument("--logfile", metavar="PATH",
                        help="Path to SCGRep log file (default: auto-detect via docker logs)")
    parser.add_argument("--output", metavar="PATH",
                        help="Path for dropped-message JSONL output (default: broker_drops_<timestamp>.jsonl)")
    parser.add_argument("--no-verify-tls", action="store_true",
                        help="Disable TLS certificate verification (insecure)")
    args = parser.parse_args()

    global _NO_VERIFY_TLS
    _NO_VERIFY_TLS = args.no_verify_tls
    if _NO_VERIFY_TLS:
        print("WARNING: TLS certificate verification is disabled.", file=sys.stderr)

    output_path = args.output or f"broker_drops_{datetime.now().strftime('%Y%m%dT%H%M%S')}.jsonl"
    duration_secs = args.duration * 60
    bucket_secs = args.bucket * 60

    # --- Discover SCGRep config ---
    print("Discovering SCGRep configuration…")
    try:
        config = discover_scgrep_config(logfile=args.logfile)
    except RuntimeError as exc:
        sys.exit(f"ERROR: {exc}")

    all_brokers = set(config.broker_subscriptions)
    centre_ids = sorted({cid for cids in config.broker_subscriptions.values() for cid in cids})

    if args.mirror not in all_brokers:
        sys.exit(
            f"ERROR: mirror broker '{args.mirror}' is not in SCGRep's broker list.\n"
            f"Known brokers: {', '.join(sorted(all_brokers))}\n"
            f"Use --mirror to specify a different mirror broker."
        )

    source_broker_hosts = all_brokers - {args.mirror}
    if not source_broker_hosts:
        sys.exit("ERROR: Only one broker found — need at least one source and one mirror.")

    print(f"  Subscriber-ID : {config.subscriber_id}")
    print(f"  Centre-IDs    : {', '.join(centre_ids)}")
    print(f"  Source brokers: {', '.join(sorted(source_broker_hosts))}")
    print(f"  Mirror broker : {args.mirror}")
    print(f"  Duration      : {args.duration} min  |  Bucket: {args.bucket} min  |  Grace: {args.grace}s")

    # Warn if last bucket will be partial
    leftover = (duration_secs % bucket_secs)
    if leftover > 0:
        partial_mins = leftover / 60
        full_buckets = duration_secs // bucket_secs
        print(
            f"  WARNING: {args.duration}-minute run with {args.bucket}-minute buckets → "
            f"{full_buckets} complete + 1 partial bucket ({partial_mins:.0f} min).  "
            f"Partial bucket marked ⚠ in output."
        )
    print()

    # --- Connect to all brokers ---
    store: list[MsgRecord] = []
    lock = threading.Lock()
    ready: dict[str, bool] = {}
    clients: list[mqtt.Client] = []

    for broker_host, centre_set in config.broker_subscriptions.items():
        subs = [
            f"replay/a/wis2/{cid}/{config.subscriber_id}/#"
            for cid in centre_set
        ]
        client = build_client(broker_host, args.port, subs, store, lock, ready)
        client.connect_async(broker_host, args.port, keepalive=60)
        client.loop_start()
        clients.append(client)

    # Wait for all connections
    print("Connecting to brokers…")
    deadline = time.time() + 20
    while not all(ready.get(h) for h in config.broker_subscriptions):
        if time.time() > deadline:
            missing = [h for h in config.broker_subscriptions if not ready.get(h)]
            print(f"  WARNING: timed out waiting for: {', '.join(missing)}", file=sys.stderr)
            break
        time.sleep(0.2)

    connected = [h for h in config.broker_subscriptions if ready.get(h)]
    print(f"  Connected to {len(connected)}/{len(config.broker_subscriptions)} brokers: {', '.join(sorted(connected))}")
    print()

    # --- Collect messages ---
    run_start = time.time()
    progress = ProgressPrinter(duration_secs, store, lock, args.mirror)
    progress.start()

    time.sleep(duration_secs)
    progress.stop()

    print(f"Collection complete.  Waiting {args.grace}s grace period for late mirror arrivals…",
          file=sys.stderr, flush=True)
    time.sleep(args.grace)
    run_end = time.time() - args.grace  # analysis window excludes grace period

    for client in clients:
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass

    # --- Identify source brokers per centre-id ---
    with lock:
        snapshot = list(store)

    source_brokers: dict[str, str] = {}
    for cid in centre_ids:
        src = source_broker_for(cid, snapshot, args.mirror)
        if src:
            source_brokers[cid] = src
        else:
            print(f"  WARNING: no source messages received for {cid}", file=sys.stderr)

    # --- Analyse ---
    results = analyse(
        snapshot, args.mirror, centre_ids, source_brokers,
        run_start, run_end, bucket_secs, args.grace,
    )

    # --- Output ---
    print_chart(results, source_brokers, args.mirror)
    save_drops(snapshot, results, args.mirror, output_path)


if __name__ == "__main__":
    main()
