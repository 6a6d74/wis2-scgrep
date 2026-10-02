#!/usr/bin/env python3
"""Check whether GRep assigns a fresh message-id on every async MQTT replay.

HOW IT WORKS
------------
Two separate subscriber UUIDs are generated.  The script subscribes to both
replay channels on a single MQTT connection, sends one POST request per
subscriber-id to the GRep async endpoint (same topic, same time window), waits
for delivery, then compares the two sets of received messages.

Messages are matched on data_id + pubtime — the canonical WIS2 fingerprint for
a unique resource.  For each matched pair the script checks whether the ``id``
fields differ.

WHAT A HEALTHY SERVICE LOOKS LIKE (pre-2026-09-30 restart)
-----------------------------------------------------------
The id is the same across both replays.  GRep stored the original notification
id and reused it on async delivery.

WHAT THE CURRENT SERVICE DOES
------------------------------
GRep inserts a freshly generated UUID as the message id on every async replay
delivery.  Matched fingerprints will therefore have two completely different ids.
This is observable directly: every matched pair reports ``ids: DIFFERENT``.

REQUIREMENTS
------------
  paho-mqtt >= 2.0  (pip install paho-mqtt)
  or use the project virtualenv: .venv/bin/python scripts/grep_async_id_check.py

EXAMPLES
--------
  python grep_async_id_check.py \\
      --topic cache/a/wis2/us-noaa-nws \\
      --datetime 2026-10-02T10:30:00Z/2026-10-02T10:31:00Z

  python grep_async_id_check.py \\
      --url https://wis2-grep.example.org \\
      --topic cache/a/wis2/int-eumetsat \\
      --datetime 2026-10-02T09:00:00Z/2026-10-02T09:01:00Z \\
      --wait 60
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import time
import uuid
import urllib.error
import urllib.request

try:
    import paho.mqtt.client as mqtt
    from paho.mqtt.client import CallbackAPIVersion
except ImportError:
    sys.exit(
        "paho-mqtt is required.  Install it with:\n"
        "  pip install paho-mqtt\n"
        "or use the project virtualenv:\n"
        "  .venv/bin/python scripts/grep_async_id_check.py ..."
    )

DEFAULT_URL = "https://wis2-grep.weather.gc.ca"
DEFAULT_BROKER = "wis2-grep.weather.gc.ca"
DEFAULT_PORT = 8883
CENTRE_ID = "ca-eccc-msc-global-replay"
COLLECTION = "wis2-notification-messages"
DEFAULT_WAIT = 30


# ---------------------------------------------------------------------------
# TLS
# ---------------------------------------------------------------------------

try:
    import certifi as _certifi
    _SSL_CTX = ssl.create_default_context(cafile=_certifi.where())
except ImportError:
    _certifi = None
    _SSL_CTX = ssl.create_default_context()

_NO_VERIFY_TLS = False


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def submit_replay(base_url: str, subscriber_id: str, topic: str, datetime_range: str) -> None:
    ctx = ssl._create_unverified_context() if _NO_VERIFY_TLS else _SSL_CTX
    payload = json.dumps({
        "inputs": {
            "datetime": datetime_range,
            "collection": COLLECTION,
            "subscriber-id": subscriber_id,
            "topic": topic,
        }
    }).encode()
    req = urllib.request.Request(
        f"{base_url}/processes/wis2-grep-subscriber/execution",
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            resp.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        sys.exit(f"POST failed: HTTP {exc.code}: {body}")
    except ssl.SSLCertVerificationError:
        sys.exit(
            "TLS certificate verification failed.\n"
            "Install certifi (pip install certifi) or pass --no-verify-tls."
        )
    except Exception as exc:
        sys.exit(f"POST failed: {exc}")


# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------

def collect_messages(
    broker: str,
    port: int,
    sub_id_a: str,
    sub_id_b: str,
    base_url: str,
    topic: str,
    datetime_range: str,
    wait: int,
    no_verify_tls: bool,
) -> tuple[list[dict], list[dict]]:
    """Connect, send both replay requests, collect messages, return (msgs_a, msgs_b)."""

    channel_a = f"replay/a/wis2/{CENTRE_ID}/{sub_id_a}/#"
    channel_b = f"replay/a/wis2/{CENTRE_ID}/{sub_id_b}/#"

    msgs_a: list[dict] = []
    msgs_b: list[dict] = []
    connected = [False]

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code != 0:
            sys.exit(f"MQTT connect failed: {reason_code}")
        client.subscribe([(channel_a, 0), (channel_b, 0)])
        connected[0] = True

    def on_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except (ValueError, TypeError):
            return
        mid = data.get("id", "")
        props = data.get("properties") or {}
        data_id = props.get("data_id", "")
        pubtime = props.get("pubtime", "")
        if not mid or not data_id:
            return
        entry = {"id": mid, "data_id": data_id, "pubtime": pubtime}
        if msg.topic.startswith(f"replay/a/wis2/{CENTRE_ID}/{sub_id_a}/"):
            msgs_a.append(entry)
        else:
            msgs_b.append(entry)

    connect_error: list[str] = []

    def on_connect_fail(client, userdata):
        connect_error.append("TLS handshake or TCP connection failed.")

    client = mqtt.Client(
        CallbackAPIVersion.VERSION2,
        client_id=f"grep-id-check-{sub_id_a[:8]}",
        clean_session=True,
    )
    client.username_pw_set("everyone", "everyone")
    if no_verify_tls:
        client.tls_set(cert_reqs=ssl.CERT_NONE)
        client.tls_insecure_set(True)
    elif _certifi:
        client.tls_set(ca_certs=_certifi.where())
    else:
        client.tls_set()
    client.on_connect = on_connect
    client.on_connect_fail = on_connect_fail
    client.on_message = on_message

    client.connect_async(broker, port, keepalive=60)
    client.loop_start()

    deadline = time.time() + 15
    while not connected[0]:
        if connect_error:
            client.loop_stop()
            sys.exit(
                f"MQTT connection failed: {connect_error[0]}\n"
                "Install certifi (pip install certifi) or pass --no-verify-tls."
            )
        if time.time() > deadline:
            client.loop_stop()
            sys.exit(
                "Timed out waiting for MQTT connection.\n"
                "Check broker hostname, port, and TLS certificates.\n"
                "If running on macOS without certifi: pip install certifi"
            )
        time.sleep(0.1)
    print(f"  Connected.  Subscribed to both replay channels.")

    print(f"  Sending replay 1 (subscriber {sub_id_a[:8]}…)")
    submit_replay(base_url, sub_id_a, topic, datetime_range)
    print(f"  Waiting {wait}s for delivery…")
    time.sleep(wait)
    print(f"  Replay 1 done: {len(msgs_a)} messages received so far.")

    print(f"  Sending replay 2 (subscriber {sub_id_b[:8]}…)")
    submit_replay(base_url, sub_id_b, topic, datetime_range)
    print(f"  Waiting {wait}s for delivery…")
    time.sleep(wait)
    print(f"  Replay 2 done: {len(msgs_b)} messages received so far.")

    client.loop_stop()
    client.disconnect()
    return msgs_a, msgs_b


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def analyse(msgs_a: list[dict], msgs_b: list[dict]) -> bool:
    print(f"  Replay 1 total: {len(msgs_a)} messages")
    print(f"  Replay 2 total: {len(msgs_b)} messages")
    print()

    index_a: dict[tuple, str] = {(m["data_id"], m["pubtime"]): m["id"] for m in msgs_a}
    index_b: dict[tuple, str] = {(m["data_id"], m["pubtime"]): m["id"] for m in msgs_b}

    matched = sorted(set(index_a) & set(index_b))
    only_a  = set(index_a) - set(index_b)
    only_b  = set(index_b) - set(index_a)

    diff_id = sum(1 for fp in matched if index_a[fp] != index_b[fp])
    same_id = sum(1 for fp in matched if index_a[fp] == index_b[fp])

    print(f"  Matched on data_id + pubtime : {len(matched)}")
    print(f"    ids differ (fresh per replay): {diff_id}")
    print(f"    ids same   (reused):           {same_id}")
    print(f"  Only in replay 1 (MQTT loss?) : {len(only_a)}")
    print(f"  Only in replay 2 (MQTT loss?) : {len(only_b)}")
    print()

    if matched:
        print("  Sample matched pairs (up to 5):")
        for fp in matched[:5]:
            data_id, pubtime = fp
            short = data_id.split("/")[-1] if "/" in data_id else data_id
            id_a = index_a[fp]
            id_b = index_b[fp]
            marker = "DIFFERENT" if id_a != id_b else "SAME"
            print(f"    {short}  ({pubtime})")
            print(f"      replay-1 id: {id_a}")
            print(f"      replay-2 id: {id_b}")
            print(f"      ids: {marker}")
        print()

    if not matched:
        print("RESULT: No matched fingerprints — cannot determine.  "
              "Try a larger --wait or a window with more messages.")
        return False
    if diff_id > 0 and same_id == 0:
        print("RESULT: GRep assigns a fresh message-id on every async replay.")
        return True
    if same_id > 0 and diff_id == 0:
        print("RESULT: GRep reuses the same message-id across replays.")
        return True
    print(f"RESULT: Mixed — {diff_id} fresh id(s), {same_id} reused id(s).")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--url", default=DEFAULT_URL, metavar="URL",
                        help=f"GRep HTTP base URL (default: {DEFAULT_URL})")
    parser.add_argument("--broker", default=DEFAULT_BROKER, metavar="HOST",
                        help=f"MQTT broker hostname (default: {DEFAULT_BROKER})")
    parser.add_argument("--port", default=DEFAULT_PORT, type=int, metavar="PORT",
                        help=f"MQTT broker port (default: {DEFAULT_PORT})")
    parser.add_argument("--topic", required=True, metavar="TOPIC",
                        help="Topic prefix, no trailing /#  (e.g. cache/a/wis2/us-noaa-nws)")
    parser.add_argument("--datetime", required=True, metavar="START/END",
                        help="ISO-8601 interval  (e.g. 2026-10-02T10:30:00Z/2026-10-02T10:31:00Z)")
    parser.add_argument("--wait", default=DEFAULT_WAIT, type=int, metavar="SECONDS",
                        help=f"Seconds to wait for each replay delivery (default: {DEFAULT_WAIT})")
    parser.add_argument("--no-verify-tls", action="store_true",
                        help="Disable TLS certificate verification (insecure; use only for testing)")
    args = parser.parse_args()

    global _NO_VERIFY_TLS
    _NO_VERIFY_TLS = args.no_verify_tls
    if _NO_VERIFY_TLS:
        print("WARNING: TLS certificate verification is disabled.", file=sys.stderr)

    sub_id_a = str(uuid.uuid4())
    sub_id_b = str(uuid.uuid4())

    print(f"Endpoint  : {args.url}")
    print(f"Broker    : {args.broker}:{args.port}")
    print(f"Topic     : {args.topic}")
    print(f"Window    : {args.datetime}")
    print(f"Sub-ID A  : {sub_id_a}")
    print(f"Sub-ID B  : {sub_id_b}")
    print(f"Wait      : {args.wait}s per replay")
    print()

    print("Connecting to MQTT broker…")
    msgs_a, msgs_b = collect_messages(
        args.broker, args.port,
        sub_id_a, sub_id_b,
        args.url, args.topic, args.datetime,
        args.wait, args.no_verify_tls,
    )

    print()
    print("Results")
    print("-------")
    analyse(msgs_a, msgs_b)


if __name__ == "__main__":
    main()
