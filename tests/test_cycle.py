import math
import threading
import time

import fakeredis
import responses

import scgrep.test_cycle as tc
from scgrep.config import Config
from scgrep.metrics import Metrics
from scgrep.redis_store import RedisStore
from scgrep.replay_registry import ReplayRegistry
from scgrep.replay_tester import FetchResult
from scgrep.test_cycle import run_cycle
from scgrep.util import epoch_to_iso, parse_time_to_epoch

REPLAY_URL = "https://replay.example.org"
ITEMS_URL = f"{REPLAY_URL}/collections/wis2-monitoring-event-messages/items"  # TOPIC is monitor/
EXEC_URL = f"{REPLAY_URL}/processes/wis2-grep-subscriber/execution"
TOPIC = "monitor/a/wis2/ca-eccc-msc"
CENTRE = "ca-eccc-msc-global-replay"

ENV = {
    "SENSOR_CENTRE_ID": "io-wis2dev-test-sensor-centre",
    "SUBSCRIPTION_TOPICS": TOPIC,
    "GLOBAL_REPLAY_CENTRE_IDS": CENTRE,
    "GLOBAL_REPLAY_URLS": REPLAY_URL,
    "GLOBAL_BROKER_URLS": "mqtts://everyone:everyone@globalbroker.meteo.fr:8883",
    "TIME_LAG": "10",
    "TEST_INTERVAL": "2",
}


def _gauge_value(metrics, gauge, **labels):
    return gauge.labels(**labels)._value.get()


@responses.activate
def test_run_cycle_publishes_all_metrics():
    cfg = Config.from_env(dict(ENV))
    client = fakeredis.FakeRedis(decode_responses=True)
    store = RedisStore("redis:6379", cfg.redis_expiry, cfg.subscription_topics, client=client)
    registry = ReplayRegistry()
    metrics = Metrics()

    # Anchor "now" at real wall-clock so messages stay within the expiry window.
    # Window is (now-10-2 .. now-10) = [now-12, now-10]; seed two messages there.
    now = time.time()
    in_window = epoch_to_iso(now - 11)
    store.store_message("a", in_window, TOPIC)
    store.store_message("b", in_window, TOPIC)

    channel = f"replay/a/wis2/{CENTRE}/{cfg.subscriber_id_for(CENTRE)}/{TOPIC}"
    responses.add(
        responses.GET, ITEMS_URL,
        json={
            "type": "FeatureCollection",
            "numberMatched": 7,
            "features": [
                {"type": "Feature", "id": f"h{i}",
                 "properties": {"pubtime": in_window}}
                for i in range(7)
            ],
            "links": [],
        },
        status=200,
    )
    responses.add(
        responses.POST, EXEC_URL,
        json={
            "subscriptions": [
                {
                    "rel": "items",
                    "type": "application/json",
                    "href": "mqtts://everyone:everyone@globalbroker.meteo.fr:8883",
                    "title": "Meteo France",
                    "channel": channel,
                }
            ]
        },
        status=200,
    )

    def deliver():
        # Simulate replay messages arriving after the process is triggered.
        # Each hits the registry (timing) and Redis (deduplicated count); "r1" is
        # delivered twice (as if by a second broker) and must be counted once.
        time.sleep(0.1)
        for mid in ("r1", "r2", "r3", "r1"):
            registry.handle_replay(channel)
            store.store_replay_message(CENTRE, mid, in_window, TOPIC)

    threading.Thread(target=deliver, daemon=True).start()

    run_cycle(cfg, store, registry, metrics, now=now)

    report_by = cfg.sensor_centre_id
    # Baseline count.
    assert _gauge_value(metrics, metrics.messages_received, report_by=report_by, topic=TOPIC) == 2

    http_labels = dict(report_by=report_by, centre_id=CENTRE, topic=TOPIC, protocol="http")
    assert _gauge_value(metrics, metrics.messages_fetched, **http_labels) == 7
    assert _gauge_value(metrics, metrics.test_aborted, **http_labels) == 0
    assert _gauge_value(metrics, metrics.response_invalid_format, **http_labels) == 0

    mqtt_labels = dict(report_by=report_by, centre_id=CENTRE, topic=TOPIC, protocol="mqtt")
    # 3 distinct replay ids despite "r1" being delivered twice (deduplicated).
    assert _gauge_value(metrics, metrics.messages_fetched, **mqtt_labels) == 3
    assert _gauge_value(metrics, metrics.test_aborted, **mqtt_labels) == 0
    assert _gauge_value(metrics, metrics.response_invalid_format, **mqtt_labels) == 0


@responses.activate
def test_run_cycle_replay_gap_does_not_abort_mqtt():
    """A genuine replay gap: baseline > 0 but the replay has numberMatched == 0
    and delivers nothing. The mqtt test must NOT abort (numberMatched gates the
    wait); it reports 0 fetched and the HTTP response delay, while the baseline
    vs http/mqtt difference surfaces the gap."""
    cfg = Config.from_env(dict(ENV))
    client = fakeredis.FakeRedis(decode_responses=True)
    store = RedisStore("redis:6379", cfg.redis_expiry, cfg.subscription_topics, client=client)
    registry = ReplayRegistry()
    metrics = Metrics()

    now = time.time()
    in_window = epoch_to_iso(now - 11)
    store.store_message("a", in_window, TOPIC)
    store.store_message("b", in_window, TOPIC)  # baseline = 2

    channel = f"replay/a/wis2/{CENTRE}/{cfg.subscriber_id_for(CENTRE)}/{TOPIC}"
    responses.add(
        responses.GET, ITEMS_URL,
        json={"type": "FeatureCollection", "numberMatched": 0, "features": [], "links": []},
        status=200,
    )
    responses.add(
        responses.POST, EXEC_URL,
        json={"subscriptions": [{
            "rel": "items", "type": "application/json",
            "href": "mqtts://everyone:everyone@globalbroker.meteo.fr:8883",
            "title": "Meteo France", "channel": channel,
        }]},
        status=200,
    )

    # No replay messages are delivered at all.
    run_cycle(cfg, store, registry, metrics, now=now)

    report_by = cfg.sensor_centre_id
    assert _gauge_value(metrics, metrics.messages_received, report_by=report_by, topic=TOPIC) == 2

    mqtt_labels = dict(report_by=report_by, centre_id=CENTRE, topic=TOPIC, protocol="mqtt")
    assert _gauge_value(metrics, metrics.messages_fetched, **mqtt_labels) == 0
    # The key assertion: no abort, despite a non-zero baseline and no messages.
    assert _gauge_value(metrics, metrics.test_aborted, **mqtt_labels) == 0
    # HTTP response delay, not the 95% abort value (0.95 * 2s = 1900 ms).
    assert _gauge_value(metrics, metrics.fetch_delay, **mqtt_labels) < 1900

    http_labels = dict(report_by=report_by, centre_id=CENTRE, topic=TOPIC, protocol="http")
    assert _gauge_value(metrics, metrics.messages_fetched, **http_labels) == 0


def test_all_metrics_published_together_after_fetches(monkeypatch):
    """Baseline and http metrics are held until the (slow) async fetch finishes,
    so every metric for the cycle updates at the same time."""
    cfg = Config.from_env(dict(ENV))
    client = fakeredis.FakeRedis(decode_responses=True)
    store = RedisStore("redis:6379", cfg.redis_expiry, cfg.subscription_topics, client=client)
    registry = ReplayRegistry()
    metrics = Metrics()

    now = time.time()
    store.store_message("a", epoch_to_iso(now - 11), TOPIC)  # in-window baseline

    release = threading.Event()

    def fake_sync(*a, **k):
        # The real sync_fetch records every paged feature; run_cycle re-counts
        # from Redis, so the fake must do the same.
        for i in range(5):
            store.store_sync_message(CENTRE, TOPIC, f"s{i}", epoch_to_iso(now - 11))
        return FetchResult("http", False, False, 1.0, 5, number_matched=5)

    def fake_async(*a, **k):
        release.wait(3)  # simulate the async fetch running to ~95% of the period
        # Store two replay messages; run_cycle recomputes the mqtt count from
        # Redis (deduplicated), so the FetchResult count below is a placeholder.
        store.store_replay_message(CENTRE, "r1", epoch_to_iso(now - 11), TOPIC)
        store.store_replay_message(CENTRE, "r2", epoch_to_iso(now - 11), TOPIC)
        return FetchResult("mqtt", False, False, 2.0, 0)

    monkeypatch.setattr(tc, "sync_fetch", fake_sync)
    monkeypatch.setattr(tc, "async_fetch", fake_async)

    thread = threading.Thread(target=run_cycle, args=(cfg, store, registry, metrics), kwargs={"now": now})
    thread.start()

    # While the async fetch is still running, nothing should be published yet —
    # including the (fast) baseline and http results.
    time.sleep(0.3)
    assert _gauge_value(metrics, metrics.messages_received, report_by=cfg.sensor_centre_id, topic=TOPIC) == 0
    http_labels = dict(report_by=cfg.sensor_centre_id, centre_id=CENTRE, topic=TOPIC, protocol="http")
    assert _gauge_value(metrics, metrics.messages_fetched, **http_labels) == 0

    release.set()
    thread.join(timeout=3)

    # Now everything is published together.
    assert _gauge_value(metrics, metrics.messages_received, report_by=cfg.sensor_centre_id, topic=TOPIC) == 1
    assert _gauge_value(metrics, metrics.messages_fetched, **http_labels) == 5
    mqtt_labels = dict(report_by=cfg.sensor_centre_id, centre_id=CENTRE, topic=TOPIC, protocol="mqtt")
    assert _gauge_value(metrics, metrics.messages_fetched, **mqtt_labels) == 2


@responses.activate
def test_http_count_excludes_the_boundary_second():
    """`numberMatched` is computed on closed [start, end] semantics, so it
    includes the boundary second — which the next contiguous window counts again.
    messages_fetched must instead be SCGRep's half-open [start, end) re-count, so
    it agrees with the baseline and the mqtt count."""
    cfg = Config.from_env(dict(ENV))
    client = fakeredis.FakeRedis(decode_responses=True)
    store = RedisStore("redis:6379", cfg.redis_expiry, cfg.subscription_topics, client=client)
    metrics = Metrics()

    now = float(int(time.time()))
    end_epoch = math.floor(now - cfg.time_lag)          # window upper bound
    in_window = epoch_to_iso(end_epoch - 1)             # inside
    boundary = epoch_to_iso(end_epoch)                  # exactly on the bound

    store.store_message("b1", in_window, TOPIC)         # baseline = 1 (half-open)
    store.store_message("b2", boundary, TOPIC)          # excluded by [start, end)

    # The service returns both, and counts both in numberMatched (closed).
    responses.add(
        responses.GET, ITEMS_URL,
        json={"type": "FeatureCollection", "numberMatched": 2, "links": [],
              "features": [
                  {"type": "Feature", "id": "b1", "properties": {"pubtime": in_window}},
                  {"type": "Feature", "id": "b2", "properties": {"pubtime": boundary}},
              ]},
        status=200,
    )
    responses.add(responses.POST, EXEC_URL, json={"subscriptions": []}, status=200)

    run_cycle(cfg, store, ReplayRegistry(), metrics, now=now)

    report_by = cfg.sensor_centre_id
    http_labels = dict(report_by=report_by, centre_id=CENTRE, topic=TOPIC, protocol="http")
    # Baseline and http agree: both exclude the boundary second.
    assert _gauge_value(metrics, metrics.messages_received, report_by=report_by, topic=TOPIC) == 1
    assert _gauge_value(metrics, metrics.messages_fetched, **http_labels) == 1
    # numberMatched (2) still drove the served-vs-claimed check, and matched the
    # 2 features actually paged — so no mismatch is flagged.
    assert _gauge_value(metrics, metrics.response_invalid_number_matched, **http_labels) == 0


def test_window_is_floored_so_baseline_and_replay_query_match(monkeypatch):
    """The baseline window and the ISO interval sent to the Global Replay must
    cover exactly the same interval.

    epoch_to_iso() truncates to whole seconds, so an unfloored window would count
    the baseline over sub-second bounds (e.g. 20:01:37.779) while querying the
    replay for the truncated interval (20:01:37) — misattributing bursts of
    messages sharing a pub-time instant to the adjacent window.
    """
    cfg = Config.from_env(dict(ENV, TEST_INTERVAL="1"))
    client = fakeredis.FakeRedis(decode_responses=True)
    store = RedisStore("redis:6379", cfg.redis_expiry, cfg.subscription_topics, client=client)
    metrics = Metrics()

    seen = {}

    def fake_sync(replay_url, centre_id, topic, start_iso, end_iso, *a, **k):
        seen["iso"] = (start_iso, end_iso)
        return FetchResult("http", False, False, 1.0, 0)

    # Spy on the epochs actually used to count the baseline.
    real_count = store.count_messages

    def spy_count(pattern, start_epoch, end_epoch):
        seen["epochs"] = (start_epoch, end_epoch)
        return real_count(pattern, start_epoch, end_epoch)

    monkeypatch.setattr(store, "count_messages", spy_count)
    monkeypatch.setattr(tc, "sync_fetch", fake_sync)
    monkeypatch.setattr(tc, "async_fetch", lambda *a, **k: FetchResult("mqtt", False, False, 2.0, 0))

    # A deliberately "ugly" now with a large sub-second fraction.
    now = float(int(time.time())) + 0.779
    run_cycle(cfg, store, ReplayRegistry(), metrics, now=now)

    start_iso, end_iso = seen["iso"]
    start_epoch, end_epoch = seen["epochs"]
    # The window used for the baseline must be EXACTLY the interval sent to the
    # replay — no sub-second drift between the two sides.
    assert (start_epoch, end_epoch) == (
        parse_time_to_epoch(start_iso), parse_time_to_epoch(end_iso)
    )
    assert end_epoch == math.floor(now - cfg.time_lag)      # floored, not now-lag
    assert end_epoch - start_epoch == cfg.test_interval


def test_message_counters_accumulate_across_cycles(monkeypatch):
    """messages_received / messages_fetched are cumulative counters: each cycle
    increments them by that cycle's count rather than overwriting it."""
    cfg = Config.from_env(dict(ENV, TEST_INTERVAL="1"))
    client = fakeredis.FakeRedis(decode_responses=True)
    store = RedisStore("redis:6379", cfg.redis_expiry, cfg.subscription_topics, client=client)
    registry = ReplayRegistry()
    metrics = Metrics()

    now = float(int(time.time()))  # window (now-11 .. now-10); whole second (see above)
    store.store_message("a", epoch_to_iso(now - 10.5), TOPIC)  # baseline 1 per cycle

    def fake_sync(*a, **k):
        for i in range(5):
            store.store_sync_message(CENTRE, TOPIC, f"s{i}", epoch_to_iso(now - 10.5))
        return FetchResult("http", False, False, 1.0, 5, number_matched=5)

    monkeypatch.setattr(tc, "sync_fetch", fake_sync)
    monkeypatch.setattr(tc, "async_fetch", lambda *a, **k: FetchResult("mqtt", False, False, 2.0, 0))

    run_cycle(cfg, store, registry, metrics, now=now)
    run_cycle(cfg, store, registry, metrics, now=now)

    report_by = cfg.sensor_centre_id
    # Two cycles, so the counters hold the sum, not the last cycle's value.
    assert _gauge_value(metrics, metrics.messages_received, report_by=report_by, topic=TOPIC) == 2
    http_labels = dict(report_by=report_by, centre_id=CENTRE, topic=TOPIC, protocol="http")
    assert _gauge_value(metrics, metrics.messages_fetched, **http_labels) == 10


def test_publication_held_until_95pct_even_when_fetches_finish_early(monkeypatch):
    """Metrics publish at ~95% of the test period even if the fetches return
    instantly — publication is anchored to the cycle deadline, not fetch timing."""
    cfg = Config.from_env(dict(ENV, TEST_INTERVAL="1"))  # deadline = 0.95s
    client = fakeredis.FakeRedis(decode_responses=True)
    store = RedisStore("redis:6379", cfg.redis_expiry, cfg.subscription_topics, client=client)
    registry = ReplayRegistry()
    metrics = Metrics()

    # Anchor now to a whole second: the window is only 1s wide (TEST_INTERVAL=1)
    # and epoch_to_iso truncates to seconds, so this keeps the seeded replay
    # messages deterministically inside (now-11 .. now-10).
    now = float(int(time.time()))
    def fake_sync(*a, **k):
        for i in range(5):
            store.store_sync_message(CENTRE, TOPIC, f"s{i}", epoch_to_iso(now - 10.5))
        return FetchResult("http", False, False, 1.0, 5, number_matched=5)

    monkeypatch.setattr(tc, "sync_fetch", fake_sync)

    def fake_async(*a, **k):
        # The mqtt count is always recomputed from Redis (deduplicated), so store
        # the replay messages there rather than relying on the FetchResult count.
        store.store_replay_message(CENTRE, "r1", epoch_to_iso(now - 10.5), TOPIC)
        store.store_replay_message(CENTRE, "r2", epoch_to_iso(now - 10.5), TOPIC)
        return FetchResult("mqtt", False, False, 2.0, 0)

    monkeypatch.setattr(tc, "async_fetch", fake_async)

    t0 = time.monotonic()
    run_cycle(cfg, store, registry, metrics, now=now)
    elapsed = time.monotonic() - t0

    # Held until ~95% of the 1s test period despite instant fetches.
    assert elapsed >= 0.9, elapsed
    http_labels = dict(report_by=cfg.sensor_centre_id, centre_id=CENTRE, topic=TOPIC, protocol="http")
    mqtt_labels = dict(report_by=cfg.sensor_centre_id, centre_id=CENTRE, topic=TOPIC, protocol="mqtt")
    assert _gauge_value(metrics, metrics.messages_fetched, **http_labels) == 5
    assert _gauge_value(metrics, metrics.messages_fetched, **mqtt_labels) == 2
