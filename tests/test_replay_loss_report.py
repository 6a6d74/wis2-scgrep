import importlib.util
import pathlib
from datetime import datetime, timezone

# Load the standalone script (it lives in scripts/, not the package).
_PATH = pathlib.Path(__file__).resolve().parent.parent / "scripts" / "replay_loss_report.py"
_spec = importlib.util.spec_from_file_location("replay_loss_report", _PATH)
rlr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rlr)


def _baseline(topic, msg_id, pubtime):
    return (f"2026-08-16 12:00:00,000 INFO [scgrep.mqtt_client] Global Broker "
            f"message: topic={topic} id={msg_id} time={pubtime}\n")


CA = "ca-eccc-msc-global-replay"
KR = "kr-kma-global-replay"


def _replay(kind, topic, msg_id, pubtime, centre=CA):
    return (f"2026-08-16 12:05:00,000 INFO [scgrep.replay_tester] Replay message "
            f"({kind}): centre_id={centre} topic={topic} "
            f"id={msg_id} time={pubtime}\n")


def test_parse_pubtime_variants():
    assert rlr.parse_pubtime("2026-08-16T12:08:15Z") == datetime(
        2026, 8, 16, 12, 8, 15, tzinfo=timezone.utc)
    # fractional seconds and no timezone still parse to UTC
    assert rlr.parse_pubtime("2026-08-16T12:08:15.37Z").minute == 8
    assert rlr.parse_pubtime("2026-08-16T12:08:15").tzinfo == timezone.utc
    assert rlr.parse_pubtime("not-a-time") is None


def test_scan_counts_and_dedups_per_minute():
    topic = "cache/a/wis2/us-noaa-nws"
    lines = [
        _baseline(topic + "/data/x", "b1", "2026-08-16T12:08:10Z"),
        _baseline(topic + "/data/y", "b2", "2026-08-16T12:08:59Z"),
        _baseline(topic + "/data/y", "b2", "2026-08-16T12:08:59Z"),  # dup id -> once
        _baseline(topic + "/data/y", "b3", "2026-08-16T12:09:01Z"),  # next minute
        # replay: wildcard topic form; a repeat of the same id (e.g. relayed by
        # two brokers) collapses within its kind
        _replay("synchronous", topic + "/#", "r1", "2026-08-16T12:08:10Z"),
        _replay("asynchronous", topic + "/data/x", "r2", "2026-08-16T12:08:10Z"),
        _replay("asynchronous", topic + "/data/x", "r2", "2026-08-16T12:08:10Z"),
    ]
    baseline, replay = rlr.scan(lines, "us-noaa-nws")
    m08 = datetime(2026, 8, 16, 12, 8, tzinfo=timezone.utc)
    m09 = datetime(2026, 8, 16, 12, 9, tzinfo=timezone.utc)
    assert len(baseline[m08]) == 2  # b1, b2 (dup collapsed)
    assert len(baseline[m09]) == 1  # b3
    assert replay[CA]["synchronous"][m08] == {"r1"}
    assert replay[CA]["asynchronous"][m08] == {"r2"}  # dup collapsed


def test_sync_and_async_kept_apart_even_when_ids_match():
    # Shared ids must not merge the two kinds: each is its own count.
    topic = "cache/a/wis2/us-noaa-nws"
    lines = [
        _replay("synchronous", topic + "/#", "x", "2026-08-16T12:08:10Z"),
        _replay("asynchronous", topic + "/data/x", "x", "2026-08-16T12:08:10Z"),
    ]
    _, replay = rlr.scan(lines, "us-noaa-nws")
    m08 = datetime(2026, 8, 16, 12, 8, tzinfo=timezone.utc)
    assert replay[CA]["synchronous"][m08] == {"x"}
    assert replay[CA]["asynchronous"][m08] == {"x"}


def test_centres_kept_apart():
    topic = "cache/a/wis2/us-noaa-nws"
    lines = [
        _replay("synchronous", topic + "/#", "same", "2026-08-16T12:08:10Z", CA),
        _replay("synchronous", topic + "/#", "same", "2026-08-16T12:08:10Z", KR),
        _replay("synchronous", topic + "/#", "kr-only", "2026-08-16T12:08:20Z", KR),
    ]
    _, replay = rlr.scan(lines, "us-noaa-nws")
    m08 = datetime(2026, 8, 16, 12, 8, tzinfo=timezone.utc)
    assert replay[CA]["synchronous"][m08] == {"same"}
    assert replay[KR]["synchronous"][m08] == {"same", "kr-only"}


def test_resolve_centre():
    assert rlr.resolve_centre(None, {CA}) == (CA, None)
    assert rlr.resolve_centre(None, set()) == (None, None)
    assert rlr.resolve_centre(KR, {CA, KR}) == (KR, None)
    centre, error = rlr.resolve_centre(None, {CA, KR})
    assert centre is None and "--centre" in error and CA in error and KR in error
    centre, error = rlr.resolve_centre("xx", {CA})
    assert centre is None and "xx" in error


def test_source_filtering(tmp_path, capsys):
    log = tmp_path / "scgrep.log"
    topic = "cache/a/wis2/uk-metoffice"
    log.write_text(
        _baseline(topic + "/data/z", "b1", "2026-08-16T12:08:00Z")
        + _replay("synchronous", topic + "/#", "s1", "2026-08-16T12:08:00Z")
    )
    window = ["--since", "2026-08-16T12:08:00Z", "--until", "2026-08-16T12:08:30Z"]
    rlr.main(["-t", "uk-metoffice", "-s", "sync", *window, str(log)])
    out = capsys.readouterr().out
    assert "syncΔ" in out and "asyncΔ" not in out
    rlr.main(["-t", "uk-metoffice", "-s", "async", *window, str(log)])
    out = capsys.readouterr().out
    assert "asyncΔ" in out and " syncΔ" not in out


def test_topic_substring_isolation():
    lines = [
        _baseline("cache/a/wis2/us-noaa-nws/data/x", "b1", "2026-08-16T12:08:00Z"),
        _baseline("cache/a/wis2/uk-metoffice/data/x", "b2", "2026-08-16T12:08:00Z"),
    ]
    baseline, _ = rlr.scan(lines, "us-noaa-nws")
    m08 = datetime(2026, 8, 16, 12, 8, tzinfo=timezone.utc)
    assert baseline[m08] == {"b1"}  # uk-metoffice excluded


def test_build_rows_diff_and_skips_empty():
    m08 = datetime(2026, 8, 16, 12, 8, tzinfo=timezone.utc)
    m09 = datetime(2026, 8, 16, 12, 9, tzinfo=timezone.utc)
    m10 = datetime(2026, 8, 16, 12, 10, tzinfo=timezone.utc)
    m11 = datetime(2026, 8, 16, 12, 11, tzinfo=timezone.utc)
    baseline = {m08: {"a", "b", "c"}, m09: {"d"}, m11: set()}
    sync = {m08: {"a"}, m10: {"e", "f"}}
    async_ = {m08: {"p", "q"}}
    rows = rlr.build_rows(baseline, [sync, async_], m08, m11)
    # (minute, baseline, [sync, async]); m11 has no activity -> skipped
    assert rows == [
        (m08, 3, [1, 2]),
        (m09, 1, [0, 0]),
        (m10, 0, [2, 0]),
    ]


def test_window_bounds_default_anchors_to_latest_replay():
    class Args:
        since = None
        until = None
        minutes = 3
    m05 = datetime(2026, 8, 16, 12, 5, tzinfo=timezone.utc)
    m20 = datetime(2026, 8, 16, 12, 20, tzinfo=timezone.utc)
    m18 = datetime(2026, 8, 16, 12, 18, tzinfo=timezone.utc)
    baseline = {m20: {"late-baseline"}}
    replay = {m05: {"r1"}, m18: {"r2"}}
    since, until = rlr.window_bounds(Args(), baseline, replay)
    assert until == m18  # latest replay, not the later baseline
    assert since == datetime(2026, 8, 16, 12, 16, tzinfo=timezone.utc)  # 3-min window


def _period(start, end):
    return (f"2026-08-16 12:15:00,000 INFO [scgrep.test_cycle] Test period begins: "
            f"window {start} .. {end}\n")


def _result(centre, topic, protocol, baseline, fetched):
    return (f"2026-08-16 12:15:00,000 INFO [scgrep.test_cycle] Result: "
            f"centre_id={centre} topic={topic} protocol={protocol} "
            f"baseline={baseline} fetched={fetched} delay_ms=100 aborted=0 "
            f"invalid_format=0 invalid_numberMatched=0\n")


def test_scan_summary_pairs_window_with_results():
    topic = "cache/a/wis2/us-noaa-nws/#"
    lines = [
        _period("2026-08-16T12:08:52Z", "2026-08-16T12:09:52Z"),
        _result("ca-eccc", topic, "http", 420, 251),
        _result("ca-eccc", topic, "mqtt", 420, 240),
        _result("ca-eccc", "cache/a/wis2/uk-metoffice/#", "http", 6, 6),  # other topic
    ]
    records = rlr.scan_summary(lines, "us-noaa-nws")
    start = datetime(2026, 8, 16, 12, 8, 52, tzinfo=timezone.utc)
    end = datetime(2026, 8, 16, 12, 9, 52, tzinfo=timezone.utc)
    assert records[(start, end, "ca-eccc", topic)] == {
        "baseline": 420, "http": 251, "mqtt": 240}
    assert len(records) == 1  # uk-metoffice excluded by the topic filter


def test_build_summary_rows_aggregates_and_skips_empty():
    s1 = datetime(2026, 8, 16, 12, 8, 52, tzinfo=timezone.utc)
    e1 = datetime(2026, 8, 16, 12, 9, 52, tzinfo=timezone.utc)
    s2 = datetime(2026, 8, 16, 12, 9, 52, tzinfo=timezone.utc)
    e2 = datetime(2026, 8, 16, 12, 10, 52, tzinfo=timezone.utc)
    records = {
        # two matching series in the same window -> summed
        (s1, e1, "c", "a"): {"baseline": 400, "http": 251, "mqtt": 240},
        (s1, e1, "c", "b"): {"baseline": 20, "http": 0, "mqtt": 0},
        (s2, e2, "c", "a"): {"baseline": 0, "http": 0, "mqtt": 0},  # empty -> skipped
    }
    rows = rlr.build_summary_rows(records, s1, e2)
    assert rows == [(s1, e1, 251, 240, 420)]  # (start, end, http, mqtt, baseline)


def test_run_summary_end_to_end(tmp_path, capsys):
    log = tmp_path / "scgrep.log"
    topic = "cache/a/wis2/us-noaa-nws/#"
    log.write_text(
        _period("2026-08-16T12:08:52Z", "2026-08-16T12:09:52Z")
        + _result("ca-eccc", topic, "http", 420, 0)   # a genuine gap
        + _result("ca-eccc", topic, "mqtt", 420, 0)
    )
    rc = rlr.main(["-t", "us-noaa-nws", "-s", "summary",
                   "--since", "2026-08-16T12:00:00Z", "--until", "2026-08-16T12:30:00Z",
                   str(log)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Source: summary" in out
    assert "12:08:52–12:09:52" in out
    assert "+420" in out  # baseline 420 - http 0
    assert "http numberMatched" in out  # histogram legend


def test_main_end_to_end(tmp_path, capsys):
    log = tmp_path / "scgrep.log"
    topic = "cache/a/wis2/us-noaa-nws"
    log.write_text(
        _baseline(topic + "/data/x", "b1", "2026-08-16T12:08:10Z")
        + _baseline(topic + "/data/x", "b2", "2026-08-16T12:08:20Z")
        + _replay("synchronous", topic + "/#", "b1", "2026-08-16T12:08:10Z")
    )
    rc = rlr.main(["-t", "us-noaa-nws", "--since", "2026-08-16T12:08:00Z",
                   "--until", "2026-08-16T12:08:30Z", str(log)])
    out = capsys.readouterr().out
    assert rc == 0
    assert f"Centre: {CA}" in out
    assert "12:08–12:09" in out
    assert "baseline − sync replay" in out
    assert "baseline − async replay" in out
    assert "+1" in out  # baseline 2 - sync 1
    assert "+2" in out  # baseline 2 - async 0


def test_main_with_two_centres_requires_choice(tmp_path, capsys):
    log = tmp_path / "scgrep.log"
    topic = "cache/a/wis2/us-noaa-nws"
    log.write_text(
        _baseline(topic + "/data/x", "b1", "2026-08-16T12:08:10Z")
        + _baseline(topic + "/data/x", "b2", "2026-08-16T12:08:20Z")
        # fresh ids per replay service: pooled, these would read as 3 replays
        + _replay("synchronous", topic + "/#", "u1", "2026-08-16T12:08:10Z", CA)
        + _replay("synchronous", topic + "/#", "u2", "2026-08-16T12:08:10Z", KR)
        + _replay("synchronous", topic + "/#", "u3", "2026-08-16T12:08:20Z", KR)
    )
    window = ["--since", "2026-08-16T12:08:00Z", "--until", "2026-08-16T12:08:30Z"]
    assert rlr.main(["-t", "us-noaa-nws", "-s", "sync", *window, str(log)]) == 1
    assert "--centre" in capsys.readouterr().err

    assert rlr.main(["-t", "us-noaa-nws", "-s", "sync", "-c", CA, *window,
                     str(log)]) == 0
    out = capsys.readouterr().out
    assert "TOTAL" in out and "+1" in out  # CA: 2 - 1

    assert rlr.main(["-t", "us-noaa-nws", "-s", "sync", "-c", KR, *window,
                     str(log)]) == 0
    out = capsys.readouterr().out
    total = next(line for line in out.splitlines() if line.startswith("TOTAL"))
    assert total.split() == ["TOTAL", "2", "2", "+0"]  # KR: 2 - 2


def test_run_summary_filters_by_centre(tmp_path, capsys):
    log = tmp_path / "scgrep.log"
    topic = "cache/a/wis2/us-noaa-nws/#"
    log.write_text(
        _period("2026-08-16T12:08:52Z", "2026-08-16T12:09:52Z")
        + _result(CA, topic, "http", 420, 400)
        + _result(KR, topic, "http", 420, 100)
    )
    window = ["--since", "2026-08-16T12:00:00Z", "--until", "2026-08-16T12:30:00Z"]
    assert rlr.main(["-t", "us-noaa-nws", "-s", "summary", *window, str(log)]) == 1
    capsys.readouterr()
    assert rlr.main(["-t", "us-noaa-nws", "-s", "summary", "-c", KR, *window,
                     str(log)]) == 0
    out = capsys.readouterr().out
    assert "+320" in out and "+20" not in out
