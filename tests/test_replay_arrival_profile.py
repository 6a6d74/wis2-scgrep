import importlib.util
import pathlib

# Load the standalone script (it lives in scripts/, not the package).
_PATH = pathlib.Path(__file__).resolve().parent.parent / "scripts" / "replay_arrival_profile.py"
_spec = importlib.util.spec_from_file_location("replay_arrival_profile", _PATH)
rap = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rap)

CA = "ca-eccc-msc-global-replay"
KR = "kr-kma-global-replay"
TOPIC = "cache/a/wis2/us-noaa-nws/#"


def _begin(hms):
    return (f"2026-08-16 {hms},000 INFO [scgrep.test_cycle] Test period begins: "
            f"window 2026-08-16T12:00:00Z .. 2026-08-16T12:01:00Z\n")


def _arrival(hms, centre, msg_id):
    return (f"2026-08-16 {hms},000 INFO [scgrep.mqtt_client] Replay message "
            f"(asynchronous): centre_id={centre} topic={TOPIC} id={msg_id} "
            f"time=2026-08-16T12:00:30Z\n")


def _result(hms, centre, fetched):
    return (f"2026-08-16 {hms},000 INFO [scgrep.test_cycle] Result: "
            f"centre_id={centre} topic={TOPIC} protocol=mqtt baseline=5 "
            f"fetched={fetched} delay_ms=100 aborted=0\n")


def _log(tmp_path):
    log = tmp_path / "scgrep.log"
    log.write_text(
        _begin("12:05:00")
        + _arrival("12:05:01", CA, "a")
        + _arrival("12:05:02", KR, "b")
        + _arrival("12:05:03", KR, "c")
        + _result("12:05:57", CA, 1)
        + _result("12:05:57", KR, 2)
    )
    return str(log)


def test_scan_keeps_centres_apart(tmp_path):
    _starts, arrivals, results = rap.scan([_log(tmp_path)], "2026-08-16", "us-noaa-nws")
    assert len(arrivals[CA]) == 1
    assert len(arrivals[KR]) == 2
    assert results[KR][0][2] == 2  # fetched


def test_main_requires_centre_when_several(tmp_path, capsys):
    log = _log(tmp_path)
    assert rap.main(["-t", "us-noaa-nws", "-d", "2026-08-16", log]) == 1
    assert "--centre" in capsys.readouterr().err

    assert rap.main(["-t", "us-noaa-nws", "-d", "2026-08-16", "-c", KR, log]) == 0
    out = capsys.readouterr().out
    assert f"centre {KR}" in out
    assert "arrivals=2" in out
