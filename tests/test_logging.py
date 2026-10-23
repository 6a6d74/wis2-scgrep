import logging
import threading
import time

import pytest

from scgrep.main import _configure_logging, _level_from_env


@pytest.fixture
def restore_root_logging():
    """_configure_logging replaces the root handlers; put pytest's back after."""
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    for handler in list(root.handlers):
        handler.close()
        root.removeHandler(handler)
    for handler in handlers:
        root.addHandler(handler)
    root.setLevel(level)


def _log_and_flush():
    log = logging.getLogger("scgrep.test")
    log.info("info-record")
    log.warning("warning-record")
    for handler in logging.getLogger().handlers:
        handler.flush()


def test_stdout_level_filters_stdout_but_not_file(
    monkeypatch, tmp_path, capsys, restore_root_logging
):
    log_file = tmp_path / "scgrep.log"
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("LOG_STDOUT_LEVEL", "WARNING")
    monkeypatch.setenv("LOG_FILE", str(log_file))

    _configure_logging()
    _log_and_flush()

    console = capsys.readouterr().err
    assert "warning-record" in console
    assert "info-record" not in console
    written = log_file.read_text()
    assert "info-record" in written and "warning-record" in written


def test_stdout_level_defaults_to_log_level(monkeypatch, capsys, restore_root_logging):
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.delenv("LOG_STDOUT_LEVEL", raising=False)
    monkeypatch.delenv("LOG_FILE", raising=False)

    _configure_logging()
    _log_and_flush()

    assert "info-record" in capsys.readouterr().err


@pytest.mark.parametrize(
    "value, expected",
    [
        ("WARNING", logging.WARNING),
        (" debug ", logging.DEBUG),
        ("", logging.INFO),
        ("NOPE", logging.INFO),
        ("BASIC_FORMAT", logging.INFO),  # a logging attribute, but not a level
    ],
)
def test_level_from_env(monkeypatch, value, expected):
    monkeypatch.setenv("SCGREP_TEST_LEVEL", value)
    assert _level_from_env("SCGREP_TEST_LEVEL", logging.INFO) == expected


class _RecordingStream:
    """Stands in for the log file's stream: counts writes, can block them."""

    def __init__(self, block=False):
        self.writes = []
        self.entered = threading.Event()
        self.release = threading.Event()
        if not block:
            self.release.set()

    def write(self, text):
        self.entered.set()
        self.release.wait(10)
        self.writes.append(text)

    def flush(self):
        pass

    def close(self):
        self.release.set()


def _file_logging(monkeypatch, tmp_path, stream, interval="3600"):
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("LOG_FILE", str(tmp_path / "scgrep.log"))
    monkeypatch.setenv("LOG_FILE_FLUSH_INTERVAL", interval)
    _configure_logging()
    (handler,) = [h for h in logging.getLogger().handlers if hasattr(h, "target")]
    handler.flush()  # write the "Writing logs to …" line to the real file
    handler.target.stream = stream
    return handler


def test_logging_is_not_blocked_while_the_file_write_is_slow(
    monkeypatch, tmp_path, restore_root_logging
):
    # On EFS a flush took up to 80 s; logging threads (paho's among them)
    # must not wait for it.
    stream = _RecordingStream(block=True)
    handler = _file_logging(monkeypatch, tmp_path, stream)
    log = logging.getLogger("scgrep.test")
    log.info("before-flush")
    flusher = threading.Thread(target=handler.flush, daemon=True)
    flusher.start()
    assert stream.entered.wait(5)

    logger_thread = threading.Thread(target=log.info, args=("during-flush",), daemon=True)
    logger_thread.start()
    logger_thread.join(2)
    blocked = logger_thread.is_alive()

    stream.release.set()
    flusher.join(5)
    assert not blocked


def test_each_flush_is_one_write(monkeypatch, tmp_path, restore_root_logging):
    # Per-record writes (and stats) are what made the flush slow on NFS.
    stream = _RecordingStream()
    handler = _file_logging(monkeypatch, tmp_path, stream)
    log = logging.getLogger("scgrep.test")
    for i in range(100):
        log.info("record-%d", i)
    handler.flush()

    assert len(stream.writes) == 1
    lines = stream.writes[0].splitlines()
    assert [line.rsplit(" ", 1)[1] for line in lines] == [f"record-{i}" for i in range(100)]


def test_error_is_written_without_waiting_for_the_interval(
    monkeypatch, tmp_path, restore_root_logging
):
    stream = _RecordingStream()
    _file_logging(monkeypatch, tmp_path, stream, interval="3600")
    logging.getLogger("scgrep.test").error("error-record")

    deadline = time.monotonic() + 5
    while not any("error-record" in w for w in stream.writes) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert any("error-record" in w for w in stream.writes)
