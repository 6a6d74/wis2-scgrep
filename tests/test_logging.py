import logging

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
