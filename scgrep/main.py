"""SCGRep entry point: wire components together and run the scheduler loop."""

from __future__ import annotations

import atexit
import logging
import logging.handlers
import os
import signal
import sys
import threading
import time

from .config import Config, ConfigError
from .metrics import Metrics, start_metrics_server
from .mqtt_client import MessageHandler, MqttManager
from .redis_store import RedisStore
from .replay_registry import ReplayRegistry
from .test_cycle import run_cycle

logger = logging.getLogger("scgrep")


def _configure_logging() -> None:
    """Log to stdout, and optionally to a file written periodically.

    Timestamps are UTC. Stdout gets every record immediately. When ``LOG_FILE``
    is set, records are buffered and written to that file every
    ``LOG_FILE_FLUSH_INTERVAL`` seconds (default 60); errors are flushed at once.
    A ``WatchedFileHandler`` reopens the file if it is replaced, so the external
    hourly purge works cleanly.
    """
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    formatter.converter = time.gmtime  # UTC timestamps

    root = logging.getLogger()
    root.setLevel(getattr(logging, level_name, logging.INFO))
    for handler in list(root.handlers):
        root.removeHandler(handler)

    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    root.addHandler(stream)

    log_file = os.environ.get("LOG_FILE", "").strip()
    if log_file:
        _add_periodic_file_logging(root, log_file, formatter)


def _add_periodic_file_logging(root, log_file: str, formatter) -> None:
    try:
        interval = float(os.environ.get("LOG_FILE_FLUSH_INTERVAL", "60"))
    except ValueError:
        interval = 60.0
    if interval <= 0:
        interval = 60.0

    try:
        directory = os.path.dirname(log_file)
        if directory:
            os.makedirs(directory, exist_ok=True)
        file_handler = logging.handlers.WatchedFileHandler(log_file, encoding="utf-8")
    except OSError as exc:
        logger.warning("File logging disabled: cannot open %r: %s", log_file, exc)
        return
    file_handler.setFormatter(formatter)

    # Buffer records and write them to the file in batches every `interval`
    # seconds; flush immediately on errors so they are never lost.
    memory = logging.handlers.MemoryHandler(
        capacity=100_000, flushLevel=logging.ERROR, target=file_handler
    )
    root.addHandler(memory)

    def flush_periodically() -> None:
        while True:
            time.sleep(interval)
            memory.flush()

    threading.Thread(
        target=flush_periodically, name="log-file-flush", daemon=True
    ).start()
    atexit.register(memory.close)  # flushes the final batch on shutdown

    logger.info("Writing logs to %s (flushed every %.0fs)", log_file, interval)


def _wait_for_redis(
    store: RedisStore, url: str, timeout: int, interval: float = 2.0
) -> bool:
    """Block until Redis answers a ping or ``timeout`` seconds elapse.

    Redis is provisioned separately (there is no ``depends_on`` to gate on), so
    on startup it may not be ready yet. Retry rather than exit immediately.
    """
    deadline = time.monotonic() + timeout
    attempt = 0
    while True:
        attempt += 1
        try:
            store.ping()
            logger.info("Connected to Redis at %s", url)
            return True
        except Exception as exc:  # noqa: BLE001 - any connection error retries
            if time.monotonic() >= deadline:
                logger.error(
                    "Could not reach Redis at %s after %ds: %s", url, timeout, exc
                )
                return False
            logger.warning(
                "Redis at %s not ready (attempt %d): %s; retrying in %.0fs",
                url, attempt, exc, interval,
            )
            time.sleep(interval)


class Scheduler:
    """Runs test cycles every ``TEST_INTERVAL`` seconds until stopped.

    Cycles never overlap: each runs to completion (internally parallel, with
    every fetch capped at 95% of ``TEST_INTERVAL``) before the next is timed.
    """

    def __init__(self, config, store, registry, metrics) -> None:
        self._config = config
        self._store = store
        self._registry = registry
        self._metrics = metrics
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        interval = self._config.test_interval
        # First cycle runs after one full interval, once messages have accrued.
        if self._stop.wait(interval):
            return
        while True:
            started = time.monotonic()
            try:
                run_cycle(self._config, self._store, self._registry, self._metrics)
            except Exception:  # noqa: BLE001 - keep the service alive
                logger.exception("Test cycle failed; continuing")
            elapsed = time.monotonic() - started
            if elapsed > interval:
                logger.warning(
                    "Test cycle took %.1fs, longer than TEST_INTERVAL=%ds; "
                    "starting the next cycle immediately",
                    elapsed, interval,
                )
                if self._stop.is_set():
                    return
                # No extra wait: the next cycle starts right away.
            else:
                # Sleep only the remainder so cycles start every TEST_INTERVAL.
                if self._stop.wait(interval - elapsed):
                    return


def main() -> int:
    _configure_logging()

    try:
        config = Config.from_env()
    except ConfigError as exc:
        logger.error("Configuration error: %s", exc)
        return 2

    logger.info(
        "Starting SCGRep sensor centre %s (subscriber-id=%s)",
        config.sensor_centre_id, config.subscriber_id,
    )
    for centre_id, _ in config.replay_targets:
        logger.info(
            "Global Replay %s: subscriber-id=%s",
            centre_id, config.subscriber_id_for(centre_id),
        )
    logger.info(
        "Testing %d topic(s) against %d Global Replay service(s)",
        len(config.subscription_topics), len(config.replay_targets),
    )

    metrics = Metrics()
    start_metrics_server(metrics, config.metrics_endpoint, config.metrics_port)

    store = RedisStore(
        config.redis_url, config.redis_expiry, config.subscription_topics
    )
    if not _wait_for_redis(store, config.redis_url, config.redis_startup_timeout):
        return 3

    registry = ReplayRegistry()
    handler = MessageHandler(store, registry)
    mqtt = MqttManager(config, handler.on_message, metrics)
    mqtt.start()

    scheduler = Scheduler(config, store, registry, metrics)

    def _handle_signal(signum, frame):
        logger.info("Received signal %s; shutting down", signum)
        scheduler.stop()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        scheduler.run()
    finally:
        mqtt.stop()
        logger.info("SCGRep stopped")

    return 0


if __name__ == "__main__":
    sys.exit(main())
