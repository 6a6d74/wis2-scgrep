"""Configuration for SCGRep, loaded and validated from environment variables."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from urllib.parse import urlparse


class ConfigError(Exception):
    """Raised when the environment configuration is invalid."""


def _split(value: str) -> list[str]:
    """Split a comma-delimited environment value, trimming whitespace/empties."""
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass(frozen=True)
class BrokerConfig:
    """A single Global Broker connection, parsed from an ``mqtt(s)://`` URL."""

    url: str
    host: str
    port: int
    username: str | None
    password: str | None
    tls: bool

    @classmethod
    def from_url(cls, url: str) -> "BrokerConfig":
        parsed = urlparse(url)
        if parsed.scheme not in ("mqtt", "mqtts"):
            raise ConfigError(f"Unsupported MQTT scheme in broker URL: {url!r}")
        if not parsed.hostname:
            raise ConfigError(f"Missing host in broker URL: {url!r}")
        tls = parsed.scheme == "mqtts"
        port = parsed.port or (8883 if tls else 1883)
        return cls(
            url=url,
            host=parsed.hostname,
            port=port,
            username=parsed.username,
            password=parsed.password,
            tls=tls,
        )


@dataclass(frozen=True)
class Config:
    """Fully validated runtime configuration for SCGRep."""

    sensor_centre_id: str
    brokers: list[BrokerConfig]
    subscription_topics: list[str]
    replay_centre_ids: list[str]
    replay_urls: list[str]
    redis_url: str
    metrics_endpoint: str
    metrics_port: int
    time_lag: int
    test_interval: int
    replay_brokers: list[BrokerConfig]
    redis_startup_timeout: int = 60
    replay_broker_tls_insecure: bool = False
    log_http_response_max_chars: int = 1000
    subscriber_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def redis_expiry(self) -> int:
        """Seconds after which stored broker messages expire from Redis."""
        return self.time_lag + self.test_interval + 60

    @property
    def replay_targets(self) -> list[tuple[str, str]]:
        """(centre_id, url) pairs for every Global Replay service under test."""
        return list(zip(self.replay_centre_ids, self.replay_urls))

    def subscriber_id_for(self, centre_id: str) -> str:
        """The subscriber-id sent to (and subscribed for) one Global Replay.

        Each instance gets its own UUID, derived deterministically from
        ``subscriber_id``. With a shared id, an instance misconfigured with
        another's centre-id publishes onto that centre's channel and is counted
        as its replays (kr-kma did this as ca-eccc until 2026-10-21, doubling
        ca-eccc's async count). With distinct ids those messages land on a
        channel SCGRep never subscribes to.
        """
        return str(uuid.uuid5(uuid.UUID(self.subscriber_id), centre_id))

    def replay_wildcard_topics(self) -> list[str]:
        """MQTT topics to subscribe to for asynchronous replay responses."""
        return [
            f"replay/a/wis2/{centre_id}/{self.subscriber_id_for(centre_id)}/#"
            for centre_id in self.replay_centre_ids
        ]

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Config":
        env = os.environ if env is None else env

        sensor_centre_id = env.get("SENSOR_CENTRE_ID", "").strip()
        if not sensor_centre_id:
            raise ConfigError("SENSOR_CENTRE_ID is required")

        subscription_topics = _split(env.get("SUBSCRIPTION_TOPICS", ""))
        if not subscription_topics:
            raise ConfigError("SUBSCRIPTION_TOPICS is required")
        # Global Replay services match whole topic levels as a prefix; they have
        # no wildcard support. A '+' is silently rewritten to '*' by at least one
        # implementation, and neither matches anything — the request succeeds and
        # returns zero messages, which is indistinguishable from a real gap. Fail
        # fast instead. (A trailing '/#' is fine: it is stripped per topic_to_query.)
        bad = [t for t in subscription_topics if "+" in t or "*" in t]
        if bad:
            raise ConfigError(
                "SUBSCRIPTION_TOPICS must not contain '+' or '*' wildcards — "
                "Global Replay services do not support them and would return no "
                f"messages: {', '.join(bad)}. Use a topic prefix (optionally "
                "ending '/#') instead."
            )

        broker_urls = _split(
            env.get(
                "GLOBAL_BROKER_URLS",
                "mqtts://everyone:everyone@globalbroker.meteo.fr:8883",
            )
        )
        if not broker_urls:
            raise ConfigError("GLOBAL_BROKER_URLS must contain at least one URL")
        brokers = [BrokerConfig.from_url(url) for url in broker_urls]

        replay_centre_ids = _split(
            env.get("GLOBAL_REPLAY_CENTRE_IDS", "ca-eccc-msc-global-replay")
        )
        replay_urls = _split(
            env.get("GLOBAL_REPLAY_URLS", "https://wis2-grep.weather.gc.ca")
        )
        if len(replay_centre_ids) != len(replay_urls):
            raise ConfigError(
                "GLOBAL_REPLAY_CENTRE_IDS and GLOBAL_REPLAY_URLS must have the "
                f"same number of entries ({len(replay_centre_ids)} != "
                f"{len(replay_urls)})"
            )
        if not replay_centre_ids:
            raise ConfigError("At least one Global Replay service must be configured")
        # Normalise: strip trailing slashes so URL joins are predictable.
        replay_urls = [url.rstrip("/") for url in replay_urls]

        try:
            time_lag = int(env.get("TIME_LAG", "300"))
            test_interval = int(env.get("TEST_INTERVAL", "300"))
            metrics_port = int(env.get("METRICS_PORT", "8000"))
            redis_startup_timeout = int(env.get("REDIS_STARTUP_TIMEOUT", "60"))
            log_http_response_max_chars = int(
                env.get("LOG_HTTP_RESPONSE_MAX_CHARS", "1000")
            )
        except ValueError as exc:
            raise ConfigError(f"Invalid numeric configuration: {exc}") from exc
        if test_interval <= 0:
            raise ConfigError("TEST_INTERVAL must be positive")
        if time_lag < 0:
            raise ConfigError("TIME_LAG must not be negative")

        metrics_endpoint = env.get("METRICS_ENDPOINT", "/metrics").strip()
        if not metrics_endpoint.startswith("/"):
            metrics_endpoint = "/" + metrics_endpoint

        # Broker(s) on which the Global Replay service delivers async replay
        # messages. In the preoperational phase this is a single broker (the GRep
        # instance's own broker or the WIS2 test Global Broker) rather than the
        # operational Global Brokers. Leave blank to receive replays via the
        # Global Brokers themselves (GLOBAL_BROKER_URLS) — the operational case.
        replay_broker_urls = _split(
            env.get(
                "GLOBAL_REPLAY_BROKER_URLS",
                "mqtts://everyone:everyone@wis2-grep.weather.gc.ca:8883",
            )
        )
        if replay_broker_urls:
            replay_brokers = [BrokerConfig.from_url(url) for url in replay_broker_urls]
        else:
            # Blank: replays arrive on the operational Global Brokers.
            replay_brokers = list(brokers)
        replay_broker_tls_insecure = env.get(
            "GLOBAL_REPLAY_BROKER_TLS_INSECURE", "false"
        ).strip().lower() in ("1", "true", "yes", "on")

        return cls(
            sensor_centre_id=sensor_centre_id,
            brokers=brokers,
            subscription_topics=subscription_topics,
            replay_centre_ids=replay_centre_ids,
            replay_urls=replay_urls,
            redis_url=env.get("REDIS_URL", "redis:6379").strip(),
            metrics_endpoint=metrics_endpoint,
            metrics_port=metrics_port,
            time_lag=time_lag,
            test_interval=test_interval,
            replay_brokers=replay_brokers,
            redis_startup_timeout=redis_startup_timeout,
            replay_broker_tls_insecure=replay_broker_tls_insecure,
            log_http_response_max_chars=log_http_response_max_chars,
        )
