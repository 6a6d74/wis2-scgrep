"""MQTT connectivity to the Global Brokers and message routing.

One paho client is created per Global Broker. Every client subscribes to the
configured test topics (whose messages are stored in Redis for baselines) and to
the per-subscriber replay wildcard topics (whose messages drive the async test
counters). Incoming messages are routed by :class:`MessageHandler`.
"""

from __future__ import annotations

import json
import logging
import ssl
import time

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

from .config import BrokerConfig, Config
from .redis_store import RedisStore
from .replay_registry import ReplayRegistry

logger = logging.getLogger(__name__)


class MessageHandler:
    """Routes broker messages to Redis, and replay messages also to the registry.

    Baseline (test-topic) messages are stored in Redis. Replay messages are both
    recorded in the registry (for first-arrival timing / abort detection) and
    stored in Redis (for the deduplicated per-cycle count across replay brokers).
    """

    def __init__(self, store: RedisStore, registry: ReplayRegistry) -> None:
        self._store = store
        self._registry = registry

    def on_message(self, client, userdata, msg) -> None:
        # paho passes each client's user_data, which we set to the (credential-free)
        # broker URL so we can attribute every message to its source broker.
        broker_url = userdata if isinstance(userdata, str) else "?"
        topic = msg.topic
        if topic.startswith("replay/"):
            self._handle_replay(broker_url, topic, msg.payload)
            return

        parsed = self._parse_message(msg.payload)
        if parsed is None:
            logger.debug(
                "MQTT message received: broker=%s topic=%s (unparseable: "
                "not JSON / missing id/time)",
                broker_url, topic,
            )
            return
        msg_id, time_value = parsed
        # store_message returns False for a duplicate (already seen / discarded).
        is_new = self._store.store_message(msg_id, time_value, topic)
        if is_new:
            logger.info(
                "Global Broker message: topic=%s id=%s time=%s",
                topic, msg_id, time_value,
            )
        self._log_received(broker_url, topic, msg_id, time_value, is_new)

    def _handle_replay(self, broker_url: str, topic: str, payload) -> None:
        # First-arrival timing / abort detection (every arrival, any broker).
        self._registry.handle_replay(topic)

        # Deduplicated counting via Redis. The replay topic embeds the original
        # topic: replay/a/wis2/<centre-id>/<subscriber-id>/<original-topic...>
        parts = topic.split("/")
        if len(parts) < 6:
            return
        centre_id = parts[3]
        original_topic = "/".join(parts[5:])
        parsed = self._parse_message(payload)
        if parsed is None:
            logger.debug(
                "MQTT message received: broker=%s topic=%s (replay, unparseable)",
                broker_url, topic,
            )
            return
        msg_id, time_value = parsed
        # store_replay_message returns False for a duplicate (e.g. the same
        # message relayed by more than one broker).
        is_new = self._store.store_replay_message(
            centre_id, msg_id, time_value, original_topic
        )
        if is_new:
            logger.info(
                "Replay message (asynchronous): centre_id=%s topic=%s id=%s time=%s",
                centre_id, original_topic, msg_id, time_value,
            )
        self._log_received(broker_url, topic, msg_id, time_value, is_new)

    @staticmethod
    def _log_received(broker_url, topic, msg_id, time_value, is_new) -> None:
        """DEBUG line for *every* MQTT message (before dedup), attributing it to a
        broker and flagging whether it was a discarded duplicate."""
        logger.debug(
            "MQTT message received: broker=%s topic=%s id=%s time=%s Duplicate?=%s",
            broker_url, topic, msg_id, time_value, "false" if is_new else "true",
        )

    @staticmethod
    def _parse_message(payload) -> tuple[str, str] | None:
        """Extract (id, time) from a WIS2 notification/event payload, or None."""
        try:
            data = json.loads(payload)
        except (ValueError, TypeError):
            return None
        msg_id = data.get("id")
        time_value = data.get("time")
        if time_value is None:
            props = data.get("properties")
            if isinstance(props, dict):
                time_value = props.get("pubtime")
        if not msg_id or not time_value:
            return None
        return str(msg_id), str(time_value)


class MqttManager:
    """Manages the MQTT clients SCGRep needs.

    Two distinct roles:

    * **Global Brokers** (``GLOBAL_BROKER_URLS``) — subscribe to the test topics
      to build the baseline in Redis.
    * **Replay broker(s)** (``GLOBAL_REPLAY_BROKER_URLS``) — subscribe to the
      replay wildcard topics on which the Global Replay service delivers async
      replay messages. In the preoperational phase this is a single broker (the
      GRep instance's own broker or the WIS2 test Global Broker) rather than the
      operational Global Brokers.
    """

    def __init__(self, config: Config, on_message, metrics=None) -> None:
        self._config = config
        self._on_message = on_message
        self._metrics = metrics
        self._clients: list[mqtt.Client] = []
        # url -> time.time() of the last message received from that broker. Seeded
        # at connect so the age is measured from connection when nothing has
        # arrived yet, rather than from the epoch.
        self._last_message_at: dict[str, float] = {}

    def _track_messages(self, url: str):
        """Wrap the message callback so each arrival timestamps its broker."""
        def on_message(client, userdata, msg):
            self._last_message_at[url] = time.time()
            self._on_message(client, userdata, msg)
        return on_message

    def _track_message_age(self, broker: BrokerConfig) -> None:
        """Expose 'seconds since the last message' for ``broker``.

        Evaluated at scrape time via ``set_function`` so the age keeps climbing
        while a broker is silent — no background ticker needed. This is what
        distinguishes a *silently stalled* connection (status 1, age climbing)
        from a genuinely quiet topic or a clean disconnect (status 0).
        """
        if self._metrics is None:
            return
        url = self._broker_url(broker)
        self._last_message_at.setdefault(url, time.time())
        self._metrics.broker_last_message_age.labels(
            report_by=self._config.sensor_centre_id, url=url,
        ).set_function(lambda: time.time() - self._last_message_at[url])

    @staticmethod
    def _broker_url(broker: BrokerConfig) -> str:
        """A credential-free ``scheme://host:port`` URL for use as a metric label."""
        scheme = "mqtts" if broker.tls else "mqtt"
        return f"{scheme}://{broker.host}:{broker.port}"

    def _set_broker_status(self, broker: BrokerConfig, connected: bool) -> None:
        if self._metrics is None:
            return
        self._metrics.broker_status.labels(
            report_by=self._config.sensor_centre_id,
            url=self._broker_url(broker),
        ).set(1 if connected else 0)

    def _build_client(
        self,
        broker: BrokerConfig,
        subscriptions: list[str],
        role: str,
        tls_insecure: bool = False,
    ) -> mqtt.Client:
        client_id = f"scgrep-{self._config.subscriber_id}-{broker.host}"
        client = mqtt.Client(
            CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=True
        )
        # Carry the broker URL as user_data so on_message can attribute each
        # message to the broker it arrived on.
        client.user_data_set(self._broker_url(broker))
        if broker.username is not None:
            client.username_pw_set(broker.username, broker.password)
        if broker.tls:
            if tls_insecure:
                # TLS verification disabled (e.g. a replay broker whose
                # certificate has lapsed).
                client.tls_set(cert_reqs=ssl.CERT_NONE)
                client.tls_insecure_set(True)
            else:
                client.tls_set()
        client.on_connect = self._make_on_connect(broker, subscriptions, role)
        client.on_disconnect = self._make_on_disconnect(broker)
        client.on_connect_fail = self._make_on_connect_fail(broker)
        client.on_message = self._track_messages(self._broker_url(broker))
        return client

    def _make_on_connect(
        self, broker: BrokerConfig, subscriptions: list[str], role: str
    ):
        def on_connect(client, userdata, flags, reason_code, properties=None):
            if reason_code != 0:
                logger.error(
                    "Failed to connect to %s: %s", broker.host, reason_code
                )
                self._set_broker_status(broker, False)
                return
            logger.info("Connected to %s %s", role, broker.host)
            self._set_broker_status(broker, True)
            # Subscribe (again) on every (re)connect so state survives drops.
            client.subscribe([(topic, 1) for topic in subscriptions])
            logger.info(
                "Subscribed on %s to %d topic(s): %s",
                broker.host, len(subscriptions), ", ".join(subscriptions),
            )

        return on_connect

    def _make_on_disconnect(self, broker: BrokerConfig):
        def on_disconnect(client, userdata, *args):
            logger.warning("Disconnected from %s; will auto-reconnect", broker.host)
            self._set_broker_status(broker, False)

        return on_disconnect

    def _make_on_connect_fail(self, broker: BrokerConfig):
        # paho reports a connection that never opens (TCP/TLS timeout, refused,
        # DNS failure) only here, not via on_connect or on_disconnect. Without
        # this a firewall blocking the broker port leaves no log line at all.
        # Retries back off to 60 s, so this is at most ~1 line/minute/broker.
        def on_connect_fail(client, userdata):
            logger.warning(
                "Could not connect to %s:%d; retrying", broker.host, broker.port
            )
            self._set_broker_status(broker, False)

        return on_connect_fail

    def _connect(self, client: mqtt.Client, broker: BrokerConfig) -> None:
        # Publish an initial "disconnected" reading so the series exists from
        # start-up (on_connect flips it to 1 once the connection is up), and
        # start the message-age clock for this broker.
        self._set_broker_status(broker, False)
        self._track_message_age(broker)
        client.reconnect_delay_set(min_delay=1, max_delay=60)
        try:
            client.connect_async(broker.host, broker.port, keepalive=60)
        except Exception:  # noqa: BLE001 - log and continue with others
            logger.exception("Error initiating connection to %s", broker.host)
            return
        client.loop_start()
        self._clients.append(client)

    def start(self) -> None:
        """Connect to each unique broker with the union of its subscriptions.

        Global Brokers carry the test topics (baseline); replay brokers carry the
        replay wildcards. When ``GLOBAL_REPLAY_BROKER_URLS`` is blank the replay
        brokers *are* the Global Brokers, so a broker serving both roles becomes a
        single client subscribed to both sets of topics (one connection, no
        duplicate client id).
        """
        # Accumulate a plan per unique broker (host, port).
        plans: dict[tuple[str, int], dict] = {}

        def plan_for(broker: BrokerConfig) -> dict:
            return plans.setdefault(
                (broker.host, broker.port),
                {"broker": broker, "subs": [], "roles": [], "tls_insecure": False},
            )

        for broker in self._config.brokers:
            plan = plan_for(broker)
            plan["subs"].extend(self._config.subscription_topics)
            plan["roles"].append("Global Broker")

        for broker in self._config.replay_brokers:
            plan = plan_for(broker)
            plan["subs"].extend(self._config.replay_wildcard_topics())
            plan["roles"].append("Replay broker")
            # A broker used *only* for replay honours the replay TLS setting; a
            # Global Broker keeps normal verification even if it also serves
            # replays.
            if "Global Broker" not in plan["roles"]:
                plan["tls_insecure"] = self._config.replay_broker_tls_insecure

        for plan in plans.values():
            subscriptions = list(dict.fromkeys(plan["subs"]))
            role = " + ".join(dict.fromkeys(plan["roles"]))
            client = self._build_client(
                plan["broker"], subscriptions, role=role,
                tls_insecure=plan["tls_insecure"],
            )
            self._connect(client, plan["broker"])

    def stop(self) -> None:
        for client in self._clients:
            try:
                client.disconnect()
                client.loop_stop()
            except Exception:  # noqa: BLE001
                logger.debug("Error stopping client", exc_info=True)
        self._clients.clear()
