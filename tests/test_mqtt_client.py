import json
import logging
import time
from unittest.mock import Mock

import fakeredis

from scgrep.config import BrokerConfig, Config
from scgrep.metrics import Metrics
from scgrep.mqtt_client import MessageHandler, MqttManager
from scgrep.redis_store import RedisStore
from scgrep.replay_registry import ReplayRegistry

ENV = {
    "SENSOR_CENTRE_ID": "io-wis2dev-test-sensor-centre",
    "SUBSCRIPTION_TOPICS": "cache/a/wis2/x/#",
    "GLOBAL_REPLAY_CENTRE_IDS": "ca-eccc",
    "GLOBAL_REPLAY_URLS": "https://replay.example.org",
    "GLOBAL_BROKER_URLS": "mqtts://everyone:everyone@globalbroker.meteo.fr:8883",
}
BROKER = BrokerConfig.from_url("mqtts://everyone:everyone@globalbroker.meteo.fr:8883")
URL = "mqtts://globalbroker.meteo.fr:8883"
REPORT_BY = "io-wis2dev-test-sensor-centre"


def _mgr():
    cfg = Config.from_env(dict(ENV))
    metrics = Metrics()
    return MqttManager(cfg, Mock(), metrics), metrics


def _status(metrics, url=URL):
    return metrics.broker_status.labels(report_by=REPORT_BY, url=url)._value.get()


def test_broker_url_strips_credentials_and_reflects_scheme():
    assert MqttManager._broker_url(BROKER) == "mqtts://globalbroker.meteo.fr:8883"
    plain = BrokerConfig.from_url("mqtt://broker.example:1883")
    assert MqttManager._broker_url(plain) == "mqtt://broker.example:1883"


def test_on_connect_success_sets_status_1():
    mgr, metrics = _mgr()
    on_connect = mgr._make_on_connect(BROKER, ["cache/a/wis2/x/#"], "Global Broker")
    client = Mock()
    on_connect(client, None, None, 0)
    assert _status(metrics) == 1
    client.subscribe.assert_called_once()  # subscribed on connect


def test_on_connect_failure_sets_status_0():
    mgr, metrics = _mgr()
    on_connect = mgr._make_on_connect(BROKER, [], "Global Broker")
    on_connect(Mock(), None, None, 5)  # non-zero reason code = failed connect
    assert _status(metrics) == 0


def test_on_disconnect_sets_status_0():
    mgr, metrics = _mgr()
    mgr._make_on_connect(BROKER, [], "Global Broker")(Mock(), None, None, 0)
    assert _status(metrics) == 1
    mgr._make_on_disconnect(BROKER)(Mock(), None)
    assert _status(metrics) == 0


def test_on_connect_fail_logs_warning_and_sets_status_0(caplog):
    mgr, metrics = _mgr()
    mgr._make_on_connect(BROKER, [], "Global Broker")(Mock(), None, None, 0)
    with caplog.at_level(logging.WARNING, logger="scgrep"):
        mgr._make_on_connect_fail(BROKER)(Mock(), None)
    assert _status(metrics) == 0
    assert "Could not connect to globalbroker.meteo.fr:8883" in caplog.text


def test_built_client_handles_connect_fail():
    """paho reports a connection that never opens only via on_connect_fail
    (e.g. a firewall dropping port 8883), so it must be wired up."""
    mgr, _ = _mgr()
    client = mgr._build_client(BROKER, [], "Global Broker")
    assert client.on_connect_fail is not None


def _age(metrics, url=URL):
    # set_function() values are produced at collection time, so read the sample
    # from the registry rather than the child's stored value.
    return metrics.registry.get_sample_value(
        "wmo_wis2_scgrep_broker_last_message_age_seconds",
        {"report_by": REPORT_BY, "url": url},
    )


def test_message_age_starts_at_connection_and_climbs_while_silent():
    """A broker that has delivered nothing yet reports an age measured from
    connection, and that age keeps growing at scrape time (no ticker needed)."""
    mgr, metrics = _mgr()
    mgr._track_message_age(BROKER)
    first = _age(metrics)
    assert first >= 0
    time.sleep(0.05)
    assert _age(metrics) > first          # evaluated at scrape time


def test_message_age_resets_when_a_message_arrives():
    mgr, metrics = _mgr()
    mgr._track_message_age(BROKER)
    mgr._last_message_at[URL] = time.time() - 30      # pretend 30s of silence
    assert _age(metrics) >= 30
    # A message arriving resets the clock, and is still passed to the handler.
    on_message = mgr._track_messages(URL)
    on_message(None, URL, _Msg("cache/a/wis2/x/y", _payload()))
    assert _age(metrics) < 1
    mgr._on_message.assert_called_once()


def test_message_age_tracked_per_broker():
    mgr, metrics = _mgr()
    other = BrokerConfig.from_url("mqtts://wis2-grep.weather.gc.ca:8883")
    other_url = "mqtts://wis2-grep.weather.gc.ca:8883"
    mgr._track_message_age(BROKER)
    mgr._track_message_age(other)
    mgr._last_message_at[URL] = time.time() - 60      # one broker silent
    mgr._track_messages(other_url)(None, other_url, _Msg("cache/a/wis2/x/y", _payload()))
    assert _age(metrics) >= 60                        # silent broker
    assert _age(metrics, other_url) < 1                # active broker


def test_status_helper_noop_without_metrics():
    cfg = Config.from_env(dict(ENV))
    mgr = MqttManager(cfg, Mock(), metrics=None)
    # Must not raise when no metrics object was provided.
    mgr._set_broker_status(BROKER, True)


# --------------------------------------------------------------------------
# MessageHandler DEBUG logging (broker attribution + Duplicate? flag)
# --------------------------------------------------------------------------

class _Msg:
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload


def _handler():
    client = fakeredis.FakeRedis(decode_responses=True)
    store = RedisStore("redis:6379", 300, ["cache/a/wis2/x/#"], client=client)
    return MessageHandler(store, ReplayRegistry())


def _payload(mid="m1"):
    return json.dumps(
        {"id": mid, "properties": {"pubtime": "2026-08-17T10:00:00Z"}}
    ).encode()


def test_on_message_debug_logs_broker_url_and_duplicate(caplog):
    handler = _handler()
    url = "mqtts://gb.wis2dev.io:8883"
    msg = _Msg("cache/a/wis2/x/data/core/y", _payload())
    with caplog.at_level(logging.DEBUG, logger="scgrep.mqtt_client"):
        handler.on_message(None, url, msg)  # first arrival
        handler.on_message(None, url, msg)  # duplicate
    text = caplog.text
    assert f"MQTT message received: broker={url}" in text
    assert "Duplicate?=false" in text  # first time
    assert "Duplicate?=true" in text   # second time (discarded)


def test_on_message_replay_debug_log_attributes_broker(caplog):
    handler = _handler()
    url = "mqtts://wis2-grep.weather.gc.ca:8883"
    topic = "replay/a/wis2/ca-eccc/uuid/cache/a/wis2/x/data/core/y"
    with caplog.at_level(logging.DEBUG, logger="scgrep.mqtt_client"):
        handler.on_message(None, url, _Msg(topic, _payload("r1")))
    text = caplog.text
    assert f"broker={url}" in text and "Duplicate?=false" in text
    assert "Replay message (asynchronous)" in text  # INFO line still emitted
