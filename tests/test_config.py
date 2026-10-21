import uuid

import pytest

from scgrep.config import BrokerConfig, Config, ConfigError

BASE_ENV = {
    "SENSOR_CENTRE_ID": "io-wis2dev-test-sensor-centre",
    "SUBSCRIPTION_TOPICS": "cache/a/wis2/ca-eccc-msc/data/#,monitor/a/wis2/ca-eccc-msc",
}


def test_log_http_response_max_chars():
    assert Config.from_env(dict(BASE_ENV)).log_http_response_max_chars == 1000
    cfg = Config.from_env(dict(BASE_ENV, LOG_HTTP_RESPONSE_MAX_CHARS="250"))
    assert cfg.log_http_response_max_chars == 250


def test_defaults_applied():
    cfg = Config.from_env(dict(BASE_ENV))
    assert cfg.redis_url == "redis:6379"
    assert cfg.time_lag == 300
    assert cfg.test_interval == 300
    assert cfg.metrics_endpoint == "/metrics"
    assert cfg.replay_centre_ids == ["ca-eccc-msc-global-replay"]
    assert cfg.replay_urls == ["https://wis2-grep.weather.gc.ca"]
    assert len(cfg.brokers) == 1
    assert cfg.redis_expiry == 300 + 300 + 60


def test_replay_brokers_default():
    cfg = Config.from_env(dict(BASE_ENV))
    assert len(cfg.replay_brokers) == 1
    assert cfg.replay_brokers[0].host == "wis2-grep.weather.gc.ca"
    assert cfg.replay_brokers[0].port == 8883
    assert cfg.replay_brokers[0].tls is True
    assert cfg.replay_broker_tls_insecure is False


def test_replay_brokers_override_single():
    env = dict(
        BASE_ENV,
        GLOBAL_REPLAY_BROKER_URLS="mqtts://u:p@replay.example:8883",
        GLOBAL_REPLAY_BROKER_TLS_INSECURE="true",
    )
    cfg = Config.from_env(env)
    assert len(cfg.replay_brokers) == 1
    assert cfg.replay_brokers[0].host == "replay.example"
    assert cfg.replay_brokers[0].username == "u"
    assert cfg.replay_broker_tls_insecure is True


def test_replay_brokers_multiple():
    env = dict(
        BASE_ENV,
        GLOBAL_REPLAY_BROKER_URLS="mqtts://one.example:8883, mqtts://two.example:8883",
    )
    cfg = Config.from_env(env)
    assert [b.host for b in cfg.replay_brokers] == ["one.example", "two.example"]


def test_replay_brokers_blank_falls_back_to_global_brokers():
    env = dict(
        BASE_ENV,
        GLOBAL_BROKER_URLS="mqtts://gb1.example:8883,mqtts://gb2.example:8883",
        GLOBAL_REPLAY_BROKER_URLS="",
    )
    cfg = Config.from_env(env)
    assert [(b.host, b.port) for b in cfg.replay_brokers] == [
        (b.host, b.port) for b in cfg.brokers
    ]
    assert [b.host for b in cfg.replay_brokers] == ["gb1.example", "gb2.example"]


def test_subscriber_id_is_uuid_like():
    cfg = Config.from_env(dict(BASE_ENV))
    assert len(cfg.subscriber_id) == 36
    assert cfg.subscriber_id.count("-") == 4


def test_missing_sensor_centre_id():
    env = dict(BASE_ENV)
    del env["SENSOR_CENTRE_ID"]
    with pytest.raises(ConfigError):
        Config.from_env(env)


def test_missing_subscription_topics():
    env = dict(BASE_ENV)
    del env["SUBSCRIPTION_TOPICS"]
    with pytest.raises(ConfigError):
        Config.from_env(env)


@pytest.mark.parametrize("topic", [
    "cache/a/wis2/+/data",                       # MQTT single-level wildcard
    "cache/a/wis2/uk-metoffice/+",
    "cache/a/wis2/uk-metoffice/*",               # Elasticsearch-style wildcard
    "cache/a/wis2/us-noaa-nws/#,cache/a/wis2/+", # one bad entry among good ones
])
def test_wildcard_topics_rejected_at_startup(topic):
    """'+' and '*' are unsupported by Global Replay services: they return zero
    messages, which is indistinguishable from a real gap. Fail fast instead."""
    env = dict(BASE_ENV, SUBSCRIPTION_TOPICS=topic)
    with pytest.raises(ConfigError, match=r"\+.*\*|wildcard"):
        Config.from_env(env)


def test_trailing_hash_topics_still_accepted():
    """'/#' is fine — it is stripped when querying the Global Replay."""
    env = dict(BASE_ENV, SUBSCRIPTION_TOPICS="cache/a/wis2/uk-metoffice/#")
    cfg = Config.from_env(env)
    assert cfg.subscription_topics == ["cache/a/wis2/uk-metoffice/#"]


def test_replay_list_length_mismatch():
    env = dict(BASE_ENV)
    env["GLOBAL_REPLAY_CENTRE_IDS"] = "a,b"
    env["GLOBAL_REPLAY_URLS"] = "https://one"
    with pytest.raises(ConfigError):
        Config.from_env(env)


def test_replay_urls_trailing_slash_stripped():
    env = dict(BASE_ENV)
    env["GLOBAL_REPLAY_URLS"] = "https://example.org/"
    cfg = Config.from_env(env)
    assert cfg.replay_urls == ["https://example.org"]


def test_metrics_endpoint_normalised():
    env = dict(BASE_ENV, METRICS_ENDPOINT="stats")
    cfg = Config.from_env(env)
    assert cfg.metrics_endpoint == "/stats"


def test_replay_wildcard_topics():
    cfg = Config.from_env(dict(BASE_ENV))
    topics = cfg.replay_wildcard_topics()
    assert topics == [
        f"replay/a/wis2/ca-eccc-msc-global-replay/"
        f"{cfg.subscriber_id_for('ca-eccc-msc-global-replay')}/#"
    ]


def test_subscriber_id_differs_per_replay_instance():
    # A shared id let kr-kma, misconfigured as ca-eccc, publish onto ca-eccc's
    # subscribed channel and double its async count. Each instance must get its
    # own (stable, UUID-valid) subscriber-id and wildcard.
    env = dict(
        BASE_ENV,
        GLOBAL_REPLAY_CENTRE_IDS="ca-eccc-msc-global-replay,kr-kma-global-replay",
        GLOBAL_REPLAY_URLS="https://ca.example,https://kr.example",
    )
    cfg = Config.from_env(env)
    ca = cfg.subscriber_id_for("ca-eccc-msc-global-replay")
    kr = cfg.subscriber_id_for("kr-kma-global-replay")
    assert ca != kr
    assert ca == cfg.subscriber_id_for("ca-eccc-msc-global-replay")
    for sid in (ca, kr):
        assert str(uuid.UUID(sid)) == sid
    assert cfg.replay_wildcard_topics() == [
        f"replay/a/wis2/ca-eccc-msc-global-replay/{ca}/#",
        f"replay/a/wis2/kr-kma-global-replay/{kr}/#",
    ]
    # ca-eccc's channel under kr's id is not subscribed to.
    assert f"replay/a/wis2/ca-eccc-msc-global-replay/{kr}/#" not in cfg.replay_wildcard_topics()


def test_broker_url_parsing():
    b = BrokerConfig.from_url("mqtts://user:pass@host.example:8883")
    assert b.host == "host.example"
    assert b.port == 8883
    assert b.username == "user"
    assert b.password == "pass"
    assert b.tls is True


def test_broker_url_defaults_port_for_plain_mqtt():
    b = BrokerConfig.from_url("mqtt://host.example")
    assert b.port == 1883
    assert b.tls is False


def test_broker_url_rejects_bad_scheme():
    with pytest.raises(ConfigError):
        BrokerConfig.from_url("http://host.example")
