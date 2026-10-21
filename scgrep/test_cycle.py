"""Orchestration of a single test cycle across all topics and replay services."""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace

from .config import Config
from .metrics import Metrics
from .redis_store import RedisStore
from .replay_registry import ReplayRegistry
from .replay_tester import FetchResult, async_fetch, sync_fetch
from .util import broker_authority, epoch_to_iso

logger = logging.getLogger(__name__)


def run_cycle(
    config: Config,
    store: RedisStore,
    registry: ReplayRegistry,
    metrics: Metrics,
    now: float | None = None,
) -> None:
    """Run one full test cycle.

    The test window is ``(now - TIME_LAG - TEST_INTERVAL) .. (now - TIME_LAG)``.
    Baselines are read from Redis and every (topic, replay-service) pair is tested
    with a synchronous and an asynchronous fetch, all in parallel so the cycle
    completes within one ``TEST_INTERVAL``.

    Every metric for the cycle is published together, at exactly 95% of the test
    period. All fetches share a single absolute deadline anchored to the start of
    the cycle, and publication is held to that same instant — so the baseline,
    the synchronous (``http``) results, and the asynchronous (``mqtt``) results
    all update at once and a single Prometheus scrape sees a consistent set.
    """
    now = time.time() if now is None else now
    cycle_start = time.monotonic()
    # Floor the window to whole seconds. The Global Replay is queried with an
    # ISO-8601 interval, and epoch_to_iso() truncates to whole seconds — so
    # without this the baseline would be counted over sub-second bounds
    # (e.g. 20:01:37.779) while the replay was asked for the truncated interval
    # (20:01:37). Bursts of messages sharing a pub-time instant then land in
    # different windows for the two sides. Flooring makes both cover exactly the
    # same interval, and makes consecutive windows contiguous rather than
    # drifting by a random fraction of a second.
    end_epoch = math.floor(now - config.time_lag)
    start_epoch = end_epoch - config.test_interval
    start_iso = epoch_to_iso(start_epoch)
    end_iso = epoch_to_iso(end_epoch)
    deadline_s = 0.95 * config.test_interval
    # Absolute instant, 95% through the test period, at which all fetches stop
    # and all metrics are published.
    deadline_at = cycle_start + deadline_s
    report_by = config.sensor_centre_id
    broker_authorities = [broker_authority(b.url) for b in config.brokers]

    logger.info("Test period begins: window %s .. %s", start_iso, end_iso)

    # Baselines: fast Redis reads, held for publication at the deadline.
    baselines = {
        topic: store.count_messages(topic, start_epoch, end_epoch)
        for topic in config.subscription_topics
    }
    for topic, count in baselines.items():
        logger.info(
            "Baseline: topic=%s messages=%d interval=%s/%s",
            topic, count, start_iso, end_iso,
        )

    # Clean sheet for this cycle, before any request is made: replay message
    # records (for the deduplicated mqtt count) and synchronous message records.
    store.clear_replay(config.replay_centre_ids)
    store.clear_sync(config.replay_centre_ids)

    # Fan out all fetches (the slow part) in parallel and collect their results;
    # nothing is published until every fetch has completed.
    max_workers = max(1, len(config.subscription_topics) * len(config.replay_targets) * 2)
    fetch_results: list[tuple[str, str, FetchResult]] = []
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="fetch") as ex:
        futures = {}
        for topic in config.subscription_topics:
            for centre_id, replay_url in config.replay_targets:
                sync_future = ex.submit(
                    sync_fetch, replay_url, centre_id, topic, start_iso, end_iso,
                    deadline_s, store, config.log_http_response_max_chars,
                    deadline_at,
                )
                futures[sync_future] = (centre_id, topic)
                # The async fetch expects replay messages only when the replay's
                # own numberMatched (from the synchronous fetch above) is > 0.
                futures[ex.submit(
                    async_fetch, replay_url, centre_id, topic,
                    config.subscriber_id_for(centre_id),
                    broker_authorities, start_iso, end_iso, deadline_s, registry,
                    baselines[topic],
                    number_matched_provider=_number_matched_from(sync_future),
                    max_chars=config.log_http_response_max_chars,
                    deadline_at=deadline_at,
                )] = (centre_id, topic)
        for future, (centre_id, topic) in futures.items():
            try:
                result = future.result()
            except Exception:  # noqa: BLE001 - one failure must not sink the cycle
                logger.exception("A fetch task raised an unexpected error")
                continue
            # The async (mqtt) count comes from Redis: deduplicated across replay
            # brokers, counted the same way as the baseline. The registry is used
            # only for timing/abort, so its (non-deduplicated) count is replaced.
            # (Redis holds 0 for a window where no messages were expected, so this
            # is safe regardless of the numberMatched/baseline decision.)
            if result.protocol == "mqtt" and not result.aborted:
                deduped = store.count_replay_messages(
                    centre_id, topic, start_epoch, end_epoch
                )
                result = replace(result, messages_fetched=deduped)
            # The synchronous count needs the same treatment. `numberMatched` is
            # computed by the service on **closed** [start, end] semantics (OGC
            # API - Features selects features *intersecting* the interval), so it
            # includes the whole boundary second — which the next, contiguous
            # window then counts again. Re-count the messages the fetch actually
            # paged through, from Redis, over the same half-open [start, end)
            # window as the baseline and mqtt. `numberMatched` is preserved on the
            # result and still drives `invalid_numberMatched`.
            elif result.protocol == "http" and not result.aborted:
                counted = store.count_sync_messages(
                    centre_id, topic, start_epoch, end_epoch
                )
                result = replace(result, messages_fetched=counted)
            fetch_results.append((centre_id, topic, result))

    # Hold publication to exactly 95% of the test period. Fetches normally run
    # right up to this instant; this also guards the case where they finish early
    # (e.g. errors), so every cycle publishes at the same point.
    remaining = deadline_at - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)

    # Publish everything together so all metrics for this cycle update at once.
    # messages_received / messages_fetched are cumulative counters: increment by
    # this cycle's count rather than setting an absolute value.
    for topic, count in baselines.items():
        metrics.messages_received.labels(report_by=report_by, topic=topic).inc(count)
    for centre_id, topic, result in fetch_results:
        _publish(metrics, report_by, centre_id, topic, result)
        # Concise per-result summary for quick scanning of a cycle's outcome.
        # `fetched` is SCGRep's half-open re-count; `numberMatched` is what the
        # service claimed (http only, closed interval). Logging both lets a reader
        # tell the boundary-second effect from genuine loss without re-deriving it.
        logger.info(
            "Result: centre_id=%s topic=%s protocol=%s baseline=%d fetched=%d "
            "numberMatched=%d delay_ms=%.0f aborted=%d invalid_format=%d "
            "invalid_numberMatched=%d",
            centre_id, topic, result.protocol, baselines.get(topic, 0),
            result.messages_fetched, result.number_matched, result.fetch_delay_ms,
            int(result.aborted), int(result.invalid_format),
            int(result.invalid_number_matched),
        )

    logger.info("Test period complete: window %s .. %s", start_iso, end_iso)


def _number_matched_from(sync_future: "Future[FetchResult]") -> Callable[[], int | None]:
    """Build a provider that yields the synchronous fetch's ``numberMatched``.

    Calling the returned function blocks on ``sync_future`` (the parallel
    synchronous fetch) and returns its ``numberMatched``, or ``None`` when it is
    unavailable — the fetch aborted, raised, or returned an invalid format — so
    the async fetch can fall back to the baseline. A ``numberMatched`` value is
    used even when it failed the count validation (``invalid_number_matched``):
    the service's own claim is still the right basis for "are messages expected?".
    """
    def provider() -> int | None:
        try:
            result = sync_future.result()
        except Exception:  # noqa: BLE001 - unavailable numberMatched -> fall back
            return None
        if result.protocol != "http" or result.aborted or result.invalid_format:
            return None
        return result.messages_fetched
    return provider


def _publish(
    metrics: Metrics,
    report_by: str,
    centre_id: str,
    topic: str,
    result: FetchResult,
) -> None:
    labels = dict(
        report_by=report_by,
        centre_id=centre_id,
        topic=topic,
        protocol=result.protocol,
    )
    metrics.test_aborted.labels(**labels).set(1 if result.aborted else 0)
    metrics.response_invalid_format.labels(**labels).set(
        1 if result.invalid_format else 0
    )
    metrics.fetch_delay.labels(**labels).set(result.fetch_delay_ms)
    metrics.http_response_code.labels(**labels).set(result.http_status)
    # Cumulative counter: add this cycle's fetched count.
    metrics.messages_fetched.labels(**labels).inc(result.messages_fetched)
    # numberMatched validation applies to the synchronous (http) fetch only.
    if result.protocol == "http":
        metrics.response_invalid_number_matched.labels(**labels).set(
            1 if result.invalid_number_matched else 0
        )
