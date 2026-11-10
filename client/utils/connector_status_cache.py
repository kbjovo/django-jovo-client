"""
Redis-backed cache for live Debezium connector status.

Dashboards and list pages used to call Kafka Connect's REST API once per connector
on every page load — sequentially and blocking the gunicorn worker for the whole
round-trip. That status is now refreshed in the background by a Celery beat task
(``client.tasks.refresh_connector_status_cache``) and served from Redis, so page
loads never block on Connect health.

Cached value == the raw status dict returned by
``DebeziumConnectorManager.get_connector_status()``, i.e.
``{'connector': {'state': ...}, 'tasks': [...], ...}``, or the ``NOT_FOUND``
sentinel for connectors Kafka Connect doesn't know about (so a known-absent
connector is a cache hit, not a repeated live probe).
"""

import logging
from concurrent.futures import ThreadPoolExecutor

from django.conf import settings
from django.core.cache import cache

logger = logging.getLogger(__name__)

CACHE_PREFIX = 'connstat:'

# TTL is a safety net; the beat task refreshes well within this window.
DEFAULT_TTL = int(getattr(settings, 'CONNECTOR_STATUS_CACHE_TTL', 180))

# Short per-call timeout for the status path — one sick connector must not stall
# the whole refresh (or a cold-cache page load).
STATUS_TIMEOUT = int(getattr(settings, 'CONNECTOR_STATUS_TIMEOUT', 8))

# Sentinel stored for connectors Connect returns 404 for.
NOT_FOUND = {'__not_found__': True}

_MAX_WORKERS = 8


def _key(name):
    return f'{CACHE_PREFIX}{name}'


def _safe(fn, default):
    """
    Run a cache-backend call, degrading to `default` if Redis is unreachable.

    A cache outage must never take down the dashboards — on any backend error we
    fall back to the live path (cache miss) rather than propagating the exception.
    """
    try:
        return fn()
    except Exception as e:
        logger.warning(f"Connector status cache backend unavailable: {e}")
        return default


def _normalize(value):
    """Map cache miss (None) and the NOT_FOUND sentinel both back to None."""
    if value is None:
        return None
    if isinstance(value, dict) and value.get('__not_found__'):
        return None
    return value


# Marker for "Kafka Connect didn't answer" — never cached as NOT_FOUND.
_UNREACHABLE = object()


def _live_fetch(name):
    """
    Fetch a single connector's status live from Kafka Connect (cache miss path).

    Returns (name, data) where data is the status dict, None for a real 404, or
    _UNREACHABLE when Connect timed out / errored (so callers don't cache a
    connector as missing just because Connect was slow).
    """
    from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
    try:
        manager = DebeziumConnectorManager()
        outcome, data = manager.get_connector_status_detailed(name, timeout=STATUS_TIMEOUT)
        if outcome == 'ok':
            return name, data
        if outcome == 'not_found':
            return name, None
        return name, _UNREACHABLE
    except Exception as e:
        logger.warning(f"Live status fetch failed for {name}: {e}")
        return name, _UNREACHABLE


def set_status(name, status_data):
    """Write one connector's status to the cache (None -> NOT_FOUND sentinel)."""
    if not name:
        return
    value = status_data if status_data is not None else NOT_FOUND
    _safe(lambda: cache.set(_key(name), value, DEFAULT_TTL), None)


def get_status(name):
    """Return cached status dict for one connector, or None on miss/not-found."""
    if not name:
        return None
    return _normalize(_safe(lambda: cache.get(_key(name)), None))


def get_statuses(names, allow_live_fallback=True):
    """
    Bulk-read cached status for many connector names.

    Returns ``{name: status_data_or_None}``. On a cold cache, missing names are
    fetched live in parallel (ThreadPool) and written back, so the first page load
    after a deploy/restart is bounded by the slowest single call rather than the
    sum of all of them.
    """
    names = [n for n in dict.fromkeys(names) if n]  # dedupe, drop falsy, keep order
    if not names:
        return {}

    cached = _safe(lambda: cache.get_many([_key(n) for n in names]), {})

    result = {}
    misses = []
    for n in names:
        k = _key(n)
        if k in cached:
            result[n] = _normalize(cached[k])
        else:
            misses.append(n)

    if not misses:
        return result

    if not allow_live_fallback:
        for n in misses:
            result[n] = None
        return result

    to_cache = {}
    with ThreadPoolExecutor(max_workers=min(_MAX_WORKERS, len(misses))) as pool:
        for name, data in pool.map(_live_fetch, misses):
            if data is _UNREACHABLE:
                result[name] = None      # unknown right now; don't cache it
                continue
            result[name] = data
            to_cache[_key(name)] = data if data is not None else NOT_FOUND
    if to_cache:
        _safe(lambda: cache.set_many(to_cache, DEFAULT_TTL), None)

    return result


def refresh_all():
    """
    Refresh cached status for every connector known to Kafka Connect.

    Called by the beat task. Lists connectors once, fetches each status in parallel,
    and writes the whole set with a single ``set_many``. Returns the number of
    connectors refreshed.
    """
    from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager

    manager = DebeziumConnectorManager()
    names = [n for n in manager.list_connectors() if n]
    if not names:
        return 0

    payload = {}
    with ThreadPoolExecutor(max_workers=min(_MAX_WORKERS, len(names))) as pool:
        for name, data in pool.map(_live_fetch, names):
            if data is _UNREACHABLE:
                continue  # keep the previous cached value until Connect answers
            payload[_key(name)] = data if data is not None else NOT_FOUND

    if payload:
        _safe(lambda: cache.set_many(payload, DEFAULT_TTL), None)
    return len(payload)
