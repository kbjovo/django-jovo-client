#!/usr/bin/env python3
"""
Replication Edge Agent
======================
The small supervisor that makes the box an *appliance*:

  1. Waits for the local Kafka Connect to be healthy.
  2. Registers with central  ->  POST {CENTRAL_BASE_URL}/api/edge/register
     (Bearer = one-time ENROLL_TOKEN). Central returns a long-lived agent_secret.
  3. Heartbeats forever       ->  POST {CENTRAL_BASE_URL}/api/edge/heartbeat
     (Bearer = agent_secret), reporting stack health + versions.

It has NO role in the data path — data flows source -> Connect -> Kafka ->
Connect -> sink entirely on this box. The agent only carries control/telemetry
back to central over the Netbird mesh.

Everything is stdlib (urllib) to keep the image tiny.
"""
import json
import logging
import os
import time
import urllib.request
import urllib.error

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s edge-agent: %(message)s")
log = logging.getLogger("edge-agent")

CENTRAL = os.environ["CENTRAL_BASE_URL"].rstrip("/")
APPLIANCE_UID = os.environ["APPLIANCE_UID"]
ENROLL_TOKEN = os.environ.get("ENROLL_TOKEN", "")
NETBIRD_IP = os.environ.get("NETBIRD_IP", "")
PLATFORM = os.environ.get("PLATFORM", "vm_hyperv")
LOCAL_CONNECT = os.environ.get("LOCAL_CONNECT_URL", "http://kafka-connect:8083").rstrip("/")
HEARTBEAT = int(os.environ.get("HEARTBEAT_INTERVAL_SEC", "30"))
STATE_FILE = os.environ.get("AGENT_STATE_FILE", "/state/agent.json")

# Endpoints central will use to reach THIS appliance, on the mesh IP.
MESH = {
    "connect_url": f"http://{NETBIRD_IP}:8083",
    "kafka_bootstrap": f"{NETBIRD_IP}:9092",
    "schema_registry_url": f"http://{NETBIRD_IP}:8082",
    "jolokia_url": f"http://{NETBIRD_IP}:8778/jolokia",
}


def _post(path, token, payload):
    req = urllib.request.Request(
        f"{CENTRAL}{path}",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode() or "{}")


def _get_json(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read().decode() or "{}")


def _load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, ValueError):
        return {}


def _save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def wait_for_connect():
    """Block until the local Connect REST answers (stack is up)."""
    while True:
        try:
            _get_json(f"{LOCAL_CONNECT}/")
            log.info("Local Kafka Connect is healthy.")
            return
        except (urllib.error.URLError, ConnectionError) as e:
            log.info("Waiting for local Connect (%s)...", e)
            time.sleep(5)


def stack_health():
    """Best-effort component snapshot reported to central on each heartbeat."""
    health = {"connect": "unknown", "connectors": 0, "versions": {}}
    try:
        root = _get_json(f"{LOCAL_CONNECT}/")
        health["connect"] = "running"
        health["versions"]["connect"] = root.get("version", "")
        connectors = _get_json(f"{LOCAL_CONNECT}/connectors")
        health["connectors"] = len(connectors)
    except Exception as e:  # noqa: BLE001 — telemetry must never crash the agent
        health["connect"] = f"error: {e}"
    return health


def register():
    payload = {
        "appliance_uid": APPLIANCE_UID,
        "platform": PLATFORM,
        "netbird_ip": NETBIRD_IP,
        "agent_version": "1.0.0",
        "stack_versions": stack_health().get("versions", {}),
        **MESH,
    }
    log.info("Registering with central at %s ...", CENTRAL)
    resp = _post("/api/edge/register", ENROLL_TOKEN, payload)
    secret = resp["agent_secret"]
    _save_state({"agent_secret": secret})
    log.info("Registered. appliance_uid=%s", APPLIANCE_UID)
    return secret


def main():
    if not NETBIRD_IP:
        log.error("NETBIRD_IP is empty — the mesh must be up before the agent starts. Exiting.")
        raise SystemExit(1)

    wait_for_connect()

    state = _load_state()
    secret = state.get("agent_secret")
    if not secret:
        # Retry registration until central accepts (token valid, reachable).
        while not secret:
            try:
                secret = register()
            except urllib.error.HTTPError as e:
                log.error("Register rejected (%s). Check ENROLL_TOKEN / appliance_uid.", e.code)
                time.sleep(30)
            except Exception as e:  # noqa: BLE001
                log.error("Register failed (%s). Retrying...", e)
                time.sleep(15)

    log.info("Entering heartbeat loop (every %ss).", HEARTBEAT)
    while True:
        try:
            health = stack_health()
            status = "online" if health["connect"] == "running" else "degraded"
            _post("/api/edge/heartbeat", secret, {
                "appliance_uid": APPLIANCE_UID,
                "status": status,
                "stack_versions": health.get("versions", {}),
                "connectors": health.get("connectors", 0),
            })
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                log.warning("Heartbeat unauthorized — re-registering.")
                state = {}
                _save_state(state)
                secret = None
                while not secret:
                    try:
                        secret = register()
                    except Exception:  # noqa: BLE001
                        time.sleep(15)
            else:
                log.error("Heartbeat HTTP error: %s", e)
        except Exception as e:  # noqa: BLE001
            log.error("Heartbeat failed: %s", e)
        time.sleep(HEARTBEAT)


if __name__ == "__main__":
    main()
