#!/usr/bin/env bash
# =============================================================================
# start-edge.sh — bring up the Netbird mesh, then the edge data-plane stack.
# Invoked by the replication-edge systemd service (Linux) or the Windows
# service wrapper. Idempotent: safe to run on every boot.
# =============================================================================
set -euo pipefail

EDGE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${EDGE_DIR}/.env.edge"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "FATAL: ${ENV_FILE} not found. Copy .env.edge.example and fill in enrollment values." >&2
  exit 1
fi

# shellcheck disable=SC1090
set -a; source "${ENV_FILE}"; set +a

# --- 1. Join the Netbird mesh (host-level; manages the host network) ----------
if ! command -v netbird >/dev/null 2>&1; then
  echo "FATAL: netbird client not installed on host." >&2
  exit 1
fi

if [[ -n "${NETBIRD_SETUP_KEY:-}" ]]; then
  # `netbird up` is idempotent — no-op if already connected.
  netbird up --setup-key "${NETBIRD_SETUP_KEY}" || true
fi

# Wait for the mesh IP to be assigned, then persist it for docker-compose.
echo "Waiting for Netbird mesh IP..."
for _ in $(seq 1 30); do
  MESH_IP="$(netbird status --json 2>/dev/null | grep -oE '"netbirdIp":\s*"[0-9.]+"' | grep -oE '[0-9.]+' | head -n1 || true)"
  [[ -n "${MESH_IP:-}" ]] && break
  sleep 2
done

if [[ -z "${MESH_IP:-}" ]]; then
  echo "FATAL: could not determine Netbird mesh IP." >&2
  exit 1
fi
echo "Netbird mesh IP: ${MESH_IP}"

# Write NETBIRD_IP back into .env.edge so Kafka advertises on the mesh address.
if grep -q '^NETBIRD_IP=' "${ENV_FILE}"; then
  sed -i "s|^NETBIRD_IP=.*|NETBIRD_IP=${MESH_IP}|" "${ENV_FILE}"
else
  echo "NETBIRD_IP=${MESH_IP}" >> "${ENV_FILE}"
fi

# --- 2. Bring up the data-plane stack ----------------------------------------
cd "${EDGE_DIR}"
docker compose --env-file "${ENV_FILE}" up -d --remove-orphans

echo "Edge stack started. Central will see this appliance once the agent registers."
