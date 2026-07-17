# Replication Edge Appliance — Packaging, Installer & Service Specification

**Status:** Draft v0.1 — for discussion
**Scope of this document:** *only* the packaging / installer / service layer — how the edge
data-plane stack is delivered to a client, installed, kept running as a durable service, and
made reachable by the central application, **on any client operating system.**

Out of scope here (separate specs): the central-app code changes (per-client endpoint model,
`/api/edge/register`), the trimmed edge `docker-compose` internals, and connector logic. Those
are referenced where they touch packaging but specified elsewhere.

---

## 1. Why this exists

Today the entire data plane (Kafka, Kafka Connect/Debezium, Schema Registry) runs centrally on
jovoserver. When a client's **source and sink databases are on their own site**, every changed
row makes a wasteful round trip: client → jovoserver (capture, buffer, triple-write) → client
(write). The heavy database work lands right back on the client's box anyway, plus two network
hops and a fake 3-broker cluster in the middle.

The fix is to move the **data plane** next to the client's databases while keeping the **control
plane** (Django/Celery) central. The unit that carries the data plane to the client is the
**Replication Edge Appliance**.

**Core mental model — a self-registering appliance:** install once (as a VM or a service), it
phones home over an outbound tunnel, and the existing central Django drives it over HTTP exactly
as it drives the local Connect today.

---

## 2. What the appliance contains

A single, self-contained bundle running on the client's local network, next to their databases:

| Component | Role | Notes |
|---|---|---|
| **Kafka broker** | Local event buffer / log | Single broker, `RF=1`, short retention — *not* the 3-broker RF=3 central profile |
| **Kafka Connect (Debezium 3.3 + Avro)** | Source + sink connectors | Same image as central (`kafka-connect-debezium`) |
| **Schema Registry** | Avro schemas | Local to the appliance |
| **Edge Agent** | Enrollment, tunnel, heartbeat, self-update, watchdog | The piece that makes it an *appliance*, not just a compose file |

Everything is **Linux containers.** The host OS is normalized away by the container runtime or
by shipping our own Linux inside a VM (see §4).

> **Design note (thin appliance).** An earlier draft moved the **DDL consumer** onto the
> appliance to keep the tunnel HTTP-only. Now that the tunnel is **self-hosted Netbird** — a full
> WireGuard *mesh* with real L3 reachability, not an HTTP-only tunnel — central can reach the edge
> Kafka directly and safely. So the **DDL consumer stays central** and the appliance stays thin
> (Kafka + Connect + Schema Registry + agent). Only tiny DDL/metadata + control traffic crosses
> the mesh; the actual row data never leaves the client LAN. See §7.

**Stays central (never on the appliance):** Django app, management/fleet console, metadata DB,
Redis, alerting, all UI, **and the DDL consumer** (reaches the edge Kafka over the mesh).

---

## 3. Design principle: normalize the OS away

The stack is three JVM programs plus one Python process, all packaged as Linux containers. We do
**not** port them per OS. "Run on any system" is achieved by one of two moves:

1. **Bring a container runtime** to the host (Docker/Podman), or
2. **Bring our own OS** — ship a minimal Linux VM that carries the whole stack.

A Linux container behaves identically regardless of the host underneath. This is the entire
portability strategy.

---

## 4. Delivery form factors ("any kind of system")

We ship the **same stack** in three form factors. Together they cover essentially every client
environment.

### Form factor A — VM Appliance ✅ *(committed default)*
- A pre-built, hardened **minimal Linux image** with Docker + the stack + Agent baked in and set
  to auto-start on boot. First-boot config via cloud-init / a config seed.
- Exported in the formats client hypervisors accept: **OVA** (VMware/VirtualBox), **VHDX**
  (Hyper-V — built into every Windows Server), **QCOW2/raw** (KVM/Proxmox/OpenStack).
- Client action: *import the VM, give it a network, power it on.* Nothing else.
- **Pros:** OS-independent, self-contained, known-good environment, easiest to support.
  **Cons:** needs a hypervisor + RAM/disk; larger artifact; VM lifecycle to manage.

### Form factor B — Native host install (installer + OS service)
For clients who want it directly on an existing host, no VM.
- An installer detects the OS, ensures a container runtime, lays down the compose + config, and
  registers an **OS service** that runs the stack and starts on boot.
- **Linux:** `install.sh` (also packaged as `.deb` / `.rpm`) → ensures Docker/Podman → **systemd
  unit** → `docker compose up -d`.
- **Windows Server / 10 / 11:** signed **`.exe`/MSI** (WiX/Inno Setup) → ensures Docker or
  **Podman on WSL2** → registers a **Windows Service** (NSSM-wrapped) → autostart.
- **macOS** (rare for a server): `.pkg` → Docker → **launchd** service.
- **Pros:** no VM overhead, uses existing host. **Cons:** OS-specific installer variants to build
  and maintain; depends on host's runtime; more heterogeneous to support.

### Form factor C — Kafka-less native service *(constrained fallback)*
For locked-down hosts that permit **no** containers and **no** VM.
- **Debezium Server / embedded engine** as a native JVM service (Windows Service / systemd) — no
  Docker, no Kafka.
- **Trade-off:** loses the local buffer/replay resilience. Offer only when A and B are impossible,
  or keep that client on the central model.

> ⚠️ **Do not run the Kafka *broker* natively on Windows.** Kafka has a long, documented history
> of file-handle / log-segment-deletion failures on Windows. On Windows always use a Linux
> container (WSL2) or the VM appliance for the broker.

---

## 5. Cross-platform support matrix

| Client environment | Delivery | Runtime under the hood |
|---|---|---|
| Windows Server w/ Hyper-V | **VM appliance (VHDX)** | Linux VM |
| Windows Server / 10 / 11, no VM | Windows installer (.exe/MSI) | Docker or Podman on WSL2 |
| Linux server | `install.sh` / `.deb` / `.rpm` | Docker/Podman native |
| VMware / Proxmox / Nutanix shop | **VM appliance (OVA/QCOW2)** | Linux VM |
| Client cloud (AWS/Azure/GCP) | VM image / Terraform module | Linux instance |
| Locked-down, no container & no VM | Native Debezium Server service | JVM service (Form factor C) |

**The database's OS is irrelevant.** Debezium reaches source/sink over TCP/JDBC. A SQL Server on
Windows can be fed by a Linux appliance on the same LAN. "On the client's server" means **on the
client's local network** (sub-millisecond hop), not literally the same host.

---

## 6. The Edge Agent (what makes it an appliance)

A small supervisor process bundled in every form factor. Responsibilities:

- **Enroll:** on first boot, take a one-time **enrollment token** + central URL, authenticate to
  central, and bind this appliance to a client.
- **Tunnel:** bring up the **outbound** tunnel (WireGuard / Tailscale / Netbird) so central can
  reach it with **no inbound firewall change** at the client (see §8).
- **Register (phone home):** `POST /api/edge/register` with its reachable Connect URL, stack
  versions, and health. Central stores the per-client endpoint (replaces today's hardcoded global
  `KAFKA_CONNECT_URL`).
- **Heartbeat:** periodic health + version report to central; drives the fleet console's online/
  degraded/offline state.
- **Self-update:** on a central signal, pull pinned container image tags and roll the stack with
  rollback on failure.
- **Local watchdog:** restart unhealthy containers, rotate local logs, ship *health* (never data)
  to central.

---

## 7. Networking & reachability — self-hosted Netbird mesh

**Decision: self-hosted Netbird** (WireGuard mesh overlay we run ourselves — no third-party SaaS).

- **Outbound-initiated.** On boot the appliance runs `netbird up` with a per-appliance setup key
  and joins the mesh. It dials **out**; the client opens **no** inbound port.
- **Stable mesh address.** Netbird assigns the appliance a stable overlay IP (`100.x.y.z`).
  Central reaches the appliance's Connect (`:8083`), Kafka (`:9092`), Schema Registry, and Jolokia
  at that IP. The appliance reports this IP to central at registration.
- **Mesh carries Kafka safely.** Because Netbird is real L3 (not an HTTP-only tunnel), the edge
  Kafka can advertise a listener on its mesh IP and central's DDL consumer / peek / metrics reach
  it directly. The edge broker therefore runs **two listeners**: an internal one for on-box
  Connect, and a **mesh listener advertised on the Netbird IP** for central. This is the standard
  multi-listener pattern and the one subtlety worth getting right.
- **What crosses the mesh:** Connect REST (control), DDL/schema-history topics + peek (tiny),
  Jolokia metrics. **What never crosses:** the replicated row data — source→Connect→Kafka→Connect
  →sink all happen on the appliance.
- **Self-hosted vs. SaaS:** we run our own Netbird management + signal server (no external
  dependency, no per-node SaaS cost, data stays in our control) at the cost of operating that
  control plane ourselves.

---

## 8. Enrollment / install flow (unified across OS)

1. **Central:** operator clicks *Enroll appliance* → picks the client → picks the form factor →
   central mints a **one-time enrollment token** and serves the right artifact (OVA / installer /
   script).
2. **Client site:** field tech runs the installer or imports the VM, supplies the token (typed,
   config-seed file, or QR).
3. **Appliance:** Agent authenticates with the token → establishes the tunnel → starts the stack →
   `POST /api/edge/register`.
4. **Central:** marks the appliance **online**, stores its per-client Connect URL. Replication can
   now be driven remotely — identical to how central drives local Connect today.

Same four steps on every OS; only step 2's artifact differs.

---

## 9. Service & lifecycle requirements

- **Auto-start on boot** and **restart on crash** (systemd / Windows Service / launchd, plus
  container `restart: unless-stopped`).
- **Survive host reboot / power loss** — persistent volume for the Kafka log; clean recovery.
- **Ordered startup:** Kafka → Schema Registry → Connect → DDL consumer → Agent.
- **Resource guardrails** — memory limits sized for a small box (see §10).
- **Updates** — Agent-driven, version-pinned, rolling, with rollback; central controls cadence.
- **Observability** — local log rotation; health/heartbeat shipped to central; **no data leaves
  the client** except through the sink write itself.
- **Uninstall / decommission** — clean teardown, tunnel key revoked, appliance deregistered from
  central.

---

## 10. Sizing profiles

Single-broker `RF=1` edge is far lighter than the central 3× profile. Suggested tiers:

| Profile | vCPU | RAM | Disk | For |
|---|---|---|---|---|
| Small | 2 | 4 GB | 20 GB | Low-volume, few tables |
| Medium | 4 | 8 GB | 60 GB | Typical client |
| Large | 8 | 16 GB | 200 GB | High write volume / many tables |

Retention is short by design (edge is a relay, not an archive); disk is dominated by the Kafka
log + a safety window.

---

## 11. Security requirements

- Signed installers and signed/pinned container images.
- DB credentials and tunnel keys stored in the OS keystore / encrypted at rest on the appliance.
- Enrollment tokens are single-use and expire.
- Least-privilege service account; no inbound ports opened at the client.
- Appliance holds client DB credentials **and** a persistent tunnel into our network — this will
  require the client's security review; provide a hardening doc + data-flow diagram.

---

## 12. Open questions for management

**Resolved:**
- ✅ **Default form factor — VM appliance.** Native installers are secondary.
- ✅ **Tunnel — self-hosted Netbird mesh.** We run the Netbird control plane ourselves.

**Still open:**
1. **Who supplies the box/VM** — client hardware, client hypervisor, or we ship a physical
   mini-appliance?
2. **Netbird control plane hosting** — where our self-hosted Netbird management/signal servers
   live, and their HA.
3. **Update cadence & control** — auto-update vs. client-approved windows.
4. **Support model & SLA** — we're now debugging on client hardware over the mesh.
5. **Pricing** — per appliance / per client / bundled?
6. **Fallback policy** — which clients stay on the central model instead of an appliance?

---

## 13. Non-goals (for this phase)

- Multi-broker HA on the edge (defeats the "light box next to the DB" purpose).
- Running the Django app or UI on the appliance.
- Supporting database engines beyond those already supported centrally.

---

## 14. Rough phasing

1. **M1 — Manual edge:** trimmed compose runs on a Linux box; central points at it via a manually
   set per-client URL. Proves the data-locality win.
2. **M2 — Agent + registration:** phone-home, tunnel, `/api/edge/register`, per-client endpoint
   model. Turns it into a self-registering appliance.
3. **M3 — VM appliance:** OVA/VHDX build pipeline + first-boot config. The turnkey default.
4. **M4 — Native installers:** Linux `.deb`/`.rpm` + Windows MSI + service wrappers.
5. **M5 — Fleet console + self-update:** management UI (see wireframe), update orchestration,
   decommission.
