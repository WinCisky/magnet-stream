# Phase 0 spike report

Measurement of docs/architecture.md §Risks, open questions and delivery ›
Delivery (Phase 0) and › Risks (and §Cost model), done during the M1-08
review on a real Cloudflare account (free plan). It answers OQ-02 (CPU
limit of a free Durable Object) and OQ-03 (does the 6-connection limit also
apply to sockets opened later in the WebSocket's life?).

> Do not commit hostnames of real deployments, tokens, account ids or peer
> addresses here: describe them, don't paste them.

## How to run it

The spike can pull from either source:

- **a fake peer** (`tools/fake-peer`) on a host with a public IP and an open
  port ≥ 1024 — the spike rebuilds the same torrent from
  `--seed/--size/--piece-length`;
- **a real, legal torrent** (`--torrent file.torrent`) and candidate peers
  (`--peer`, `--peers-file`), e.g. a Linux ISO swarm, with peers from
  `services/seeders` running locally (`GET /peers?hash=…`). The spike keeps
  up to `--connections` peers delivering and replaces the ones that fail.

1. **Deploy the gateway** from `apps/gateway` (token from the environment,
   or `wrangler login`):

   ```sh
   npx wrangler deploy
   ```

   Check: `ALLOWED_ORIGINS` restrictive, `ALLOW_PRIVATE_PEERS` absent. To look
   past 6 connections, deploy the *test* worker with
   `npx wrangler deploy --var TUNNEL_MAX_CONNECTIONS:16`, and redeploy without
   it afterwards.
2. **Capture the Durable Object's CPU time**, in another terminal:

   ```sh
   npx wrangler tail <worker-name> --format json > tail.jsonl
   ```

   Each `TunnelSession` event has `cpuTime`, `wallTime` and `outcome`
   (`exceededCpu` / `exceededResources` would show there); its logs end with
   `tunnel.session_end` and the session counters. The same data is in the
   dashboard (*Workers & Pages → worker → Observability*).
3. **Run the spike**:

   ```sh
   # fake peer
   npm run spike -- --gateway wss://<deploy> --peer <seed-ip>:51413 --bytes 1GiB --size 1GiB --seed 7 --out spike.json
   # real torrent
   npm run spike -- --gateway wss://<deploy> --torrent debian-DVD-1.iso.torrent --peers-file peers.txt \
     --bytes 1GiB --connections 6 --later 60000 --out spike.json
   ```

   `--probe-open 8` (with `TUNNEL_MAX_CONNECTIONS` raised) probes past 6;
   `--no-probe` skips the probe.
4. **Remove or lock down** the test deployment afterwards
   (`npx wrangler delete`, or restrict `ALLOWED_ORIGINS`).

## What the spike's JSON contains

| field | meaning |
|---|---|
| `download.firstPieceMs` | from opening the WebSocket to the first verified piece |
| `download.bytes`, `ms`, `mibPerSecond` | verified payload, wall time, throughput |
| `download.piecesVerified`, `piecesCorrupt` | pieces verified / failing SHA-1 (re-fetched elsewhere) |
| `peers` | candidates tried, opened, delivering, failure reasons (counts only) |
| `websocket.upgrades` | WebSocket upgrades (each is one billed request) |
| `websocket.messagesSent` | client → gateway messages (the ones the Durable Object is billed for) |
| `websocket.messagesReceived` | gateway → client messages (not billed as requests) |
| `billing.estimatedRequests` | `upgrades + ceil(messagesSent / 20)` (incoming WebSocket messages are billed at 20:1) |
| `billing.estimatedRequestsPerGiB` | the same, scaled to 1 GiB |
| `connections` | the probe: N connections held on one WebSocket, then more after `laterAfterMs` |

## Measurements

Runs of 16–17 Nov 2026. Gateway: this repository's `apps/gateway`
(compatibility date 2026-09-01, wrangler 4.163) deployed on a free-plan
account under `*.workers.dev`. Client: a residential connection in Italy.
Source: the Debian 13.7 amd64 DVD-1 ISO swarm (3.7 GiB, 256 KiB pieces),
candidates from `services/seeders` (`/peers`, about 400 IPv4 peers). No
public host was available for a fake seed, so the real swarm was used,
which also measures the conditions the product will meet.

The three phase numbers (docs/architecture.md › Delivery):

| run | bytes | peers (delivering) | first verified piece | throughput | billed requests / GiB |
|---|---|---|---|---|---|
| 1 | 1 GiB | 6 (8 over the run) | 0.51 s | 4.7 MiB/s | 86 |
| 2 | 3 GiB | 6 (9 over the run) | 0.43 s | 8.1 MiB/s | 79 |

Every piece was SHA-1 verified; 2 pieces per run failed verification
(corrupt data from real peers), were dropped and fetched again from others.

### 1. CPU per gigabyte (OQ-02)

From `wrangler tail` (one `TunnelSession` event per WebSocket session: with
`accept()`, the whole session is a single event).

| run | bytes | wall time | DO CPU time | CPU per GiB | outcome |
|---|---|---|---|---|---|
| 1 | 1 GiB | 217 s | 11.5 s | 11.2 s (≈ 11 ms/MiB) | `ok` |
| 2 | 3 GiB | 380 s | 33.8 s | 11.0 s | `responseStreamDisconnected` (the client closing), no `exceededCpu` |
| smoke | 64 MiB | 39 s | 1.08 s | 17 s | |

- The free-plan Durable Object is **not** capped near 10 ms per event: one
  event used 33.8 s of CPU and was not cut. That is above the 30 s per
  request figure too; incoming messages (one every ~80 ms here, 4,739 in
  run 2) evidently keep renewing the allowance, as the documentation says
  for the paid limit. No `ping` was needed at this message rate.
- Cost: about **11 s of CPU per GiB**, almost all of it the per-chunk
  relay (≈ 330 k gateway→client messages per GiB, one per TCP read).
  A 2 GB film is ≈ 22 s of Durable Object CPU.
- Duration billing: run 2 kept the object busy 380 s for 3 GiB
  (≈ 127 s/GiB at this throughput).

### 2. Connection limit (OQ-03)

| case | outcome |
|---|---|
| 7th connection with the gateway's default limit (6) | refused by the gateway: `too_many_connections`, as specified |
| Connections opened later in the socket's life (after 30–60 s, slots freed by closing) | open in 0.1–0.7 s and deliver verified data |
| `TUNNEL_MAX_CONNECTIONS=8`: 7th and 8th with 6 established | open in 0.1–0.7 s |
| `TUNNEL_MAX_CONNECTIONS=16`: 14 established on one WebSocket | all 14 open; 12 delivered a verified piece concurrently (2 kept us choked) |
| 10 `open`s at once on a fresh WebSocket (limit 16) | all open within 0.53 s |
| 6 `open`s to unreachable hosts (SYN never answered), then reachable peers on the same WebSocket | reachable peers **queued**: they fail the 5 s gateway timeout until ~20 s after the dead connects started, then open normally |
| 1 unreachable host, then reachable peers | reachable peers open at once |
| A new WebSocket while another is stuck | opens at once (the limit is per WebSocket / invocation) |

Conclusions:

- The documented "6 open connections per invocation, the 7th queued" did
  not limit **established** connections on a Durable Object WebSocket: 14
  worked at once. What it does limit is connections **still connecting**:
  6 connects stuck in SYN block every new connect on that WebSocket for
  ~20 s (the platform's own connect timeout), and `close()` does not free
  them sooner. The gateway's 5 s connect timeout frees its own slot while
  the platform's is still held.
- Later-opened sockets behave like the first ones: the limit is about
  simultaneity, not about when in the WebSocket's life a socket opens.
- Practical effect seen in the swarm: dialling a list where most peers are
  dead (≈ 25 % of DHT/tracker peers answered a handshake) through one
  WebSocket starved it — of the same 90 candidates, 2 opened via Cloudflare
  and 16 directly, while the same reachable peers opened 38/40
  via Cloudflare when dialled without dead ones in front.

### 3. Billed requests

| run | upgrades | messages sent | estimated requests | per GiB |
|---|---|---|---|---|
| 1 | 2 | 1,677 | 86 | 86 |
| 2 | 1 | 4,739 | 238 | 79 |

Cross-check with the account analytics (GraphQL
`durableObjectsPeriodicGroups` / `durableObjectsInvocationsAdaptiveGroups`,
the data behind the dashboard), for 16 Nov 2026, the day of runs 1, 2 and
the smoke run (≈ 4.1 GiB in total, plus a few short probes):

| metric | value |
|---|---|
| Durable Object invocations (WebSocket upgrades) | 14 |
| inbound WebSocket messages | 6,847 (the spike counted 6,581 for the three runs) |
| billed requests (invocations + inbound / 20) | ≈ 356, i.e. ≈ 87 per GiB |
| Durable Object CPU time | 47.4 s (≈ 11.5 s per GiB) |
| active time | 1,064 s |
| `exceededCpuErrors` / `exceededMemoryErrors` / `fatalInternalErrors` | 0 / 0 / 0 |

The analytics agree with the spike's estimates. Upstream is one message per refill of one peer's
request queue (every 32 blocks of 16 KiB): ≈ 1,600 messages/GiB, about
twice the architecture's estimate. Coalescing all peers' frames into one
message every 300–500 ms (M2, "Batch upstream") brings it to ≈ 1–3 messages
per second whatever the throughput, i.e. ≈ 10–20 requests per GiB at these
rates.

## Decision

- [x] **Durable Object, free plan** — CPU per GiB (≈ 11 s) and per-event
      CPU (33.8 s without being cut) fit; requests per GiB (≈ 80 now,
      ≈ 10–20 with M2 batching) fit the daily budget; duration
      (≈ 130 s per GiB at 8 MiB/s) is the binding limit, as the cost model
      expects.
- [ ] **Worker/Durable Object on the paid plan** — only if the duration
      budget or an undocumented free-plan CPU cap shows up in production.
- [ ] **VPS gateway** — not needed as default; stays the fallback (M4).

Decision (M1-08): **the default data path is the Durable Object on the free
plan.** Reasons:

- CPU is not the obstacle feared in §Risks: ≈ 11 s per GiB, a 33.8 s event
  not cut, zero `exceededCpu` errors in the account analytics.
- Requests fit: ≈ 80–87 per GiB today against 100,000 per day, and the M2
  upstream batching should bring them to ≈ 10–20 per GiB.
- The binding free-plan limit is Durable Object duration (≈ 127 s per GiB
  at the measured 8 MiB/s), as the cost model predicted: a few films per
  user per day fit.
- Workers Paid stays the escape hatch if production meets an undocumented
  free-plan cap; the VPS gateway (M4) remains the fallback for swarms
  Cloudflare cannot reach.

Recorded in `implementation-plan.json` (`resolved_decisions` OQ-02, OQ-03).

Follow-ups for M2 (from OQ-03):

- **Engine (M2-06):** never let unknown candidates fill a WebSocket's
  connecting slots: at most 2–3 untested dials per socket, or a separate
  "scouting" WebSocket, and keep proven peers on their own sockets.
- **Gateway (M2-01):** with the default 6 the gateway never exceeds the
  established-connection limit it was designed around; the measurements
  allow raising it (14 worked). Keep 6 until M2-06 decides; consider a
  connect timeout aligned with the platform's (~20 s) or counting a failed
  connect's slot as busy for that long.

OQ-02 and OQ-03 are now in `resolved_decisions`; M2-01, M2-06 and M5-04 are
unblocked.
