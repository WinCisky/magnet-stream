# Phase 0 spike report

Template for the measurement of docs/architecture.md §Risks, open
questions and delivery › Delivery (Phase 0) and › Risks (and §Cost model). It is filled in by hand
during the M1-08 review, on a real Cloudflare account (free plan), and it
closes OQ-02 (CPU limit of a free Durable Object) and OQ-03 (does the
6-connection limit also apply to sockets opened later in the WebSocket's
life?).

> Do not commit hostnames of real deployments, tokens, account ids or peer
> addresses here: describe them ("test seed on a VPS in eu-central"), don't
> paste them.

## How to run it

1. **Test seed**, reachable from the Internet (any host with a public IP
   and a port ≥ 1024 open):

   ```sh
   npm run fake-peer -- --host 0.0.0.0 --port 51413 --size 1GiB --seed 7
   ```

   It prints the info-hash; the spike rebuilds the same torrent from
   `--seed/--size/--piece-length`, so pass the same values to both.
2. **Deploy the gateway** from `apps/gateway` with a personal API token
   scoped to *Edit Cloudflare Workers*, read from the environment only:

   ```sh
   export CLOUDFLARE_API_TOKEN=...   # never written to a file in the repo
   npx wrangler deploy
   ```

   Check before running: `ALLOWED_ORIGINS` is restrictive and
   `ALLOW_PRIVATE_PEERS` is **absent** (it is never in `wrangler.jsonc`; do not
   add it as a secret). For the connection-limit probe past 6 you may set
   `TUNNEL_MAX_CONNECTIONS` (e.g. 8) on this test deployment only.
3. **Run the spike** (from any machine with Node ≥ 22.18):

   ```sh
   npm run spike -- --gateway wss://<deploy> --peer <seed-ip>:51413 \
     --bytes 1GiB --size 1GiB --seed 7 --out spike-1gib.json
   ```

   Useful variants: `--connections 6` (six parallel connections on one
   WebSocket), `--later 60000` (open the "later" connections after a minute),
   `--probe-open 8` (with `TUNNEL_MAX_CONNECTIONS=8` on the gateway).
4. **Read the CPU time** in the dashboard: *Workers & Pages → the worker →
   Observability (Workers Logs)*. Filter on the Durable Object invocations of
   the run (the `TunnelSession` class; the gateway logs
   `tunnel.session_start` / `tunnel.session_end` events, the latter with the
   session's message and byte counters). For each invocation note
   **CPU time** and **wall time**; sum the CPU time of the session's
   invocations (WebSocket events of a Durable Object are billed and limited
   per event) and divide by the GiB transferred. Note also any
   `exceededCpu` / `exceededResources` outcome or session that ended early.
5. **Remove or lock down** the test deployment afterwards
   (`npx wrangler delete`, or restrict `ALLOWED_ORIGINS` and the route).

## What the spike's JSON contains

| field | meaning |
|---|---|
| `download.firstPieceMs` | from opening the WebSocket to the first verified piece |
| `download.bytes`, `ms`, `mibPerSecond` | verified payload, wall time, throughput |
| `download.pieces*` | verified / corrupt / missing pieces (all must verify) |
| `websocket.upgrades` | WebSocket upgrades (each is one billed request) |
| `websocket.messagesSent` | client → gateway messages (the ones the Durable Object is billed for) |
| `websocket.messagesReceived` | gateway → client messages (not billed as requests) |
| `billing.estimatedRequests` | `upgrades + ceil(messagesSent / 20)` (incoming WebSocket messages are billed at 20:1) |
| `billing.estimatedRequestsPerGiB` | the same, scaled to 1 GiB |
| `connections.initial[]` | opening 6 (+1) connections on one WebSocket: time to `opened`, whether a block came through |
| `connections.seventh` | outcome of the 7th connection |
| `connections.later[]` | after `laterAfterMs`, two slots are freed and three connections opened: do they open and carry data? |

## Measurements

Date / wrangler & compatibility date / region of the seed:
`…`

The three numbers every phase is measured on (docs/architecture.md ›
Delivery): seconds to first verified piece (`download.firstPieceMs`),
sustained MiB/s (`download.mibPerSecond`), billed requests per GiB
(`billing.estimatedRequestsPerGiB`).

### 1. CPU per gigabyte (OQ-02)

| run | bytes | connections | first piece (s) | throughput (MiB/s) | total CPU (ms) | CPU per GiB (ms) | max CPU of one event (ms) | errors / exceeded |
|---|---|---|---|---|---|---|---|---|
| 1 | 1 GiB | 1 | | | | | | |
| 2 | 1 GiB | 6 | | | | | | |

Observed CPU limit behaviour of the free Durable Object (30 s per request
or ~10 ms per event?): `…`

### 2. Connection limit (OQ-03)

| case | outcome |
|---|---|
| 7th connection opened at once (gateway limit 6) | |
| 7th/8th with `TUNNEL_MAX_CONNECTIONS` raised (platform behaviour) | |
| connections opened later in the socket's life (after `…` s) | |

Conclusion: does the limit of 6 also apply to sockets opened later? `…`

### 3. Billed requests

| run | upgrades | messages sent | estimated requests | per GiB |
|---|---|---|---|---|
| 1 | | | | |
| 2 | | | | |

Compare with the request count in the dashboard (*Metrics → Requests*) for
the same time window: `…`

## Decision

Pick the default data path (docs/architecture.md §Cost model):

- [ ] **Durable Object, free plan** — CPU per GiB and per-event CPU stay
      within the free limits with margin; requests per GiB fit the daily
      budget.
- [ ] **Worker/Durable Object on the paid plan** — works, but needs the paid
      CPU or request allowance.
- [ ] **VPS gateway** — the Durable Object path is not viable (CPU limit hit,
      connection limit too strict, or cost); the VPS gateway (M4) becomes the
      default.

Decision and reasons: `…`

Record the outcome in `implementation-plan.json` too: close OQ-02 and OQ-03
(move them from `open_questions` to `resolved_decisions`) and unblock M2-01,
M2-06 and M5-04.
