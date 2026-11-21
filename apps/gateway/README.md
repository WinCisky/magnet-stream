# magnet-stream gateway

A Cloudflare Worker that holds BitTorrent peer connections open for the
browser. The page cannot open TCP itself, so it opens a WebSocket to this
worker; a Durable Object dials the peers and relays bytes. The browser
speaks peer-wire and checks every piece. The worker does not hash, store,
or choose blocks.

It is built for the Workers free plan. Incoming WebSocket messages are
billed 20:1, so the browser batches them. Details: `docs/tunnel-protocol.md`.

| variable | meaning |
|---|---|
| `ALLOWED_ORIGINS` | origins allowed to open tunnels; others get 403 before the upgrade |
| `ALLOW_PRIVATE_PEERS` | `1`: tunnels may reach private/loopback peers. `wrangler dev` only (`.dev.vars` / `--var`), honoured only for loopback request URLs; never in `wrangler.jsonc` |
| `TUNNEL_CONNECT_TIMEOUT_MS`, `TUNNEL_HANDSHAKE_TIMEOUT_MS` | default 5000 / 10000 |
| `TUNNEL_MAX_CONNECTIONS` | default 6 (the protocol rule); raise only on a Phase 0 spike deployment |
| `TUNNEL_IDLE_TIMEOUT_MS` | default 90000: idle connections and silent WebSockets are closed |
| `TUNNEL_SESSION_TIMEOUT_S` | default 7200: maximum session length |
| `TUNNEL_SOCKETS_PER_SESSION` | default 3 WebSockets per `?session=` object, 6 connections each |
| `TUNNEL_FAILED_CONNECT_HOLD_MS` | default 20000: a timed-out connect keeps its slot this long |
| `TUNNEL_UPGRADES_PER_MINUTE` | default 30 upgrades per minute per client IP, then 429 |
| `TUNNEL_MAX_PENDING_WRITE_BYTES` | default 4194304 (4 MiB): bytes held for a peer and not yet written to it; past this the peer is not reading and its connection is closed (`close` WRITE_OVERFLOW), the others go on |

Values are validated: an unset variable takes its default, a malformed or
out-of-range one makes the gateway refuse tunnels with 500
`gateway_misconfigured` (and log the variable's name).

Tests: `npm test` (unit), `npm run test:integration` (wrangler dev + fake
peers), `npm run e2e` (removed routes answer 404).

## API

### `GET /v1/tunnel`

WebSocket upgrade, subprotocol `magnet-tunnel.v1`. Optional
`?session=<32 hex>` shares one Durable Object across up to 3 sockets.
Frames and reason codes: `docs/tunnel-protocol.md`.

### `GET /health`

Returns `ok`.

Anything else, including the old block and info routes, is **404**.

## Security

- **Origin.** Browsers whose `Origin` is not in `ALLOWED_ORIGINS` get 403
  before the upgrade. Requests without `Origin` (curl, scripts) are allowed
  through to the tunnel rules.
- **Address policy.** The gateway dials only public addresses on ports
  ≥ 1024, unless `ALLOW_PRIVATE_PEERS` is set for local `wrangler dev`.
- **Handshake gate.** No byte is relayed until the remote proves it serves
  the info-hash in `open`.

## Development

```sh
npm install
cp .dev.vars.example .dev.vars
npm run dev                      # wrangler dev on :8787
npm test
npm run test:integration
npm run e2e
npm run check
```

## Deployment

Pushes to `main` deploy automatically after a green CI run
(`.github/workflows/deploy.yml`; setup and rollback in `docs/operations.md`).
By hand:

1. Authenticate. On a headless machine, create an API token with the "Edit
   Cloudflare Workers" template and `export CLOUDFLARE_API_TOKEN=...`.
   Otherwise run `npx wrangler login`.
2. Deploy:
   ```sh
   npm run deploy -- --var "ALLOWED_ORIGINS:https://<site origin>"
   ```
   `wrangler.jsonc` only allows `http://localhost:4321`.
3. Confirm `ALLOWED_ORIGINS` is restrictive and `ALLOW_PRIVATE_PEERS` is
   absent from the deployed configuration.
