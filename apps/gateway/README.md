# magnet-worker

A Cloudflare Worker that fetches torrent blocks from BitTorrent peers for
[magnet-watcher](https://github.com/WinCisky/magnet-watcher). Browsers can't
open TCP connections to peers, so the page asks this worker for blocks and
verifies every piece's SHA-1 itself. The worker never hashes, buffers or
parses piece data.

It is built for the **Workers free plan** (10 ms CPU per request). After a
short setup, the peer's socket is piped straight into the response. That
pipe runs inside the runtime rather than in JavaScript, so CPU time per
request stays roughly constant however many megabytes flow.

> Monorepo note: this is `apps/gateway` (ex magnet-worker). Besides the
> legacy `/v1/blocks` route below (removed in M2-08), it now serves the
> **tunnel**: `GET /v1/tunnel` (WebSocket, subprotocol `magnet-tunnel.v1`),
> one Durable Object (`TunnelSession`, SQLite-backed, stores nothing) per
> WebSocket. Frames, rules and reason codes: `docs/tunnel-protocol.md`.
>
> | variable | meaning |
> |---|---|
> | `ALLOWED_ORIGINS` | origins allowed to open tunnels; others get 403 before the upgrade |
> | `ALLOW_PRIVATE_PEERS` | `1`: tunnels may reach private/loopback peers. `wrangler dev` only (`.dev.vars` / `--var`), honoured only for loopback request URLs; never in `wrangler.jsonc` |
> | `TUNNEL_CONNECT_TIMEOUT_MS`, `TUNNEL_HANDSHAKE_TIMEOUT_MS` | default 5000 / 10000 |
> | `TUNNEL_MAX_CONNECTIONS` | default 6 (the protocol rule); raise only on a Phase 0 spike deployment |
>
> Tests: `npm test` (unit), `npm run test:integration` (wrangler dev + fake
> peers), `npm run e2e` (legacy `/v1/blocks`, random `TOKEN_SECRET`).

## API

### `GET /v1/blocks`

| Param | Meaning |
|---|---|
| `ih` | Info-hash, 40 hex chars |
| `pl` | Piece length, in bytes |
| `len` | Total torrent length, in bytes (the last piece and last block are shorter) |
| `want` | Blocks to fetch: `12,13,14:0-63,15:7` (whole pieces, inclusive 16 KiB block ranges, single blocks), at most 2000 blocks |
| `c` | Candidate peer, `addr\|exp\|token`, repeated 1–40 times, best first. Tokens come from magnet-seeders' `/swarm` |
| `wait` | Milliseconds to wait for an unchoke (default 8000, 1000–30000) |
| `hold` | Milliseconds a handshaken but still choked candidate keeps its connection while others wait their turn (default `wait`, 500–30000) |
| `stagger` | Milliseconds the first candidate is tried alone before the others join in (default 0: all at once, 0–30000). They join at once if it fails |

The worker races the candidates, in the given order, with at most 6
connections open at once (the Workers limit per invocation). It connects to
each, handshakes (extension and fast bits set), sends have-none (when both
sides are fast) plus interested, and waits for an unchoke. Each candidate
that fails (refused, timed out after 3 s, hung up) makes room for the next
one, and so does one still choked `hold` ms after we started connecting to
it, as long as someone is waiting. So a long list where most peers are dead
still finds the live ones quickly. With `stagger`, the first candidate
(the client's best pick) gets a head start, so a fallback that happens to
unchoke a moment sooner doesn't take the transfer from it.

The first peer that unchokes while holding some of the wanted pieces wins,
and the others are closed. The winner is sent requests for the wanted
blocks it has, capped at its `reqq` (default 250).

**200** `application/octet-stream`:

```
[u32 big-endian n][n bytes of JSON preamble][the peer's raw bytes, starting with its handshake]
```

```json
{
  "v": 1,
  "peer": "71.250.8.233:9076",
  "reqq": 500,
  "client": "qBittorrent/4.6.7",
  "sent": "10,11,12,13",
  "blocks": 32,
  "ms": { "connect": 122, "handshake": 154, "unchoke": 155 },
  "others": [
    { "peer": "173.56.47.230:6881", "err": "lost", "hs": 1 },
    { "peer": "98.11.4.2:51413", "err": "connect_timeout" },
    { "peer": "5.9.13.37:6881", "err": "untried" }
  ]
}
```

`sent` is what was actually requested, in `want` syntax. Anything missing
from it (pieces the peer lacks, or blocks beyond its queue depth) needs
another request. The stream then carries ordinary peer-wire messages: piece,
have, choke, reject, keep-alive. **The response never ends on its own.**
Abort the request once the blocks are in, or on a choke or reject.

**Errors** (JSON `{error, detail?, others?}`):
- 400 `bad_request`
- 403 `bad_token` or `origin_not_allowed`
- 502 `no_peer`, with a reason per candidate in `others`: `connect_failed`, `connect_timeout`, `handshake_timeout`, `bad_handshake`, `infohash_mismatch`, `choked`, `closed`, `no_pieces`, `protocol`, `lost`, `evicted`, `untried`, `bad_token`

In `others`:
- `lost`: it was still setting up when another candidate won.
- `evicted`: still choked after `hold`, so its connection went to the next candidate.
- `untried`: the race ended before its turn (says nothing about the peer).
- `hs: 1`: it completed the handshake (it's reachable and has the torrent).

### `GET /v1/info`

What this worker supports, so clients can adapt to older deployments (which
answer 404 and take 3 candidates):

```json
{ "v": 2, "maxCandidates": 40, "maxOpen": 6 }
```

### `GET /health`

Returns `ok`.

## Security

- **Tokens.** The worker only connects to a peer if the request carries a
  valid, unexpired token for that exact (info-hash, peer address):
  `base64url(HMAC-SHA256(TOKEN_SECRET, "mw1|<ih hex>|<addr>|<exp>"))`, minted
  by magnet-seeders with the same secret. So it can't be used to open
  connections to arbitrary hosts. And even to a listed peer, it only ever
  sends BitTorrent messages it builds itself.
- **CORS.** Browsers can only read responses from `ALLOWED_ORIGINS`.
  Requests without an `Origin` header (curl, scripts) are still gated by
  the tokens.

## Development

```sh
npm install
cp .dev.vars.example .dev.vars   # TOKEN_SECRET for local dev
npm run dev                      # wrangler dev on :8787
npm test                         # unit tests (wire, params, tokens)
npm run e2e                      # real worker under wrangler dev vs. fake seeds
npm run check                    # wrangler types + tsc
```

`npm run e2e` starts `wrangler dev` itself and scripts fake seeds on
localhost. It covers:
- a full download with every piece hash-checked
- racing
- a long candidate list, mostly dead or choked
- slow and never-unchoking peers
- a mid-stream choke
- partial peers
- refused and hung-up connections
- the peer connection being freed after the client aborts
- bad or expired tokens
- origins
- IPv6

## Deployment

1. Use the same random string on both sides:
   ```sh
   openssl rand -base64 32
   ```
   - on the VPS: `WORKER_TOKEN_SECRET=<it>` in `/etc/magnet-seeders/magnet-seeders.env`
   - here: `npx wrangler secret put TOKEN_SECRET`
2. Authenticate. On a headless machine, create an API token with the "Edit
   Cloudflare Workers" template and `export CLOUDFLARE_API_TOKEN=...`.
   Otherwise run `npx wrangler login`.
3. Deploy:
   ```sh
   npm run deploy
   ```
   `wrangler.jsonc` routes the custom domain `magnet-worker.opentrust.it`. If
   that zone isn't in your account, remove the `routes` entry and use the
   `*.workers.dev` URL.
4. Check CPU time per `/v1/blocks` request in the dashboard (Workers Logs are
   enabled). It should stay in the low milliseconds whatever the response
   size.

Free-plan budget:
- 100,000 requests/day. One request fetches up to `reqq` × 16 KiB (4–32 MiB
  for typical seeds), so a 2 GB video takes a few hundred successful
  requests.
- 50 subrequests per request: each candidate costs one connect, hence at
  most 40 candidates.
- 6 simultaneous outgoing connections: the race never has more open.
- 10 ms CPU: the race is a little JavaScript per candidate, and the
  transfer itself is a native pipe.

### Closing peer connections

The browser aborts the request as soon as it has its blocks. By then the
pipe is usually idle, because the peer has nothing left to send, and an idle
pipe only notices a hang-up on its next write. Two things free the peer's
connection slot anyway:
- `request.signal` (the `enable_request_signal` compatibility flag) closes
  the socket as soon as the runtime notices the disconnect.
- Each response is capped at the time its blocks would take at 32 KB/s
  (20–120 s).

Locally (plain HTTP/1.1), `wrangler dev` often doesn't notice the hang-up,
so the cap is what closes the socket. Over HTTP/2 and HTTP/3 the browser's
abort resets the stream explicitly. After deploying, confirm in Workers Logs
that `/v1/blocks` invocations end right after the browser's abort.
