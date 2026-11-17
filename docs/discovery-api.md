# Discovery API: `GET /peers` (streamed)

Peer discovery on the VPS (`services/seeders`), as in
docs/architecture.md §Chunk recovery › Session start (step 2) and §Target
architecture (Discovery): **one request per session**, answered as a stream
of newline-delimited JSON. Every tracker (UDP and HTTP, the magnet's `tr=`
plus the configured defaults) and a DHT `get_peers` lookup run in parallel;
each address is written **the moment a source returns it**. No probing, no
polling, no tokens: the browser dials the addresses itself through a tunnel
(docs/tunnel-protocol.md), whose handshake gate checks they serve the
torrent.

## Request

```
GET /peers?ih=<info-hash>
GET /peers?magnet=<url-encoded magnet URI>
GET /peers?ih=<info-hash>&magnet=<url-encoded magnet URI>
```

| parameter | meaning |
|---|---|
| `ih` | info-hash: 40 hex digits or 32 base32 characters (v1, SHA-1), or 64 hex digits (v2 SHA-256, BEP 52), truncated to its first 20 bytes as DHT and trackers use it |
| `magnet` | a `magnet:` URI: its `tr=` trackers are announced to as well (at most 50 are read; longer than 8 KiB is refused). Its info-hash is `urn:btih:` (v1), or `urn:btmh:1220<64 hex>` (v2, truncated); a hybrid magnet uses the v1 hash. With `ih` too, the two must name the same info-hash |

Public endpoint, no authentication (OQ-09). CORS allows only the origins in
`ALLOWED_ORIGINS` (the frontend site; local development origins only in a
development configuration).

(`GET /peers?hash=…` alone still returns the PoC's JSON list after a 3 s
lookup; it goes away with `/swarm`, `/metadata` and `/files` in M5-01.)

## Response

`200`, `Content-Type: application/x-ndjson`, `Cache-Control: no-store`,
`X-Accel-Buffering: no` (so nginx does not hold lines back). One JSON object
per line:

```
{"peer":"203.0.113.7:51413"}
{"peer":"[2001:db8::1]:6881"}
…
{"done":true,"count":2,"cached":false,"complete":true}
```

- `peer` lines: `ip:port` (IPv6 in brackets), **each address once** per
  response. Private, loopback, link-local, CGNAT, multicast, documentation
  and reserved addresses and ports below 1024 are dropped (the gateways
  refuse them anyway).
- The last line is always `done`: `count` peers were sent; `cached` if
  they came from the cache; `complete` is `false` when the stream deadline
  cut sources that were still running.
- The response closes when every source has finished, and **at the latest
  after ~30 s** (`PEERS_STREAM_SECS`), whatever the sources are doing.
- A finished lookup is cached per info-hash for **5 minutes**
  (`CACHE_TTL_SECS`; 30 s when it found nothing, `EMPTY_CACHE_TTL_SECS`):
  a later request gets the cached addresses at once, then `done` with
  `cached: true`. A lookup the client abandoned is not cached.

Clients read lines as they arrive (`fetch` + a streaming reader), dial
each address immediately, and treat a connection that ends without the
`done` line as a normal end of discovery.

## Errors

JSON bodies `{"error": "…"}`, `Cache-Control` unset.

| status | when |
|---|---|
| 400 | `ih` missing or malformed, magnet unusable, or `ih` and magnet disagree. No network request is made |
| 429 | over a limit (below), `Retry-After: 30`. No lookup is started. `error`: `rate_limited`, `too_many_streams_from_client`, `too_many_streams` |

## Limits (OQ-14)

| limit | default | variable |
|---|---|---|
| requests per minute per client IP | 30 | `PEERS_PER_IP_PER_MINUTE` |
| concurrent streams per client IP | 4 | `PEERS_STREAMS_PER_IP` |
| concurrent streams in total | 200 | `PEERS_STREAMS_TOTAL` |
| stream length | 30 s | `PEERS_STREAM_SECS` (1–120) |

The client IP is the one the trusted reverse proxy puts in
`CLIENT_IP_HEADER` (default `X-Real-IP`, set by the nginx config in
`services/seeders/deploy/`); the header is trusted only on connections
from loopback, i.e. from nginx on the same host, otherwise the TCP peer
address is used. The IP is used only as the limiter's key.

## Logging

Logs never contain info-hashes, magnets, tracker URLs from magnets, client
or peer addresses: request spans carry the method and path only, and a
refusal is logged with its code.
