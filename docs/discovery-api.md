# Discovery API: `GET /peers` (streamed)

Peer discovery and scouting on the VPS (`services/seeders`), as in
docs/architecture.md §Chunk recovery › Session start (step 2), › Scouting on
the VPS, and §Target architecture (Discovery and scout); decisions OQ-21 and
OQ-22: **one request per session**, answered as a stream of
newline-delimited JSON. Every tracker (UDP and HTTP, the magnet's `tr=` plus
the configured defaults) and a DHT `get_peers` lookup run in parallel; each
address they return is **probed** (TCP connect, BitTorrent handshake, the
first messages), **scored** from 0 to 1 and written with what the probe
learned, so the browser dials live peers first. No polling, no tokens: the
browser dials the addresses itself through a tunnel
(docs/tunnel-protocol.md), whose handshake gate still checks they serve the
torrent. Scores are advice for the dial order, nothing more.

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
lookup; it goes away with `/swarm` and `/files` in M5-01.)

## Response

`200`, `Content-Type: application/x-ndjson`, `Cache-Control: no-store`,
`X-Accel-Buffering: no` (so nginx does not hold lines back). One JSON object
per line, every address line has `peer` and `score`:

```
{"peer":"203.0.113.7:51413","score":0.91,"alive":true,"seed":true,"md":true,"unchoked":true,"have":1.00,"rtt":38}
{"info":"ZDY6bGVuZ3Ro…","size":31744}
{"peer":"198.51.100.4:6881","score":0.61,"alive":true,"seed":false,"md":false,"unchoked":false,"have":0.25,"rtt":120}
{"peer":"[2001:db8::1]:6881","score":0.20}
{"peer":"192.0.2.9:40000","score":0.02,"alive":false}
{"done":true,"count":4,"cached":false,"complete":true,"alive":2,"dead":1,"unknown":1}
```

| field | meaning |
|---|---|
| `peer` | `ip:port` (IPv6 in brackets), **each address once** per response. Private, loopback, link-local, CGNAT, multicast, documentation and reserved addresses and ports below 1024 are dropped (the gateways refuse them anyway) |
| `score` | 0..1, two decimals. About 0.5 for a completed handshake, more for a seed (or the share of pieces), for offering the metadata, for unchoking the probe and for a short round trip; about 0.02 for a dead address; **0.2 for an address not probed**. A remembered score fades toward 0.2 with a half-life of 30 min (`SCOUT_HALF_LIFE_SECS`) |
| `alive` | present when probed: `true` if it completed the BitTorrent handshake for this info-hash, `false` if the connect was refused or timed out, the handshake failed, or it named another torrent. Absent: not probed (unknown) |
| `seed` | it has every piece (`have_all`, or a full bitfield) |
| `have` | share of pieces it has, 0..1 (absent if it did not say). Without the piece count it is estimated from the bitfield's length |
| `md` | it offers the metadata (`ut_metadata` and `metadata_size`, BEP 9) |
| `unchoked` | it unchoked the probe within `PROBE_UNCHOKE_WAIT_MS` |
| `rtt` | TCP connect time from the VPS, ms (absent if unknown) |

The **`info` line** (at most one per response) carries the torrent's info
dict, base64 (standard alphabet, with padding), and its size in bytes. It
is written as soon as a verified info dict is available, **first of all**
when it is already in the VPS's cache. It is always verified against the
info-hash: SHA-1 for v1 and hybrid torrents, SHA-256 truncated to 20 bytes
for v2-only ones. The browser checks it again before using it. A response
may have no `info` line (no live peer offered a verified one in time), and
clients ignore lines they don't know.

Order:

1. Addresses this info-hash had **probed in the last 10 minutes**
   (`SCOUT_REPROBE_SECS`) come first, from memory, best first, live ones
   only (they are not probed again).
2. Then **live** addresses as their probe ends.
3. **Unknown** addresses (`score` 0.2, no `alive`): at once when the
   info-hash used its probe budget (`MAX_PROBE_PEERS` probes per 10
   minutes); otherwise, whatever is still unprobed **1 s before the
   deadline**, and any address found after that.
4. **Dead** addresses last, just before `done`. An address learned again
   later (peer exchange) may have come back, so the browser can still try
   them last.

The last line is always `done`: `count` addresses were sent (`alive` +
`dead` + `unknown`); `cached` if the lookup came from the cache; `complete`
is `false` when the stream deadline cut sources that were still running.
The response closes when every source and probe has finished, and **at the
latest after ~30 s** (`PEERS_STREAM_SECS`), whatever the sources are doing.

A finished lookup is cached per info-hash for **5 minutes**
(`CACHE_TTL_SECS`; 30 s when it found nothing, `EMPTY_CACHE_TTL_SECS`): a
later request replays its addresses at once with their **current** scores
(probing any it has no fresh score for), then `done` with `cached: true`. A
lookup the client abandoned is not cached; probes it had started finish and
are remembered.

Clients read lines as they arrive (`fetch` + a streaming reader), dial in
score order, and treat a connection that ends without the `done` line as a
normal end of discovery. A client that reads only `peer` still gets every
address.

### Metadata

- **When.** As soon as the scout finds live peers offering `ut_metadata`
  (`md`), they are handed to the metadata fetcher. It asks up to **3 at a
  time**; the first answer that hashes to the info-hash wins, and the others
  are dropped. An answer that doesn't match is never served or cached, and
  the next peer is asked. A fetch gives up after `METADATA_DEADLINE_MS`
  (15 s), or once every peer it was given has failed.
- **Single-flight, no queue.** One fetch per info-hash at a time: concurrent
  streams (and `/metadata`) for the same magnet share it. At most
  `METADATA_FETCHES` (4) fetches in flight in total. Beyond that, an offer
  is dropped (nothing is queued), and a peer found later tries again. A
  stream stays open for a fetch in the air, up to the stream deadline. A
  fetch that ends after its stream closed still fills the cache.
- **Memory** (OQ-22). Verified info dicts are kept in memory, weighed in
  bytes: `METADATA_CACHE_MB` (32 MiB). One over
  `METADATA_CACHE_MAX_ENTRY_MB` (4 MiB) is served to whoever waited for it
  but not cached. A peer may announce at most 8 MiB (checked before
  anything is allocated), and buffers come out of a budget of
  `METADATA_FETCHES` × 8 MiB.

### Scouting

- **Probe.** TCP connect (`PROBE_CONNECT_TIMEOUT_MS`, 2.5 s), handshake
  with the extension and fast bits, then `have_none`, our extended
  handshake and `interested`; the probe reads bitfield / `have_all` /
  `have_none` / `have`, the peer's extended handshake and an unchoke until
  it knows all three or `PROBE_UNCHOKE_WAIT_MS` (2.5 s) passes, within
  `PROBE_TIMEOUT_MS` (5 s) in all. It never requests pieces.
- **Limits.** At most `PROBE_CONCURRENCY` (256) probes in flight in total
  (shared with the legacy `/swarm`), `SCOUT_PER_STREAM` (64) per stream,
  `MAX_PROBE_PEERS` (1,000) per info-hash per `SCOUT_REPROBE_SECS` window,
  and one probe per (info-hash, address) at a time: concurrent streams for
  the same magnet share it. A probe whose every stream went away before it
  started is skipped.
- **Memory** (decision OQ-22: in memory only, nothing on disk, lost on
  restart). Scores per info-hash, at most `SCOUT_MAX_ADDRS` (1,000)
  addresses each (the oldest dead ones are dropped first), weighed at
  about 96 bytes per address, `SCOUT_CACHE_MB` (16 MiB) in total; an
  info-hash nobody asked about for `SCOUT_IDLE_SECS` (2 h) is forgotten.
  Discovery results are weighed in bytes too: `PEERS_CACHE_MB` (16 MiB).
- **Development.** `ALLOW_PRIVATE_PEERS=1` lets streams carry and probe
  loopback and private addresses (local tests only); the port rule stays.

## `GET /metadata?ih=`

The raw verified info dict, `application/octet-stream`,
`Cache-Control: public, max-age=31536000, immutable` (content-addressed).
`ih` takes the same forms as for `/peers`; `magnet=` and the legacy `hash=`
also work.

- **From the cache, if it is there:** no tracker, DHT or peer is contacted.
- **Otherwise,** a discovery runs as for `/peers` (its lines are not sent)
  and the metadata is fetched as above. The answer comes as soon as it is
  verified.
- **Same per-IP and total limits as `/peers`** (each request counts, and a
  miss holds a stream slot while it runs).

| status | when |
|---|---|
| 200 | the info dict |
| 400 | malformed `ih`, `magnet` or `hash` |
| 404 | no peer offered an info dict that hashes to the info-hash (discovery ended, or every fetch failed) |
| 429 | over a limit, `Retry-After: 30` |
| 503 | every metadata fetch slot is busy (`METADATA_FETCHES`) |
| 504 | still nothing verified at the deadline (`PEERS_STREAM_SECS`) |

Errors are JSON `{"error": "…"}` with `Cache-Control: no-store`; nothing is
cached, so a retry tries again.

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
or peer addresses: request spans carry the method and path only, a
refusal is logged with its code, and a finished stream logs only its
counts (alive, dead, unknown, whether it carried the info dict) at debug
level. A metadata fetch logs only its outcome and counts.
