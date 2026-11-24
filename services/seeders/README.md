# magnet-seeders

A small, fast HTTP API that takes a magnet link (or a bare info-hash) and
returns the list of peers currently in that torrent's swarm.

Peers are gathered from two independent sources, in parallel, under one hard
deadline:

- **Trackers** — every tracker listed in the magnet link (`tr=`), plus a
  built-in list of reliable public trackers, announced to over UDP (BEP 15)
  and HTTP(S) (BEP 3/23/7).
- **The BitTorrent mainline DHT** — a `get_peers` lookup (BEP 5), useful when
  the magnet's trackers are dead or the magnet has none.

A source that errors out or is too slow just contributes nothing; the
response is whatever peers were found by the deadline. Results are cached
per info-hash so that repeated lookups (e.g. from the same web page) return
instantly and don't re-query trackers/DHT on every request.

For [magnet-watcher](https://github.com/WinCisky/magnet-watcher)'s in-browser
piece recovery, three more endpoints go further:

- **`/swarm`** probes every peer over TCP (handshake, bitfield, extension
  handshake, a quick "interested"). It reports which peers are actually
  reachable, which are seeds, the pieces each partial peer has, how fast
  each unchokes, and how many requests each queues. It also returns a
  signed token per peer for
  [magnet-worker](https://github.com/WinCisky/magnet-worker). Swarms that
  clients keep asking about are re-announced and re-probed in the
  background every 5 minutes.
- **`/metadata`** returns the torrent's info dict (piece length and SHA-1
  piece hashes included), fetched from peers via BEP 9 and verified against
  the info-hash.
- **`/files`** lists the torrent's files (paths, sizes, offsets) from that
  same info dict, as JSON: what the page shows to pick a video.

> **Monorepo note:** the browser engine uses the **streamed**
> `GET /peers?ih=…` (NDJSON, closed after ~30 s, per-IP limits): every
> address is probed, scored 0..1 and written with what the probe learned,
> live peers first, with scores remembered in memory (`src/scout.rs`): see
> `docs/discovery-api.md`. The JSON form below (`?hash=`), `/swarm` and
> `/files` are the PoC's and are removed in M5-01; probing and `/metadata`
> stay (decision OQ-21).

## Running

```sh
cargo run --release
```

By default it listens on `0.0.0.0:8080` and starts a DHT node on UDP port
`6881`.

```sh
curl "http://localhost:8080/peers?magnet=magnet%3A%3Fxt%3Durn%3Abtih%3A<HASH>%26tr%3D..."
curl "http://localhost:8080/peers?hash=<40-hex-or-32-base32-info-hash>"
curl "http://localhost:8080/health"
```

Example response:

```json
{
  "info_hash": "c9e15763f722f23e98a29decdfae341b98d53056",
  "peers": ["1.2.3.4:6881", "5.6.7.8:51413"],
  "count": 2,
  "cached": false,
  "fetched_at": 1732450000,
  "expires_at": 1732450300,
  "sources": {
    "dht": 40,
    "trackers": [
      { "url": "udp://tracker.opentrackr.org:1337/announce", "ok": true, "peers": 12 },
      { "url": "udp://dead.example.org:1337/announce", "ok": false, "peers": 0, "error": "timed out" }
    ]
  }
}
```

A `Cache-Control: public, max-age=<seconds until expiry>` header is also set,
so browsers and any CDN in front of the service can cache the response too.

`400 Bad Request` is returned for an invalid/missing magnet or hash.

### `GET /swarm?magnet=…` or `?hash=…`

The announced peers, each probed over TCP. The first request for a swarm
starts probing peers as soon as the first tracker or DHT node names them,
and answers with a partial snapshot (`"complete": false`) once 8 peers have
unchoked a probe, or after 3 s if anyone is reachable. Poll again every
couple of seconds for the rest until `complete` is true. After that it's
served from memory and refreshed in the background while clients keep
polling.

```json
{
  "info_hash": "08ada5a7a6183aae1e09d831df6748d566095a10",
  "announced_at": 1790549913,
  "probed_at": 1790549918,
  "token_exp": 1790550818,
  "num_pieces": 987,
  "complete": true,
  "counts": { "known": 163, "probed": 163, "pending": 0, "reachable": 30, "seeds": 29, "unchoked": 18 },
  "peers": [
    { "addr": "71.250.8.233:9076", "ok": true, "seed": true, "client": "qBittorrent/4.6.7",
      "reqq": 500, "rtt_ms": 33, "unchoke_ms": 0, "fast": true, "t": "xtYrvdDY…" },
    { "addr": "5.6.7.8:51413", "ok": true, "seed": false, "have": "8AA=", "have_count": 4, "…": "…" },
    { "addr": "9.9.9.9:6881", "ok": false, "err": "connect_timeout", "seed": false, "fast": false, "t": "…" }
  ]
}
```

- **Ordering.** Peers come best first: reachable, then quickest to unchoke,
  then seeds, then lowest RTT. Peers still being probed (`"err":
  "probing"`, partial snapshots only) come before the ones that failed.
- **`have`.** A base64 bitfield, present for reachable peers that aren't
  seeds.
- **`num_pieces`.** Present once the metadata is known. It makes seed
  detection exact.
- **`t`.** The magnet-worker token for that peer. Present only when
  `WORKER_TOKEN_SECRET` is set, and valid until `token_exp`.
- **Caching.** Sent with `Cache-Control: no-store`, because tokens are
  minted per response.

### `GET /metadata?ih=…` (or `?magnet=…`, `?hash=…`)

The raw bencoded info dict (`application/octet-stream`), whose SHA-1 (v1,
hybrid) or truncated SHA-256 (v2-only) is the info-hash, so clients can
check it themselves. Sent with `Cache-Control: public, max-age=31536000,
immutable`. Answered from the in-memory cache when it is there; otherwise a
scouted discovery runs and live peers offering the metadata are asked.
Details, errors and limits: docs/discovery-api.md › `GET /metadata?ih=`.

Errors: `404` no peer offered a verified info dict, `504` deadline, `503`
every fetch slot busy, `429` over the per-IP limits of `/peers`. Failures
aren't cached, so retrying later works.

### `GET /files?magnet=…` or `?hash=…`

The torrent's files, read from its info dict (fetched from peers first, as
for `/metadata`, if it isn't cached):

```json
{
  "info_hash": "08ada5a7a6183aae1e09d831df6748d566095a10",
  "name": "Sintel",
  "size": 129302391,
  "piece_length": 131072,
  "files": [
    { "path": "Sintel.mp4", "size": 129241752, "offset": 7884 },
    { "path": "poster.jpg", "size": 46115, "offset": 129256276 },
    { "path": "Sintel.ru.srt", "size": 2016, "offset": 129254260 },
    …
  ]
}
```

- **Order.** Largest file first; files of the same size keep their order in
  the torrent. magnet-watcher stores a file's position in this list (in
  `?file=` links and saved videos), so the order must not change.
- **Paths** are relative to the torrent's root folder, joined with `/`. A
  single-file torrent lists its name.
- **Offsets** are the file's byte offset in the torrent's content, so a
  client can map files to pieces.
- **Left out:** padding files (BEP 47). Their bytes still count in the
  offsets.
- **Caching.** Content-addressed, so sent with
  `Cache-Control: public, max-age=31536000, immutable`.

Errors: as for `/metadata`, plus `422` for an info dict with no v1 file
list (a BitTorrent v2-only torrent).

## Tunnel gateway: `GET /v1/tunnel`

The fallback data path (docs/architecture.md › Target architecture,
*Fallback gateway*): a WebSocket endpoint speaking the same protocol as the
Cloudflare gateway (`docs/tunnel-protocol.md`, subprotocol
`magnet-tunnel.v1`) for **outbound TCP** to peers. The engine's
`TunnelClient` uses it by changing only the gateway URL. Code:
`src/tunnel/` (`codec.rs` checked against the shared vectors in
`packages/tunnel-protocol/fixtures`, `gate.rs` the handshake gate,
`session.rs` one WebSocket and its connections).

- **Same rules as the Cloudflare gateway**: handshake gate (nothing is
  relayed before the remote's valid handshake for the same info-hash),
  6 connections per WebSocket, 3 WebSockets per `?session=`, no
  private/reserved addresses (`src/netguard.rs`, the same policy as
  `packages/tunnel-protocol`) and no ports below 1024, idle timeout 90 s,
  session timeout 2 h, 4 MiB held for a peer that does not read, 30
  upgrades per minute per IP, the same close codes and close-reason
  counters. A connect that timed out does not keep its slot for 20 s (that
  hold is a Cloudflare platform quirk, OQ-03).
- **Access** (OQ-09): no browser authentication and no secret. `Origin`
  outside `ALLOWED_ORIGINS` gets **403 before the upgrade** (browsers do not
  apply CORS to WebSocket upgrades); requests without `Origin` are not from
  a page and pass, limited like the rest by the handshake gate and the
  limits. Beyond Cloudflare's rules, concurrent WebSockets are capped per IP
  and in total (`TUNNEL_SOCKETS_PER_IP`, `TUNNEL_MAX_SOCKETS`).
- **Logs**: `tunnel.session_start` / `tunnel.session_end` with counters and
  reason codes (per-connection events at `debug`); never the client IP, a
  peer address or an info-hash.
- **Shutdown**: SIGTERM closes every tunnel with code 4002 and `close`
  SESSION_ENDING frames; the browser reconnects.
- **Announce and inbound peers** (M4-02, `src/tunnel/inbound.rs`,
  docs/tunnel-protocol.md › Announce and inbound connections): on a
  session's `announce` frame the gateway announces its port for that
  info-hash to the session's trackers, the defaults and the DHT, opens the
  listener (`TUNNEL_INBOUND_ADDR`) and hands every peer that connects in
  with a valid handshake for an announced info-hash to that WebSocket
  (`accept`). Anything else is closed without a reply. Nothing is announced
  and no port listens until a session asks; `withdraw` or the end of the
  WebSocket sends `stopped` to the trackers and, with no announce left,
  closes the port. Unset `TUNNEL_INBOUND_ADDR`: `announced` UNSUPPORTED.
  The port must be reachable from the Internet over TCP and UDP (firewall),
  at least 1024 (the service is not root) and different from `DHT_PORT`
  (both use UDP: on a clash inbound peers are turned off, logged).
- **uTP in the tunnel** (M4-04e, docs/tunnel-protocol.md › Peer fields):
  while it listens, the gateway also binds a UDP socket on the listener's
  port number and speaks uTP there. Inbound uTP peers go through the same
  handshake gate, session check and limits as TCP ones and reach the
  browser as `accept` with transport uTP; a refused one is forgotten
  without a single packet in reply. An `open` with transport uTP dials
  from that port, only while the `open`'s session announces its
  info-hash, only to addresses `netguard` allows (else `failed`
  UNSUPPORTED_TRANSPORT / FORBIDDEN_ADDRESS). Nothing is bound on UDP for
  uTP before the first announce; the uTP gate closes with the TCP listener
  and the socket is released once its last uTP connection ends
  (`src/tunnel/inbound.rs`, `src/tunnel/stream.rs`).
- **Hole punching** (M4-05, BEP 55, docs/tunnel-protocol.md › Hole
  punching): the browser sends the rendezvous through a relay and, on its
  `connect`, a uTP `open` to the peer behind NAT. While that connect is
  pending the gateway holds the multiplexer's acceptor for the peer's
  exact address, so the peer's own SYN (sent at the same time, through
  its NAT) is that `open`'s and not an inbound peer; every connection to
  the address that comes up gets the handshake and the first that
  answers it is kept, the others closed (`src/tunnel/punch.rs`). No new
  frame, setting or rule: uTP `open`s stay limited to sessions that
  announce and to addresses `netguard` allows.
- **uTP** (BEP 29, our own implementation: docs/decisions/utp.md):
  `src/utp/packet.rs` is the packet codec (M4-04a; strict parsing, no
  allocation sized by the packet, no I/O). `src/utp/conn.rs` is the
  connection state machine (M4-04b), sans-IO: SYN retransmitted with
  backoff until the connect timeout, one `ST_STATE` per received SYN and
  never a retransmitted one, FIN/RESET, reorder buffer with selective ACK,
  RTT/RTO as BEP 29, fast retransmit, send/receive buffers and packets in
  flight bounded per connection, idle timeout; packets out of window or
  acknowledging unsent data are dropped without a reply.
  `src/utp/ledbat.rs` is the congestion control (M4-04c): LEDBAT as BEP 29
  (100 ms target queueing delay, base delay = minimum of the last two
  minutes, at most 3 000 B of growth per round trip, halving on loss once
  per window, one packet after a timeout, never below one packet), plus
  slow start and no growth while application-limited, as libutp; packets
  carry at most 1 400 B (no PMTU probing). On a simulated bottleneck it
  reaches over 80 % of capacity with about 100 ms of queueing
  (`src/utp/ledbat_sim_tests.rs`). `src/utp/socket.rs` is the
  multiplexer on one UDP socket (M4-04d): connections demultiplexed by
  (address, connection id), outbound connects and inbound connections on
  the same local port, `UtpStream: AsyncRead + AsyncWrite` (like a
  `TcpStream`: 0 at the remote's FIN, `shutdown` sends ours, a dropped
  stream closes gracefully and is aborted after a linger time). A SYN is
  answered only through a gate the caller opens: the listener (with an
  optional address filter), an expected address (hole punch), or the
  address of a pending connect (simultaneous open); anything else, and any
  packet for an unknown connection, gets no reply, never an `ST_RESET`.
  Connections in total and per IP, half-open connections in total and per
  IP and each acceptor's backlog are bounded; SYNs beyond them are
  ignored. The gateway uses it on its announced port (above).
  Interoperability (M4-04f): full verified downloads over uTP from
  libtorrent-rasterbar 2.0 and Transmission 4.0 (its libutp), with the
  gateway dialling and with the client dialling in, run by
  `npm run e2e:interop -w tools/e2e` (Docker; docs/decisions/utp.md ›
  Interoperability).
- nginx must forward WebSocket
  upgrades on `/v1/tunnel` (production setup: M5-04).

## Configuration

All configuration is via environment variables; every one is optional and
falls back to a sensible default (an invalid value is logged and ignored,
never a hard crash).

| Variable | Default | Meaning |
|---|---|---|
| `BIND_ADDR` | `0.0.0.0:8080` | HTTP listen address |
| `CACHE_TTL_SECS` | `300` | How long a non-empty result is cached |
| `EMPTY_CACHE_TTL_SECS` | `30` | How long an empty/all-failed result is cached (kept short so a temporary outage isn't stuck for the full TTL) |
| `DEADLINE_MS` | `3000` | Max time to spend on a cache miss before returning whatever peers were found |
| `ALLOWED_ORIGINS` | *(none)* | Comma-separated list of origins allowed to call this API from a browser (CORS), or `*` for any origin. If unset, no CORS headers are sent — only non-browser clients can use the API |
| `DEFAULT_TRACKERS` | a built-in list of ~8 public trackers | Comma-separated list overriding the built-in defaults |
| `MAX_TRACKERS` | `20` | Cap on how many trackers are queried per request (magnet's own + defaults, deduplicated) |
| `PEERS_CACHE_MB` | `16` | Memory for cached discovery results, weighed in bytes (each of the two caches: streamed `/peers` and the legacy one) |
| `DHT_PORT` | `6881` | UDP port for the DHT node |
| `DHT_BOOTSTRAP` | unset | DHT bootstrap nodes: unset, the DHT library's public routers; `none`, no bootstrap (the node never contacts the public DHT: the test harness sets it); or a comma-separated `host:port` list |
| `PROBE_CONCURRENCY` | `256` | Max peer probes (TCP connections) in flight in total, scout and `/swarm` together |
| `MAX_PROBE_PEERS` | `1000` | Max peers probed per swarm build, and per info-hash per `SCOUT_REPROBE_SECS` window by the scout (beyond it, `/peers` sends addresses unprobed) |
| `PROBE_CONNECT_TIMEOUT_MS` | `2500` | TCP connect timeout for a probe or metadata fetch |
| `PROBE_TIMEOUT_MS` | `5000` | Hard cap on one probe |
| `PROBE_UNCHOKE_WAIT_MS` | `2500` | How long a probe waits for an unchoke after saying it's interested |
| `REFRESH_INTERVAL_SECS` | `300` | Active swarms are re-announced and re-probed this often |
| `ACTIVE_TTL_SECS` | `1800` | A swarm stops being refreshed (and is forgotten) this long after its last `/swarm` request |
| `METADATA_DEADLINE_MS` | `15000` | Time budget for fetching an info dict from peers |
| `METADATA_CACHE_MB` | `32` | Memory for cached info dicts, weighed in bytes (shared by `/peers`, `/metadata`, `/files`, `/swarm`) |
| `METADATA_CACHE_MAX_ENTRY_MB` | `4` | Larger info dicts are served but not cached |
| `METADATA_FETCHES` | `4` | Info dict fetches in flight in total (each asks up to 3 peers; buffers bounded to this × 8 MiB) |
| `WORKER_TOKEN_SECRET` | *(none)* | Secret shared with magnet-worker. If unset, `/swarm` issues no tokens |
| `TOKEN_TTL_SECS` | `900` | Lifetime of the tokens in a `/swarm` response |
| `SCOUT_PER_STREAM` | `64` | Scout: probes in flight per `/peers` stream |
| `SCOUT_REPROBE_SECS` | `600` | Scout: an address probed more recently is answered from memory, not probed again |
| `SCOUT_HALF_LIFE_SECS` | `1800` | Scout: remembered scores fade toward 0.2 (unknown) with this half-life |
| `SCOUT_IDLE_SECS` | `7200` | Scout: an info-hash nobody asked about for this long is forgotten |
| `SCOUT_CACHE_MB` | `16` | Scout: memory for remembered scores (about 96 bytes per address) |
| `SCOUT_MAX_ADDRS` | `1000` | Scout: addresses remembered per info-hash (oldest dead dropped first) |
| `ALLOW_PRIVATE_PEERS` | `false` | `1` or `true`: stream and probe loopback/private peer addresses, and let tunnels dial them for requests made straight to a loopback listener on a loopback host without `CLIENT_IP_HEADER` (never through nginx). Only for local testing |
| `PEERS_STREAM_SECS` | `30` | Streamed `/peers?ih=`: the response closes after this long (1–120) |
| `PEERS_PER_IP_PER_MINUTE` | `30` | Streamed `/peers`: requests per minute per client IP, then 429 |
| `PEERS_STREAMS_PER_IP` | `4` | Streamed `/peers`: concurrent streams per client IP, then 429 |
| `PEERS_STREAMS_TOTAL` | `200` | Streamed `/peers`: concurrent streams in total, then 429 |
| `CLIENT_IP_HEADER` | `X-Real-IP` | Header with the client IP set by the reverse proxy; trusted only on connections from loopback |
| `TUNNEL_CONNECT_TIMEOUT_MS` | `5000` | Tunnel: TCP connect timeout of an `open` (100–60000) |
| `TUNNEL_HANDSHAKE_TIMEOUT_MS` | `10000` | Tunnel: wait for the remote's BitTorrent handshake (100–60000) |
| `TUNNEL_MAX_CONNECTIONS` | `6` | Tunnel: connections per WebSocket (1–64) |
| `TUNNEL_IDLE_TIMEOUT_MS` | `90000` | Tunnel: a connection, or a WebSocket, without traffic is closed after this (1000–600000) |
| `TUNNEL_SESSION_TIMEOUT_S` | `7200` | Tunnel: maximum session length (5–86400) |
| `TUNNEL_SOCKETS_PER_SESSION` | `3` | Tunnel: WebSockets per `?session=` (1–8), then 429 |
| `TUNNEL_UPGRADES_PER_MINUTE` | `30` | Tunnel: upgrades per minute per client IP (1–600), then 429 |
| `TUNNEL_MAX_PENDING_WRITE_BYTES` | `4194304` | Tunnel: bytes held for a peer that does not read before its connection is closed (65536–268435456) |
| `TUNNEL_SOCKETS_PER_IP` | `12` | Tunnel: concurrent WebSockets per client IP (1–1000), then 429 |
| `TUNNEL_MAX_SOCKETS` | `256` | Tunnel: concurrent WebSockets in total (1–100000), then 503 |
| `TUNNEL_INBOUND_ADDR` | unset | Inbound peers: listen address while a session announces (e.g. `0.0.0.0:6883`), TCP and UDP (uTP) on the same port, which must differ from `DHT_PORT`; unset: no announce, no listener |
| `TUNNEL_ANNOUNCE_PORT` | `0` | Port announced to trackers and the DHT; `0` = the listener's (else 1024–65535, for a forwarded port) |
| `TUNNEL_ANNOUNCE_INTERVAL_S` | `900` | Re-announce period while announcing (60–3600) |
| `TUNNEL_ANNOUNCE_DHT` | `1` | Also announce to the DHT (`0` to disable) |
| `TUNNEL_INBOUND_PER_SESSION` | `16` | Inbound connections per session (1–256) |
| `TUNNEL_INBOUND_MAX` | `256` | Inbound connections in total (1–100000) |
| `TUNNEL_INBOUND_PENDING` | `64` | Inbound connections still handshaking, in total (1–10000) |
| `TUNNEL_ANNOUNCES_PER_SOCKET` | `2` | Info-hashes one WebSocket may announce (1–16) |
| `TUNNEL_ANNOUNCES_MAX` | `64` | Info-hashes announced at once on the gateway (1–10000) |
| `TUNNEL_ANNOUNCES_PER_IP` | `4` | Info-hashes the WebSockets of one client IP (an IPv6 /64) may announce at once (1–1000) |
| `TUNNEL_ANNOUNCE_ROUNDS_PER_IP_PER_MINUTE` | `6` | Announce rounds (a new swarm, or trackers added to one) a client IP may start per minute (1–600) |
| `TUNNEL_ANNOUNCE_ROUNDS_PER_MINUTE` | `60` | Announce rounds started per minute on the gateway, all clients together (1–10000) |

Example:

```sh
CACHE_TTL_SECS=600 ALLOWED_ORIGINS=https://example.com,https://app.example.com cargo run --release
```

## Design notes / trade-offs

- **Peer list, not a seeder count.** Trackers can report seeder/leecher
  *counts* cheaply (the `/scrape` endpoint), but nothing in the protocol
  tells you which peer in the peer list is currently seeding vs. leeching.
  This service returns the full peer list from announces + DHT, which is
  what you need if you actually want addresses to connect to.
- **Deadline over completeness.** DHT lookups in particular can take much
  longer than a few seconds to fully converge. `DEADLINE_MS` returns
  whatever has been found so far rather than waiting for every source to
  finish; a cache hit afterwards is effectively instant regardless.
- **In-memory, single-instance cache.** Simplest and fastest option; it does
  not survive a restart and is not shared across replicas. Concurrent
  requests for the same info-hash are coalesced into one lookup.
- **SSRF guard.** Tracker URLs come from user-supplied magnet links and are
  fetched by this server, so both the UDP and HTTP tracker paths resolve the
  hostname and refuse to connect to anything that isn't a public, routable
  address (loopback/private/link-local/etc. are rejected). Announced peers
  get the same check before they're probed.
- **Probing is what a normal client does.** Connect, handshake, say
  interested, read the first messages, disconnect. Probes never request
  pieces. A global limit (`PROBE_CONCURRENCY`) bounds the load however many
  swarms are active.
- **Why probe at all.** In practice most announced peers aren't reachable
  over TCP: Sintel had 25 reachable out of 161. Knowing up front which ones
  are, which are seeds and which unchoke straight away saves the browser a
  worker request and several seconds per dead peer.
- **Metadata is trustless.** An info dict is only cached if its SHA-1 equals
  the info-hash, and the browser checks it again.

## Development

```sh
cargo test        # unit tests (magnet parsing, bencode, SSRF guard, cache, ...)
cargo build --release
```

## Docker

```sh
# from the monorepo root (the Cargo workspace lives there)
docker build -f services/seeders/Dockerfile -t magnet-seeders .
docker run --rm -p 8080:8080 -p 6881:6881/udp magnet-seeders
```

The image is a single static (musl) binary on `scratch` — no shell, no
package manager, minimal attack surface.

## Bare-metal deployment (systemd + nginx, no Docker)

Ready-to-use configs are in `deploy/`. This assumes systemd (virtually all
current VPS distros) and that nginx + certbot are already handling TLS for
your other services the standard way.

1. **Build on the VPS itself.** Since the binary runs on the same machine
   forever, there's no cross-compilation or static-linking to worry about
   (that only matters for portable artifacts like Docker images):

   ```sh
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
   source "$HOME/.cargo/env"
   sudo apt-get install -y build-essential pkg-config   # C toolchain, for the `ring` crate
   git clone <monorepo-url> /tmp/magnet-stream && cd /tmp/magnet-stream
   cargo build --release -p magnet-seeders
   ```

2. **Install the binary and config:**

   ```sh
   sudo mkdir -p /opt/magnet-seeders /etc/magnet-seeders
   sudo cp target/release/magnet-seeders /opt/magnet-seeders/   # run from the monorepo root
   sudo cp services/seeders/deploy/magnet-seeders.env.example /etc/magnet-seeders/magnet-seeders.env
   sudo $EDITOR /etc/magnet-seeders/magnet-seeders.env   # set ALLOWED_ORIGINS at minimum
   ```

3. **Install and start the systemd unit:**

   ```sh
   sudo cp services/seeders/deploy/magnet-seeders.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now magnet-seeders
   systemctl status magnet-seeders     # should be active (running)
   curl 127.0.0.1:8080/health          # -> ok
   journalctl -u magnet-seeders -f     # logs (stdout/stderr go to the journal)
   ```

   The unit binds nothing as root and runs under a `DynamicUser`-managed,
   unprivileged, sandboxed account — see the comments in the unit file if
   it fails to start under the hardening options.

4. **Firewall.** `BIND_ADDR=127.0.0.1:8080` means the HTTP port is never
   reachable except through nginx on the same host — no firewall rule
   needed (and don't open it externally). The DHT's UDP port (`6881` by
   default) needs *no* inbound firewall rule either for normal lookups: a
   stateful firewall (ufw/iptables default) already lets replies to our
   own outbound queries back in. Opening it inbound is optional and only
   matters if you want this node to be a fully reachable DHT participant:

   ```sh
   sudo ufw allow 6881/udp   # optional
   ```

   With `TUNNEL_INBOUND_ADDR` set (tunnel inbound peers), its port must be
   open inbound for TCP and UDP (uTP), and differ from the DHT's:

   ```sh
   sudo ufw allow 6883/tcp && sudo ufw allow 6883/udp
   ```

5. **DNS.** Point your subdomain (e.g. `peers.example.com`) at the VPS's
   IP (A/AAAA record).

6. **nginx + TLS:**

   ```sh
   sudo cp services/seeders/deploy/nginx-magnet-seeders.conf /etc/nginx/sites-available/magnet-seeders
   sudo $EDITOR /etc/nginx/sites-available/magnet-seeders   # set server_name
   sudo ln -s /etc/nginx/sites-available/magnet-seeders /etc/nginx/sites-enabled/
   sudo nginx -t && sudo systemctl reload nginx
   sudo certbot --nginx -d peers.example.com   # adds the HTTPS server block + redirect
   ```

   The shipped config sets `proxy_read_timeout 30s`. A cold `/metadata` can
   take ~15 s, above nginx's usual 10 s. Also note the warning in
   `deploy/nginx-magnet-seeders.conf`: don't add CORS
   headers in nginx (globally or for this vhost) — the app already sets
   them from `ALLOWED_ORIGINS`, and a second copy causes browsers to
   reject the response outright due to duplicate headers.

7. **Verify end-to-end:**

   ```sh
   curl https://peers.example.com/health
   curl "https://peers.example.com/peers?hash=<some 40-hex info-hash>"
   curl -i -H "Origin: https://example.com" https://peers.example.com/health \
     | grep -i access-control-allow-origin
   ```

**Updating later:** rebuild (`cargo build --release`), copy the new binary
over `/opt/magnet-seeders/magnet-seeders`, then `sudo systemctl restart
magnet-seeders`. It's stateless (only the in-memory cache is lost), so
there's nothing to migrate.
