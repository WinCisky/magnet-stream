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

### `GET /metadata?magnet=…` or `?hash=…`

The raw bencoded info dict (`application/octet-stream`), whose SHA-1 is the
info-hash, so clients can check it themselves. It is content-addressed and
therefore sent with `Cache-Control: public, max-age=31536000, immutable`.

Errors:
- `504` if no peer delivered the metadata before `METADATA_DEADLINE_MS`
- `502` if every candidate failed

Failures aren't cached, so retrying later works.

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
| `MAX_CACHE_ENTRIES` | `50000` | Max number of distinct info-hashes held in the cache at once |
| `DHT_PORT` | `6881` | UDP port for the DHT node |
| `PROBE_CONCURRENCY` | `256` | Max peer probes (TCP connections) in flight across all swarms |
| `MAX_PROBE_PEERS` | `1000` | Max peers probed per swarm build (peers reachable last time come first, peers that failed last time last) |
| `PROBE_CONNECT_TIMEOUT_MS` | `2500` | TCP connect timeout for a probe or metadata fetch |
| `PROBE_TIMEOUT_MS` | `5000` | Hard cap on one probe |
| `PROBE_UNCHOKE_WAIT_MS` | `2500` | How long a probe waits for an unchoke after saying it's interested |
| `REFRESH_INTERVAL_SECS` | `300` | Active swarms are re-announced and re-probed this often |
| `ACTIVE_TTL_SECS` | `1800` | A swarm stops being refreshed (and is forgotten) this long after its last `/swarm` request |
| `METADATA_DEADLINE_MS` | `12000` | Time budget for fetching an info dict from peers |
| `METADATA_CACHE_MB` | `256` | Memory for cached info dicts |
| `WORKER_TOKEN_SECRET` | *(none)* | Secret shared with magnet-worker. If unset, `/swarm` issues no tokens |
| `TOKEN_TTL_SECS` | `900` | Lifetime of the tokens in a `/swarm` response |
| `ALLOW_PRIVATE_PEERS` | `false` | Probe loopback/private peer addresses. Only for local testing |

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
