# Magnet Streaming Architecture

Nov 16, 2026 · @Simone · revised Nov 19, 2026 (VPS scouting, metadata on the VPS, upload: OQ-04, OQ-21)

## Summary

The browser becomes the whole BitTorrent client, and the servers shrink to byte tunnels that hold peer connections open for as long as the session lasts.

The proof of concept is slow and request-hungry for one reason: every batch of blocks is a new HTTP request that reconnects, re-handshakes and waits to be unchoked again. The fix is to stop reconnecting.

- **Browser (all the logic).** Peer-wire protocol, metadata fetch, peer exchange, piece picking, SHA-1 verification, piece buffer, playback.
- **Cloudflare (the default data path).** One Durable Object per session accepts a WebSocket and multiplexes up to 6 persistent TCP connections to peers per socket. It forwards bytes and understands nothing beyond the BitTorrent handshake. Cost: about 30 to 150 billed requests per film.
- **VPS (only what Cloudflare cannot do).** UDP discovery (DHT, UDP trackers) and scouting in one streamed request: it probes the peers it finds, ranks them, and fetches the metadata, because it can try a hundred addresses a second where a tunnel tries one every few seconds. Later a second tunnel gateway that accepts inbound connections and speaks uTP, for swarms whose few seeders sit behind NAT.
- **No database and no persistent server-side storage.** Supabase is not used. Pieces live only in the browser. The VPS keeps bounded in-memory caches (metadata, peer scores, discovery results) that a restart clears.
- **The browser gives back.** It uploads verified pieces to peers that upload to it (tit-for-tat), so it is unchoked as a trading partner and not only on the optimistic slot.

One tunnel protocol serves both gateways, so the browser treats a Cloudflare tunnel, a VPS tunnel, a WebRTC peer and a web seed as the same thing: a peer connection.

## Goals and hard limits

Priorities, in order: recover chunks reliably (including from swarms with few seeders), recover them fast, spend few Cloudflare requests, then render.

Three sets of limits shape the design.

**Chromium.** A page cannot open TCP or UDP sockets. It has fetch, WebSocket, WebRTC and WebTransport. Direct Sockets exist only for Isolated Web Apps, so they are out. It does have fast SHA-1 (WebCrypto), threads (workers) and a large private file system (OPFS).

**Cloudflare** (figures from the [Workers limits](https://developers.cloudflare.com/workers/platform/limits/), [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/), [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/) and [TCP sockets](https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/) pages, read on 16 Nov 2026):

| Limit | Free plan | What it means here |
| --- | --- | --- |
| Worker requests | 100,000 per day | A WebSocket to a plain Worker costs 1 request, messages are free |
| Worker CPU | 10 ms per request | Too little to forward a film in JavaScript from a plain Worker |
| Open outbound connections | 6 per invocation, the 7th is queued | 6 peers per WebSocket; more peers means more sockets. A connect to a dead host keeps its slot about 20 s (OQ-03), so blind dialling through a tunnel tries only a few addresses per minute |
| Outbound TCP (`connect()`) | Allowed, not to Cloudflare or private IPs | Peers reachable over TCP can be dialled from the edge |
| UDP, inbound TCP | Not available | No DHT, no UDP trackers, no uTP, no listening port |
| Durable Objects | SQLite-backed class, 100,000 requests and 13,000 GB-s per day | About 28 hours of active object time per day |
| Durable Object WebSocket billing | 20 incoming messages = 1 request, outgoing free | Downloaded data costs nothing; upstream must be batched |
| Durable Object CPU | 30 s, reset by each incoming message (free-plan figure not stated) | Must be measured before committing: see Risks |
| WebSocket message size | 32 MiB | No constraint in practice |

**The swarm.** Most peers in a typical swarm are unreachable from outside, because they sit behind NAT and only connect outward: in a large public swarm often 80 to 90 % of the announced addresses never answer. A peer unchokes a handful of peers and rotates an optimistic slot roughly every 30 seconds, so a fresh connection often waits, and a connection that keeps downloading keeps its slot. Leechers give their regular slots to the peers that upload to them (tit-for-tat), so a client that never uploads lives on optimistic unchokes alone, which in a crowded swarm come rarely. WebRTC (WebTorrent) peers are rare. With few seeders, the only way to reach a NATed seeder is to be reachable yourself: announce a public port and let it connect in, or punch through over uTP. Only the VPS can do that.

## What the proof of concept does, and where it loses

The three repositories prove the idea works and already contain most of the hard parts. Their shared weakness is that a peer connection lives only as long as one HTTP request.

| Repository | Role today | What costs time or requests |
| --- | --- | --- |
| magnet-seeders (Rust, VPS) | Finds peers (DHT, UDP and HTTP trackers), probes every peer over TCP, fetches metadata, mints a signed token per peer | The page polls `/swarm` every 1.5 s until probing ends, then every 5 minutes. Each peer is contacted once by the VPS only to be contacted again by the Worker |
| magnet-worker (Cloudflare Worker) | Per request: races up to 40 candidates, 6 at a time, then pipes the winner's socket into the response | Every request pays connect, handshake and unchoke (0.2 to 1 s when lucky, up to the 8 s wait when not). One request carries at most the peer's queue depth, 1 to 8 MiB in practice |
| magnet-watcher (Astro, Svelte) | Scheduler, SHA-1 checks, piece cache, libmedia player | Up to 24 parallel requests, hedges and steals to work around slow setups. A 2 GB film takes several hundred successful requests plus the failed races |

Consequences of reconnecting per batch:

- **Slow start on every batch.** TCP slow start and the unchoke wait repeat hundreds of times.
- **Lost unchoke slots.** A seed that finally unchoked us is dropped a few megabytes later. With few seeders this is the difference between playing and stalling.
- **Peers see churn.** Hundreds of short connections from the same address range look abusive, and some clients ban for it.
- **No peer exchange.** Short connections never live long enough to learn new peers from the ones we have.
- **NATed seeders are invisible.** Nothing listens for inbound connections, so a swarm whose seeders are all behind NAT yields nothing.

What carries over unchanged: the DHT and tracker code, the bencode and wire codecs, the scheduler's deadline logic, the player, and the test harnesses with fake peers.

## Target architecture

Three places run code, and only the browser makes decisions.

&#91;embedded content: target architecture · browser, two gateways, swarm\]

Pieces flow right to left: peers, gateway, engine, buffer, player. Path 1 carries almost all bytes at no bandwidth cost; path 3 exists for seeders that Cloudflare cannot reach.

| Component | Runs | Responsibilities | Deliberately does not |
| --- | --- | --- | --- |
| Torrent engine | Browser, dedicated worker | Peer-wire protocol, metadata (BEP 9), peer exchange, piece picking, SHA-1 checks, connection scoring, escalation | Depend on which gateway carries a connection |
| Piece buffer | Browser, OPFS | Random-access store of verified pieces, range reads for the player; every video opened stays saved until the user deletes it (OQ-13, docs/storage.md) | Leave the browser, except as upload to peers |
| Player | Browser | Native `<video>` over a service-worker URL, or libmedia | Touch the network |
| Tunnel gateway | Cloudflare Durable Object (magnet-worker) | Accept WebSockets, dial peers, gate on the BitTorrent handshake, forward bytes | Parse messages, hash, store, or choose pieces |
| Discovery and scout | VPS (magnet-seeders) | DHT lookups, UDP and HTTP tracker announces; probing and ranking every address found; metadata fetch (BEP 9); all streamed as found; bounded in-memory caches | Mint tokens, carry piece data, store anything on disk, make choices for the browser (it only ranks) |
| Fallback gateway | VPS (magnet-seeders) | Same tunnel protocol, plus announce, inbound listener, uTP and hole punching | Carry sessions that path 1 can serve |

Repository mapping: magnet-watcher gains the engine and loses the batch scheduler's request plumbing; magnet-worker becomes the tunnel gateway; magnet-seeders slims to discovery and scouting (it keeps the PoC's probe and metadata code, and drops `/swarm`, `/files` and tokens) and later grows the fallback gateway.

## Chunk recovery

Recovery is a real BitTorrent client running in a dedicated worker in the page, speaking to peers over long-lived tunnels. Everything below happens in the browser unless a gateway is named.

### Session start

1. **Parse the magnet** and open the first WebSocket to the Cloudflare gateway. This runs in parallel with step 2, so the tunnel is warm when the first peer address arrives. Verified metadata already in IndexedDB for this info-hash skips step 3.
2. **Discover and scout with one request.** `GET /peers?ih=…` on the VPS answers as a stream of newline-delimited JSON, closed after about 30 s. The VPS asks every tracker and the DHT, and probes each address it gets back (see Scouting below). Addresses scouted in the last minutes come first, from memory, best first; the others are written as their probe ends, alive ones with a score and their facts, dead ones marked dead, and those not probed before the deadline as unknown. No polling, no tokens.
3. **Metadata from whoever answers first.** As soon as the VPS has found live peers offering `ut_metadata`, it fetches the info dict (BEP 9) and writes it as one line of the same stream. The browser asks its own tunnel peers in parallel. Either way the page checks the info dict against the info-hash before using it, so the VPS cannot forge a file list.
4. **Dial best first.** Each address becomes an `open` command on a tunnel, in score order: live seeds first, then live partial peers, then unknown addresses. Addresses the scout saw dead are not dialled, unless a peer names them again by peer exchange (they may have come back); then they go last.
5. **Start requesting** the head and tail of the chosen file as soon as the piece hashes are known.

Expected time from paste to first verified piece on a healthy swarm: 2 to 4 s, against 5 to 15 s today.

### Scouting on the VPS

A tunnel pays about 20 s for each dead address it dials (OQ-03) and has 6 slots, so on its own it tries a few addresses per minute, while a large swarm announces hundreds of which most never answer. The VPS has no such limit, so it does the exploring and the tunnels connect only where someone is listening.

- **Probe.** TCP connect (2.5 s timeout), BitTorrent handshake, then whatever the peer sends within a short window: bitfield or `have_all`/`have_none`, the extended handshake (`ut_metadata`, `metadata_size`, `reqq`), an unchoke. At most 256 probes in flight across all sessions and 64 per stream; one probe per address and info-hash at a time, shared by concurrent streams. Probing reads only these first messages and never requests pieces.
- **Rank.** Each probed address gets a score from 0 to 1: about 0.5 for a completed handshake, more for being a seed (or for the share of pieces it has), for offering the metadata, for unchoking the probe, for a short round trip; near 0 for a refused or timed-out connect; 0.2 for an address not probed yet. Scores fade toward 0.2 with a half-life of about 30 minutes, because home addresses and ports change within hours. The browser uses the score as a prior and replaces it with what it measures itself.
- **Remember, in memory only.** Scores are kept per info-hash and address for up to about 2 hours without use; an address probed in the last 10 minutes is not probed again. A second session for the same magnet gets the ranked list at once. A restart clears everything, and the next stream rebuilds it in seconds.
- **Metadata.** The info dict is fetched from up to 3 live peers in parallel (first verified answer wins, 15 s deadline), checked against the info-hash (SHA-1 for v1, truncated SHA-256 for v2), and kept in a size-bounded memory cache. Concurrent requests for the same info-hash share one fetch. It is also served alone at `GET /metadata?ih=`, immutable and cacheable by the browser.
- **Trust.** The scout is advice. The handshake gate on every tunnel still decides what the browser may reach, the browser verifies the metadata, and a lying score only costs dial order.

### Peer sources, cheapest first

| Source | Where it runs | Cost |
| --- | --- | --- |
| Peer exchange (`ut_pex`) from connected peers | Browser, over existing tunnels | Free, continuous |
| WebSocket trackers and WebRTC peers | Browser, direct | Free, rarely fruitful |
| Web seeds (`ws=` in the magnet, `url-list` in the metadata) | Browser, direct HTTP range requests when CORS allows | Free |
| DHT and UDP or HTTP trackers, probed and ranked | VPS, one streamed request (with the metadata) | 1 VPS request |
| Inbound and uTP peers | VPS tunnel gateway | VPS bandwidth, only when needed |

Peer exchange is what makes one discovery request enough: after the first connection, the swarm describes itself.

### Connection management

- **Keep connections for the whole session.** A peer that unchoked us stays connected and stays interested. This single change removes most of today's latency.
- **Race at dial time only.** Dial up to 6 candidates per tunnel socket (a Cloudflare limit). Addresses the scout saw alive minutes ago may take every slot; addresses nobody has tried take at most 3 per socket, because each dead one holds its slot about 20 s (OQ-03), and a held slot counts as in flight. A scouted address that then times out counts as untried from then on. Replace the slowest or still-choked connections when better candidates appear, never the ones delivering.
- **Keep dead connects off the sockets that carry data.** Untried addresses are dialled only on sockets none of whose peers has unchoked us (when every socket carries data, on the one with the fewest unchoked peers). An exploring socket jammed by held dead connects, with nothing else on it, is closed and replaced by a new WebSocket (a new invocation with its own 6 slots): at most 4 times per session, one per 20 s, each one billed request. Once 3 peers deliver and the rate is above the threshold, untried addresses are not dialled at all.
- **Score peers on live facts.** Throughput, unchoke delay, bitfield, queue depth (`reqq`), rejects, with the scout's score as the starting prior for peers not connected yet.
- **Prefer peers that have what we need.** Seeds and peers holding wanted pieces go first; a connection with none of the wanted pieces is the first to be replaced when a better candidate waits.
- **Open width early while nothing flows.** Until a peer delivers (metadata or first piece still missing), if there are more candidates than free slots, open the second and third socket at once rather than one every few seconds. Each costs one billed request.
- **Scale width to need.** Once data flows, one socket (6 peers) is enough when rate exceeds twice the video bitrate. Add a second or third socket only when it does not.
- **The file-list wait follows progress.** Looking up the metadata gives up only when nothing is left to try (no live or unknown candidate, discovery finished) or at a hard cap, not after a fixed time while candidates remain, and a failed lookup keeps its session so a retry starts where it stopped.
- **Remember within the session.** Addresses that failed, addresses that delivered, and the metadata (as soon as it is verified, before any file is opened) are kept in IndexedDB so a reload or a second paste reconnects in one step. With the metadata and a peer that delivered, the session redials it and asks the VPS for peers only if nothing delivers within 10 s (docs/storage.md › Session memory).

### Piece picking

Requests are assigned per 16 KiB block, by deadline:

1. **Container index.** Head and tail of the file first (MP4 `moov`, Matroska cues).
2. **Playback window.** The next 20 to 30 s of video, strictly in order, from the fastest peers only. A block that is late is requested again from a second peer.
3. **Read-ahead.** Sequential past the window, spread over all unchoked peers, each kept 2 to 4 s of requests deep. The fastest peers are kept at least 4 MiB deep (at most 60 s of their rate): on a slow swarm, 4 s of a 70 KB/s peer meant a refill message every 2 s, about 440 billed requests per GB (M3-07). A window block held by a peer that is no longer among the fastest is asked of a fast one at once.
4. **Scarce pieces.** When the buffer is comfortably ahead, fetch pieces that only one or two peers hold. They may not be there later.
5. **Back-fill.** Gaps before the playback position, last.

A seek cancels requests outside the new window with `cancel` messages; the connections stay.

### Reciprocation (upload)

The browser uploads, so that leechers unchoke it as a trading partner instead of only on their optimistic slot (OQ-04).

- **What it serves.** Only verified pieces, read from the piece buffer (OPFS), including pieces of saved videos from earlier sessions. After the metadata it sends a bitfield (or `have_none` plus `have`s), then `have` for each newly verified piece, batched into the next upstream message. It also answers `ut_metadata` requests once it holds the verified info dict.
- **Whom it serves (choker).** Every 10 s it unchokes the 4 interested peers that delivered most to it over the last 20 s, plus one optimistic slot rotated every 30 s that favours newly connected peers. Seeds need nothing and are never unchoked. A peer that has delivered nothing yet can only get the optimistic slot, which is how a trade starts (`packages/engine/src/upload.ts`). Requests from a choked peer, for a piece we lack, or beyond the queue depth we advertise are rejected (BEP 6) or ignored, and never read from storage.
- **What it costs.** Uploaded bytes ride in the batched upstream messages. When no other message is due, upload data is flushed at most every 2 s per socket, so upload adds at most about half a message per second per socket, and only while a download is active: an idle, suspended or finished session chokes everyone. Total upload rate is capped (default 2 MiB/s), and so are each peer's queued bytes; the gateway also bounds the bytes it holds for a peer that reads slowly. The 40 billed requests per downloaded GB budget is unchanged.
- **The user decides.** A setting in the page turns upload off; it is on by default. With upload off the engine behaves as before (no bitfield, no unchoke).

### Swarms with few seeders

The engine escalates only as far as it must:

1. **Patience on Cloudflare tunnels.** Stay connected and interested to every reachable seeder and wait out the optimistic unchoke (about 30 s per rotation). Persistent connections make this work where the proof of concept gave up after 8 s. Reciprocation helps with partial peers here: a leecher that has pieces we lack and lacks pieces we have trades with us.
2. **Use partial peers.** Leechers with some pieces are sources too. Their bitfields and `have` messages feed the scarce-piece rule above.
3. **Become reachable.** If fewer than 2 peers deliver, or rate stays under the bitrate, the page asks the VPS gateway to join the swarm for this info-hash: it announces its own port to trackers and DHT, accepts inbound TCP and uTP connections, and hands each one to the browser as another tunnel. NATed seeders connect to it within one announce interval.
4. **Hole punching.** uTP with `ut_holepunch` (BEP 55) through a peer we are already connected to reaches NATed peers that do not re-announce soon. The relay must have reached the VPS gateway's announced port (an inbound or uTP peer), so that it gives the target that port; the browser sends the rendezvous, and on the relay's `connect` asks the VPS gateway for a uTP connection to the target, which takes the target's simultaneous SYN for it (`packages/engine/src/holepunch.ts`, `services/seeders/src/tunnel/punch.rs`, docs/tunnel-protocol.md › Hole punching). The browser never relays for others.
5. **Degrade honestly.** If rate is below bitrate, the page says so and offers to keep buffering; the file still completes.

Thresholds (OQ-11, configurable, deliberately demanding): step 3 starts when fewer than 2 peers deliver for 90 s in a row (counted once the scout's live addresses are used up) or the rate stays under the bitrate for 90 s in a row; step 5's notice after 30 s under the bitrate. A choked seed keeps its slot for about four optimistic rotations (120 s) before a waiting candidate may replace it. Inbound peers join the picker and the choker like any other (`packages/engine/src/escalation.ts`, `connection-manager.ts`). The session counts the bytes each gateway carried (Cloudflare, VPS), and the page's diagnostics report them, so a review can tell how much of a film needed the VPS (M4-08). The scenarios (one NATed seed, a seed late to unchoke, partial leechers that together cover the file, the VPS gateway dying mid-download) run end to end on every CI run (`tools/e2e/src/few-seeders-scenarios.ts`).

### Verification

Every piece is SHA-1 checked in the browser before it is stored or played. Blocks are attributed to the peer that sent them, so a peer that causes two failed pieces is dropped for the session. Gateways never see a verified piece and never hash anything.

- **Attribution.** A failed piece whose blocks all came from one peer is that peer's fault. A failed piece with blocks from several peers strikes nobody: it is fetched again from a single peer, and once it verifies, the blocks of the failed attempt that differ from the good data name who sent them. For v2 torrents the piece's leaf hashes (checked against the verified piece hash) name the bad blocks at once. One mixed piece can therefore never get an honest peer excluded.
- **Exclusion.** At the second proven failure the peer's connection is closed, its address is banned in the session's address book (peer exchange cannot bring it back) and the unverified blocks it sent to other pieces are thrown away. Hashing runs in the engine's worker, never on the page's thread.

## Tunnel protocol and request budget

A gateway is a handshake-gated TCP multiplexer: it opens connections, forwards bytes in both directions, and nothing else.

### Frames

Binary WebSocket messages, each a sequence of frames `[type u8][conn u16][length u32][payload]`.

| Frame | Direction | Meaning |
| --- | --- | --- |
| `open` | Browser to gateway | Dial `ip:port` for this info-hash on connection id `conn` (TCP, or uTP on the VPS gateway) |
| `opened` / `failed` | Gateway to browser | Handshake accepted, or the reason it was not (refused, timeout, not BitTorrent) |
| `data` | Both | Raw peer-wire bytes for `conn` |
| `close` | Both | Connection ended, with a reason |
| `accept` | VPS gateway to browser | An inbound peer connected for this info-hash |
| `ping` | Browser to gateway | Keeps the session alive and renews the CPU allowance |

### Rules that keep it cheap and safe

- **Handshake gate.** The gateway itself writes the 68-byte BitTorrent handshake and forwards nothing until the remote answers with a valid one for the same info-hash. The tunnel can therefore only reach things that speak BitTorrent for that torrent. This replaces the signed peer tokens, which could not cover peers learned by exchange.
- **Batch upstream.** On the Durable Object, 20 incoming messages cost 1 request. The browser coalesces all frames for a socket into one message at most every 300 to 500 ms, and refills each peer's request queue several seconds at a time. Urgent frames (a cancel after a seek) skip the timer. Upload data and `have`s join these messages; alone they are flushed at most every 2 s.
- **Flow control by request.** Peers only send what was requested, so the browser bounds in-flight data (16 MiB per socket) and the gateway never buffers more than that. No extra credit scheme is needed. In the other direction the browser caps its upload, and the gateway closes a connection whose unwritten bytes exceed a bound (a peer that does not read).
- **Six connections per socket.** One WebSocket is one invocation and therefore 6 outbound sockets (a Cloudflare limit). Wider sessions open a second and third WebSocket to the same object, only while there are candidates the open sockets have no room for; a surplus socket left without any connection for 25 s is closed (an idle one still pings every 30 s).
- **Abuse limits.** Origin allow-list, no private or reserved addresses, no ports below 1024, idle timeout of 90 s per connection, a cap on session length, a per-IP rate limit rule. A Turnstile check can be added in front of session creation if the endpoint is abused.
- **Reconnect is normal.** Gateways restart on deploys. The browser reopens the socket, redials its best peers from memory and re-requests unfinished blocks.

### Requests per film

Estimate for a 2 GB film recovered in 10 minutes over 2 sockets:

| Item | Count | Billed Cloudflare requests |
| --- | --- | --- |
| Static site (Pages assets) | about 30 files | 0 |
| WebSocket upgrades | 2 | 2 |
| Upstream messages (about 1 per second across both sockets) | about 600 | 30 |
| Reconnects, seeks, pings | about 100 messages | 5 to 10 |
| Upload flushes not riding on other messages (at most 1 per 2 s per socket, only when peers ask) | 0 to about 600 | 0 to 30 |
| **Total** |  | **about 40** (upload mostly rides on existing messages; the budget stays 40 per GB) |

A slow two-hour session on a thin swarm sends about 2,500 messages, or roughly 130 billed requests. The proof of concept spends several hundred to a few thousand for the same film. The VPS sees 1 request per session (the scouted `/peers` stream, which also carries the metadata) plus 1 WebSocket when its gateway is engaged.

## Playback

Playback reads from the piece buffer through one interface, `read(offset, length)`, and every read doubles as a scheduling hint. This stays close to what magnet-watcher does today.

- **Reads steer recovery.** A read of missing data sets the playback window there and waits for the pieces. The player never talks to the network.
- **Native player when Chromium can decode the file.** A service worker serves `/stream/<info-hash>/<file>` with HTTP range support from the piece buffer, so MP4 and WebM play in a plain `<video>` element with native seeking and buffering. The same URL gives a download link. The worker stores nothing: it answers only for files a page of the site has open, and asks that page for the bytes, which reads them through `read(offset, length)` (`apps/web/src/lib/stream/`).
- **libmedia for everything else.** MKV, HEVC, AC3, DTS and the other formats in the current support table keep using `@libmedia/avplayer` (WebCodecs, MSE, or wasm decoders) over the same `read` interface. It needs cross-origin isolation (SharedArrayBuffer), which the site's own COOP/COEP headers give (Cloudflare Pages `_headers`, OQ-06); the streaming worker is the only service worker.
- **Which player.** The page reads the codecs from the file's own index (MP4 `moov`, WebM `Tracks`) before either player loads, and asks Chromium about each; one audio track at most and no subtitle tracks inside, or libmedia plays (`apps/web/src/lib/player/probe.ts`, `codecs.ts`).
- **Start threshold.** Play once the container index and about 10 s of video are verified. Rebuffer only when the window is empty, and resume at 5 s buffered (`apps/web/src/lib/player/buffering.ts`; seconds become bytes at the average bitrate).
- **Bitrate feeds the picker.** Once the container is parsed, the average bitrate sets the window size in bytes and the threshold for adding sockets (twice the bitrate for 30 s, OQ-11) or engaging the VPS.

## Storage and database

Nothing is stored persistently on any server, and no database is needed. The VPS keeps only memory caches with fixed byte budgets, lost on restart.

| Data | Where | Lifetime |
| --- | --- | --- |
| Verified pieces | Browser, one sparse OPFS file per torrent, written from the engine worker | Every video opened is kept, even partly recovered, until the user deletes it (one video or all; OQ-13, docs/storage.md) |
| Metadata, peer notes, playback position | Browser, IndexedDB | Until the pieces are deleted (metadata of magnets only looked up: a small bounded cache) |
| Tunnel state | Durable Object memory | The WebSocket's lifetime |
| DHT routing table, tracker responses | VPS memory, 5 minute cache | Process lifetime |
| Verified info dicts | VPS memory, size-bounded cache | Until evicted (least used first) or restart |
| Peer scores per info-hash | VPS memory, size-bounded cache | About 2 h without use, or restart |

VPS memory budgets (defaults, each configurable, all weighed in bytes rather than entries, so a burst of large torrents cannot grow memory):

| Cache or pool | Default | Holds about | Why this size |
| --- | --- | --- | --- |
| Info dicts | 32 MiB, entries over 4 MiB served but not cached | 500 to 1,500 torrents (a 4 GB film with 4 MiB pieces is about 25 KB) | Immutable and the most valuable entry: a hit makes the file list instant |
| Peer scores | 16 MiB, at most 1,000 addresses per info-hash (about 96 bytes each) | about 170 torrents at 1,000 addresses, 800 at 200 | Scores fade within hours; keeping more is keeping stale data |
| Discovery results | 16 MiB, 5 min | Recent lookups | Covers retries and reloads |
| Probes in flight | 256 sockets | | A few MiB of buffers |
| Metadata fetches in flight | 4, at most 8 MiB each | | Bounds the worst case to 32 MiB; typical fetches are a few KB |

About 64 MiB of caches plus bounded transient memory: small enough for a modest VPS, large enough that any magnet looked up recently is answered from memory.

- **OPFS replaces Cache Storage.** A synchronous access handle in a worker gives random reads and writes without one cache entry per piece, which matters for range reads during playback.
- **Why no Postgres.** Every magnet is watched once by one user, so there is nothing to share between sessions. Session state dies with the socket. Supabase would add a component and a failure mode for no gain.
- **Why no server-side piece cache.** A cache hit needs a second viewer of the same torrent, which is the rare case. R2 or disk would cost money and add legal exposure for data that is almost never reused.
- **The Durable Object stores nothing.** The free plan requires a SQLite-backed class, but the object is used for its WebSocket and CPU allowances, not for storage.
- **Why memory and not SQLite on the VPS.** Peer scores go stale within hours and the metadata is cheap to fetch again, so surviving a restart is worth little; a disk store would keep a lasting record of which torrents were looked up and which addresses share them. Memory with byte budgets gives the same hit rate for recent magnets with none of that.
- **If this changes.** Should a catalogue of popular magnets emerge, the metadata (immutable, 20 to 200 KB, keyed by info-hash) could also go to the Workers Cache API, still without a database.

## Cost model

On the free plan the binding limit is Durable Object duration, not requests: about 28 hours of open tunnel sessions per day.

Duration is billed at 128 MB while the WebSocket is open, so 13,000 GB-s per day is 101,500 s. The browser closes its sockets as soon as the file is complete, so a session costs download time, not viewing time.

| Session type | Object time | Billed requests | Sessions per day on the free plan |
| --- | --- | --- | --- |
| 2 GB film, healthy swarm | about 10 min | about 40 | about 170 (limited by duration) |
| 2 GB film, thin swarm at playback speed | about 2 h | about 130 | about 14 (limited by duration) |

A few users a day fit the free plan with a wide margin on requests (100,000 per day).

| Component | Free | When it is outgrown |
| --- | --- | --- |
| Static site on Cloudflare Pages or GitHub Pages | Unlimited asset requests | Stays free |
| Tunnel gateway on Durable Objects | 28 object-hours and 100,000 requests per day | Workers Paid, 5 USD per month: 868 object-hours included, then about 0.006 USD per hour |
| Tunnel gateway on a plain Worker (paid plan only) | Not viable at 10 ms CPU | No duration charge at all; 30 million CPU-ms included, which is the cheaper option once on the paid plan |
| VPS | Existing machine | Bandwidth only matters for sessions escalated to its gateway. Scouting costs a few KB per probed address and up to about 64 MiB of memory for caches |
| Supabase | Not used | Not applicable |

Cloudflare does not charge for Worker egress, which is why the default data path runs there and the VPS carries bytes only for swarms that need it.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| Keep one HTTP request per batch and tune it further | The setup cost per request is structural. Lost unchoke slots cannot be tuned away |
| Full-duplex streaming fetch through a plain Worker with native pipes | Chromium only supports half-duplex request streams: the response is not readable until the upload ends |
| WebTorrent only (WebRTC peers) | Too few peers support it; most torrents would have none |
| Protocol logic in the Durable Object, browser sends only piece ranges | Fewer upstream messages, but more edge CPU, two schedulers to keep in agreement, and it contradicts doing as much as possible in the client. Batching already makes upstream cheap |
| Everything on the VPS (a normal torrent client plus HTTP streaming) | Simplest to build, but all bytes cross the VPS, it needs disk or large memory per session, and it scales with VPS size instead of for free |
| Direct Sockets API in Chromium | Only available to Isolated Web Apps, not to a normal site |
| WebTransport to the gateways | Not offered by Workers; possible on the VPS later, with no clear gain over WebSocket for this load |
| Server-side piece cache (R2, disk) or a Postgres catalogue | One viewer per magnet means almost no hits; see Storage |
| Keep the PoC's signed tokens and `/swarm` polling | Needs polling and cannot authorise peers found by exchange; the handshake gate does the same job statelessly. Probing itself is kept (Scouting), streamed and without tokens: it doubles the connections to live peers, but saves the tunnels about 20 s per dead address |
| Browser-only exploration (no scouting) | Measured in M2: through 6-slot tunnels that hold a dead connect for 20 s, a session tries a few addresses per minute; with 80 to 90 % dead addresses, file lists timed out and large swarms delivered from 2 peers out of 1,582 known |
| Peer scores and a magnet queue in SQLite on the VPS | Scores go stale within hours, so persistence buys little; a disk store keeps a record of lookups. Memory with byte budgets serves the same purpose (Storage) |
| Never upload | Simplest, but leechers serve their regular slots to peers that upload to them; a non-uploading client waits for optimistic unchokes, rare in large swarms |

## Risks, open questions and delivery

The largest unknown is whether a free-plan Durable Object may spend enough CPU to forward a film; a one-day spike answers it before anything else is built.

### Risks

| Risk | Likelihood | Mitigation |
| --- | --- | --- |
| Free-plan Durable Object CPU is capped near 10 ms per event rather than 30 s (the docs do not state the free figure) | Unknown | Phase 0 measures it. The `ping` frame renews the allowance each second for about 180 billed requests per hour. Otherwise: Workers Paid at 5 USD, or the VPS gateway as the default path |
| The 6-connection limit behaves differently for sockets opened later in a WebSocket's life | Low | Phase 0 measures it. The design already scales by adding sockets |
| Cloudflare objects to BitTorrent traffic from its network, or peers block its address ranges | Medium | The VPS gateway speaks the same protocol; switching the default path is a configuration change |
| The gateway is used as a general proxy | Low | Handshake gate, origin check, address and port rules, rate limit, optional Turnstile |
| Copyright complaints reach the VPS provider once it announces to swarms | Medium | Announce only for active sessions and only when escalation is needed; pick the provider with this in mind |
| Gateway restarts drop all tunnels mid-film | Certain, a few times a week | Reconnect and redial from memory; the buffer covers the gap |
| A peer reachable from the VPS is not reachable from Cloudflare (or bans one of the two) | Medium | The score is only a prior: a scouted address that fails through the tunnel is treated as untried, and unknown addresses are still dialled after the live ones. One reachable from Cloudflare but not from the VPS is lost unless a peer names it again by peer exchange |
| VPS probing looks like port scanning to providers or peers | Low | Probes go only to addresses that trackers and the DHT announced for the info-hash (never to addresses a browser supplies), speak BitTorrent, never re-probe within 10 minutes, and are capped globally |
| Closing a jammed exploring WebSocket does not free the Durable Object's pending connects | Unknown | Not measured: the spike showed a new WebSocket opens at once while another is stuck, not what happens after the stuck one closes. Recycling is capped (4 per session) and can be turned off (`maxRecycles: 0`); measure with tools/spike before relying on it |
| VPS memory | Low | Every cache is weighed in bytes with a fixed budget; probe and fetch pools are bounded |
| Uploading changes the user's legal position and uses their upstream | Certain | Decided (OQ-04): on by default with a setting to turn it off, capped rate, only verified pieces, only while downloading |
| One lossy client link stalls all peers on a socket (head-of-line blocking) | Low | Two sockets for sessions that need throughput |

### Open questions

- [x] Should the page upload to peers that reciprocate? Yes (OQ-04, revised Nov 19): tit-for-tat choker, capped, on by default with a setting to turn it off. See Reciprocation.
- [ ] Are BitTorrent v2-only torrents (SHA-256 merkle trees) in scope for the first version, or hybrids and v1 only?
- [x] Is the static site moving to Cloudflare Pages, which would let it send the isolation headers itself and drop the service-worker workaround? Yes (OQ-06): `_headers` sends COOP/COEP and the isolation worker is gone.
- [ ] What session length cap is acceptable for a slow swarm on the free plan: 2 hours, or until complete?

### Delivery

1. **Phase 0, spike (1 to 2 days).** A minimal Durable Object tunnel and a script that pulls 1 GB from a test seed through it. Measure CPU per gigabyte, the connection limit, and billed requests. Decide free object, paid Worker, or VPS as the default path.
2. **Phase 1, tunnel and engine.** Gateway in magnet-worker (replacing `/v1/blocks`). Peer-wire engine, metadata fetch and peer exchange in magnet-watcher, behind the existing scheduler interface. Streamed `/peers` in magnet-seeders. Outcome: persistent connections, about 40 requests per film.
3. **Phase 2, picker, buffer, scouting and reciprocation.** Deadline-based block assignment, scarce-piece rule, OPFS store, session memory, range-serving service worker. Scouting and metadata in the `/peers` stream with bounded memory caches; scout-aware dialling and early width in the engine; upload with a tit-for-tat choker.
4. **Phase 3, few-seeder path.** VPS tunnel gateway: session announce, inbound TCP listener, then uTP and hole punching. Escalation rules in the engine.
5. **Phase 4, cleanup.** Remove `/swarm`, `/files`, the PoC's JSON `/peers?hash=` and tokens from magnet-seeders; keep probing, the scout, the metadata fetch and `GET /metadata?ih=`. Add web seeds and WebRTC peers as extra connection types.

Each phase ships on its own and is measured on the same three numbers: seconds to first verified piece, sustained megabytes per second, billed requests per gigabyte.
