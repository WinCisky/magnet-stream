# Magnet Streaming Architecture

Nov 16, 2026 · @Simone

## Summary

The browser becomes the whole BitTorrent client, and the servers shrink to byte tunnels that hold peer connections open for as long as the session lasts.

The proof of concept is slow and request-hungry for one reason: every batch of blocks is a new HTTP request that reconnects, re-handshakes and waits to be unchoked again. The fix is to stop reconnecting.

- **Browser (all the logic).** Peer-wire protocol, metadata fetch, peer exchange, piece picking, SHA-1 verification, piece buffer, playback.
- **Cloudflare (the default data path).** One Durable Object per session accepts a WebSocket and multiplexes up to 6 persistent TCP connections to peers per socket. It forwards bytes and understands nothing beyond the BitTorrent handshake. Cost: about 30 to 150 billed requests per film.
- **VPS (only what Cloudflare cannot do).** UDP discovery (DHT, UDP trackers) in one streamed request, and later a second tunnel gateway that accepts inbound connections and speaks uTP, for swarms whose few seeders sit behind NAT.
- **No database and no server-side storage.** Supabase is not used. Pieces live only in the browser, for the session.

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
| Open outbound connections | 6 per invocation, the 7th is queued | 6 peers per WebSocket; more peers means more sockets |
| Outbound TCP (`connect()`) | Allowed, not to Cloudflare or private IPs | Peers reachable over TCP can be dialled from the edge |
| UDP, inbound TCP | Not available | No DHT, no UDP trackers, no uTP, no listening port |
| Durable Objects | SQLite-backed class, 100,000 requests and 13,000 GB-s per day | About 28 hours of active object time per day |
| Durable Object WebSocket billing | 20 incoming messages = 1 request, outgoing free | Downloaded data costs nothing; upstream must be batched |
| Durable Object CPU | 30 s, reset by each incoming message (free-plan figure not stated) | Must be measured before committing: see Risks |
| WebSocket message size | 32 MiB | No constraint in practice |

**The swarm.** Most peers in a typical swarm are unreachable from outside, because they sit behind NAT and only connect outward. A seed unchokes a handful of peers and rotates an optimistic slot roughly every 30 seconds, so a fresh connection often waits, and a connection that keeps downloading keeps its slot. WebRTC (WebTorrent) peers are rare. With few seeders, the only way to reach a NATed seeder is to be reachable yourself: announce a public port and let it connect in, or punch through over uTP. Only the VPS can do that.

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
| Piece buffer | Browser, OPFS | Random-access store of verified pieces, range reads for the player | Outlive the session unless the user saves the video |
| Player | Browser | Native `<video>` over a service-worker URL, or libmedia | Touch the network |
| Tunnel gateway | Cloudflare Durable Object (magnet-worker) | Accept WebSockets, dial peers, gate on the BitTorrent handshake, forward bytes | Parse messages, hash, store, or choose pieces |
| Discovery | VPS (magnet-seeders) | DHT lookups, UDP and HTTP tracker announces, streamed as found | Probe peers, fetch metadata, mint tokens |
| Fallback gateway | VPS (magnet-seeders) | Same tunnel protocol, plus announce, inbound listener, uTP and hole punching | Carry sessions that path 1 can serve |

Repository mapping: magnet-watcher gains the engine and loses the batch scheduler's request plumbing; magnet-worker becomes the tunnel gateway; magnet-seeders slims to discovery and later grows the fallback gateway.

## Chunk recovery

Recovery is a real BitTorrent client running in a dedicated worker in the page, speaking to peers over long-lived tunnels. Everything below happens in the browser unless a gateway is named.

### Session start

1. **Parse the magnet** and open the first WebSocket to the Cloudflare gateway. This runs in parallel with step 2, so the tunnel is warm when the first peer address arrives.
2. **Discover peers with one request.** `GET /peers?ih=…` on the VPS answers as a stream of newline-delimited JSON: addresses are written the moment a tracker or DHT node returns them, and the response closes after about 30 s. No probing, no polling, no tokens.
3. **Dial as addresses arrive.** Each address becomes an `open` command on a tunnel. The first peer to complete the handshake with `ut_metadata` serves the info dict (BEP 9); the page checks its SHA-1 against the info-hash.
4. **Start requesting** the head and tail of the chosen file as soon as the piece hashes are known.

Expected time from paste to first verified piece on a healthy swarm: 2 to 4 s, against 5 to 15 s today.

### Peer sources, cheapest first

| Source | Where it runs | Cost |
| --- | --- | --- |
| Peer exchange (`ut_pex`) from connected peers | Browser, over existing tunnels | Free, continuous |
| WebSocket trackers and WebRTC peers | Browser, direct | Free, rarely fruitful |
| Web seeds (`ws=` in the magnet, `url-list` in the metadata) | Browser, direct HTTP range requests when CORS allows | Free |
| DHT and UDP or HTTP trackers | VPS, one streamed request | 1 VPS request |
| Inbound and uTP peers | VPS tunnel gateway | VPS bandwidth, only when needed |

Peer exchange is what makes one discovery request enough: after the first connection, the swarm describes itself.

### Connection management

- **Keep connections for the whole session.** A peer that unchoked us stays connected and stays interested. This single change removes most of today's latency.
- **Race at dial time only.** Dial up to 6 candidates per tunnel socket. Replace the slowest or still-choked connections when better candidates appear, never the ones delivering.
- **Score peers on live facts.** Throughput, unchoke delay, bitfield, queue depth (`reqq`), rejects. No separate probing pass.
- **Scale width to need.** One socket (6 peers) is enough when rate exceeds twice the video bitrate. Add a second or third socket only when it does not.
- **Remember within the session.** Addresses that failed, addresses that delivered, and the metadata are kept in IndexedDB so a reload reconnects in one step.

### Piece picking

Requests are assigned per 16 KiB block, by deadline:

1. **Container index.** Head and tail of the file first (MP4 `moov`, Matroska cues).
2. **Playback window.** The next 20 to 30 s of video, strictly in order, from the fastest peers only. A block that is late is requested again from a second peer.
3. **Read-ahead.** Sequential past the window, spread over all unchoked peers, each kept 2 to 4 s of requests deep.
4. **Scarce pieces.** When the buffer is comfortably ahead, fetch pieces that only one or two peers hold. They may not be there later.
5. **Back-fill.** Gaps before the playback position, last.

A seek cancels requests outside the new window with `cancel` messages; the connections stay.

### Swarms with few seeders

The engine escalates only as far as it must:

1. **Patience on Cloudflare tunnels.** Stay connected and interested to every reachable seeder and wait out the optimistic unchoke (about 30 s per rotation). Persistent connections make this work where the proof of concept gave up after 8 s.
2. **Use partial peers.** Leechers with some pieces are sources too. Their bitfields and `have` messages feed the scarce-piece rule above.
3. **Become reachable.** If fewer than 2 peers deliver, or rate stays under the bitrate, the page asks the VPS gateway to join the swarm for this info-hash: it announces its own port to trackers and DHT, accepts inbound TCP and uTP connections, and hands each one to the browser as another tunnel. NATed seeders connect to it within one announce interval.
4. **Hole punching.** uTP with `ut_holepunch` (BEP 55) through a peer we are already connected to reaches NATed peers that do not re-announce soon.
5. **Degrade honestly.** If rate is below bitrate, the page says so and offers to keep buffering; the file still completes.

### Verification

Every piece is SHA-1 checked in the browser before it is stored or played. Blocks are attributed to the peer that sent them, so a peer that causes two failed pieces is dropped for the session. Gateways never see a verified piece and never hash anything.

## Tunnel protocol and request budget

A gateway is a handshake-gated TCP multiplexer: it opens connections, forwards bytes in both directions, and nothing else.

### Frames

Binary WebSocket messages, each a sequence of frames `[type u8][conn u16][length u32][payload]`.

| Frame | Direction | Meaning |
| --- | --- | --- |
| `open` | Browser to gateway | Dial `ip:port` for this info-hash on connection id `conn` |
| `opened` / `failed` | Gateway to browser | Handshake accepted, or the reason it was not (refused, timeout, not BitTorrent) |
| `data` | Both | Raw peer-wire bytes for `conn` |
| `close` | Both | Connection ended, with a reason |
| `accept` | VPS gateway to browser | An inbound peer connected for this info-hash |
| `ping` | Browser to gateway | Keeps the session alive and renews the CPU allowance |

### Rules that keep it cheap and safe

- **Handshake gate.** The gateway itself writes the 68-byte BitTorrent handshake and forwards nothing until the remote answers with a valid one for the same info-hash. The tunnel can therefore only reach things that speak BitTorrent for that torrent. This replaces the signed peer tokens, which could not cover peers learned by exchange.
- **Batch upstream.** On the Durable Object, 20 incoming messages cost 1 request. The browser coalesces all frames for a socket into one message at most every 300 to 500 ms, and refills each peer's request queue several seconds at a time. Urgent frames (a cancel after a seek) skip the timer.
- **Flow control by request.** Peers only send what was requested, so the browser bounds in-flight data (16 MiB per socket) and the gateway never buffers more than that. No extra credit scheme is needed.
- **Six connections per socket.** One WebSocket is one invocation and therefore 6 outbound sockets. Wider sessions open a second and third WebSocket to the same object.
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
| **Total** |  | **about 40** |

A slow two-hour session on a thin swarm sends about 2,500 messages, or roughly 130 billed requests. The proof of concept spends several hundred to a few thousand for the same film. The VPS sees 1 request per session, plus 1 WebSocket when its gateway is engaged.

## Playback

Playback reads from the piece buffer through one interface, `read(offset, length)`, and every read doubles as a scheduling hint. This stays close to what magnet-watcher does today.

- **Reads steer recovery.** A read of missing data sets the playback window there and waits for the pieces. The player never talks to the network.
- **Native player when Chromium can decode the file.** A service worker serves `/stream/<info-hash>/<file>` with HTTP range support from the piece buffer, so MP4 and WebM play in a plain `<video>` element with native seeking and buffering. The same URL gives a download link.
- **libmedia for everything else.** MKV, HEVC, AC3, DTS and the other formats in the current support table keep using `@libmedia/avplayer` (WebCodecs, MSE, or wasm decoders) over the same `read` interface. The existing cross-origin isolation service worker stays.
- **Start threshold.** Play once the container index and about 10 s of video are verified. Rebuffer only when the window is empty, and resume at 5 s buffered.
- **Bitrate feeds the picker.** Once the container is parsed, the average bitrate sets the window size in bytes and the threshold for adding sockets or engaging the VPS.

## Storage and database

Nothing is stored on any server, and no database is needed.

| Data | Where | Lifetime |
| --- | --- | --- |
| Verified pieces | Browser, one sparse OPFS file per torrent, written from the engine worker | The session; deleted on close unless the user keeps the video |
| Metadata, peer notes, playback position | Browser, IndexedDB | Until the pieces are deleted |
| Tunnel state | Durable Object memory | The WebSocket's lifetime |
| DHT routing table, tracker responses | VPS memory, 5 minute cache | Process lifetime |

- **OPFS replaces Cache Storage.** A synchronous access handle in a worker gives random reads and writes without one cache entry per piece, which matters for range reads during playback.
- **Why no Postgres.** Every magnet is watched once by one user, so there is nothing to share between sessions. Session state dies with the socket. Supabase would add a component and a failure mode for no gain.
- **Why no server-side piece cache.** A cache hit needs a second viewer of the same torrent, which is the rare case. R2 or disk would cost money and add legal exposure for data that is almost never reused.
- **The Durable Object stores nothing.** The free plan requires a SQLite-backed class, but the object is used for its WebSocket and CPU allowances, not for storage.
- **If this changes.** Should a catalogue of popular magnets emerge, the first thing worth caching is the metadata (immutable, 20 to 200 KB, keyed by info-hash) in the Workers Cache API, still without a database.

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
| VPS | Existing machine | Bandwidth only matters for sessions escalated to its gateway |
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
| Keep VPS probing and signed tokens | Doubles connections to every peer, needs polling, and cannot authorise peers found by exchange. The handshake gate does the same job statelessly |

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
| One lossy client link stalls all peers on a socket (head-of-line blocking) | Low | Two sockets for sessions that need throughput |

### Open questions

- [ ] Should the page upload to peers that reciprocate? It helps in swarms with leechers and few seeds, but costs upstream messages and changes the legal position of the user.
- [ ] Are BitTorrent v2-only torrents (SHA-256 merkle trees) in scope for the first version, or hybrids and v1 only?
- [ ] Is the static site moving to Cloudflare Pages, which would let it send the isolation headers itself and drop the service-worker workaround?
- [ ] What session length cap is acceptable for a slow swarm on the free plan: 2 hours, or until complete?

### Delivery

1. **Phase 0, spike (1 to 2 days).** A minimal Durable Object tunnel and a script that pulls 1 GB from a test seed through it. Measure CPU per gigabyte, the connection limit, and billed requests. Decide free object, paid Worker, or VPS as the default path.
2. **Phase 1, tunnel and engine.** Gateway in magnet-worker (replacing `/v1/blocks`). Peer-wire engine, metadata fetch and peer exchange in magnet-watcher, behind the existing scheduler interface. Streamed `/peers` in magnet-seeders. Outcome: persistent connections, about 40 requests per film.
3. **Phase 2, picker and buffer.** Deadline-based block assignment, scarce-piece rule, OPFS store, session memory, range-serving service worker.
4. **Phase 3, few-seeder path.** VPS tunnel gateway: session announce, inbound TCP listener, then uTP and hole punching. Escalation rules in the engine.
5. **Phase 4, cleanup.** Remove `/swarm`, `/metadata`, `/files`, probing and tokens from magnet-seeders. Add web seeds and WebRTC peers as extra connection types.

Each phase ships on its own and is measured on the same three numbers: seconds to first verified piece, sustained megabytes per second, billed requests per gigabyte.
