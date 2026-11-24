# Tunnel protocol (v1)

The browser engine (`packages/engine`) talks peer-wire to BitTorrent peers
through a gateway that owns the TCP sockets: the Cloudflare Durable Object
(`apps/gateway`) and, later, the VPS gateway (M4). This document specifies
the bytes on the WebSocket between them. The frame set and the rules come
from docs/architecture.md §Tunnel protocol and request budget (› Frames,
› Rules that keep it cheap and safe); the architecture document leaves the payloads open, and this
document defines them.

The reference implementation is `packages/tunnel-protocol` (TypeScript,
no runtime dependencies); the VPS gateway has a Rust twin
(`services/seeders/src/tunnel/codec.rs`) tested on the same vectors. Binary test vectors for other implementations
are in `packages/tunnel-protocol/fixtures/` (`vectors.json` + one `.bin`
per vector).

## Session

- Transport: one WebSocket per session, **binary messages only**. A text
  message is a protocol error.
- Version: negotiated as the WebSocket subprotocol
  `Sec-WebSocket-Protocol: magnet-tunnel.v1`. A gateway refuses the upgrade
  (HTTP 400) if the client does not offer a version it speaks. A future
  incompatible version gets a new subprotocol name (`magnet-tunnel.v2`).
- Route on both gateways: `GET /v1/tunnel` with `Upgrade: websocket` (the
  Cloudflare Worker, and the VPS service `services/seeders`, M4-01).
  Requests whose `Origin` is not in `ALLOWED_ORIGINS` get **403 before the
  upgrade**. The client switches gateway by changing only the base URL.
- **Several WebSockets per session.** `GET /v1/tunnel?session=<id>`, with
  `<id>` 32 lowercase hex digits (128 random bits chosen by the client),
  routes every WebSocket of a session to the same Durable Object; without
  `session` each WebSocket gets its own object. Each WebSocket carries its
  own 6 connections; a session holds at most 3 WebSockets (default,
  `TUNNEL_SOCKETS_PER_SESSION`): the next upgrade gets **429**
  `session_full`. A malformed `session` gets **400** `bad_session`.
- **Per-IP upgrade limit.** At most 30 upgrades per minute per client IP
  (default, `TUNNEL_UPGRADES_PER_MINUTE`), checked before any Durable
  Object exists: over it, **429** `rate_limited` with `Retry-After: 60`.
  The IP is only the limiter's key; it is never logged.
- The Durable Object stores nothing (docs/architecture.md §Storage and
  database): all session state lives in memory and dies with the socket.
- **VPS gateway.** The same routes, checks, limits, close codes and
  variable names (`TUNNEL_*`, `ALLOWED_ORIGINS`), with these differences:
  the session is the set of WebSockets with the same `?session=` on the
  process (forgotten with its last WebSocket); concurrent WebSockets are
  also capped per IP (`TUNNEL_SOCKETS_PER_IP`, 12, then 429
  `too_many_sockets_from_client`) and in total (`TUNNEL_MAX_SOCKETS`, 256,
  then 503 `busy`); a connect that timed out frees its slot at once (the
  20 s hold below is a Cloudflare quirk); `ALLOW_PRIVATE_PEERS` is honoured
  only for requests made straight to a loopback listener on a loopback host
  without the proxy's client-IP header (`services/seeders/README.md`).
  Only the VPS gateway answers `announce`, sends `accept` and dials uTP
  (*Announce and inbound connections* and *Peer fields* below); the
  Cloudflare gateway answers `announce` with `announced` UNSUPPORTED and a
  uTP `open` with `failed` UNSUPPORTED_TRANSPORT.

## Message and frame layout

A WebSocket message is the concatenation of **one or more whole frames**.
A frame never spans two messages. All integers are big-endian (network
order).

```
offset  size  field
0       1     type     u8   frame type (table below)
1       2     conn     u16  connection id (0 = the session itself)
3       4     length   u32  payload length in bytes
7       len   payload       layout depends on type
```

Header: 7 bytes. A message holding `n` frames is
`7·n + Σ payload lengths` bytes.

### Frame types

| type | name     | direction        | conn            | payload |
|------|----------|------------------|-----------------|---------|
| 0x01 | `open`   | client → gateway | 0x0001–0x7fff   | peer fields (below), 55 B (IPv4) / 67 B (IPv6), +1 B for uTP |
| 0x02 | `opened` | gateway → client | the `open`'s id | `reserved[8] peer_id[20]` = 28 B |
| 0x03 | `failed` | gateway → client | the `open`'s id | `reason u8` = 1 B |
| 0x04 | `data`   | both             | ≠ 0             | 1…max bytes of peer-wire stream |
| 0x05 | `close`  | both             | ≠ 0             | `reason u8` = 1 B |
| 0x06 | `accept` | gateway → client (VPS only) | 0x8000–0xffff | peer fields, 55 / 67 B, +1 B for uTP |
| 0x07 | `ping`   | client → gateway | 0 only          | 0…8 opaque bytes |
| 0x08 | `announce`  | client → gateway | 0 only       | `info_hash[20] reserved[8] peer_id[20] count u8` + `count` × (`len u16` + URL), see *Announce and inbound connections* |
| 0x09 | `announced` | gateway → client | 0 only       | `info_hash[20] status u8` = 21 B |
| 0x0a | `withdraw`  | client → gateway | 0 only       | `info_hash[20]` = 20 B |

Any other type value (0x00, 0x0b–0xff) is an error (`unknown_type`).

### Peer fields (`open`, `accept`)

```
offset  size     field
0       20       info_hash   v1 SHA-1 info-hash, or the v2 (BEP 52) SHA-256 truncated to 20 bytes
20      8        reserved    handshake reserved bytes
28      20       peer_id     handshake peer id
48      1        family      4 = IPv4, 6 = IPv6
49      4 | 16   ip          address, network order
53 | 65 2        port        u16, 1..65535
55 | 67 1        transport   optional: absent = TCP, 1 = uTP (BEP 29)
```

The payload is exactly 48 + 1 + 4 + 2 = **55 bytes** for IPv4 and
48 + 1 + 16 + 2 = **67 bytes** for IPv6 over TCP, one byte more (56 / 68)
with a `transport` byte. Port 0, any other family and any other length are
`bad_payload`. Addresses are literals only: there are no hostnames, so
the gateway never resolves names (no DNS rebinding).

**Transport** (M4-04e). TCP is encoded by leaving the byte out, so every
TCP frame is byte-for-byte the frame of v1 before uTP and the subprotocol
stays `magnet-tunnel.v1`; a present byte is never 0 (an explicit 0 is
`bad_payload`: one encoding per frame).

| value | name | |
|---|---|---|
| (absent) | `TCP` | the default |
| 1 | `UTP` | uTP (BEP 29), VPS gateway only |
| 2–255 | | reserved: they decode, and a gateway that does not carry them answers `failed` UNSUPPORTED_TRANSPORT |

- In `open`, the transport the gateway dials with. The Cloudflare gateway
  has no UDP (Workers) and fails every non-TCP `open` with
  UNSUPPORTED_TRANSPORT. The VPS gateway dials uTP from the UDP socket of
  its announced port (*Announce and inbound connections*), so only while
  the `open`'s session (any of its WebSockets) announces the `open`'s
  info-hash; otherwise, and for an unknown transport, UNSUPPORTED_TRANSPORT.
  The address rules, the 6 slots, the timeouts and the handshake gate are
  the same as for TCP. While a uTP connect is pending (the connect
  timeout), the SYNs that come from the `open`'s exact address belong to
  that `open`, not to the inbound listener: this is the gateway's half of
  a hole punch (M4-05, *Hole punching* below), where the remote connects
  to the announced port at the same time. Every connection to the address
  that comes up gets the handshake; the first on which the remote's valid
  handshake arrives is the `open`'s, the others are closed.
- In `accept`, the transport the inbound peer connected over. A client
  closes an `accept` with a transport it does not know.
- Deployment order: a client that predates the byte rejects a uTP `accept`
  as `bad_payload` (and closes the WebSocket), and a gateway that predates
  it does the same with a uTP `open`. Clients only send uTP `open`s when
  asked to (the engine dials TCP, and uTP only for a hole punch), and
  the VPS gateway sends uTP `accept`s only to sessions that announce: ship
  the site before (or with) a VPS that takes inbound uTP.

- In `open`, `reserved` and `peer_id` are what the gateway writes in the
  handshake **on the client's behalf**: the client knows exactly which
  extension bits (BEP 10 extension protocol, BEP 6 fast, …) it advertised.
- In `accept`, they are the inbound remote's, and `info_hash` is the
  torrent it asked for.

### How the info-hash is bound to a connection

The info-hash is per connection, in `open`. The gateway:

1. checks the address (rules below) and connects over TCP;
2. writes the 68-byte BitTorrent handshake itself:
   `0x13 "BitTorrent protocol" reserved[8] info_hash[20] peer_id[20]`;
3. reads the remote's 68-byte handshake and checks the protocol string
   (else `failed` NOT_BITTORRENT) and that the info-hash equals the one in
   `open` (else `failed` INFOHASH_MISMATCH);
4. only then sends `opened` with the remote's `reserved` and `peer_id`, and
   starts relaying bytes as `data` frames.

So the client never sees, and can never send, a byte on a connection
before the remote proved it serves that torrent (the **handshake gate**).
The 68-byte handshakes themselves are not relayed: `data` carries the
peer-wire stream that follows them.

### Reason codes

`failed` (why an `open` gave no `opened`):

| code | name                   | meaning |
|------|------------------------|---------|
| 1    | `REFUSED`              | TCP connect refused / failed |
| 2    | `TIMEOUT`              | no TCP connection or no remote handshake in time |
| 3    | `NOT_BITTORRENT`       | the remote's first 68 bytes are not a BitTorrent handshake |
| 4    | `INFOHASH_MISMATCH`    | the remote handshook for another torrent |
| 5    | `FORBIDDEN_ADDRESS`    | private/reserved/loopback/link-local/multicast address, or port < 1024; no connection attempted |
| 6    | `TOO_MANY_CONNECTIONS` | the WebSocket already has 6 connections |
| 7    | `CONN_IN_USE`          | the `conn` id is still in use on this WebSocket |
| 8    | `REMOTE_CLOSED`        | the remote hung up before completing its handshake |
| 9    | `INTERNAL_ERROR`       | anything else (on the VPS: a uTP connection limit of the gateway) |
| 10   | `UNSUPPORTED_TRANSPORT`| the `open`'s transport is not available here: uTP on the Cloudflare gateway, uTP on the VPS gateway while the session does not announce the info-hash, or an unknown transport; no connection attempted |

`close` (why a connection ended):

| code | name             | meaning |
|------|------------------|---------|
| 0    | `NORMAL`         | the sender asked for it |
| 1    | `REMOTE_CLOSED`  | the remote peer closed the TCP connection |
| 2    | `REMOTE_ERROR`   | the TCP connection failed (reset, write error) |
| 3    | `PROTOCOL_ERROR` | the other side broke the per-connection rules (e.g. `data` before `opened`) |
| 4    | `SESSION_ENDING` | the gateway is ending the session (WebSocket idle, shutdown, WebSocket closed) |
| 5    | `INTERNAL_ERROR` | anything else |
| 6    | `IDLE`           | no byte either way on this connection for the idle timeout (90 s) |
| 7    | `SESSION_TIMEOUT`| the session reached its maximum length (2 h) |
| 8    | `WRITE_OVERFLOW` | the peer does not read: more bytes than the gateway's bound (4 MiB) waited to be written to it (see *Rules*) |

Receivers treat unknown codes as `INTERNAL_ERROR` (forward compatibility);
the decoder accepts any byte value.

### Announce and inbound connections (VPS gateway)

docs/architecture.md › Chunk recovery › Swarms with few seeders, step 3
(M4-02). A seeder behind NAT never answers a dial; it connects out to the
addresses trackers and the DHT give it. On request the VPS gateway becomes
such an address for one session and hands every peer that connects in to
the browser.

- **`announce`** (client → gateway, `conn` 0). Payload: `info_hash[20]`,
  `reserved[8]`, `peer_id[20]` (what the gateway answers inbound handshakes
  with, on the client's behalf, as `open` does for outbound ones), then
  `count u8` (0…16) and `count` tracker URLs, each `len u16` (1…512) and
  that many printable ASCII bytes (0x21–0x7e). Anything else is
  `bad_payload`. Only `http`, `https` and `udp` URLs with a host, on port
  80, 443 or >= 1024 (a `udp` URL names its port), are used; others are
  ignored. The gateway's own default trackers are added.
- **`announced`** (gateway → client, `conn` 0): `info_hash[20] status u8`,
  the answer to every `announce` and `withdraw`, and sent alone when the
  gateway changes the state itself.

  | status | name | meaning |
  |---|---|---|
  | 0 | `ACTIVE` | announcing (trackers `started`, then every 15 min; DHT `announce_peer`); inbound peers for it come to this WebSocket |
  | 1 | `STOPPED` | the announce ended (the answer to `withdraw`) |
  | 2 | `UNSUPPORTED` | this gateway takes no inbound peers: the Cloudflare gateway (Workers cannot listen), or a VPS without `TUNNEL_INBOUND_ADDR` |
  | 3 | `LIMIT_REACHED` | too many info-hashes on this WebSocket (2), from this client IP (4; an IPv6 client is its /64) or on the gateway (64); or the announce rounds this minute are used up (6 per client IP, 60 in all, see *Lifetime*) |
  | 4 | `INTERNAL_ERROR` | anything else (the listener could not be opened) |

  Receivers treat unknown statuses as `INTERNAL_ERROR`.
- **`withdraw`** (client → gateway, `conn` 0): `info_hash[20]`; stop
  announcing it on this WebSocket. Connections already accepted stay.
- **Lifetime.** An announce belongs to the WebSocket that sent it and ends
  with `withdraw` or with that WebSocket (closed, idle, session timeout,
  shutdown). When no WebSocket announces an info-hash any more the gateway
  sends `stopped` to its trackers; when none announces anything it closes
  the listening port. Nothing is announced and no port listens before a
  client asks. Repeating `announce` on the same WebSocket is harmless (it
  updates the handshake fields and adds trackers). A client that reconnects
  announces again on its new WebSocket.
- **Announce rounds are rationed.** Each round sends `started`, and later
  `stopped`, to up to 32 trackers, and a DHT announce. A round starts when
  an `announce` creates a swarm or adds trackers to one (joining a swarm
  another WebSocket announces, or repeating an `announce`, costs nothing;
  the periodic re-announce is not counted). Rounds are counted per minute,
  per client IP (`TUNNEL_ANNOUNCE_ROUNDS_PER_IP_PER_MINUTE`, 6) and on the
  gateway (`TUNNEL_ANNOUNCE_ROUNDS_PER_MINUTE`, 60): past either, a new
  swarm is refused (`LIMIT_REACHED`) and added trackers are ignored (the
  announce stays `ACTIVE` with the trackers it had). A client that cycles
  `announce` and `withdraw` cannot make the gateway a generator of tracker
  or DHT traffic.
- **What the gateway announces.** Its own port (`TUNNEL_ANNOUNCE_PORT`, by
  default the listener's), with a random peer id of its own, `left` above
  0 (a leecher: trackers do not hand a seed to other seeds) and `numwant=0`.
- **TCP and uTP on the same port.** While it listens, the gateway also
  binds a UDP socket on the listener's port number and speaks uTP (BEP 29)
  on it: inbound uTP peers, and the outbound uTP `open`s of the sessions
  that announce (all from that one port, as hole punching needs, M4-05).
  The UDP socket opens with the TCP listener and its uTP gate closes with
  it; the socket itself stays bound, answering no SYN, only while uTP
  connections made through it last (as accepted TCP connections outlive
  their listener), and the next listener reuses it. Before the first
  announce nothing is bound on UDP for uTP. The port must differ from the
  DHT's (`DHT_PORT`); the gateway turns inbound peers off on a clash.
- **Inbound gate.** The remote speaks first. The gateway reads at most its
  68-byte handshake within the handshake timeout (10 s): a first byte off
  the protocol string, or an info-hash no WebSocket announces, closes the
  connection **without a byte in reply**; so does silence. Over uTP the
  same rules apply once the uTP handshake is complete (one `ST_STATE` per
  SYN received, never retransmitted; the remote has 10 s to complete it;
  a SYN from a non-public address is ignored unless an announcing session
  may reach private peers); a refused uTP peer is forgotten without even
  an `ST_FIN` or `ST_RESET`, so its later packets get no reply at all, and
  packets that match no uTP connection never get one. A valid one is
  answered with the announcing session's handshake, then announced to the
  client as **`accept`** (`conn` 0x8000–0xffff, chosen by the gateway; peer
  fields: the info-hash, the remote's reserved bytes and peer id, and the
  address it connected from — not a port it listens on) followed by any
  peer-wire bytes that came with the handshake as `data`. From then on the
  connection is like an opened one: `data` both ways, `close` either way,
  the idle timeout, the write bound. An address that is not public is
  refused unless the WebSocket may reach private peers (`ALLOW_PRIVATE_PEERS`).
  The `accept` of a uTP peer carries the transport byte (uTP).
- **Limits** (all `TUNNEL_*`, `services/seeders/README.md`): inbound
  connections per session (16) and in total (256), handshakes in flight
  (64), info-hashes per WebSocket (2) and in total (64), TCP and uTP
  together; for uTP also half-open connections (64 in total, 4 per remote
  IP) and connections per remote IP (8). Accepted
  connections do not take the 6 outbound slots of the WebSocket; past a
  limit an inbound peer is closed without a reply.
- **Client.** An `accept` for an info-hash the client does not announce
  (or no longer does) is answered with `close`. The engine runs an
  accepted connection as any other `PeerConnection` (it joins the choker
  and the upload), keyed by its address with an `in:` prefix and never
  dialled back.

### Hole punching (BEP 55, M4-05)

docs/architecture.md › Chunk recovery › Swarms with few seeders, step 4.
No new frame: the BEP 55 messages travel inside peer-wire `data` like any
extension message, and the gateway only dials.

1. The session announces on the VPS gateway (`announced` ACTIVE): only
   then can it dial uTP from the announced port.
2. A relay is a peer connected to that port and supporting `ut_holepunch`
   (an inbound peer, or one reached over uTP): it sees the gateway's
   address as the announced port, which is what it will tell the target.
   A peer the gateway dialled over TCP sees an ephemeral port and is not
   used.
3. The engine sends the relay a `rendezvous` naming a target the relay
   listed in `ut_pex` with the holepunch flag (0x08), one it could not
   reach otherwise. The relay sends `connect` to both sides.
4. On the relay's `connect` (only for a rendezvous it sent, through that
   relay, for that address) the engine sends a uTP `open` to the target,
   at once rather than with the next batch. The target's own SYN opens
   its NAT to the gateway's port; whichever side's SYN gets through, the
   connection is that `open`'s (above). If the target's SYN reached the
   gateway before the `open`, it arrives as an `accept` from the target's
   address instead; the engine keeps that one and closes the `open`'s.
5. An `error` from the relay (NoSuchPeer, NotConnected, NoSupport,
   NoSelf), no answer within 10 s or a failed `open` marks the target in
   the session's address book, and the session goes on.

The messages are `msg_type u8, addr_type u8, addr, port u16, err_code
u32`. libtorrent leaves `err_code` out of `rendezvous` and `connect`, so
the engine accepts those with or without it; an `error` must carry it.
Shown against libtorrent 2.0 as relay and as target (docs/decisions/utp.md
› Hole punching with real clients).

The engine relays for nobody: a `rendezvous` it receives is answered with
the error NotConnected, and a `connect` it did not ask for is ignored, so
a peer cannot make the gateway dial an address of its choosing. It still
lists `ut_holepunch` in its extension handshake (BEP 10 `m`), and has to:
that is the message id the relay sends the `connect` to. A peer may
therefore pick the session as a relay; every such rendezvous gets
NotConnected, which BEP 55 allows (a relay answers for the targets it is
not connected to) and which only costs the asker one round trip. The
engine sends no `ut_pex` of its own; other peers may list the session in
theirs with the flag 0x08, so a third peer may ask a relay to connect it
to the session: that `connect` reaches the session as an unsolicited one
and is ignored (counted in the diagnostics as `unsolicited`). The
gateway's rules are unchanged: uTP `open`s only while the session
announces the info-hash, only to addresses the address policy allows.

## Rules

From docs/architecture.md §Tunnel protocol and request budget › Rules that
keep it cheap and safe,
made precise here:

- **Handshake gate.** No byte is relayed in either direction before the
  remote's valid handshake for the same info-hash (see above). `data` from
  the client for a connection that is still opening closes it with
  `close` PROTOCOL_ERROR. `data`/`close` for an unknown `conn` are ignored
  (they may race with a `close` from the gateway).
- **6 connections per socket.** At most 6 connections (opening, open, or
  still closing) per WebSocket — the Workers limit on simultaneous
  outgoing connections. The 7th `open` gets `failed` TOO_MANY_CONNECTIONS
  without any connection attempt. A slot frees when its TCP socket has
  closed.
- **Batching.** A sender may put any number of frames (up to the
  receiver's `maxFrames`, default 1024) in one message; billing counts
  WebSocket messages, so senders should batch whatever is ready instead of
  sending one message per frame.
- **conn ids.** The client picks ids for `open` in 0x0001–0x7fff; the
  gateway picks ids for `accept` in 0x8000–0xffff; 0 is the session and
  only carries `ping`, `announce`, `announced` and `withdraw`. An id is reusable once its `failed`/`close` has
  been sent or received.
- **Bytes held for a peer (gateway).** Peer-wire bytes the client sends
  are written to the peer in order; those not written yet (the peer reads
  slowly or not at all, the TCP window is full) are held by the gateway,
  at most 4 MiB per connection (default, `TUNNEL_MAX_PENDING_WRITE_BYTES`
  on the Cloudflare gateway). A `data` frame that would take a connection
  past the bound is not written: the connection is closed with `close`
  WRITE_OVERFLOW, and the other connections of the WebSocket go on. The
  client keeps far below it (its upload is capped at 2 MiB/s in total,
  docs/architecture.md §Chunk recovery › Reciprocation), so only a peer
  that stopped reading hits it.
- **close** is final and needs no reply. Either side may send it; the
  gateway closes the TCP socket on receipt.
- **ping** (browser → gateway, as in the architecture) keeps the session
  alive and, on the Durable Object, renews the CPU allowance; it needs no
  reply and is not logged. The codec also decodes a gateway → client
  `ping` (reserved; the client ignores it). Cadence: see *Session
  lifetime*.
- **Address policy (gateway).** `open` (over any transport) to a private, reserved, loopback,
  link-local, CGNAT, multicast or documentation address (IPv4, IPv6 and
  IPv4-mapped/embedded IPv6), or to a port below 1024, gets `failed`
  FORBIDDEN_ADDRESS with no connection attempt. Local tests may disable the
  address part with `ALLOW_PRIVATE_PEERS` under `wrangler dev` only.
- **Protocol errors** at the session level — a frame that does not decode,
  a text message, a gateway-only frame (`opened`, `failed`, `accept`,
  `announced`) sent by the client — close the WebSocket with code 1002 and every TCP
  connection of the session.

## Session lifetime

| rule | default | variable (both gateways) |
|---|---|---|
| a connection with no byte relayed in either direction is closed with `close` IDLE | 90 s | `TUNNEL_IDLE_TIMEOUT_MS` |
| a WebSocket on which the client sent no message is closed (code 4000) | 90 s | `TUNNEL_IDLE_TIMEOUT_MS` |
| the session (all its WebSockets) ends, counted from its first WebSocket: `close` SESSION_TIMEOUT for every connection, then the WebSocket close (code 4001); later upgrades to it get **410** | 2 h | `TUNNEL_SESSION_TIMEOUT_S` |
| a connect that timed out keeps its slot taken until 20 s after it started, because the platform keeps it pending that long and queues new connects behind it (docs/spike-report.md, OQ-03); Cloudflare only | 20 s | `TUNNEL_FAILED_CONNECT_HOLD_MS` |

Client duties that follow:

- **`ping` cadence: 30 s.** The client sends a `ping` whenever it has sent
  no other message on a WebSocket for 30 s (`PING_INTERVAL_MS`): three per
  idle timeout, so one late message does not end the session. While
  downloading, the batched upstream (one message every 300–500 ms) already
  keeps the WebSocket alive and no ping is sent; the spike measured that
  incoming messages at that rate keep renewing the Durable Object's CPU
  allowance. An idle session costs 2 messages per minute, 0.1 billed
  requests.
- **Connection keep-alive.** A peer that keeps us choked often sends
  nothing for minutes; to keep such a connection the client sends a
  peer-wire keep-alive (4 zero bytes) on it within the idle timeout.

WebSocket close codes (`SessionCloseCode`):

| code | name | when |
|---|---|---|
| 1000 | `NORMAL` | the client closed it (the gateway's answer) |
| 1002 | `PROTOCOL_ERROR` | a frame did not decode, or a gateway-only frame came from the client |
| 1003 | `UNSUPPORTED_DATA` | a text message |
| 4000 | `IDLE` | no client message for the idle timeout |
| 4001 | `SESSION_TIMEOUT` | the session reached its maximum length |
| 4002 | `SHUTDOWN` | the gateway is shutting down |

**Counters in the close reason.** When the gateway closes a WebSocket, the
close reason is `"<reason> in=<n> up=<n> down=<n>"` (`formatSessionEnd`
/ `parseSessionEnd`): `in` = WebSocket messages received from the client
(what a Durable Object bills, 20:1), `up` / `down` = bytes forwarded to /
from the peers. Always within the 123-byte limit of a close reason. When
the client closes, the runtime may not relay the gateway's answer, so the
client relies on its own counts; the gateway logs the counters of every
session (`tunnel.session_end`, counters and reason codes only).

## Decoder limits

Parsing is explicit binary parsing with bounded time and memory:

| limit        | default | error              |
|--------------|---------|--------------------|
| `maxPayload` | 1 MiB   | `too_long` (checked from the header, before waiting for the payload) |
| `maxFrames`  | 1024    | `too_many_frames` (per message, or per push for the stream decoder) |

Other errors: `unknown_type`, `truncated` (the message ends inside a frame),
`bad_payload` (wrong payload layout for the type), `bad_conn` (conn outside
the range of the type), `empty` (empty message). `decodeMessage` returns
`{ ok: false, error: FrameError }` — it never throws on input.
`FrameDecoder` decodes a byte stream incrementally (frames split across
chunks) and stays failed after its first error.
