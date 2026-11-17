# Tunnel protocol (v1)

The browser engine (`packages/engine`) talks peer-wire to BitTorrent peers
through a gateway that owns the TCP sockets: the Cloudflare Durable Object
(`apps/gateway`) and, later, the VPS gateway (M4). This document specifies
the bytes on the WebSocket between them. The frame set and the rules come
from docs/architecture.md §Tunnel protocol and request budget (› Frames,
› Rules that keep it cheap and safe); the architecture document leaves the payloads open, and this
document defines them.

The reference implementation is `packages/tunnel-protocol` (TypeScript,
no runtime dependencies). Binary test vectors for other implementations
are in `packages/tunnel-protocol/fixtures/` (`vectors.json` + one `.bin`
per vector).

## Session

- Transport: one WebSocket per session, **binary messages only**. A text
  message is a protocol error.
- Version: negotiated as the WebSocket subprotocol
  `Sec-WebSocket-Protocol: magnet-tunnel.v1`. A gateway refuses the upgrade
  (HTTP 400) if the client does not offer a version it speaks. A future
  incompatible version gets a new subprotocol name (`magnet-tunnel.v2`).
- Route on the Cloudflare gateway: `GET /v1/tunnel` with
  `Upgrade: websocket`. Requests whose `Origin` is not in `ALLOWED_ORIGINS`
  get **403 before the upgrade**.
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
| 0x01 | `open`   | client → gateway | 0x0001–0x7fff   | peer fields (below), 55 B (IPv4) / 67 B (IPv6) |
| 0x02 | `opened` | gateway → client | the `open`'s id | `reserved[8] peer_id[20]` = 28 B |
| 0x03 | `failed` | gateway → client | the `open`'s id | `reason u8` = 1 B |
| 0x04 | `data`   | both             | ≠ 0             | 1…max bytes of peer-wire stream |
| 0x05 | `close`  | both             | ≠ 0             | `reason u8` = 1 B |
| 0x06 | `accept` | gateway → client (VPS only) | 0x8000–0xffff | peer fields, 55 / 67 B |
| 0x07 | `ping`   | client → gateway | 0 only          | 0…8 opaque bytes |

Any other type value (0x00, 0x08–0xff) is an error (`unknown_type`).

### Peer fields (`open`, `accept`)

```
offset  size     field
0       20       info_hash   v1 SHA-1 info-hash, or the v2 (BEP 52) SHA-256 truncated to 20 bytes
20      8        reserved    handshake reserved bytes
28      20       peer_id     handshake peer id
48      1        family      4 = IPv4, 6 = IPv6
49      4 | 16   ip          address, network order
53 | 65 2        port        u16, 1..65535
```

The payload is exactly 48 + 1 + 4 + 2 = **55 bytes** for IPv4 and
48 + 1 + 16 + 2 = **67 bytes** for IPv6. Port 0 and any other family are
`bad_payload`. Addresses are literals only: there are no hostnames, so
the gateway never resolves names (no DNS rebinding).

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
| 9    | `INTERNAL_ERROR`       | anything else |

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

Receivers treat unknown codes as `INTERNAL_ERROR` (forward compatibility);
the decoder accepts any byte value.

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
  only carries `ping`. An id is reusable once its `failed`/`close` has
  been sent or received.
- **close** is final and needs no reply. Either side may send it; the
  gateway closes the TCP socket on receipt.
- **ping** (browser → gateway, as in the architecture) keeps the session
  alive and, on the Durable Object, renews the CPU allowance; it needs no
  reply and is not logged. The codec also decodes a gateway → client
  `ping` (reserved; the client ignores it). Cadence: see *Session
  lifetime*.
- **Address policy (gateway).** `open` to a private, reserved, loopback,
  link-local, CGNAT, multicast or documentation address (IPv4, IPv6 and
  IPv4-mapped/embedded IPv6), or to a port below 1024, gets `failed`
  FORBIDDEN_ADDRESS with no connection attempt. Local tests may disable the
  address part with `ALLOW_PRIVATE_PEERS` under `wrangler dev` only.
- **Protocol errors** at the session level — a frame that does not decode,
  a text message, a gateway-only frame (`opened`, `failed`, `accept`) sent
  by the client — close the WebSocket with code 1002 and every TCP
  connection of the session.

## Session lifetime

| rule | default | variable (Cloudflare gateway) |
|---|---|---|
| a connection with no byte relayed in either direction is closed with `close` IDLE | 90 s | `TUNNEL_IDLE_TIMEOUT_MS` |
| a WebSocket on which the client sent no message is closed (code 4000) | 90 s | `TUNNEL_IDLE_TIMEOUT_MS` |
| the session (all its WebSockets) ends, counted from its first WebSocket: `close` SESSION_TIMEOUT for every connection, then the WebSocket close (code 4001); later upgrades to it get **410** | 2 h | `TUNNEL_SESSION_TIMEOUT_S` |
| a connect that timed out keeps its slot taken until 20 s after it started, because the platform keeps it pending that long and queues new connects behind it (docs/spike-report.md, OQ-03) | 20 s | `TUNNEL_FAILED_CONNECT_HOLD_MS` |

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
