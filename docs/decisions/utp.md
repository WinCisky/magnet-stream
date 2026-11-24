# Decision record: uTP transport for the VPS gateway (M4-03)

Status: **accepted** (approved 2026-11-22) · task M4-03 · R1 shown in M4-04f (§ Interoperability)
(`implementation-plan.json`, plan 1.5), rule OQ-10. M4-04 is split into
M4-04a…M4-04f as proposed below.

**Outcome: own implementation.** No candidate library meets every
functional requirement, and none is widely used in production as a uTP
transport. The approved split of M4-04 is at the end of this document.

## Question and rule

docs/architecture.md › Chunk recovery › Swarms with few seeders (steps 3
and 4) and › Target architecture (Fallback gateway) give the VPS gateway
uTP (BEP 29): inbound uTP peers after an `announce`, and uTP connections
to NATed peers through `ut_holepunch` (BEP 55). Rule OQ-10: a library is
adopted **only** if it is widely used **and** does exactly what is needed;
when in doubt, implement it ourselves. Adopting needs unambiguous evidence;
anything less means our own implementation.

## Functional requirements

| id | requirement | why |
|---|---|---|
| R1 | BEP 29 uTP that interoperates with the common clients (libutp: µTorrent, Transmission; libtorrent-rasterbar: qBittorrent, Deluge) | the peers are those clients |
| R2 | inbound and outbound connections on the **same** UDP socket | peers reach the announced port, and BEP 55 needs our outbound packets to leave from that port so the NAT mapping matches |
| R3 | simultaneous connect to a given address: SYN retransmitted, the remote's SYN from that address accepted while our connect is pending, both succeeding handled | BEP 55: "Upon receiving the connect message, each peer initiates a uTP connection to the other peer … It is possible that both uTP connection attempts succeed simultaneously" ([BEP 55](https://www.bittorrent.org/beps/bep_0055.html)); the first SYN usually dies at the remote NAT |
| R4 | integrates with the async runtime of `services/seeders` (tokio 1, `services/seeders/Cargo.toml`; toolchain 1.96, `rust-toolchain.toml`) | one process, one runtime |
| R5 | exposed as a bidirectional byte stream (`AsyncRead` + `AsyncWrite`) | the tunnel relays it like a `TcpStream` (`src/tunnel/session.rs`) |
| R6 | licence compatible with the repository (MIT, `services/seeders/Cargo.toml`) | |

Also assessed, as the task asks: wide use (production users, dependents,
downloads, maintenance, relevant open issues), RustSec advisories, and the
UDP attack surface (reflection/amplification, state exhaustion).

## How the evidence was gathered

On 2026-11-22: crates.io search for `utp`, `libutp`, `utp bittorrent`,
`bep29`, `utp tokio`, `ledbat`, `micro transport`
(`https://crates.io/api/v1/crates?q=…`); for each candidate the crates.io
record (`https://crates.io/api/v1/crates/<name>`) and reverse dependencies
(`…/<name>/reverse_dependencies`); the GitHub API for stars, open issues
and last push (`https://api.github.com/repos/<owner>/<repo>`); the RustSec
advisory database (`https://github.com/rustsec/advisory-db/tree/main/crates/<name>`);
the published source of the version named (readable at
`https://docs.rs/crate/<name>/<version>/source/`; file:line references
below are into that source). Nothing was added to this repository: the
crates were downloaded and read outside it.

Download figures are crates.io totals (and the last 90 days); they count
CI and mirror fetches too, so they are reported, not interpreted.

## Candidates

| | crate (version) | what | repository |
|---|---|---|---|
| A | `librqbit-utp` 0.7.0 | pure Rust, tokio, written for the rqbit client | [ikatson/librqbit-utp](https://github.com/ikatson/librqbit-utp) |
| B | `utp-rs` 0.1.0-alpha.17 | pure Rust, tokio, Ethereum Portal Network | [ethereum/utp](https://github.com/ethereum/utp) |
| C | `libutp-rs2` 0.1.4 + `libutp-rs2-sys` 0.1.3 | tokio binding of the C++ reference libutp | [ikatson/libutp-rs2](https://github.com/ikatson/libutp-rs2), C library [bittorrent/libutp](https://github.com/bittorrent/libutp) |
| D | `libutp-rs` 1.0.0 + `libutp-sys` 0.1.4 | older binding of libutp (mio 0.7, tokio 1) | [johsunds/libutp-rs](https://github.com/johsunds/libutp-rs) |
| E | `utp` 0.7.0 / `temp_utp` 0.8.1 | pure Rust, blocking `std::net` | [meqif/rust-utp](https://github.com/meqif/rust-utp) (archived) |
| F | `tokio-utp` 0.3.0 | pure Rust, tokio-core 0.1 / futures 0.1 | [canndrew/tokio-utp](https://github.com/canndrew/tokio-utp) |
| G | `async-utp` 0.8.0-alpha1 (and `async-std-utp` 0.1.1) | forks of E, async | [SonicFrog/tokio-utp](https://github.com/SonicFrog/tokio-utp), [bltavares/async-std-utp](https://github.com/bltavares/async-std-utp) |
| H | `irontide-utp` 1.7.0 | pure Rust, tokio, for the irontide client | [codeberg.org/alan090/irontide](https://codeberg.org/alan090/irontide) |
| I | `urtorrent-utp` 0.14.3 | sans-IO state machine (BEP 29, LEDBAT) | [giejqf/urtorrent](https://github.com/giejqf/urtorrent) |

`bip_utp` 0.0.0 (an empty placeholder, 2016) was found and not examined
further. Non-Rust implementations (libtorrent-rasterbar's
`src/utp_stream.cpp`, Go's anacrolix/utp) have no maintained Rust binding
and are not candidates.

## Evidence of use and maintenance

| | downloads total (90 d) | crates.io dependents | known production users | last release | repository activity |
|---|---|---|---|---|---|
| A | 147,008 (116,219) | 4: `librqbit` 9.1.0, `libtortillas` 0.0.1-alpha, `swarmforge` 0.1.0, `aktor-remote` 0.0.12 | rqbit, **with uTP off by default**: `ListenerOptions::default()` is `TcpOnly` with the comment "TODO: once uTP is stable upgrade default to both" (librqbit 9.1.0 `src/listen.rs:62-66`); the CLI flag is `--experimental-enable-utp-listen` (rqbit 9.1.0 `src/main.rs:171-176`); rqbit merged it as "uTP support (disabled by default)" ([rqbit#322](https://github.com/ikatson/rqbit/pull/322)) | 2026-02-14 | 8 stars, 1 maintainer; 2 open PRs; fixes merged on 2026-09-28/29 ([#3](https://github.com/ikatson/librqbit-utp/pull/3), [#4](https://github.com/ikatson/librqbit-utp/pull/4)) are unreleased; LEDBAT is an open PR ([#5](https://github.com/ikatson/librqbit-utp/pull/5)) |
| B | 36,086 (1,746) | 0 | trin (Portal Network, not BitTorrent: [ethereum/trin Cargo.toml](https://github.com/ethereum/trin/blob/master/Cargo.toml) `utp-rs = "0.1.0-alpha.17"`) | 2025-03-27 (alpha) | README: "WARNING: UNDER CONSTRUCTION … unstable, with known issues"; 42 open issues+PRs; last push 2025-03-27 |
| C | 2,784 (364) | 1: `librqbit-utp`, as a **dev**-dependency (interop tests) | none found | 2025-08-02 | 1 star; the C library: last commit 2018-05-15, 47 open issues+PRs; Transmission ships its own fork ([transmission/libutp](https://github.com/transmission/libutp), submodule in [transmission/.gitmodules](https://github.com/transmission/transmission/blob/main/.gitmodules)) |
| D | 2,958 (5) | 0 | none found | 2021-03-14 | superseded per C's README ("Working and tested alternative to libutp-rs") |
| E | 52,617 / 72,720 (349 / 899) | `utp`: 1 (`webtorrent-rs` 1.0.2, 72 downloads) | none found | 2017-05-07 / 2016-04-08 | archived, 140 stars |
| F | 19,272 (116) | 0 | none found | 2017-12-21 | |
| G | 2,321 / 3,326 (65 / 11) | 0 | none found | 2021 (one alpha) | |
| H | 2,182 (824) | 5, all irontide's own crates | none found | 2026-07-18 | created 2026-04 |
| I | 262 (262) | 1, urtorrent's own | none found | 2026-10-11 | created 2026-09 |

Sources: crates.io records and reverse dependencies, GitHub API (URLs
above), retrieved 2026-11-22.

## Requirements × candidates

✅ satisfied · ❌ not satisfied · ❓ not verifiable from documentation or code.

| | R1 interop | R2 same socket | R3 simultaneous connect | R4 tokio 1 | R5 byte stream | R6 licence |
|---|---|---|---|---|---|---|
| A `librqbit-utp` | ❓ | ✅ | ❌ | ✅ | ✅ | ✅ Apache-2.0 |
| B `utp-rs` | ❓ | ✅ | ❓ | ✅ | ❌ | ✅ MIT |
| C `libutp-rs2` | ✅ (C library) | ✅ | ❓ | ✅ | ✅ | ✅ MIT |
| D `libutp-rs` | ✅ (C library) | ✅ | ❓ | ✅ | ✅ | ✅ MIT |
| E `utp` / `temp_utp` | ❓ | ❌ | ❌ | ❌ | ✅ (blocking) | ✅ MIT/Apache-2.0 |
| F `tokio-utp` | ❓ | ✅ | ❓ | ❌ | ✅ (futures 0.1) | ✅ MIT/Apache-2.0 |
| G `async-utp` | ❓ | ❌ | ❌ | ✅ | ✅ | ✅ MIT/Apache-2.0 |
| H `irontide-utp` | ❓ | ✅ | ❓ | ✅ | ✅ | ❌ GPL-3.0-or-later |
| I `urtorrent-utp` | ❓ | ❓ | ❓ | ❌ | ❌ | ✅ Apache-2.0 |

### Notes, by candidate

**A — `librqbit-utp` 0.7.0** (the strongest candidate).
- R1 ❓: its interop tests run only against libutp, through `libutp-rs2`
  (`src/e2e_tests/mod.rs:246-266`); nothing tests libtorrent-rasterbar, the
  implementation of qBittorrent and Deluge. Deviations from BEP 29 in the
  code: congestion control is CUBIC, not LEDBAT (`src/lib.rs:9` "TODO:
  LEDBAT congestion control"; `src/socket.rs:49-52`), and the SYN carries
  its timestamp in milliseconds in the microseconds field
  (`src/socket.rs:439`, also reported in PR #5). Its only production user
  keeps it experimental and off (table above).
- R2 ✅: one dispatcher per `UtpSocket` routes `ST_SYN` to `accept()` and
  `ST_STATE` to pending `connect()`s (`src/socket.rs:651-673`, `847`, `869`).
- R3 ❌: `connect()` sends one SYN and never retransmits it, and has no
  timeout of its own (`src/socket.rs:428-475`; `TODO.md:2` "[ ] connect:
  need to retry SYN packets"). A remote SYN goes to the socket-wide accept
  queue, not to the pending connect for that address. Simultaneous open
  would have to be rebuilt above the library (repeated `connect()`s with new
  connection ids, matching `accept()`ed streams by address), untested.
- R4 ✅ tokio 1 (`Cargo.toml`); R5 ✅ `UtpStream: AsyncRead + AsyncWrite`
  (`src/stream.rs:40-50`); R6 ✅ Apache-2.0 (needs its notice kept).
- Security: no RustSec advisory for the crate. Its lockfile and the 0.7.0
  requirement pull `ringbuf` 0.4.x, affected by
  [RUSTSEC-2026-0293](https://rustsec.org/advisories/RUSTSEC-2026-0293.html)
  (double free in `Consumer::skip`/`clear`, patched in ≥ 0.5.2; the crate
  calls `skip` in `src/stream_tx.rs:83`); the fix (#3) is merged but not
  released, so `cargo audit` in our CI would fail on adoption. UDP surface:
  every SYN is answered once an `accept()` is pending, with no hook to
  refuse it before the reply; each accepted SYN gets up to
  `max_retransmissions` (default 5) SYN-ACKs every 200 ms
  (`src/stream_dispatch.rs:1392-1427`, `src/constants.rs`
  `SYNACK_RESEND_INTERNAL`), so one spoofed 20-byte SYN draws up to five
  replies to the spoofed address; past 32 queued SYNs it answers
  `ST_RESET` (`src/socket.rs:295`, `591-630`). Unknown non-SYN packets are
  dropped silently (`src/socket.rs:669-671`), which is right. State is
  bounded (128 streams per socket by default, 4 pending connects per
  address).

**B — `utp-rs`.** R5 ❌: the stream offers only `read_to_eof` and `write`
(`src/stream.rs:70`, `96`), no `AsyncRead`/`AsyncWrite`; it is built for
Portal Network transfers whose connection ids are agreed out of band
(`accept_with_cid`/`connect_with_cid`, `src/socket.rs:294`, `344`). R1 ❓:
no interop tests with BitTorrent clients. Security: answers `RESET` to any
packet for an unknown connection (`src/socket.rs:158-170`), a reflection
vector. Self-declared unstable (README).

**C, D — libutp bindings.** The C++ library is BEP 29's reference
implementation, so R1 holds at the protocol level, but the bindings are
not widely used (table above), the C library has been dormant since 2018,
and it would parse untrusted UDP in C++ behind `unsafe` FFI (C:
25 lines with `unsafe` in `src/lib.rs`; a process-wide lock, `src/lib.rs:44`),
outside RustSec's view, and add a C++ build step (`libutp-rs2-sys`
`build.rs`, `cc`). R3 ❓: neither binding exposes or tests simultaneous
open.

**E, G — rust-utp and forks.** R2 ❌: `UtpListener::accept()` moves each
accepted connection to a new UDP socket on another port ("This may confuse
the remote peer!", `utp` 0.7.0 `src/socket.rs:1125-1145`); `async-utp`'s
`accept(self)` consumes the socket, one connection per socket
(`src/socket.rs:1338-1342`). E is blocking `std::net`.

**F — `tokio-utp`.** R4 ❌: tokio-core 0.1, futures 0.1, mio 0.6
(`Cargo.toml`), last release 2017.

**H — `irontide-utp`.** R6 ❌: GPL-3.0-or-later (`Cargo.toml`). Otherwise
shaped right (`bind` → socket + listener, `connect` on the socket,
`src/socket.rs:246`, `375`; `AsyncRead`/`AsyncWrite`, `src/stream.rs:74`),
but created in April 2026 with no users outside its own crates.

**I — `urtorrent-utp`.** A sans-IO state machine: R4/R5 ❌ as shipped (we
would write the socket, the timers and the stream ourselves), and its
`rust-version = "1.98"` is above our pinned 1.96. Created September 2026,
262 downloads.

RustSec: no advisory is filed for any candidate crate
(`advisory-db/crates/<name>` absent for all of them on 2026-11-22); the
only one found is the transitive `ringbuf` advisory under A.

## Outcome

**Own implementation.**

- No candidate satisfies all six requirements. R3, which M4-05 cannot do
  without, is ❌ or ❓ for every one of them.
- Wide use is not documented for any of them as a uTP transport: the
  closest, A, has four dependents and its single production user ships it
  disabled and labelled experimental; the widely used code (C++ libutp)
  reaches Rust only through bindings nobody depends on.
- A also fails our CI's `cargo audit` as released today and replies to
  unsolicited SYNs more than once, against M4-04's security rule ("no
  response to packets not associated with a connection or valid session").

Reading A (8.4 k lines, smoltcp-style) and libutp is still useful as a
reference for M4-04; nothing is copied without keeping its licence notice.

## Split of M4-04 (one session each; approved, tasks M4-04a…M4-04f)

Shared design rules, from M4-04's and M4-05's security sections and the
evidence above:

- **No reflection, no amplification.** Never answer a packet that matches
  no connection; never send `ST_RESET` to a stranger. A SYN is answered
  only while a session announces (an inbound listener is open, M4-02) or
  when it comes from an address with a pending outbound or hole-punch
  connect; each received SYN draws at most one `ST_STATE` of the same size
  (no SYN-ACK retransmission timer: the remote retransmits its SYN).
- **Bounded state.** Half-open connections per remote IP and in total,
  connections per session and in total (the `TUNNEL_*` limits of the TCP
  inbound path), receive and send buffers per connection, handshake timeout
  (10 s, as TCP inbound), idle timeout (90 s, as tunnels).
- **Strict parsing.** version 1, type 0–4, bounded extension chain, SACK
  bitmask length a multiple of 4, payload only on `ST_DATA`; anything else
  is dropped silently.
- **Logs** carry counters and reason codes only (AGENTS.md).

| sub-task | scope | tests | done when |
|---|---|---|---|
| **M4-04a** packet codec | `services/seeders/src/utp/packet.rs`: header, extensions (selective ACK; unknown ones skipped), wrapping 16-bit sequence arithmetic, timestamps in µs; no I/O | unit: encode/decode round trip, BEP 29 header vectors, every malformed form rejected, a fuzz-style property test | codec accepts exactly BEP 29 packets |
| **M4-04b** connection state machine (sans-IO) | `utp/conn.rs`: SYN (retransmitted with backoff) / SYN-RECV / CONNECTED / FIN / RESET, reorder buffer, retransmission timer (RTT/RTO per BEP 29), selective-ACK and duplicate-ACK fast retransmit, advertised window; driven by `(now, packet in) → packets out, bytes delivered`; simultaneous open: an outbound connect and an inbound SYN from the same address can both complete | unit with a simulated clock and an in-memory link: loss, reordering, duplication, both directions; simultaneous-open scenarios | a 16 MiB transfer over a 10 % loss / reordering simulated link is byte-identical |
| **M4-04c** congestion control | `utp/ledbat.rs`: LEDBAT as BEP 29 specifies (100 ms target, base-delay history, window ≥ 1 packet), packet size from a conservative fixed MTU (1 400 B payload or less, no PMTU probing yet) | unit with simulated delay/bandwidth: converges, backs off on rising delay and on loss, never stalls | throughput on the simulated link within a stated factor of its capacity |
| **M4-04d** UDP socket multiplexer | `utp/socket.rs`: one tokio `UdpSocket`, demux by `(addr, conn_id)`, `connect(addr)`, an accept gate decided by the caller (listening, or expected address), the bounds and no-reply rules above, a `UtpStream: AsyncRead + AsyncWrite` | integration on loopback with a lossy UDP relay: echo and bulk transfer both ways, unsolicited packets get no reply, limits enforced | a uTP stream behaves as a `TcpStream` for the tunnel |
| **M4-04e** gateway integration | `docs/tunnel-protocol.md`: transport field in `open` (TCP / uTP) and the transport of an `accept`; Rust and TS codecs plus shared fixtures; outbound uTP dial in `tunnel/session.rs`, inbound uTP through the same handshake gate and session check as TCP in `tunnel/inbound.rs`; UDP socket on the announced port, opened and closed with the TCP listener, on a port distinct from `DHT_PORT` (both default to 6881 today: `services/seeders/README.md`, `deploy/*.env.example`); config and README | integration: full verified download from a Rust test peer over uTP with simulated loss; an inbound uTP peer for an info-hash nobody announces gets no reply | M4-04's acceptance criteria hold |
| **M4-04f** interoperability | a test (CI or a documented manual step) against libtorrent-rasterbar (for example its Python bindings) and libutp/Transmission: inbound and outbound connections, a full download | interop e2e | R1 shown, not assumed |

a → b → c is linear; d needs b (c can land after d with a fixed window);
e needs d; f needs e. M4-05 (hole punching) then needs only the
simultaneous open of b and the accept gate of d. Whether the engine dials
uTP itself (for example after a TCP connect fails) is left to M4-05/M4-06
and is not part of this split.

## Notes from the sub-tasks

**M4-04e (gateway integration).** Choices made within the approved scope:

- `open` and `accept` carry the transport as one optional trailing byte
  (absent = TCP, 1 = uTP; an explicit 0 is `bad_payload`), so every TCP
  frame is unchanged and the subprotocol stays `magnet-tunnel.v1`; the new
  `failed` reason 10 UNSUPPORTED_TRANSPORT is what the Cloudflare gateway
  answers to a uTP `open` (docs/tunnel-protocol.md › Peer fields).
- Outbound uTP leaves from the announced port's UDP socket, which exists
  only while some session announces; a uTP `open` is therefore accepted
  only while the `open`'s own session announces its info-hash ("only for
  active sessions"), after the same `netguard` check as TCP.
- An inbound uTP peer refused by the handshake gate or the session check
  is forgotten without a packet (no `ST_FIN`, no `ST_RESET`), the uTP
  counterpart of "closed without a byte in reply"; the uTP handshake
  itself (one `ST_STATE` per SYN) is answered only while the gate is open.
- The UDP socket's gate closes with the TCP listener; the socket stays
  bound only while uTP connections made through it last, and the next
  listener reuses it rather than binding the port twice.

**M4-05 (hole punching).** The duplicate case of a simultaneous open
(each side completing a different one of the two connections, then
dropping the other) is solved above the multiplexer, in the gateway's uTP
dial (`services/seeders/src/tunnel/punch.rs`): while the connect is
pending it holds the `expect` acceptor for the remote's address, gives
every connection that comes up the BitTorrent handshake and keeps the one
on which the remote answers, closing the others. A test with the two SYNs
and the two `ST_STATE`s crossing on a 50 ms link fails without the
acceptor and passes with it (`punch_tests.rs`). The BEP 55 messages
themselves are the engine's (`packages/engine/src/holepunch.ts`); see
docs/tunnel-protocol.md › Hole punching.

## Interoperability (M4-04f)

R1 is now shown, not assumed: the VPS gateway's uTP carries a full,
verified download from libtorrent-rasterbar and from Transmission's libutp,
with the connection opened from either side.

**Test.** `npm run e2e:interop -w tools/e2e`
(`tools/e2e/src/utp-interop-session.ts`; part of `npm run e2e`, and of CI's
e2e job, where `REQUIRE_INTEROP_E2E=1` turns a missing Docker into a
failure instead of a skip). For each client, a 16 MiB torrent (64 pieces
of 256 KiB, `tools/fake-peer` noise) is seeded by the real client with
**TCP off**, so every byte rides uTP, and the engine downloads it through
the gateway (`services/seeders`), every piece SHA-1 checked and compared
with the source:

- **outbound**: the session announces the info-hash (no tracker) and
  sends `open` with transport uTP; the gateway's SYN leaves the announced
  port and the client accepts it;
- **inbound**: the session announces to a fake HTTP tracker; the client,
  seeding, gets the gateway's address from it and connects over uTP; the
  engine gets `accept` with transport uTP.

Each side also checks the other's view: the client reports exactly one
peer, uTP and in the expected direction (libtorrent `peer_info` flags,
Transmission's RPC `peers[].isUTP/isIncoming`), and counts the whole
torrent as uploaded; the gateway's tunnel logs carry no peer address.

**Clients.** They run in a Docker image built by the test from
`tools/e2e/interop/Dockerfile` (Ubuntu 24.04 pinned by digest, its own
packages; test-only, no npm or Cargo dependency is added, so the
dependency audit is unchanged): libtorrent through its Python bindings
(`tools/e2e/interop/libtorrent-seed.py`), Transmission as
`transmission-daemon` driven over RPC.

**Confinement.** Nothing leaves the machine: no DHT, no public tracker, no
LSD, UPnP or NAT-PMP on either client, the gateway with
`TUNNEL_ANNOUNCE_DHT=0` and no default trackers. The clients share the
host's network namespace; every peer-facing socket (the gateway's
announced port, the fake tracker, the clients' ports) binds the host's
address on an `--internal` Docker bridge created for the run, which no
container joins and nothing routes to. Plain loopback is not enough:
Transmission drops 127.0.0.0/8 addresses that a tracker hands it as
"martian" (`is_martian_addr`, `libtransmission/net.cc` at 4.0.5), so it
would never dial the gateway in the inbound case.

**Outcome (2026-11-23, loopback path, one host).**

| client (version) | direction | pieces verified | download time |
|---|---|---|---|
| libtorrent-rasterbar 2.0.10.0, Python bindings (Ubuntu `python3-libtorrent` 2.0.10-1.1build2) | outbound (gateway dials) | 64/64 | 1.9–2.1 s (7.7–8.3 MiB/s) |
| ″ | inbound (client dials) | 64/64 | 2.0–3.0 s (5.3–8.1 MiB/s) |
| Transmission 4.0.5 (a6fe2a64aa), `transmission-daemon` with its bundled libutp fork (Ubuntu 4.0.5-1ubuntu0.1) | outbound | 64/64 | 8.3 s (1.9 MiB/s) |
| ″ | inbound | 64/64 | 17.5 s (0.9 MiB/s) |

Seven runs, all green; the figures are those of the last three. Times
run from the `open` (outbound) or the `accept` (inbound) to the last
verified piece, through the WebSocket and the engine; in the inbound case
the client connected 0.6–0.7 s (libtorrent) and 1.6 s (Transmission)
after it started seeding.

**Fixes needed: none.** Both clients completed the handshake, the
transfer and the close against the implementation of M4-04a…e as it was.

**Transmission's rate is its own.** For reference, on the same host and
setup, libtorrent 2.0.10 leeching the same torrent over uTP took 1.1 s
from a libtorrent seed and 53–56 s from the Transmission 4.0.5 seed: the
sender, not our receiver, bounds the Transmission figures (Transmission's
4.1.0 release notes list "Improved µTP download performance", #6508,
#6586).

**Not covered.** µTorrent's original libutp (no Linux build) is
represented by Transmission's fork of it; real swarms on the Internet are
out of scope. Hole punching against real clients: see below (M4 review).

## Hole punching with real clients (M4 review)

The M4 review found M4-05 tested only against a scripted relay. The same
e2e now runs BEP 55 with libtorrent on both other ends
(`holepunch()` in `tools/e2e/src/utp-interop-session.ts`; Transmission
implements no `ut_holepunch`): a libtorrent leecher, the **relay**, kept
connected to a libtorrent seed, the **target**, by a torrent download
limit; both uTP only, finding each other at the fake tracker, where the
gateway is not (the session announces to no tracker). The session reaches
the relay over uTP from the announced port, waits for the relay's
`ut_pex` to name the target with the holepunch flag, sends `rendezvous`,
and on the relay's `connect` takes the target's connection: the target,
told the gateway's address by the relay only, connects to the announced
port over uTP, and the whole torrent is downloaded from it and verified.

**Outcome (libtorrent-rasterbar 2.0.10.0).** Green on every run (three
in a row, about 17 s for the libtorrent part of the e2e): the relay
lists the target with flags 0x0e (uTP, holepunch, reachable), answers
`connect`, tells the target, and the target's SYN reaches the announced
port before the relay's `connect` reaches the engine, so the connection
arrives as an `accept` (64/64 pieces, 16 MiB in about 2 s).

**Fix needed: one, in the engine.** libtorrent writes `err_code` only in
`error` messages (`bt_peer_connection::write_holepunch_msg`): its
`connect` is 8 bytes for IPv4, not 12. The engine's decoder required the
12, dropped every libtorrent `connect`, and no punch through a libtorrent
relay could ever have succeeded. It now takes `rendezvous` and `connect`
with or without the code (an `error` must carry it); what it sends keeps
the code (libtorrent relays it either way, as the test shows).

**Still not covered.** Both containers share the host's network: no NAT
filters the target, so the test shows that real clients relay, answer and
connect as BEP 55 says, not the traversal of a real NAT, which stays
simulated (`services/seeders/src/tunnel/punch_tests.rs`, two SYNs and
two `ST_STATE`s crossing). On loopback the target's SYN always wins the
race, so the other order (the gateway's `open` holding the target's SYN)
is shown by `punch_tests.rs` only. In that race an `accept` that comes
while the engine's `open` is in flight makes two connections to the
target; the engine keeps the `accept` and closes the `open`'s, while
libtorrent keeps one of the two by peer id and may keep the other: the
punch then fails and may be tried again. The M4-09 staging test with a
seeder behind a real NAT is the first evidence of the traversal itself.
