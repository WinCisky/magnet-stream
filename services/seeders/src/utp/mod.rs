//! uTP (BEP 29) for the VPS gateway, our own implementation
//! (docs/decisions/utp.md, M4-04a…M4-04f).
//!
//! - `packet`: the packet codec (header, extension chain, selective ACK,
//!   16-bit sequence arithmetic, µs timestamps); no I/O (M4-04a).
//! - `conn`: the connection state machine, sans-IO (SYN / SYN-RECV /
//!   CONNECTED / FIN / RESET, reorder buffer, RTT/RTO, selective-ACK and
//!   duplicate-ACK fast retransmit, bounded buffers) (M4-04b).
//! - `ledbat`: LEDBAT congestion control as BEP 29 (100 ms target delay,
//!   base-delay history, window of at least one packet, halving on loss),
//!   driven by `conn`; fixed packet size, no PMTU probing (M4-04c).
//!
//! - `socket`: the multiplexer on one tokio `UdpSocket` (M4-04d): demux
//!   by `(address, connection id)`, outbound connects and inbound
//!   connections on the same port, an accept gate decided by the caller
//!   (listener or expected address), half-open and connection limits, no
//!   reply to unsolicited packets, `UtpStream: AsyncRead + AsyncWrite`.
//!
//! The tunnel gateway uses the socket on its announced port (M4-04e,
//! `crate::tunnel::inbound`, `crate::tunnel::session`). Part of the API is
//! not used by it yet: the expected-address gate (`UtpSocket::expect`) is
//! for hole punching (M4-05), and the accessors and constructors for
//! diagnostics and the tests; hence the module-wide `dead_code` allowance.
#![allow(dead_code)]

pub mod conn;
pub mod ledbat;
pub mod packet;
pub mod socket;
#[cfg(test)]
pub mod testutil;
