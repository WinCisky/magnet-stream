//! A peer connection of the tunnel, over TCP or uTP (M4-04e): one byte
//! stream type for the handshake gate and the relay, whichever transport
//! carries it.

use super::codec::transport;
use crate::utp::socket::UtpStream;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

pub enum PeerStream {
    Tcp(TcpStream),
    Utp(UtpStream),
}

impl PeerStream {
    /// The `transport` code of the frames (`open`, `accept`).
    pub fn transport(&self) -> u8 {
        match self {
            PeerStream::Tcp(_) => transport::TCP,
            PeerStream::Utp(_) => transport::UTP,
        }
    }

    /// Drop a remote that is refused before a byte of ours: TCP closes as
    /// usual (the kernel's FIN); uTP forgets the connection without sending
    /// anything (no `ST_FIN`, no `ST_RESET` to a stranger).
    pub fn discard(self) {
        match self {
            PeerStream::Tcp(s) => drop(s),
            PeerStream::Utp(s) => s.abandon(),
        }
    }
}

impl AsyncRead for PeerStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            PeerStream::Utp(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PeerStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            PeerStream::Tcp(s) => Pin::new(s).poll_write(cx, data),
            PeerStream::Utp(s) => Pin::new(s).poll_write(cx, data),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(s) => Pin::new(s).poll_flush(cx),
            PeerStream::Utp(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            PeerStream::Utp(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}
