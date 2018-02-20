//! Implements the [muxrpc protocol](https://github.com/ssbc/muxrpc) in rust.
#![deny(missing_docs)]

extern crate futures;
extern crate packet_stream;
extern crate tokio_io;

use std::str::FromStr;
use std::io;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::convert::From;

use futures::prelude::*;
use tokio_io::{AsyncRead, AsyncWrite};
use packet_stream::{PsIn, PsOut, packet_stream};

const SOURCE: &'static str = "source";
const SINK: &'static str = "sink";
const DUPLEX: &'static str = "duplex";
const ASYNC: &'static str = "async";
const SYNC: &'static str = "sync";

// The different rpc types.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum RpcType {
    Source,
    Sink,
    Duplex,
    Async,
    Sync,
}

impl RpcType {
    fn needs_stream_flag(&self) -> bool {
        match *self {
            RpcType::Source | RpcType::Sink | RpcType::Duplex => true,
            RpcType::Async | RpcType::Sync => false,
        }
    }

    fn as_str(&self) -> &'static str {
        match *self {
            RpcType::Source => SOURCE,
            RpcType::Sink => SINK,
            RpcType::Duplex => DUPLEX,
            RpcType::Async => ASYNC,
            RpcType::Sync => SYNC,
        }
    }
}

impl FromStr for RpcType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == SOURCE {
            Ok(RpcType::Source)
        } else if s == SINK {
            Ok(RpcType::Sink)
        } else if s == DUPLEX {
            Ok(RpcType::Duplex)
        } else if s == ASYNC {
            Ok(RpcType::Async)
        } else if s == SYNC {
            Ok(RpcType::Sync)
        } else {
            Err(())
        }
    }
}

/// An error that can be emitted during the rpc process.
#[derive(Debug)]
pub enum RpcError {
    /// An io-error that was emitted by the underlying transports or the
    /// underlying packet-stream.
    IoError(io::Error),
}

impl Display for RpcError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match *self {
            RpcError::IoError(ref err) => write!(f, "Rpc error: {}", err),
        }
    }
}

impl Error for RpcError {
    fn description(&self) -> &str {
        match *self {
            RpcError::IoError(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            RpcError::IoError(ref err) => Some(err),
        }
    }
}

impl From<io::Error> for RpcError {
    fn from(err: io::Error) -> RpcError {
        RpcError::IoError(err)
    }
}

/// Take ownership of an AsyncRead and an AsyncWrite to create the two halves of
/// a muxrpc connection.
///
/// `R` is the `AsyncRead` for reading bytes from the peer, `W` is the
/// `AsyncWrite` for writing bytes to the peer, and `B` is the type that is used
/// as input for sending data.
///
/// Note that the AsyncRead may be polled for data even after it has signalled
/// end of data. Only use AsyncReads that can correctly handle this.
pub fn muxrpc<R: AsyncRead, W: AsyncWrite, B: AsRef<[u8]>>(r: R,
                                                           w: W)
                                                           -> (RpcIn<R, W, B>, RpcOut<R, W, B>) {
    let (ps_in, ps_out) = packet_stream(r, w);
    (RpcIn::new(ps_in), RpcOut::new(ps_out))
}

/// A stream of incoming rpcs from the peer.
pub struct RpcIn<R: AsyncRead, W, B>(PsIn<R, W, B>);

impl<R: AsyncRead, W, B> RpcIn<R, W, B> {
    fn new(ps_in: PsIn<R, W, B>) -> RpcIn<R, W, B> {
        RpcIn(ps_in)
    }
}

impl<R: AsyncRead, W: AsyncWrite, B: AsRef<[u8]>> Stream for RpcIn<R, W, B> {
    type Item = IncomingRpc<R, W, B>;
    type Error = RpcError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}

/// An incoming packet, initiated by the peer.
pub enum IncomingRpc<R: AsyncRead, W: AsyncWrite, B: AsRef<[u8]>> {
    /// A source request. You get a sink, the peer gets a stream.
    Source(RpcSink<W, B>),
    /// A sink request. You get a stream, the peer gets a sink.
    Stream(RpcStream<R>),
    /// A duplex request. Both peers get a stream and a sink.
    Duplex(RpcSink<W, B>, RpcStream<R>),
    /// An async request. You get an InAsync, the peer gets an OutAsync.
    Async(InAsync<W, B>),
    /// A sync request. You get an InSync, the peer gets an OutSync.
    Sync(InSync<W, B>),
}

/// Allows sending rpcs to the peer.
pub struct RpcOut<R: AsyncRead, W, B>(PsOut<R, W, B>);

impl<R: AsyncRead, W, B> RpcOut<R, W, B> {
    fn new(ps_out: PsOut<R, W, B>) -> RpcOut<R, W, B> {
        RpcOut(ps_out)
    }
}

/// A sink for writing data to the peer.
pub struct RpcSink<W: AsyncWrite, B: AsRef<[u8]>> {}

/// A stream for receiving data from the peer.
pub struct RpcStream<R: AsyncRead> {}

/// Allows sending a single value to the peer.
pub struct InAsync<W: AsyncWrite, B: AsRef<[u8]>> {}

/// Allows sending a single value to the peer.
pub struct InSync<W: AsyncWrite, B: AsRef<[u8]>> {}

/// Allows receiving a single value from the peer.
pub struct OutAsync<R: AsyncRead> {}

/// Allows receiving a single value from the peer.
pub struct OutSync<R: AsyncRead> {}

#[cfg(test)]
mod tests {
    #[test] // TODO
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
