//! Implements the [muxrpc protocol](https://github.com/ssbc/muxrpc) in rust.
#![deny(missing_docs)]

#[macro_use]
extern crate futures;
extern crate packet_stream;
extern crate tokio_io;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

use std::str::FromStr;
use std::io;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::convert::From;

use futures::prelude::*;
use tokio_io::{AsyncRead, AsyncWrite};
use packet_stream::{packet_stream, PsIn, PsOut, PsSink, PsStream, InRequest, InResponse,
                    IncomingPacket};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
struct Rpc {
    name: Vec<String>,
    #[serde(rename = "type")]
    type_: RpcType,
    args: Vec<Value>,
}

const SOURCE: &'static str = "source";
const SINK: &'static str = "sink";
const DUPLEX: &'static str = "duplex";
const ASYNC: &'static str = "async";
const SYNC: &'static str = "sync";

// The different rpc types.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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
    /// Received a packet containing invalid data.
    ///
    /// For example, the packet could have the wrong type, it could contain
    /// malformed json, it could set inappropriate flags, it could lack required
    /// json fields, etc.
    InvalidData,
}

impl Display for RpcError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match *self {
            RpcError::IoError(ref err) => write!(f, "Rpc error: {}", err),
            RpcError::InvalidData => write!(f, "Rpc error: Invalid data"),
        }
    }
}

impl Error for RpcError {
    fn description(&self) -> &str {
        match *self {
            RpcError::IoError(ref err) => err.description(),
            RpcError::InvalidData => "Received a packet that contained invalid data",
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            RpcError::IoError(ref err) => Some(err),
            RpcError::InvalidData => None,
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
pub struct RpcIn<R: AsyncRead, W, B> {
    ps_in: PsIn<R, W, B>,
}

impl<R: AsyncRead, W, B> RpcIn<R, W, B> {
    fn new(ps_in: PsIn<R, W, B>) -> RpcIn<R, W, B> {
        RpcIn { ps_in }
    }
}

impl<R: AsyncRead, W: AsyncWrite, B: AsRef<[u8]>> Stream for RpcIn<R, W, B> {
    /// Name(s), args, communication-handle
    type Item = (Vec<String>, Vec<Value>, IncomingRpc<R, W, B>);
    type Error = RpcError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let packet = try_ready!(self.ps_in.poll());

        match packet {
            None => Ok(Async::Ready(None)),

            Some(IncomingPacket::Request(req)) => {
                if !req.packet().is_json_packet() {
                    return Err(RpcError::InvalidData);
                }

                match serde_json::from_slice::<Rpc>(req.packet().data()) {
                    Ok(rpc) => {
                        match rpc.type_ {
                            RpcType::Sync => {
                                Ok(Async::Ready(Some((rpc.name,
                                                      rpc.args,
                                                      IncomingRpc::Sync(InSync::new(req))))))
                            }
                            RpcType::Async => {
                                Ok(Async::Ready(Some((rpc.name,
                                                      rpc.args,
                                                      IncomingRpc::Async(InAsync::new(req))))))
                            }
                            _ => Err(RpcError::InvalidData),
                        }
                    }
                    Err(err) => Err(RpcError::InvalidData),
                }
            }

            Some(IncomingPacket::Duplex(p, ps_sink, ps_stream)) => {
                if !p.is_json_packet() {
                    return Err(RpcError::InvalidData);
                }

                match serde_json::from_slice::<Rpc>(p.data()) {
                    Ok(rpc) => {
                        match rpc.type_ {
                            RpcType::Source => {
                                Ok(Async::Ready(Some((rpc.name,
                                                      rpc.args,
                                                      IncomingRpc::Source(RpcSink::new(ps_sink))))))
                            }
                            RpcType::Sink => {
                                Ok(Async::Ready(Some((rpc.name,
                                                      rpc.args,
                                                      IncomingRpc::Sink(RpcStream::new(ps_stream))))))
                            }
                            RpcType::Duplex => Ok(Async::Ready(Some((rpc.name, rpc.args,
                                IncomingRpc::Duplex(RpcSink::new(ps_sink),
                                RpcStream::new(ps_stream)))))),
                            _ => Err(RpcError::InvalidData),
                        }
                    }
                    Err(err) => Err(RpcError::InvalidData),
                }
            }
        }
    }
}

/// An incoming packet, initiated by the peer.
pub enum IncomingRpc<R: AsyncRead, W: AsyncWrite, B: AsRef<[u8]>> {
    /// A source request. You get a sink, the peer gets a stream.
    Source(RpcSink<W, B>),
    /// A sink request. You get a stream, the peer gets a sink.
    Sink(RpcStream<R>),
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
pub struct RpcSink<W: AsyncWrite, B: AsRef<[u8]>> {
    sink: PsSink<W, B>,
}

impl<W: AsyncWrite, B: AsRef<[u8]>> RpcSink<W, B> {
    fn new(ps_sink: PsSink<W, B>) -> RpcSink<W, B> {
        unimplemented!()
    }
}

/// A stream for receiving data from the peer.
pub struct RpcStream<R: AsyncRead> {
    stream: PsStream<R>,
}

impl<R: AsyncRead> RpcStream<R> {
    fn new(ps_stream: PsStream<R>) -> RpcStream<R> {
        unimplemented!()
    }
}

/// Allows sending a single value to the peer.
pub struct InAsync<W: AsyncWrite, B: AsRef<[u8]>> {
    in_request: InRequest<W, B>,
}

impl<W: AsyncWrite, B: AsRef<[u8]>> InAsync<W, B> {
    fn new(in_req: InRequest<W, B>) -> InAsync<W, B> {
        unimplemented!()
    }
}

/// Allows sending a single value to the peer.
pub struct InSync<W: AsyncWrite, B: AsRef<[u8]>> {
    in_request: InRequest<W, B>,
}

impl<W: AsyncWrite, B: AsRef<[u8]>> InSync<W, B> {
    fn new(in_req: InRequest<W, B>) -> InSync<W, B> {
        unimplemented!()
    }
}

/// Allows receiving a single value from the peer.
pub struct OutAsync<R: AsyncRead> {
    in_response: InResponse<R>,
}

/// Allows receiving a single value from the peer.
pub struct OutSync<R: AsyncRead> {
    in_response: InResponse<R>,
}

#[cfg(test)]
mod tests {
    #[test] // TODO
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
