use std::convert::From;
use std::io;

use futures::prelude::*;
use futures::sink::Send;
use tokio_io::AsyncWrite;
use packet_stream::{PsSink, Metadata};

use super::*;

type MuxrpcSink<W> = PsSink<W, Box<[u8]>>;

/// An outgoing sink request, created by this muxrpc.
///
/// Poll it to actually start sending the sink request. It yields the `RpcSink`.
pub struct OutSink<W: AsyncWrite>(Send<MuxrpcSink<W>>);

pub fn new_out_sink<W: AsyncWrite>(ps_sink: MuxrpcSink<W>, initial_data: Box<[u8]>) -> OutSink<W> {
    // OutSink(SendClose::new(ps_sink, (initial_data, META_NON_END)))
    OutSink(ps_sink.send((initial_data, META_NON_END)))
}

impl<W: AsyncWrite> Future for OutSink<W> {
    type Item = RpcSink<W>;
    type Error = Option<io::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let sink = try_ready!(self.0.poll());
        Ok(Async::Ready(new_rpc_sink(sink)))
    }
}

/// A sink to the peer.
pub struct RpcSink<W: AsyncWrite> {
    ps_sink: MuxrpcSink<W>,
    buffer: Option<(Box<[u8]>, Metadata)>,
    buffered_final: bool,
}

pub fn new_rpc_sink<W: AsyncWrite>(ps_sink: MuxrpcSink<W>) -> RpcSink<W> {
    RpcSink {
        ps_sink,
        buffer: None,
        buffered_final: false,
    }
}

impl<W: AsyncWrite> Sink for RpcSink<W> {
    /// An `Err` is emitted as an error for the peer.
    type SinkItem = Result<Value, Value>;
    type SinkError = Option<io::Error>;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match self.buffer {
            Some(_) => {
                match self.poll_complete() {
                    Ok(Async::Ready(())) => self.start_send(item),
                    Ok(Async::NotReady) => Ok(AsyncSink::NotReady(item)),
                    Err(err) => Err(err),
                }
            }

            None => {
                match item {
                    Ok(val) => self.buffer = Some((unwrap_serialize(val), META_NON_END)),
                    Err(val) => self.buffer = Some((unwrap_serialize(val), META_END)),
                }
                Ok(AsyncSink::Ready)
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        match self.buffer.take() {
            Some(buffered) => {
                match self.ps_sink.start_send(buffered) {
                    Ok(AsyncSink::Ready) => self.poll_complete(),
                    Ok(AsyncSink::NotReady(item)) => {
                        self.buffer = Some(item);
                        Ok(Async::NotReady)
                    }
                    Err(err) => Err(err),
                }
            }

            None => self.ps_sink.poll_complete(),
        }
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        match self.buffer.take() {
            Some(buffered) => {
                match self.ps_sink.start_send(buffered) {
                    Ok(AsyncSink::Ready) => {
                        if !self.buffered_final {
                            self.buffer = Some((Box::new(JSON_TRUE), META_END));
                            self.buffered_final = true;
                        }
                        self.close()
                    }
                    Ok(AsyncSink::NotReady(item)) => {
                        self.buffer = Some(item);
                        Ok(Async::NotReady)
                    }
                    Err(err) => Err(err),
                }
            }

            None => self.ps_sink.close(),
        }
    }
}
