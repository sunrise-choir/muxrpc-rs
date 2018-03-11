use std::convert::From;
use std::io;

use futures_core::{Future, Poll};
use futures_core::Async::Ready;
use futures_core::task::Context;
use futures_io::AsyncWrite;
use futures_sink::Sink;
use futures_util::SinkExt;
use futures_util::sink::Send;
use packet_stream::{PsSink, Metadata};

use super::*;

type MuxrpcSink<W> = PsSink<W, Box<[u8]>>;

/// An outgoing sink request, created by this muxrpc.
///
/// Poll it to actually start sending the sink request. It yields the `RpcSink`.
pub struct MuxSink<W: AsyncWrite>(Send<MuxrpcSink<W>>);

pub fn new_sink<W: AsyncWrite>(ps_sink: MuxrpcSink<W>, initial_data: Box<[u8]>) -> MuxSink<W> {
    // MuxSink(SendClose::new(ps_sink, (initial_data, META_NON_END)))
    MuxSink(ps_sink.send((initial_data, META_NON_END)))
}

impl<W: AsyncWrite> Future for MuxSink<W> {
    type Item = RpcSink<W>;
    type Error = Option<io::Error>;

    fn poll(&mut self, cx: &mut Context) -> Poll<Self::Item, Self::Error> {
        let sink = try_ready!(self.0.poll(cx));
        Ok(Ready(new_rpc_sink(sink)))
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

    fn poll_ready(&mut self, cx: &mut Context) -> Poll<(), Self::SinkError> {
        if self.buffer.is_none() {
            Ok(Ready(()))
        } else {
            let _ = try_ready!(self.ps_sink.poll_ready(cx));
            self.ps_sink.start_send(self.buffer.take().unwrap())?;
            Ok(Ready(()))
        }
    }

    fn start_send(&mut self, item: Self::SinkItem) -> Result<(), Self::SinkError> {
        match item {
            Ok(val) => self.buffer = Some((unwrap_serialize(val), META_NON_END)),
            Err(val) => self.buffer = Some((unwrap_serialize(val), META_END)),
        }
        Ok(())
    }

    fn poll_flush(&mut self, cx: &mut Context) -> Poll<(), Self::SinkError> {
        if self.buffer.is_none() {
            self.ps_sink.poll_flush(cx)
        } else {
            let _ = try_ready!(self.ps_sink.poll_ready(cx));
            self.ps_sink.start_send(self.buffer.take().unwrap())?;
            self.ps_sink.poll_flush(cx)
        }
    }

    fn poll_close(&mut self, cx: &mut Context) -> Poll<(), Self::SinkError> {
        if self.buffer.is_some() {
            let _ = try_ready!(self.poll_ready(cx));
            if !self.buffered_final {
                self.buffer = Some((Box::new(JSON_TRUE), META_END));
                self.buffered_final = true;
            }
            self.poll_close(cx)
        } else {
            if !self.buffered_final {
                self.buffer = Some((Box::new(JSON_TRUE), META_END));
                self.buffered_final = true;
                self.poll_close(cx)
            } else {
                self.ps_sink.poll_close(cx)
            }
        }
    }
}
