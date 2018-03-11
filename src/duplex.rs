use std::convert::From;
use std::io;

use futures_core::{Future, Poll};
use futures_core::Async::Ready;
use futures_core::task::Context;
use futures_io::AsyncWrite;
use futures_util::SinkExt;
use futures_util::sink::Send;
use packet_stream::PsSink;

use super::*;

type MuxrpcSink<W> = PsSink<W, Box<[u8]>>;

/// An outgoing duplex request, created by this muxrpc.
///
/// Poll it to actually start sending the duplex request. It yields the `RpcSink`.
pub struct Duplex<W: AsyncWrite>(Send<MuxrpcSink<W>>);

pub fn new_duplex<W: AsyncWrite>(ps_sink: MuxrpcSink<W>, initial_data: Box<[u8]>) -> Duplex<W> {
    Duplex(ps_sink.send((initial_data, META_NON_END)))
}

impl<W: AsyncWrite> Future for Duplex<W> {
    type Item = RpcSink<W>;
    type Error = Option<io::Error>;

    fn poll(&mut self, cx: &mut Context) -> Poll<Self::Item, Self::Error> {
        let sink = try_ready!(self.0.poll(cx));
        Ok(Ready(new_rpc_sink(sink)))
    }
}
