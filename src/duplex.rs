use std::convert::From;
use std::io;

use futures::prelude::*;
use futures::sink::Send;
use tokio_io::AsyncWrite;
use packet_stream::PsSink;

use super::*;

type MuxrpcSink<W> = PsSink<W, Box<[u8]>>;

/// An outgoing duplex request, created by this muxrpc.
///
/// Poll it to actually start sending the duplex request. It yields the `RpcSink`.
pub struct OutDuplex<W: AsyncWrite>(Send<MuxrpcSink<W>>);

pub fn new_out_duplex<W: AsyncWrite>(ps_sink: MuxrpcSink<W>,
                                     initial_data: Box<[u8]>)
                                     -> OutDuplex<W> {
    OutDuplex(ps_sink.send((initial_data, META_NON_END)))
}

impl<W: AsyncWrite> Future for OutDuplex<W> {
    type Item = RpcSink<W>;
    type Error = Option<io::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let sink = try_ready!(self.0.poll());
        Ok(Async::Ready(new_rpc_sink(sink)))
    }
}
