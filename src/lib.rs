//! Implements the [muxrpc protocol](https://github.com/ssbc/muxrpc) in rust.
//!
//! All futures, sinks and streams with an error type of `Option<io::Error>` use `None` to signal
//! that an error happend on the underlying transport, but on another handle to the transport.
#![deny(missing_docs)]

extern crate atm_async_utils;
#[macro_use]
extern crate futures;
extern crate packet_stream;
extern crate tokio_io;
extern crate serde;
extern crate serde_json;
#[macro_use(Serialize, Deserialize)]
extern crate serde_derive;

#[cfg(test)]
extern crate partial_io;
#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
extern crate async_ringbuffer;
#[cfg(test)]
extern crate rand;

mod errors;
mod common;
mod async;
mod sync;
mod source;
mod sink;
mod duplex;

use std::convert::From;
use std::io;

use futures::prelude::*;
use futures::unsync::oneshot::Canceled;
use tokio_io::{AsyncRead, AsyncWrite};
use packet_stream::{packet_stream, PsIn, PsOut, IncomingPacket, PacketType, ClosePs};
use serde_json::Value;
use serde_json::{to_vec, from_slice};
use serde::Serialize;
use serde::de::DeserializeOwned;

pub use errors::*;
use common::*;
use async::{new_in_async, new_out_async, new_in_async_response};
pub use async::{OutAsync, OutAsyncResponse, InAsync, InAsyncResponse};
use sync::{new_in_sync, new_out_sync, new_in_sync_response};
pub use sync::{OutSync, OutSyncResponse, InSync, InSyncResponse};
use source::{new_rpc_stream, new_out_source, new_out_source_cancelable};
pub use source::{OutSource, OutSourceCancelable, RpcStream, CancelSource};
use sink::{new_out_sink, new_rpc_sink};
pub use sink::{OutSink, RpcSink};
use duplex::new_out_duplex;
pub use duplex::OutDuplex;

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
struct OutRpc<'a, 'b, A: 'b> {
    name: Box<[&'a str]>,
    #[serde(rename = "type")]
    type_: RpcType,
    args: &'b A,
}

impl<'a, 'b, A: Serialize + 'b> OutRpc<'a, 'b, A> {
    fn new(name: Box<[&'a str]>, type_: RpcType, args: &'b A) -> OutRpc<'a, 'b, A> {
        OutRpc { name, type_, args }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
struct InRpc {
    name: Box<[String]>,
    #[serde(rename = "type")]
    type_: RpcType,
    args: Box<[Value]>,
}

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

/// Implementors of this trait are rpcs that can be sent to the peer. The serilize implementation
/// should provide the argument value(s).
pub trait Rpc: Serialize {
    /// The names for this rpc.
    fn names() -> Box<[&'static str]>;
}

/// A future that emits the wrapped writer of a muxrpc connection once the outgoing half of the
/// has been fully closed.
pub struct Closed<W>(packet_stream::Closed<W, Box<[u8]>>);

impl<W> Future for Closed<W> {
    type Item = W;
    /// This can only be emitted if a handle to the underlying transport has been polled but was
    /// dropped before it was done. If all handles are polled/closed properly, this error is never
    /// emitted.
    type Error = Canceled;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

/// Take ownership of an AsyncRead and an AsyncWrite to create the two halves of
/// a muxrpc connection.
///
/// `R` is the `AsyncRead` for reading bytes from the peer, `W` is the
/// `AsyncWrite` for writing bytes to the peer, and `B` is the type that is used
/// as input for sending data.
pub fn muxrpc<R: AsyncRead, W: AsyncWrite>(r: R, w: W) -> (RpcIn<R, W>, RpcOut<R, W>, Closed<W>) {
    let (ps_in, ps_out, closed) = packet_stream(r, w);
    (RpcIn::new(ps_in), RpcOut::new(ps_out), Closed(closed))
}

/// A stream of incoming rpcs from the peer.
pub struct RpcIn<R: AsyncRead, W>(PsIn<R, W, Box<[u8]>>);

impl<R: AsyncRead, W> RpcIn<R, W> {
    fn new(ps_in: PsIn<R, W, Box<[u8]>>) -> RpcIn<R, W> {
        RpcIn(ps_in)
    }
}

impl<R: AsyncRead, W: AsyncWrite> Stream for RpcIn<R, W> {
    /// Name(s) and args of the rpc, and a handle for reacting to the rpc
    type Item = (Box<[String]>, Box<[Value]>, IncomingRpc<R, W>);
    type Error = RpcError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match try_ready!(self.0.poll()) {
            None => Ok(Async::Ready(None)),

            Some((data, metadata, handle)) => {
                if metadata.packet_type == PacketType::Json {
                    let rpc = from_slice::<InRpc>(&data)?;

                    match handle {
                        IncomingPacket::Request(req) => {
                            match rpc.type_ {
                                RpcType::Sync => {
                                    Ok(Async::Ready(Some((rpc.name,
                                                          rpc.args,
                                                          IncomingRpc::Sync(new_in_sync(req))))))
                                }

                                RpcType::Async => {
                                    Ok(Async::Ready(Some((rpc.name,
                                                          rpc.args,
                                                          IncomingRpc::Async(new_in_async(req))))))
                                }

                                _ => Err(RpcError::InvalidData),
                            }
                        }

                        IncomingPacket::Duplex(ps_sink, ps_stream) => {
                            match rpc.type_ {
                                RpcType::Source => Ok(Async::Ready(Some((rpc.name, rpc.args, IncomingRpc::Source(new_rpc_sink(ps_sink)))))),

                                RpcType::Sink => Ok(Async::Ready(Some((rpc.name, rpc.args, IncomingRpc::Sink(new_rpc_stream(ps_stream)))))),

                                RpcType::Duplex => Ok(Async::Ready(Some((rpc.name, rpc.args, IncomingRpc::Duplex(new_rpc_sink(ps_sink), new_rpc_stream(ps_stream)))))),

                                _ => Err(RpcError::InvalidData),
                            }
                        }
                    }
                } else {
                    Err(RpcError::InvalidData)
                }
            }
        }
    }
}

fn unwrap_serialize<S: Serialize>(s: S) -> Box<[u8]> {
    match to_vec(&s) {
        Ok(data) => data.into_boxed_slice(),
        Err(_) => panic!("Muxrpc serialization to bytes failed"),
    }
}

/// Allows sending rpcs to the peer.
pub struct RpcOut<R: AsyncRead, W>(PsOut<R, W, Box<[u8]>>);

impl<R: AsyncRead, W> RpcOut<R, W> {
    fn new(ps_out: PsOut<R, W, Box<[u8]>>) -> RpcOut<R, W> {
        RpcOut(ps_out)
    }
}

impl<R, W> RpcOut<R, W>
    where R: AsyncRead,
          W: AsyncWrite
{
    /// Send an async request to the peer.
    ///
    /// The `OutAsync` Future must be polled to actually start sending the request.
    /// The `InAsyncResponse` Future can be polled to receive the response.
    ///
    /// `Res` is the type of a successful response, `E` is the type of an error response.
    pub fn async<RPC: Rpc, Res: DeserializeOwned, E: DeserializeOwned>
        (&mut self,
         rpc: &RPC)
         -> (OutAsync<W>, InAsyncResponse<R, Res, E>) {
        let out_rpc = OutRpc::new(RPC::names(), RpcType::Async, rpc);

        let (out_req, in_res) = self.0
            .request(unwrap_serialize(&out_rpc), PacketType::Json);
        (new_out_async(out_req), new_in_async_response(in_res))
    }

    /// Send a sync request to the peer.
    ///
    /// The `OutSync` Future must be polled to actually start sending the request.
    /// The `InSyncResponse` Future can be polled to receive the response.
    ///
    /// `Res` is the type of a successful response, `E` is the type of an error response.
    pub fn sync<RPC: Rpc, Res: DeserializeOwned, E: DeserializeOwned>
        (&mut self,
         rpc: &RPC)
         -> (OutSync<W>, InSyncResponse<R, Res, E>) {
        let out_rpc = OutRpc::new(RPC::names(), RpcType::Sync, rpc);

        let (out_req, in_res) = self.0
            .request(unwrap_serialize(&out_rpc), PacketType::Json);
        (new_out_sync(out_req), new_in_sync_response(in_res))
    }

    /// Send a source request to the peer.
    ///
    /// The `OutSource` Future must be polled to actually start sending the request.
    /// The `RpcStream` can be polled to receive the responses.
    ///
    /// `I` is the type of the responses, `E` is the type of an error response.
    pub fn source<RPC: Rpc, I: DeserializeOwned, E: DeserializeOwned>
        (&mut self,
         rpc: &RPC)
         -> (OutSource<W>, RpcStream<R, I, E>) {
        let out_rpc = OutRpc::new(RPC::names(), RpcType::Source, rpc);

        let (ps_sink, ps_stream) = self.0.duplex();
        (new_out_source(ps_sink, unwrap_serialize(&out_rpc)), new_rpc_stream(ps_stream))
    }

    /// Send a source request to the peer, that may be cancelled at any time.
    ///
    /// The `OutSource` Future must be polled to actually start sending the request, and it yields
    /// a handle for cancelling the source. Note that if the handle is not used for cancelling, it
    /// *must* still be closed.
    /// The `InSink` can be polled to receive the responses.
    ///
    /// `I` is the type of the responses, `E` is the type of an error response.
    pub fn source_cancelable<RPC: Rpc, I: DeserializeOwned, E: DeserializeOwned>
        (&mut self,
         rpc: &RPC)
         -> (OutSourceCancelable<W>, RpcStream<R, I, E>) {
        let out_rpc = OutRpc::new(RPC::names(), RpcType::Source, rpc);

        let (ps_sink, ps_stream) = self.0.duplex();
        (new_out_source_cancelable(ps_sink, unwrap_serialize(&out_rpc)), new_rpc_stream(ps_stream))
    }

    /// Send a sink request to the peer.
    ///
    /// The `OutSink` Future must be polled to actually start sending the request, and it yields
    /// a sink for sending more data to the peer.
    pub fn sink<RPC: Rpc>(&mut self, rpc: &RPC) -> OutSink<W> {
        let out_rpc = OutRpc::new(RPC::names(), RpcType::Sink, rpc);

        let (ps_sink, _) = self.0.duplex();
        new_out_sink(ps_sink, unwrap_serialize(&out_rpc))
    }

    /// Send a duplex request to the peer.
    ///
    /// The `OutSDuplex` Future must be polled to actually start sending the request, and it yields
    /// a sink for sending more data to the peer.
    /// The `RpcStream` can be polled to receive the responses.
    ///
    /// `I` is the type of the responses, `E` is the type of an error response.
    pub fn duplex<RPC: Rpc, I: DeserializeOwned, E: DeserializeOwned>
        (&mut self,
         rpc: &RPC)
         -> (OutDuplex<W>, RpcStream<R, I, E>) {
        let out_rpc = OutRpc::new(RPC::names(), RpcType::Duplex, rpc);

        let (ps_sink, ps_stream) = self.0.duplex();
        (new_out_duplex(ps_sink, unwrap_serialize(&out_rpc)), new_rpc_stream(ps_stream))
    }

    /// Close the muxrpc session. If there are still active handles to the underlying transport,
    /// it is not closed immediately. It will get closed once the last of them is done.
    pub fn close(self) -> CloseRpc<R, W> {
        CloseRpc(self.0.close())
    }
}

/// A future for closing the muxrpc session. If there are still active handles to the underlying transport,
/// it is not closed immediately. It will get closed once the last of them is done.
pub struct CloseRpc<R: AsyncRead, W>(ClosePs<R, W, Box<[u8]>>);

impl<R: AsyncRead, W: AsyncWrite> Future for CloseRpc<R, W> {
    type Item = ();
    type Error = Option<io::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

/// An incoming packet, initiated by the peer.
pub enum IncomingRpc<R: AsyncRead, W: AsyncWrite> {
    /// A source request. You get a sink, the peer got a stream.
    Source(RpcSink<W>),
    /// A sink request. You get a stream, the peer got a sink.
    Sink(RpcStream<R, Value, Value>),
    /// A duplex request. Both peers get a stream and a sink.
    Duplex(RpcSink<W>, RpcStream<R, Value, Value>),
    /// An async request. You get an InAsync, the peer got an InAsyncResponse.
    Async(InAsync<W>),
    /// A sync request. You get an InSync, the peer got an InAsyncResponse.
    Sync(InSync<W>),
}

#[cfg(test)]
mod tests {
    use super::*;

    use partial_io::{PartialAsyncRead, PartialAsyncWrite, PartialWithErrors};
    use partial_io::quickcheck_types::GenInterruptedWouldBlock;
    use quickcheck::{QuickCheck, StdGen};
    use async_ringbuffer::*;
    use rand;
    use futures::stream::iter_ok;
    use futures::future::ok;

    #[derive(Serialize)]
    struct TestRpc([u8; 8]);

    impl Rpc for TestRpc {
        fn names() -> Box<[&'static str]> {
            vec!["foo", "bar"].into_boxed_slice()
        }
    }

    #[test]
    fn async() {
        let rng = StdGen::new(rand::thread_rng(), 20);
        let mut quickcheck = QuickCheck::new().gen(rng).tests(1000);
        quickcheck.quickcheck(test_async as
                              fn(usize,
                                 usize,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>)
                                 -> bool);
    }

    fn test_async(buf_size_a: usize,
                  buf_size_b: usize,
                  write_ops_a: PartialWithErrors<GenInterruptedWouldBlock>,
                  read_ops_a: PartialWithErrors<GenInterruptedWouldBlock>,
                  write_ops_b: PartialWithErrors<GenInterruptedWouldBlock>,
                  read_ops_b: PartialWithErrors<GenInterruptedWouldBlock>)
                  -> bool {
        let (writer_a, reader_a) = ring_buffer(buf_size_a + 1);
        let writer_a = PartialAsyncWrite::new(writer_a, write_ops_a);
        let reader_a = PartialAsyncRead::new(reader_a, read_ops_a);

        let (writer_b, reader_b) = ring_buffer(buf_size_b + 1);
        let writer_b = PartialAsyncWrite::new(writer_b, write_ops_b);
        let reader_b = PartialAsyncRead::new(reader_b, read_ops_b);

        let (a_in, mut a_out, _) = muxrpc(reader_a, writer_b);
        let (b_in, b_out, _) = muxrpc(reader_b, writer_a);

        let echo = b_in.for_each(|(names, args, in_rpc)| {
                assert_eq!(names,
                           vec!["foo".to_string(), "bar".to_string()].into_boxed_slice());
                match in_rpc {
                    IncomingRpc::Async(in_async) => {
                        in_async.respond(&args).map_err(|_| unreachable!())
                    }
                    _ => unreachable!(),                
                }
            })
            .and_then(|_| b_out.close().map_err(|_| unreachable!()));

        let consume_a = a_in.for_each(|_| ok(()));

        let (req0, res0) = a_out.async::<_, [u8; 8], i32>(&TestRpc([0, 1, 2, 3, 4, 5, 6, 7]));
        let (req1, res1) = a_out.async::<_, [u8; 8], i32>(&TestRpc([8, 9, 10, 11, 12, 13, 14, 15]));
        let (req2, res2) = a_out.async::<_, [u8; 8], i32>(&TestRpc([16, 17, 18, 19, 20, 21, 22,
                                                                    23]));

        let send_all = req0.join3(req1, req2).and_then(|_| a_out.close());

        let receive_all = res0.join3(res1, res2)
            .map(|(r0_data, r1_data, r2_data)| {
                     return r0_data == [0, 1, 2, 3, 4, 5, 6, 7] &&
                            r1_data == [8, 9, 10, 11, 12, 13, 14, 15] &&
                            r2_data == [16, 17, 18, 19, 20, 21, 22, 23];
                 });

        return echo.join4(consume_a.map_err(|_| unreachable!()),
                          send_all.map_err(|_| unreachable!()),
                          receive_all.map_err(|_| unreachable!()))
                   .map(|(_, _, _, worked)| worked)
                   .wait()
                   .unwrap();
    }

    #[test]
    fn sync() {
        let rng = StdGen::new(rand::thread_rng(), 20);
        let mut quickcheck = QuickCheck::new().gen(rng).tests(1000);
        quickcheck.quickcheck(test_sync as
                              fn(usize,
                                 usize,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>)
                                 -> bool);
    }

    fn test_sync(buf_size_a: usize,
                 buf_size_b: usize,
                 write_ops_a: PartialWithErrors<GenInterruptedWouldBlock>,
                 read_ops_a: PartialWithErrors<GenInterruptedWouldBlock>,
                 write_ops_b: PartialWithErrors<GenInterruptedWouldBlock>,
                 read_ops_b: PartialWithErrors<GenInterruptedWouldBlock>)
                 -> bool {
        let (writer_a, reader_a) = ring_buffer(buf_size_a + 1);
        let writer_a = PartialAsyncWrite::new(writer_a, write_ops_a);
        let reader_a = PartialAsyncRead::new(reader_a, read_ops_a);

        let (writer_b, reader_b) = ring_buffer(buf_size_b + 1);
        let writer_b = PartialAsyncWrite::new(writer_b, write_ops_b);
        let reader_b = PartialAsyncRead::new(reader_b, read_ops_b);

        let (a_in, mut a_out, _) = muxrpc(reader_a, writer_b);
        let (b_in, b_out, _) = muxrpc(reader_b, writer_a);

        let echo = b_in.for_each(|(names, args, in_rpc)| {
                assert_eq!(names,
                           vec!["foo".to_string(), "bar".to_string()].into_boxed_slice());
                match in_rpc {
                    IncomingRpc::Sync(in_sync) => {
                        in_sync.respond(&args).map_err(|_| unreachable!())
                    }
                    _ => unreachable!(),                
                }
            })
            .and_then(|_| b_out.close().map_err(|_| unreachable!()));

        let consume_a = a_in.for_each(|_| ok(()));

        let (req0, res0) = a_out.sync::<_, [u8; 8], i32>(&TestRpc([0, 1, 2, 3, 4, 5, 6, 7]));
        let (req1, res1) = a_out.sync::<_, [u8; 8], i32>(&TestRpc([8, 9, 10, 11, 12, 13, 14, 15]));
        let (req2, res2) = a_out.sync::<_, [u8; 8], i32>(&TestRpc([16, 17, 18, 19, 20, 21, 22,
                                                                   23]));

        let send_all = req0.join3(req1, req2).and_then(|_| a_out.close());

        let receive_all = res0.join3(res1, res2)
            .map(|(r0_data, r1_data, r2_data)| {
                     return r0_data == [0, 1, 2, 3, 4, 5, 6, 7] &&
                            r1_data == [8, 9, 10, 11, 12, 13, 14, 15] &&
                            r2_data == [16, 17, 18, 19, 20, 21, 22, 23];
                 });

        return echo.join4(consume_a.map_err(|_| unreachable!()),
                          send_all.map_err(|_| unreachable!()),
                          receive_all.map_err(|_| unreachable!()))
                   .map(|(_, _, _, worked)| worked)
                   .wait()
                   .unwrap();
    }

    #[test]
    fn sink() {
        let rng = StdGen::new(rand::thread_rng(), 20);
        let mut quickcheck = QuickCheck::new().gen(rng).tests(1000);
        quickcheck.quickcheck(test_sink as
                              fn(usize,
                                 usize,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>)
                                 -> bool);
    }

    fn test_sink(buf_size_a: usize,
                 buf_size_b: usize,
                 write_ops_a: PartialWithErrors<GenInterruptedWouldBlock>,
                 read_ops_a: PartialWithErrors<GenInterruptedWouldBlock>,
                 write_ops_b: PartialWithErrors<GenInterruptedWouldBlock>,
                 read_ops_b: PartialWithErrors<GenInterruptedWouldBlock>)
                 -> bool {
        let (writer_a, reader_a) = ring_buffer(buf_size_a + 1);
        let writer_a = PartialAsyncWrite::new(writer_a, write_ops_a);
        let reader_a = PartialAsyncRead::new(reader_a, read_ops_a);

        let (writer_b, reader_b) = ring_buffer(buf_size_b + 1);
        let writer_b = PartialAsyncWrite::new(writer_b, write_ops_b);
        let reader_b = PartialAsyncRead::new(reader_b, read_ops_b);

        let (a_in, mut a_out, _) = muxrpc(reader_a, writer_b);
        let (b_in, b_out, _) = muxrpc(reader_b, writer_a);

        let echo = b_in.fold(true, |acc, (names, _, in_rpc)| {
                assert_eq!(names,
                           vec!["foo".to_string(), "bar".to_string()].into_boxed_slice());
                match in_rpc {
                    IncomingRpc::Sink(rpc_stream) => {
                        rpc_stream
                            .collect()
                            .map(move |data| {
                                     acc && data == vec![Value::Bool(true), Value::Bool(false)]
                                 })
                            .map_err(|_| RpcError::InvalidData)
                    }
                    _ => unreachable!(),                
                }
            })
            .and_then(|worked| {
                          b_out
                              .close()
                              .map(move |_| worked)
                              .map_err(|_| unreachable!())
                      });

        let consume_a = a_in.for_each(|_| ok(()));

        let out_sink0 = a_out.sink(&TestRpc([0, 1, 2, 3, 4, 5, 6, 7]));
        let out0 =
            out_sink0.and_then(|rpc_sink| {
                                   rpc_sink.send_all(iter_ok::<_, Option<io::Error>>(vec![Ok(Value::Bool(true)),
                                                                          Ok(Value::Bool(false))]))
                               });
        let out_sink1 = a_out.sink(&TestRpc([0, 1, 2, 3, 4, 5, 6, 99]));
        let out1 =
            out_sink1.and_then(|rpc_sink| {
                                   rpc_sink.send_all(iter_ok::<_, Option<io::Error>>(vec![Ok(Value::Bool(true)),
                                                                          Ok(Value::Bool(false))]))
                               });

        let send_all = out0.join(out1).and_then(|_| a_out.close());

        return echo.join3(consume_a.map_err(|_| unreachable!()),
                          send_all.map_err(|_| unreachable!()))
                   .map(|(worked, _, _)| worked)
                   .wait()
                   .unwrap();
    }

    #[test]
    fn source() {
        let rng = StdGen::new(rand::thread_rng(), 20);
        let mut quickcheck = QuickCheck::new().gen(rng).tests(1000);
        quickcheck.quickcheck(test_source as
                              fn(usize,
                                 usize,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>)
                                 -> bool);
    }

    fn test_source(buf_size_a: usize,
                   buf_size_b: usize,
                   write_ops_a: PartialWithErrors<GenInterruptedWouldBlock>,
                   read_ops_a: PartialWithErrors<GenInterruptedWouldBlock>,
                   write_ops_b: PartialWithErrors<GenInterruptedWouldBlock>,
                   read_ops_b: PartialWithErrors<GenInterruptedWouldBlock>)
                   -> bool {
        let (writer_a, reader_a) = ring_buffer(buf_size_a + 1);
        let writer_a = PartialAsyncWrite::new(writer_a, write_ops_a);
        let reader_a = PartialAsyncRead::new(reader_a, read_ops_a);

        let (writer_b, reader_b) = ring_buffer(buf_size_b + 1);
        let writer_b = PartialAsyncWrite::new(writer_b, write_ops_b);
        let reader_b = PartialAsyncRead::new(reader_b, read_ops_b);

        let (a_in, mut a_out, _) = muxrpc(reader_a, writer_b);
        let (b_in, b_out, _) = muxrpc(reader_b, writer_a);

        let echo = b_in.for_each(|(names, _, in_rpc)| {
                assert_eq!(names,
                           vec!["foo".to_string(), "bar".to_string()].into_boxed_slice());
                match in_rpc {
                    IncomingRpc::Source(rpc_sink) => {
                        rpc_sink
                            .send_all(iter_ok::<_, Option<io::Error>>(vec![Ok(Value::Bool(true)),
                                                                           Ok(Value::Bool(false))]))
                            .map_err(|_| unreachable!())
                            .map(|_| ())
                    }
                    _ => unreachable!(),                
                }
            })
            .and_then(|_| b_out.close().map_err(|_| unreachable!()));

        let consume_a = a_in.for_each(|_| ok(()));

        let (out_source0, stream0) =
            a_out.source::<_, bool, i32>(&TestRpc([0, 1, 2, 3, 4, 5, 6, 7]));
        let stream0 = stream0.collect().map(|data| data == vec![true, false]);

        let (out_source1, stream1) =
            a_out.source::<_, bool, i32>(&TestRpc([0, 1, 2, 3, 4, 5, 6, 99]));
        let stream1 = stream1.collect().map(|data| data == vec![true, false]);

        let send_all = out_source0.join(out_source1).and_then(|_| a_out.close());
        let process_all = stream0.join(stream1);

        return echo.join4(consume_a.map_err(|_| unreachable!()),
                          send_all.map_err(|_| unreachable!()),
                          process_all.map_err(|_| unreachable!()))
                   .map(|(_, _, _, (worked0, worked1))| worked0 && worked1)
                   .wait()
                   .unwrap();
    }
}
