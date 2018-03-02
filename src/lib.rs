//! Implements the [muxrpc protocol](https://github.com/ssbc/muxrpc) in rust.
#![deny(missing_docs)]

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
mod async;

use std::convert::From;
use std::fmt::{Display, Formatter};
use std::io;

use futures::prelude::*;
use futures::unsync::oneshot::Canceled;
use tokio_io::{AsyncRead, AsyncWrite};
use packet_stream::{packet_stream, PsIn, PsOut, ConnectionError, PsSink, PsStream, InRequest,
                    InResponse, IncomingPacket, PacketType, OutRequest, OutResponse, Metadata};
use serde_json::Value;
use serde_json::{to_vec, from_slice};
use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;

pub use errors::*;
use async::{new_in_async, new_out_async, new_in_async_response};
pub use async::{OutAsync, OutAsyncResponse, InAsync, InAsyncResponse};

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
    /// This can only be emitted if a previously polled/written `OutRequest`,
    /// `OutResponse`s or `PsSink` is dropped whithout waiting for it to finish. TODO put muxrpc stuff here
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
                                RpcType::Sync => unimplemented!(),
                                RpcType::Async => {
                                    Ok(Async::Ready(Some((rpc.name,
                                                          rpc.args,
                                                          IncomingRpc::Async(new_in_async(req))))))
                                }
                                _ => Err(RpcError::InvalidData),
                            }
                        }

                        IncomingPacket::Duplex(ps_sink, ps_stream) => unimplemented!(),
                    }
                } else {
                    Err(RpcError::InvalidData)
                }
            }
        }

        // let packet = try_ready!(self.ps_in.poll());
        //
        // match packet {
        //     None => Ok(Async::Ready(None)),
        //
        //     Some(IncomingPacket::Request(req)) => {
        //         if !req.packet().is_json_packet() {
        //             return Err(RpcError::InvalidData);
        //         }
        //
        //         match serde_json::from_slice::<Rpc>(req.packet().data()) {
        //             Ok(rpc) => {
        //                 match rpc.type_ {
        //                     RpcType::Sync => {
        //                         Ok(Async::Ready(Some((rpc.name,
        //                                               rpc.args,
        //                                               IncomingRpc::Sync(InSync::new(req))))))
        //                     }
        //                     RpcType::Async => {
        //                         Ok(Async::Ready(Some((rpc.name,
        //                                               rpc.args,
        //                                               IncomingRpc::Async(InAsync::new(req))))))
        //                     }
        //                     _ => Err(RpcError::InvalidData),
        //                 }
        //             }
        //             Err(err) => Err(RpcError::InvalidData),
        //         }
        //     }
        //
        //     Some(IncomingPacket::Duplex(p, ps_sink, ps_stream)) => {
        //         if !p.is_json_packet() {
        //             return Err(RpcError::InvalidData);
        //         }
        //
        //         match serde_json::from_slice::<Rpc>(p.data()) {
        //             Ok(rpc) => {
        //                 match rpc.type_ {
        //                     RpcType::Source => {
        //                         Ok(Async::Ready(Some((rpc.name,
        //                                               rpc.args,
        //                                               IncomingRpc::Source(RpcSink::new(ps_sink))))))
        //                     }
        //                     RpcType::Sink => {
        //                         Ok(Async::Ready(Some((rpc.name,
        //                                               rpc.args,
        //                                               IncomingRpc::Sink(RpcStream::new(ps_stream))))))
        //                     }
        //                     RpcType::Duplex => Ok(Async::Ready(Some((rpc.name, rpc.args,
        //                         IncomingRpc::Duplex(RpcSink::new(ps_sink),
        //                         RpcStream::new(ps_stream)))))),
        //                     _ => Err(RpcError::InvalidData),
        //                 }
        //             }
        //             Err(err) => Err(RpcError::InvalidData),
        //         }
        //     }
        // }
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
    /// Send an async to the peer.
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

    /// Close the rpc channel, indicating that no more rpcs will be sent.
    ///
    /// This does not immediately close if there are still unfinished
    /// TODO list stuff. In that case, the closing
    /// happens when the last of them finishes.
    ///
    /// The error contains a `None` if an TODO list stuff errored previously.
    pub fn close(&mut self) -> Poll<(), Option<io::Error>> {
        self.0.close()
    }
}


/// An incoming packet, initiated by the peer.
pub enum IncomingRpc<R: AsyncRead, W: AsyncWrite> {
    /// Temporary filler during dev TODO remove this
    Tmp(R), // /// A source request. You get a sink, the peer got a stream.
    // Source(RpcSink<W, B>),
    // /// A sink request. You get a stream, the peer got a sink.
    // Sink(RpcStream<R>),
    // /// A duplex request. Both peers get a stream and a sink.
    // Duplex(RpcSink<W, B>, RpcStream<R>),
    /// An async request. You get an InAsync, the peer got an InAsyncResponse.
    Async(InAsync<W>),
               // /// A sync request. You get an InSync, the peer got an OutSync.
               // Sync(InSync<W, B>)
}


// /// A sink for writing data to the peer.
// pub struct RpcSink<W: AsyncWrite, B: AsRef<[u8]>> {
//     sink: PsSink<W, B>,
// }
//
// impl<W: AsyncWrite, B: AsRef<[u8]>> RpcSink<W, B> {
//     fn new(ps_sink: PsSink<W, B>) -> RpcSink<W, B> {
//         unimplemented!()
//     }
// }
//
// /// A stream for receiving data from the peer.
// pub struct RpcStream<R: AsyncRead> {
//     stream: PsStream<R>,
// }
//
// impl<R: AsyncRead> RpcStream<R> {
//     fn new(ps_stream: PsStream<R>) -> RpcStream<R> {
//         unimplemented!()
//     }
// }
//
// /// Allows sending a single value to the peer.
// pub struct InAsync<W: AsyncWrite, B: AsRef<[u8]>> {
//     in_request: InRequest<W, B>,
// }
//
// impl<W: AsyncWrite, B: AsRef<[u8]>> InAsync<W, B> {
//     fn new(in_req: InRequest<W, B>) -> InAsync<W, B> {
//         unimplemented!()
//     }
// }
//
// /// Allows sending a single value to the peer.
// pub struct InSync<W: AsyncWrite, B: AsRef<[u8]>> {
//     in_request: InRequest<W, B>,
// }
//
// impl<W: AsyncWrite, B: AsRef<[u8]>> InSync<W, B> {
//     fn new(in_req: InRequest<W, B>) -> InSync<W, B> {
//         unimplemented!()
//     }
// }

// /// Allows receiving a single value from the peer.
// pub struct OutSync<R: AsyncRead> {
//     in_response: InResponse<R>,
// }


#[cfg(test)]
mod tests {
    use super::*;

    use partial_io::{PartialAsyncRead, PartialAsyncWrite, PartialWithErrors};
    use partial_io::quickcheck_types::GenInterruptedWouldBlock;
    use quickcheck::{QuickCheck, StdGen};
    use async_ringbuffer::*;
    use rand;
    use futures::stream::iter_ok;
    use futures::future::{ok, poll_fn};

    #[derive(Serialize)]
    struct TestRpc([u8; 8]);

    impl Rpc for TestRpc {
        fn names() -> Box<[&'static str]> {
            vec!["foo", "bar"].into_boxed_slice()
        }
    }

    #[test]
    fn requests() {
        let rng = StdGen::new(rand::thread_rng(), 20);
        let mut quickcheck = QuickCheck::new().gen(rng).tests(1000);
        quickcheck.quickcheck(test_requests as
                              fn(usize,
                                 usize,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>,
                                 PartialWithErrors<GenInterruptedWouldBlock>)
                                 -> bool);
    }

    fn test_requests(buf_size_a: usize,
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
        let (b_in, mut b_out, _) = muxrpc(reader_b, writer_a);

        let echo = b_in.for_each(|(names, args, in_rpc)| {
                assert_eq!(names,
                           vec!["foo".to_string(), "bar".to_string()].into_boxed_slice());
                match in_rpc {
                    IncomingRpc::Async(in_async) => {
                        in_async.respond(&args).map_err(|_| unreachable!())
                    }
                    IncomingRpc::Tmp(_) => unreachable!(),                
                }
            })
            .and_then(|_| poll_fn(|| b_out.close()).map_err(|_| unreachable!()));

        let consume_a = a_in.for_each(|_| ok(()));

        let (req0, res0) = a_out.async::<_, [u8; 8], i32>(&TestRpc([0, 1, 2, 3, 4, 5, 6, 7]));
        let (req1, res1) = a_out.async::<_, [u8; 8], i32>(&TestRpc([8, 9, 10, 11, 12, 13, 14, 15]));
        let (req2, res2) = a_out.async::<_, [u8; 8], i32>(&TestRpc([16, 17, 18, 19, 20, 21, 22,
                                                                    23]));

        let send_all = req0.join3(req1, req2)
            .and_then(|_| poll_fn(|| a_out.close()));

        let receive_all = res0.join3(res1, res2)
            .map(|(r0_data, r1_data, r2_data)| {
                     return r0_data.is_ok() && r0_data.ok().unwrap() == [0, 1, 2, 3, 4, 5, 6, 7] &&
                            r1_data.ok().unwrap() == [8, 9, 10, 11, 12, 13, 14, 15] &&
                            r2_data.ok().unwrap() == [16, 17, 18, 19, 20, 21, 22, 23];
                 });

        return echo.join4(consume_a.map_err(|_| unreachable!()),
                          send_all.map_err(|_| unreachable!()),
                          receive_all.map_err(|_| unreachable!()))
                   .map(|(_, _, _, worked)| worked)
                   .wait()
                   .unwrap();
    }
}
