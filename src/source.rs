use std::convert::From;
use std::io;
use std::marker::PhantomData;

use atm_async_utils::sink_futures::{Close, SendClose};
use futures::prelude::*;
use futures::sink::Send;
use tokio_io::{AsyncRead, AsyncWrite};
use packet_stream::{PsSink, PsStream, PacketType};
use serde_json::from_slice;
use serde::de::DeserializeOwned;

use super::*;

type MuxrpcSink<W> = PsSink<W, Box<[u8]>>;

/// An outgoing source request, created by this muxrpc.
///
/// Poll it to actually start sending the source request.
pub struct OutSource<W: AsyncWrite>(SendClose<MuxrpcSink<W>>);

pub fn new_out_source<W: AsyncWrite>(ps_sink: MuxrpcSink<W>,
                                     initial_data: Box<[u8]>)
                                     -> OutSource<W> {
    OutSource(SendClose::new(ps_sink, (initial_data, META_NON_END)))
}

impl<W: AsyncWrite> Future for OutSource<W> {
    type Item = ();
    type Error = Option<io::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let _ = try_ready!(self.0.poll());
        Ok(Async::Ready(()))
    }
}

/// An outgoing source request, created by this muxrpc.
///
/// Poll it to actually start sending the source request and get a handle to cancel the source.
pub struct OutSourceCancelable<W: AsyncWrite>(Send<MuxrpcSink<W>>);

pub fn new_out_source_cancelable<W: AsyncWrite>(ps_sink: MuxrpcSink<W>,
                                                initial_data: Box<[u8]>)
                                                -> OutSourceCancelable<W> {
    OutSourceCancelable(ps_sink.send((initial_data, META_NON_END)))
}

impl<W: AsyncWrite> Future for OutSourceCancelable<W> {
    type Item = CancelSource<W>;
    type Error = Option<io::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let sink = try_ready!(self.0.poll());
        Ok(Async::Ready(CancelSource::new(sink)))
    }
}

enum CancelSourceState<W: AsyncWrite> {
    PrePoll(MuxrpcSink<W>),
    PostPoll(SendClose<MuxrpcSink<W>>),
}

/// This future can be used to signal to the peer that you are no longer interested in receiving
/// more values from this source.
///
/// Note that if you don't poll this future, you *must* use the `close` method instead (otherwise,
/// there might be unclosed sinks under the hood).
pub struct CancelSource<W: AsyncWrite>(Option<CancelSourceState<W>>);

impl<W: AsyncWrite> CancelSource<W> {
    fn new(ps_sink: MuxrpcSink<W>) -> CancelSource<W> {
        CancelSource(Some(CancelSourceState::PrePoll(ps_sink)))
    }

    /// Close this `CancelSource`. If it has not been run as a future, you *must* instead run the
    /// future returned from this method.
    pub fn close(mut self) -> CloseCancelSource<W> {
        match self.0.take().unwrap() {
            CancelSourceState::PrePoll(sink) => CloseCancelSource(Close::new(sink)),
            CancelSourceState::PostPoll(_) => panic!("Tried to close an already polled CancelSource"),
        }
    }
}

impl<W: AsyncWrite> Future for CancelSource<W> {
    type Item = ();
    type Error = Option<io::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0.take().unwrap() {
            CancelSourceState::PrePoll(sink) => {
                self.0 = Some(CancelSourceState::PostPoll(SendClose::new(sink,
                                                                         (Box::new(JSON_TRUE),
                                                                          META_END))));
                self.poll()
            }

            CancelSourceState::PostPoll(mut future) => {
                let _ = try_ready!(future.poll());
                Ok(Async::Ready(()))
            }
        }
    }
}

/// A future for performing all necessary cleanup for a `CancelSource`. This *must* be completed
/// before being dropped.
pub struct CloseCancelSource<W: AsyncWrite>(Close<MuxrpcSink<W>>);

impl<W: AsyncWrite> Future for CloseCancelSource<W> {
    type Item = ();
    type Error = Option<io::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let _ = try_ready!(self.0.poll());
        Ok(Async::Ready(()))
    }
}

/// A stream from the peer.
///
/// `I` is the type of the emitted items, `E` is the type of the errors the peer can send.
pub struct RpcStream<R: AsyncRead, I, E> {
    ps_stream: PsStream<R>,
    _item_type: PhantomData<I>,
    _error_type: PhantomData<E>,
}

pub fn new_rpc_stream<R: AsyncRead, I, E>(ps_stream: PsStream<R>) -> RpcStream<R, I, E> {
    RpcStream {
        ps_stream,
        _item_type: PhantomData,
        _error_type: PhantomData,
    }
}

impl<R: AsyncRead, I: DeserializeOwned, E: DeserializeOwned> Stream for RpcStream<R, I, E> {
    type Item = I;
    /// A `ConnectionRpcError::InvalidData` is non-fatal and polling the stream may be safely
    /// continued.
    type Error = ConnectionRpcError<E>;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let (data, metadata) = try_ready!(self.ps_stream.poll()).unwrap(); // PsStream never emits Ok(None)

        if metadata.packet_type == PacketType::Json {
            if metadata.is_end {
                if data[..] == JSON_TRUE[..] {
                    Ok(Async::Ready(None))
                } else {
                    Err(ConnectionRpcError::PeerError(from_slice::<E>(&data)?))
                }
            } else {
                Ok(Async::Ready(Some(from_slice::<I>(&data)?)))
            }
        } else {
            Err(ConnectionRpcError::InvalidData)
        }
    }
}
