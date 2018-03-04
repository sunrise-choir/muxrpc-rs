use std::convert::From;
use std::io;
use std::marker::PhantomData;

use futures::prelude::*;
use tokio_io::{AsyncRead, AsyncWrite};
use packet_stream::{InRequest, InResponse, PacketType, OutRequest, OutResponse};
use serde_json::from_slice;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::*;

type OutRes<W> = OutResponse<W, Box<[u8]>>;
type OutReq<W> = OutRequest<W, Box<[u8]>>;
type InReq<W> = InRequest<W, Box<[u8]>>;

/// An outgoing async request, created by this muxrpc.
///
/// Poll it to actually start sending the async request.
pub struct OutAsync<W: AsyncWrite> {
    out_request: OutReq<W>,
}

pub fn new_out_async<W: AsyncWrite>(out_request: OutReq<W>) -> OutAsync<W> {
    OutAsync { out_request }
}

impl<W: AsyncWrite> Future for OutAsync<W> {
    type Item = ();
    type Error = Option<io::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.out_request.poll()
    }
}

/// A response to an async that will be received from the peer.
pub struct InAsyncResponse<R: AsyncRead, Res, E> {
    in_response: InResponse<R>,
    _res_type: PhantomData<Res>,
    _err_type: PhantomData<E>,
}

pub fn new_in_async_response<R: AsyncRead, Res, E>(in_response: InResponse<R>)
                                                   -> InAsyncResponse<R, Res, E> {
    InAsyncResponse {
        in_response,
        _res_type: PhantomData,
        _err_type: PhantomData,
    }
}

impl<R: AsyncRead, Res: DeserializeOwned, E: DeserializeOwned> Future
    for InAsyncResponse<R, Res, E> {
    type Item = Res;
    type Error = ConnectionRpcError<E>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (data, metadata) = try_ready!(self.in_response.poll());

        if metadata.packet_type == PacketType::Json {
            if metadata.is_end {
                Err(ConnectionRpcError::PeerError(from_slice::<E>(&data)?))
            } else {
                Ok(Async::Ready(from_slice::<Res>(&data)?))
            }
        } else {
            Err(ConnectionRpcError::InvalidData)
        }
    }
}

/// An async initiated by the peer. Drop to ignore it, or use `respond` or `respond_err` to send a
/// response.
pub struct InAsync<W: AsyncWrite> {
    in_request: InReq<W>,
}

pub fn new_in_async<W: AsyncWrite>(in_request: InReq<W>) -> InAsync<W> {
    InAsync { in_request }
}

impl<W: AsyncWrite> InAsync<W> {
    /// Send the given response to the peer.
    pub fn respond<Res: Serialize>(self, res: &Res) -> OutAsyncResponse<W> {
        OutAsyncResponse::new(self.in_request
                                  .respond(unwrap_serialize(res), META_NON_END))
    }

    /// Send the given error response to the peer.
    pub fn respond_error<E: Serialize>(self, err: &E) -> OutAsyncResponse<W> {
        OutAsyncResponse::new(self.in_request.respond(unwrap_serialize(err), META_END))
    }
}

/// Future that completes when the async response has been sent to the peer.
pub struct OutAsyncResponse<W: AsyncWrite> {
    out_response: OutRes<W>,
}

impl<W: AsyncWrite> OutAsyncResponse<W> {
    fn new(out_response: OutRes<W>) -> OutAsyncResponse<W> {
        OutAsyncResponse { out_response }
    }
}

impl<W: AsyncWrite> Future for OutAsyncResponse<W> {
    type Item = ();
    /// This error contains a `None` if an TODO list muxrpc stuff here
    type Error = Option<io::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.out_response.poll()
    }
}
