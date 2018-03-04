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

/// An outgoing sync request, created by this muxrpc.
///
/// Poll it to actually start sending the sync request.
pub struct OutSync<W: AsyncWrite> {
    out_request: OutReq<W>,
}

pub fn new_out_sync<W: AsyncWrite>(out_request: OutReq<W>) -> OutSync<W> {
    OutSync { out_request }
}

impl<W: AsyncWrite> Future for OutSync<W> {
    type Item = ();
    type Error = Option<io::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.out_request.poll()
    }
}

/// A response to an sync that will be received from the peer.
pub struct InSyncResponse<R: AsyncRead, Res, E> {
    in_response: InResponse<R>,
    _res_type: PhantomData<Res>,
    _err_type: PhantomData<E>,
}

pub fn new_in_sync_response<R: AsyncRead, Res, E>(in_response: InResponse<R>)
                                                  -> InSyncResponse<R, Res, E> {
    InSyncResponse {
        in_response,
        _res_type: PhantomData,
        _err_type: PhantomData,
    }
}

impl<R: AsyncRead, Res: DeserializeOwned, E: DeserializeOwned> Future
    for InSyncResponse<R, Res, E> {
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

/// An sync initiated by the peer. Drop to ignore it, or use `respond` or `respond_err` to send a
/// response.
pub struct InSync<W: AsyncWrite> {
    in_request: InReq<W>,
}

pub fn new_in_sync<W: AsyncWrite>(in_request: InReq<W>) -> InSync<W> {
    InSync { in_request }
}

impl<W: AsyncWrite> InSync<W> {
    /// Send the given response to the peer.
    pub fn respond<Res: Serialize>(self, res: &Res) -> OutSyncResponse<W> {
        OutSyncResponse::new(self.in_request
                                 .respond(unwrap_serialize(res), META_NON_END))
    }

    /// Send the given error response to the peer.
    pub fn respond_error<E: Serialize>(self, err: &E) -> OutSyncResponse<W> {
        OutSyncResponse::new(self.in_request.respond(unwrap_serialize(err), META_END))
    }
}

/// Future that completes when the sync response has been sent to the peer.
pub struct OutSyncResponse<W: AsyncWrite> {
    out_response: OutRes<W>,
}

impl<W: AsyncWrite> OutSyncResponse<W> {
    fn new(out_response: OutRes<W>) -> OutSyncResponse<W> {
        OutSyncResponse { out_response }
    }
}

impl<W: AsyncWrite> Future for OutSyncResponse<W> {
    type Item = ();
    type Error = Option<io::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.out_response.poll()
    }
}
