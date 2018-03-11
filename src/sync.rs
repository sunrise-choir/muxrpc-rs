use std::convert::From;
use std::io;
use std::marker::PhantomData;

use futures_core::{Future, Poll};
use futures_core::Async::Ready;
use futures_core::task::Context;
use futures_io::{AsyncRead, AsyncWrite};
use packet_stream::{PeerRequest, Response, PacketType, Request, PeerResponse};
use serde_json::from_slice;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::*;

type PeerRes<W> = PeerResponse<W, Box<[u8]>>;
type Req<W> = Request<W, Box<[u8]>>;
type PeerReq<W> = PeerRequest<W, Box<[u8]>>;

/// An outgoing sync request, created by this muxrpc.
///
/// Poll it to actually start sending the sync request.
pub struct Sync<W: AsyncWrite> {
    out_request: Req<W>,
}

pub fn new_sync<W: AsyncWrite>(out_request: Req<W>) -> Sync<W> {
    Sync { out_request }
}

impl<W: AsyncWrite> Future for Sync<W> {
    type Item = ();
    type Error = Option<io::Error>;

    fn poll(&mut self, cx: &mut Context) -> Poll<Self::Item, Self::Error> {
        self.out_request.poll(cx)
    }
}

/// A response to an sync that will be received from the peer.
pub struct SyncResponse<R: AsyncRead, Res, E> {
    in_response: Response<R>,
    _res_type: PhantomData<Res>,
    _err_type: PhantomData<E>,
}

pub fn new_sync_response<R: AsyncRead, Res, E>(in_response: Response<R>)
                                               -> SyncResponse<R, Res, E> {
    SyncResponse {
        in_response,
        _res_type: PhantomData,
        _err_type: PhantomData,
    }
}

impl<R: AsyncRead, Res: DeserializeOwned, E: DeserializeOwned> Future for SyncResponse<R, Res, E> {
    type Item = Res;
    type Error = ConnectionRpcError<E>;

    fn poll(&mut self, cx: &mut Context) -> Poll<Self::Item, Self::Error> {
        let (data, metadata) = try_ready!(self.in_response.poll(cx));

        if metadata.packet_type == PacketType::Json {
            if metadata.is_end {
                Err(ConnectionRpcError::PeerError(from_slice::<E>(&data)?))
            } else {
                Ok(Ready(from_slice::<Res>(&data)?))
            }
        } else {
            Err(ConnectionRpcError::NotJson)
        }
    }
}

/// An sync initiated by the peer. Drop to ignore it, or use `respond` or `respond_err` to send a
/// response.
pub struct PeerSync<W: AsyncWrite> {
    in_request: PeerReq<W>,
}

pub fn new_peer_sync<W: AsyncWrite>(in_request: PeerReq<W>) -> PeerSync<W> {
    PeerSync { in_request }
}

impl<W: AsyncWrite> PeerSync<W> {
    /// Send the given response to the peer.
    pub fn respond<Res: Serialize>(self, res: &Res) -> PeerSyncResponse<W> {
        PeerSyncResponse::new(self.in_request
                                  .respond(unwrap_serialize(res), META_NON_END))
    }

    /// Send the given error response to the peer.
    pub fn respond_error<E: Serialize>(self, err: &E) -> PeerSyncResponse<W> {
        PeerSyncResponse::new(self.in_request.respond(unwrap_serialize(err), META_END))
    }
}

/// Future that completes when the sync response has been sent to the peer.
pub struct PeerSyncResponse<W: AsyncWrite> {
    out_response: PeerRes<W>,
}

impl<W: AsyncWrite> PeerSyncResponse<W> {
    fn new(out_response: PeerRes<W>) -> PeerSyncResponse<W> {
        PeerSyncResponse { out_response }
    }
}

impl<W: AsyncWrite> Future for PeerSyncResponse<W> {
    type Item = ();
    type Error = Option<io::Error>;

    fn poll(&mut self, cx: &mut Context) -> Poll<Self::Item, Self::Error> {
        self.out_response.poll(cx)
    }
}
