use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;

use serde_json::error::Error as SerdeError;
use packet_stream::ConnectionError;

/// An error that can be emitted during the rpc process.
#[derive(Debug)]
pub enum RpcError {
    /// An io-error that was emitted by the underlying transports or the
    /// underlying packet-stream.
    IoError(io::Error),
    /// Received a packet containing invalid data.
    ///
    /// For example, the packet could have the wrong type, it could contain
    /// malformed json, it could set inappropriate flags, it could lack required
    /// json fields, etc.
    InvalidData,
}

impl Display for RpcError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match *self {
            RpcError::IoError(ref err) => write!(f, "Rpc error: {}", err),
            RpcError::InvalidData => write!(f, "Rpc error: Invalid data"),
        }
    }
}

impl Error for RpcError {
    fn description(&self) -> &str {
        match *self {
            RpcError::IoError(ref err) => err.description(),
            RpcError::InvalidData => "Received a packet that contained invalid data",
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            RpcError::IoError(ref err) => Some(err),
            RpcError::InvalidData => None,
        }
    }
}

impl From<io::Error> for RpcError {
    fn from(err: io::Error) -> RpcError {
        RpcError::IoError(err)
    }
}

impl From<SerdeError> for RpcError {
    fn from(_: SerdeError) -> RpcError {
        RpcError::InvalidData
    }
}

/// An error that can be emitted during the rpc process when receiving multiplexed data.
#[derive(Debug)]
pub enum ConnectionRpcError<E> {
    /// An error signaled by the peer via the muxrpc protocol.
    PeerError(E),
    /// A `ConnectionError` occured.
    ConnectionError(ConnectionError),
    /// Received a packet containing invalid data.
    ///
    /// For example, the packet could have the wrong type, it could contain
    /// malformed json, it could set inappropriate flags, it could lack required
    /// json fields, etc.
    InvalidData,
}

impl<E: Display> Display for ConnectionRpcError<E> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match *self {
            ConnectionRpcError::PeerError(ref err) => {
                write!(f, "Connection rpc error: Peer error: {}", err)
            }
            ConnectionRpcError::ConnectionError(ref err) => {
                write!(f, "Connection rpc error: {}", err)
            }
            ConnectionRpcError::InvalidData => write!(f, "Connection rpc error: Invalid data"),
        }
    }
}

impl<E: Error> Error for ConnectionRpcError<E> {
    fn description(&self) -> &str {
        match *self {
            ConnectionRpcError::PeerError(ref err) => err.description(),
            ConnectionRpcError::ConnectionError(ref err) => err.description(),
            ConnectionRpcError::InvalidData => "Received a packet that contained invalid data",
        }
    }
}

impl<E> From<ConnectionError> for ConnectionRpcError<E> {
    fn from(err: ConnectionError) -> ConnectionRpcError<E> {
        ConnectionRpcError::ConnectionError(err)
    }
}

impl<E> From<SerdeError> for ConnectionRpcError<E> {
    fn from(_: SerdeError) -> ConnectionRpcError<E> {
        ConnectionRpcError::InvalidData
    }
}
