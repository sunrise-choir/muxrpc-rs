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
    /// Received a packet containing invalid data. This error is non-fatal.
    InvalidData(SerdeError),
    /// Got a packet without the json type. This error is non-fatal.
    NotJson,
}

impl Display for RpcError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match *self {
            RpcError::IoError(ref err) => write!(f, "Rpc error: {}", err),
            RpcError::InvalidData(ref err) => write!(f, "Rpc error: {}", err),
            RpcError::NotJson => write!(f, "Rpc error: Not json"),
        }
    }
}

impl Error for RpcError {
    fn description(&self) -> &str {
        match *self {
            RpcError::IoError(ref err) => err.description(),
            RpcError::InvalidData(ref err) => err.description(),
            RpcError::NotJson => "got a packet not of type json",
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            RpcError::IoError(ref err) => Some(err),
            RpcError::InvalidData(ref err) => Some(err),
            RpcError::NotJson => None,
        }
    }
}

impl From<io::Error> for RpcError {
    fn from(err: io::Error) -> RpcError {
        RpcError::IoError(err)
    }
}

impl From<SerdeError> for RpcError {
    fn from(err: SerdeError) -> RpcError {
        RpcError::InvalidData(err)
    }
}

/// An error that can be emitted during the rpc process when receiving multiplexed data.
#[derive(Debug)]
pub enum ConnectionRpcError<E> {
    /// An error signaled by the peer via the muxrpc protocol.
    PeerError(E),
    /// A `ConnectionError` occured.
    ConnectionError(ConnectionError),
    /// Received a packet containing invalid data. This error is non-fatal.
    InvalidData(SerdeError),
    /// Got a packet without the json type. This error is non-fatal.
    NotJson,
}

impl<E: Display> Display for ConnectionRpcError<E> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match *self {
            ConnectionRpcError::PeerError(ref err) => {
                write!(f, "Connection rpc error: Peer error: {}", err)
            }
            ConnectionRpcError::ConnectionError(ref err) => {
                write!(f, "Connection rpc error: Connection error: {}", err)
            }
            ConnectionRpcError::InvalidData(ref err) => {
                write!(f, "Connection rpc error: Invalid data: {}", err)
            }
            ConnectionRpcError::NotJson => write!(f, "Connection rpc error: Not json"),
        }
    }
}

impl<E: Error> Error for ConnectionRpcError<E> {
    fn description(&self) -> &str {
        match *self {
            ConnectionRpcError::PeerError(ref err) => err.description(),
            ConnectionRpcError::ConnectionError(ref err) => err.description(),
            ConnectionRpcError::InvalidData(ref err) => err.description(),
            ConnectionRpcError::NotJson => "got a packet not of type json",
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            ConnectionRpcError::PeerError(ref err) => Some(err),
            ConnectionRpcError::ConnectionError(ref err) => Some(err),
            ConnectionRpcError::InvalidData(ref err) => Some(err),
            ConnectionRpcError::NotJson => None,
        }
    }
}

impl<E> From<ConnectionError> for ConnectionRpcError<E> {
    fn from(err: ConnectionError) -> ConnectionRpcError<E> {
        ConnectionRpcError::ConnectionError(err)
    }
}

impl<E> From<SerdeError> for ConnectionRpcError<E> {
    fn from(err: SerdeError) -> ConnectionRpcError<E> {
        ConnectionRpcError::InvalidData(err)
    }
}
