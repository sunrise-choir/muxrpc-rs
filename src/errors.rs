use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::marker::PhantomData;

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
    fn from(err: SerdeError) -> RpcError {
        RpcError::InvalidData
    }
}

/// An error that can be emitted during the rpc process when receiving multiplexed data.
#[derive(Debug)]
pub enum ConnectionRpcError {
    /// A `ConnectionError` occured.
    ConnectionError(ConnectionError),
    /// Received a packet containing invalid data.
    ///
    /// For example, the packet could have the wrong type, it could contain
    /// malformed json, it could set inappropriate flags, it could lack required
    /// json fields, etc.
    InvalidData,
}

impl Display for ConnectionRpcError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match *self {
            ConnectionRpcError::ConnectionError(ref err) => {
                write!(f, "Connection rpc error: {}", err)
            }
            ConnectionRpcError::InvalidData => write!(f, "Connection rpc error: Invalid data"),
        }
    }
}

impl Error for ConnectionRpcError {
    fn description(&self) -> &str {
        match *self {
            ConnectionRpcError::ConnectionError(ref err) => err.description(),
            ConnectionRpcError::InvalidData => "Received a packet that contained invalid data",
        }
    }
}

impl From<ConnectionError> for ConnectionRpcError {
    fn from(err: ConnectionError) -> ConnectionRpcError {
        ConnectionRpcError::ConnectionError(err)
    }
}

impl From<SerdeError> for ConnectionRpcError {
    fn from(err: SerdeError) -> ConnectionRpcError {
        ConnectionRpcError::InvalidData
    }
}
