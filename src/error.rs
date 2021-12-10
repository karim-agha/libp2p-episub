use libp2p::{multiaddr, multihash};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EpisubHandlerError {
  #[error("Exceeded maximum transmission size")]
  MaxTransmissionSize,

  /// IO error.
  #[error("IO Error: {0}")]
  Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum SubscriptionError {
  #[error("Couldn't publish our subscription: {0}")]
  PublishError(#[from] PublishError),
}

/// Error associated with publishing a gossipsub message.
#[derive(Debug, Error)]
pub enum PublishError {
  #[error("The compression algorithm failed: {0}")]
  TransformFailed(#[from] std::io::Error),
}

/// Errors associated with converting values from
/// wire format to internal represenation
#[derive(Debug, Error)]
pub enum FormatError {
  #[error("Invalid multihash: {0}")]
  Multihash(#[from] multihash::Error),

  #[error("Invalid multiaddress: {0}")]
  Multiaddr(#[from] multiaddr::Error),
}
