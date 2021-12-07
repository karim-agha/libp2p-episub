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
pub enum MeshError {
  #[error("Cannot find peer in the partial view")]
  PeerNotFound,
}

#[derive(Debug, Error)]
pub enum SubscriptionError {
  #[error("Couldn't publish our subscription: {0}")]
  PublishError(#[from] PublishError),

  #[error("Not allowed to subscribe to this topic by the subscription filter")]
  NotAllowed,
}

/// Error associated with publishing a gossipsub message.
#[derive(Debug, Error)]
pub enum PublishError {
  #[error("This message has already been published.")]
  Duplicate,

  #[error("There were no peers to send this message to.")]
  InsufficientPeers,

  #[error("The overall message was too large.")]
  MessageTooLarge,

  #[error("The compression algorithm failed: {0}")]
  TransformFailed(#[from] std::io::Error),
}
