mod behaviour;
mod codec;
mod error;
mod handler;
mod protocol;
mod rpc;

pub use behaviour::{Config, Episub, EpisubEvent};

use libp2p_core::PeerId;
use serde::Serialize;

#[derive(Debug, Copy, Clone, Serialize)]
pub enum NodeEvent {
  None,
  Up,
  Down,
  Connected,
  Disconnected,
  Graft,
  Prune,
  IHave,
  Message,
}

#[derive(Debug, Copy, Clone)]
#[repr(packed)]
pub struct NodeUpdate {
  pub node_id: PeerId,
  pub peer_id: PeerId,
  pub event: NodeEvent,
}

impl Default for NodeUpdate {
  fn default() -> Self {
    Self {
      node_id: PeerId::random(),
      peer_id: PeerId::random(),
      event: NodeEvent::None,
    }
  }
}
