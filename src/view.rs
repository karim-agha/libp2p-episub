//! [HyParView]: a membership protocol for reliable gossip-based broadcast
//! [HyParView]: http://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf

use crate::{
  behaviour::EpisubNetworkBehaviourAction, error::FormatError, rpc, Config,
};
use futures::Future;
use libp2p::{
  core::{Connected, ConnectedPoint, Multiaddr, PeerId},
  swarm::NotifyHandler,
};
use std::{
  collections::{HashSet, VecDeque},
  pin::Pin,
  task::{Context, Poll},
};
use tracing::debug;

/// A partial view is a set of node identiﬁers maintained locally at each node that is a small
/// subset of the identiﬁers of all nodes in the system (ideally, of logarithmic size with the
/// number of processes in the system).
///
/// An identiﬁer is a [`Multiaddr`] that allows a node to be reached. The membership protocol
/// is in charge of initializing and maintaining the partial views at each node in face of dynamic
/// changes in the system. For instance, when a new node joins the system, its identiﬁer should
/// be added to the partial view of (some) other nodes and it has to create its own partial view,
/// including identiﬁers of nodes already in the system.
///
/// Also, if a node fails or leaves the system, its identiﬁer should be removed from all partial
/// views as soon as possible.
///
/// Partial views establish neighboring associations among nodes. Therefore, partial views deﬁne
/// an overlay network, in other words, partial views establish an directed graph that captures
/// the neighbor relation between all nodes executing the protocol.
///
/// The HyParView protocol maintains two distinct views at each node:
///   - a small active view, of size log(n) + c,
///   - and a larger passive view, of size k(log(n) + c).
/// where n is the total number of online nodes participating in the protocol.
pub struct HyParView {
  config: Config,
  topic: String,
  active: HashSet<AddressablePeer>,
  passive: HashSet<AddressablePeer>,

  /// events that need to yielded to the outside when polling
  out_events: VecDeque<EpisubNetworkBehaviourAction>,
}

/// Access to Partial View network overlays
impl HyParView {
  pub fn new(topic: String, config: Config) -> Self {
    Self {
      config,
      topic,
      active: HashSet::new(),
      passive: HashSet::new(),
      out_events: VecDeque::new(),
    }
  }
  /// The active views of all nodes create an overlay that is used for message dissemination.
  /// Links in the overlay are symmetric, this means that each node keeps an open TCP connection
  /// to every other node in its active view.
  ///
  /// The active view is maintained using a reactive strategy, meaning nodes are remove
  /// when they fail.
  pub fn active(&self) -> impl Iterator<Item = &AddressablePeer> {
    self.active.iter()
  }

  /// The goal of the passive view is to maintain a list of nodes that can be used to
  /// replace failed members of the active view. The passive view is not used for message
  /// dissemination.
  ///
  /// The passive view is maintained using a cyclic strategy. Periodically, each node
  /// performs shuffle operation with one of its neighbors in order to update its passive view.
  pub fn passive(&self) -> impl Iterator<Item = &AddressablePeer> {
    self.passive.iter()
  }

  pub fn all_peers_id(&self) -> impl Iterator<Item = &PeerId> {
    self
      .active()
      .map(|ap| &ap.peer_id)
      .chain(self.passive().map(|p| &p.peer_id))
  }
}

impl HyParView {
  fn max_active_view_size(&self) -> usize {
    ((self.config.network_size as f64).log2()
      + self.config.active_view_factor as f64)
      .round() as usize
  }

  fn max_passive_view_size(&self) -> usize {
    ((self.config.network_size as f64).log2()
      * self.config.passive_view_factor as f64)
      .round() as usize
  }

  /// This is invoked when a node sends us a JOIN request,
  /// it will see if the active view is full, and if so, removes
  /// a random node from the active view and makes space for the new
  /// node.
  fn free_up_active_slot(&mut self) -> Option<AddressablePeer> {
    if self.active.len() >= self.max_active_view_size() {
      Some(self.active.drain().next().unwrap())
    } else {
      None
    }
  }
}

/// public handlers of HyParView protocol control messages
impl HyParView {
  pub fn join(&mut self, peer: AddressablePeer, ttl: u32) {
    debug!("hyparview: join");
    
    if self.active.contains(&peer) {
      return;
    }

    if let Some(dropped) = self.free_up_active_slot() {
      self
        .out_events
        .push_back(EpisubNetworkBehaviourAction::NotifyHandler {
          peer_id: dropped.peer_id,
          handler: NotifyHandler::Any,
          event: rpc::Rpc {
            topic: self.topic.clone(),
            action: Some(rpc::rpc::Action::Disconnect(rpc::Disconnect {
              alive: true,
            })),
          },
        });
      self.passive.insert(dropped);
    }

    self.active.insert(peer.clone());

    for active_peer in self.active.iter().collect::<Vec<_>>() {
      self
        .out_events
        .push_back(EpisubNetworkBehaviourAction::NotifyHandler {
          peer_id: active_peer.peer_id,
          handler: NotifyHandler::Any,
          event: rpc::Rpc {
            topic: self.topic.clone(),
            action: Some(rpc::rpc::Action::ForwardJoin(rpc::ForwardJoin {
              peer: peer.clone().into(),
              ttl,
            })),
          },
        });
    }
  }

  pub fn forward_join(&mut self, from: PeerId, params: rpc::ForwardJoin) {
    debug!("hyparview: forward_join");
  }

  pub fn neighbor(&mut self, peer: PeerId, priority: i32) {
    debug!("hyparview: neighbor");
  }

  pub fn disconnect(&mut self, peer: PeerId, alive: bool) {
    debug!("hyparview: disconnect");
  }

  pub fn shuffle(&mut self, peer: PeerId, params: rpc::Shuffle) {
    debug!("hyparview: shuffle");
  }

  pub fn shuffle_reply(&mut self, peer: PeerId, params: rpc::ShuffleReply) {
    debug!("hyparview: shuffle_reply");
  }

  pub fn message(&mut self, peer: PeerId, data: Vec<u8>) {
    debug!("hyparview: message");
  }
}

impl Future for HyParView {
  type Output = EpisubNetworkBehaviourAction;
  fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
    if let Some(event) = self.out_events.pop_front() {
      Poll::Ready(event)
    } else {
      Poll::Pending
    }
  }
}

/// This struct uniquely identifies a node on the internet.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct AddressablePeer {
  pub peer_id: PeerId,
  pub address: Multiaddr,
}

impl From<&Connected> for AddressablePeer {
  fn from(c: &Connected) -> Self {
    Self {
      peer_id: c.peer_id,
      address: match &c.endpoint {
        ConnectedPoint::Dialer { address } => address.clone(),
        ConnectedPoint::Listener { send_back_addr, .. } => {
          send_back_addr.clone()
        }
      },
    }
  }
}

/// Conversion from wire format to internal representation
impl TryFrom<rpc::AddressablePeer> for AddressablePeer {
  type Error = FormatError;

  fn try_from(value: rpc::AddressablePeer) -> Result<Self, Self::Error> {
    Ok(Self {
      peer_id: PeerId::from_bytes(value.peer_id.as_slice())
        .map_err(FormatError::Multihash)?,
      address: Multiaddr::try_from(value.multiaddr)
        .map_err(FormatError::Multiaddr)?,
    })
  }
}

/// Conversion from internal representation to wire format
impl Into<rpc::AddressablePeer> for AddressablePeer {
  fn into(self) -> rpc::AddressablePeer {
    rpc::AddressablePeer {
      peer_id: self.peer_id.to_bytes(),
      multiaddr: self.address.to_vec(),
    }
  }
}
