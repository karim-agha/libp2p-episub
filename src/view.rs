//! [HyParView]: a membership protocol for reliable gossip-based broadcast
//! [HyParView]: http://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf

use crate::{
  behaviour::EpisubNetworkBehaviourAction, error::FormatError,
  handler::EpisubHandler, rpc, Config, EpisubEvent,
};
use futures::Future;
use libp2p::{
  core::{Multiaddr, PeerId},
  swarm::{
    dial_opts::{DialOpts, PeerCondition},
    NotifyHandler,
  },
};
use rand::prelude::SliceRandom;
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
  passive: VecDeque<AddressablePeer>,

  /// events that need to be yielded to the outside when polling
  out_events: VecDeque<EpisubNetworkBehaviourAction>,
}

/// Access to Partial View network overlays
impl HyParView {
  pub fn new(topic: String, config: Config) -> Self {
    Self {
      config,
      topic,
      active: HashSet::new(),
      passive: VecDeque::new(),
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
    if self.active.contains(&peer) {
      return;
    }

    if let Some(dropped) = self.free_up_active_slot() {
      debug!(
        "Moving peer {} from active view to passive.",
        dropped.peer_id
      );
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
      self.add_node_to_passive_view(dropped);
    }

    // notify the peer that we have accepted their join request, so
    // it can add us to its active view, and in case it is the first
    // join request on a pending topic, it moves to the fully joined
    // state
    self
      .out_events
      .push_back(EpisubNetworkBehaviourAction::NotifyHandler {
        peer_id: peer.peer_id,
        handler: NotifyHandler::Any,
        event: rpc::Rpc {
          topic: self.topic.clone(),
          action: Some(rpc::rpc::Action::JoinAccepted(rpc::JoinAccepted {
            peer: peer.clone().into(),
          })),
        },
      });

    // the send a forward-join to all members of our current active view
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

    self.add_node_to_active_view(peer);
  }

  pub fn join_accepted(&mut self, peer: AddressablePeer) {
    self.add_node_to_active_view(peer);
  }

  pub fn forward_join(
    &mut self,
    peer: AddressablePeer,
    ttl: usize,
    local_node: PeerId,
    sender: PeerId,
  ) {
    if peer.peer_id == local_node {
      debug!("ignoring cyclic forward join from this node");
      return;
    }

    if ttl == 0 {
      return;
    }

    if self.active.len() < self.max_active_view_size() {
      self.add_node_to_active_view(peer.clone());
    } else {
      self.add_node_to_passive_view(peer.clone());
    }

    for n in &self.active {
      if n.peer_id == sender || n.peer_id == peer.peer_id {
        continue;
      }
      self
        .out_events
        .push_back(EpisubNetworkBehaviourAction::NotifyHandler {
          peer_id: n.peer_id,
          handler: NotifyHandler::Any,
          event: rpc::Rpc {
            topic: self.topic.clone(),
            action: Some(rpc::rpc::Action::ForwardJoin(rpc::ForwardJoin {
              peer: peer.clone().into(),
              ttl: (ttl - 1) as u32,
            })),
          },
        });
    }
  }

  pub fn neighbor(&mut self, _peer: PeerId, _priority: i32) {
    debug!("hyparview: neighbor");
  }

  /// Handles DISCONNECT message, if the parameter 'alive' is true, then
  /// the peer is moved from active view to passive view, otherwise it is
  /// remove completely from all views.
  ///
  /// Also invoked by the behaviour when a connection with a peer is dropped.
  /// This ensures that dropped connections on not remain in the active view.
  pub fn disconnect(&mut self, peer: PeerId, alive: bool) {
    debug!("hyparview: disconnect");
    if alive {
      let found: Vec<_> = self
        .active()
        .filter(|p| p.peer_id == peer)
        .cloned()
        .collect();
      found
        .into_iter()
        .for_each(|p| self.add_node_to_passive_view(p));
    } else {
      self.passive.retain(|p| p.peer_id != peer);
    }

    self.active.retain(|p| p.peer_id != peer);
    self
      .out_events
      .push_back(EpisubNetworkBehaviourAction::GenerateEvent(
        EpisubEvent::ActivePeerRemoved(peer),
      ));

    if self.active.len() < self.max_active_view_size() {
      self
        .passive
        .make_contiguous()
        .shuffle(&mut rand::thread_rng());
      let random = self.passive.pop_front();
      if let Some(random) = random {
        self.join(random, self.config.active_walk_length as u32);
      }
    }
  }

  pub fn shuffle(&mut self, _peer: PeerId, _params: rpc::Shuffle) {
    debug!("hyparview: shuffle");
  }

  pub fn shuffle_reply(&mut self, _peer: PeerId, _params: rpc::ShuffleReply) {
    debug!("hyparview: shuffle_reply");
  }

  pub fn message(&mut self, _peer: PeerId, _data: Vec<u8>) {
    debug!("hyparview: message");
  }
}

impl HyParView {
  fn add_node_to_active_view(&mut self, node: AddressablePeer) {
    if self.active.insert(node.clone()) {
      debug!("Adding peer to active view: {:?}", node);
      self
        .out_events
        .push_back(EpisubNetworkBehaviourAction::Dial {
          opts: DialOpts::peer_id(node.peer_id)
            .addresses(node.addresses)
            .condition(PeerCondition::Disconnected)
            .build(),
          handler: EpisubHandler::new(self.config.max_transmit_size),
        });

      self
        .out_events
        .push_back(EpisubNetworkBehaviourAction::GenerateEvent(
          EpisubEvent::ActivePeerAdded(node.peer_id),
        ));
    }
  }

  fn add_node_to_passive_view(&mut self, node: AddressablePeer) {
    debug!("Adding peer to passive view: {:?}", node);
    self.passive.push_back(node);
    if self.passive.len() > self.max_passive_view_size() {
      self
        .passive
        .make_contiguous()
        .shuffle(&mut rand::thread_rng());
      self.passive.pop_front();
    }
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
#[derive(Debug, Clone)]
pub struct AddressablePeer {
  pub peer_id: PeerId,
  pub addresses: Vec<Multiaddr>,
}

impl PartialEq for AddressablePeer {
  fn eq(&self, other: &Self) -> bool {
    self.peer_id == other.peer_id
  }
}

impl Eq for AddressablePeer {}

impl std::hash::Hash for AddressablePeer {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.peer_id.hash(state);
  }
}

/// Conversion from wire format to internal representation
impl TryFrom<rpc::AddressablePeer> for AddressablePeer {
  type Error = FormatError;

  fn try_from(value: rpc::AddressablePeer) -> Result<Self, Self::Error> {
    Ok(Self {
      peer_id: PeerId::from_bytes(value.peer_id.as_slice())
        .map_err(FormatError::Multihash)?,
      addresses: value
        .addresses
        .into_iter()
        .filter_map(|a| Multiaddr::try_from(a).ok())
        .collect(),
    })
  }
}

/// Conversion from internal representation to wire format
impl Into<rpc::AddressablePeer> for AddressablePeer {
  fn into(self) -> rpc::AddressablePeer {
    rpc::AddressablePeer {
      peer_id: self.peer_id.to_bytes(),
      addresses: self.addresses.into_iter().map(|a| a.to_vec()).collect(),
    }
  }
}