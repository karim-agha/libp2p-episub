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
use rand::prelude::IteratorRandom;
use std::{
  collections::{HashSet, VecDeque},
  pin::Pin,
  task::{Context, Poll},
  time::Instant,
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
  local_node: AddressablePeer,
  active: HashSet<AddressablePeer>,
  passive: HashSet<AddressablePeer>,

  // timestamp of the last outgoing periodic
  // passive peer shuffle operation.
  last_shuffle: Instant,

  /// events that need to be yielded to the outside when polling
  out_events: VecDeque<EpisubNetworkBehaviourAction>,
}

/// Access to Partial View network overlays
impl HyParView {
  pub fn new(topic: String, config: Config, local: AddressablePeer) -> Self {
    Self {
      config,
      topic,
      last_shuffle: Instant::now(),
      active: HashSet::new(),
      passive: HashSet::new(),
      out_events: VecDeque::new(),
      local_node: local,
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

  pub fn is_empty(&self) -> bool {
    self.active.is_empty()
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
  fn free_up_active_slot(&mut self) {
    if self.active.len() >= self.max_active_view_size() {
      let dropped = self.active.drain().next();
      if let Some(dropped) = dropped {
        debug!(
          "Moving peer {} from active view to passive.",
          dropped.peer_id
        );
        self.out_events.push_back(
          EpisubNetworkBehaviourAction::NotifyHandler {
            peer_id: dropped.peer_id,
            handler: NotifyHandler::Any,
            event: rpc::Rpc {
              topic: self.topic.clone(),
              action: Some(rpc::rpc::Action::Disconnect(rpc::Disconnect {
                alive: true,
              })),
            },
          },
        );
        self.add_node_to_passive_view(dropped);
      }
    }
  }
}

/// public handlers of HyParView protocol control messages
impl HyParView {
  pub fn join(&mut self, peer: AddressablePeer, ttl: u32) {
    if self.active.contains(&peer) {
      return;
    }

    // if we're full, free up a slot and move a node to passive.
    // JOIN is a high-priority request, that introduces a node
    // to the cluster and must be handled with priority.
    self.free_up_active_slot();

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

    // notify the peer that we have accepted their join request, so
    // it can add us to its active view, and in case it is the first
    // join request on a pending topic, it moves to the fully joined
    // state
    self.add_node_to_active_view(peer.clone(), true);
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
      self.add_node_to_active_view(peer.clone(), true);
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

  pub fn neighbor(&mut self, peer: AddressablePeer, priority: i32) {
    debug!("hyparview: neighbor");
    if self.active.len() < self.max_active_view_size() {
      self.add_node_to_active_view(peer, false);
    } else {
      // high priority NEIGHBOR messages are sent by nodes
      // that have zero active peers, in that case we make
      // space for them by moving one of the active peers
      // to the passive view and accepting their request.
      if priority == rpc::neighbor::Priority::High.into() {
        self.free_up_active_slot();
        self.add_node_to_active_view(peer, false);
      } else {
        // This is a low-priority neighbor request and we are
        // at capacity for active peers, send back a disconnect
        // message and place the peer in passive view.
        self.disconnect(peer.peer_id, true);
      }
    }
  }

  /// Handles DISCONNECT message, if the parameter 'alive' is true, then
  /// the peer is moved from active view to passive view, otherwise it is
  /// remove completely from all views.
  ///
  /// Also invoked by the behaviour when a connection with a peer is dropped.
  /// This ensures that dropped connections on not remain in the active view.
  pub fn disconnect(&mut self, peer: PeerId, alive: bool) {
    debug!("disconnecting peer {}, from passive: {}", peer, alive);
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

    // optionally, see if we need to topup our active connections
    self.maybe_move_random_passive_to_active();
  }

  /// When we receive a shuffle message from one of our neighbors,
  /// we merge both lists of passive nodes, dedupe them, shuffle them
  /// and then drop random nodes that are over the max limit of the
  /// passive view size. This way we refresh our passive view with
  /// nodes that we didn't contact directly, but our neighbors did.
  pub fn shuffle(
    &mut self,
    from: PeerId,
    mut params: rpc::Shuffle,
    origin: AddressablePeer,
  ) {
    let origin_peer_id = origin.peer_id;
    let mut deduped_passive: HashSet<AddressablePeer> = params
      .passive
      .iter()
      .chain(params.active.iter())
      .filter_map(|p| p.clone().try_into().ok())
      .collect();

    // respond to the originator of the shuffle with the unique
    // nodes that we know about and they don't know about.
    self.send_shuffle_reply(
      origin,
      self
        .active
        .union(&self.passive)
        .cloned()
        .collect::<HashSet<_>>()
        .difference(&deduped_passive)
        .cloned()
        .take(self.config.shuffle_max_size)
        .collect(),
    );

    // this is the unique list of peers that the remote node knows about
    // and we don't know about.
    deduped_passive.retain(|n| {
      !self.active.contains(n)
        && !self.passive.contains(n)
        && n.peer_id != self.local_node.peer_id
    });

    // append the newly added unique peers to our passive view
    deduped_passive.iter().for_each(|p| {
      self.add_node_to_passive_view(p.clone());
    });

    // if we are blow the desired number of active nodes,
    // and we have just learned about new passive nodes,
    // try to topup the active nodes
    self.maybe_move_random_passive_to_active();

    // The protocol mandates that the shuffle should be forwarded to
    // all active peers except the sender, as long as ttl is not zero.
    if params.ttl != 0 {
      params.ttl -= 1;
      for peer in self.active.iter() {
        // forward this shuffle request to all active peers except the
        // original initiator of the shuffle and the immediate peer that
        // forwarded this message to us
        if peer.peer_id != from && origin_peer_id != peer.peer_id {
          self.out_events.push_back(
            EpisubNetworkBehaviourAction::NotifyHandler {
              peer_id: peer.peer_id,
              handler: NotifyHandler::Any,
              event: rpc::Rpc {
                topic: self.topic.clone(),
                action: Some(rpc::rpc::Action::Shuffle(params.clone())),
              },
            },
          );
        }
      }
    }
  }

  pub fn shuffle_reply(&mut self, peer: PeerId, params: rpc::ShuffleReply) {
    let nodes = params
      .nodes
      .into_iter()
      .filter_map(|n| n.try_into().ok())
      .collect();

    // merge both lists of passive peers
    self.passive = self.passive.union(&nodes).cloned().collect();

    // and remove any excess nodes that put us
    // above the maximum passive view size.
    while self.passive.len() > self.max_passive_view_size() {
      self.passive.drain().next();
    }

    // then optionally disconnect from this peer if it
    // is not in the active peer list.
    if !self.active.iter().any(|p| p.peer_id == peer) {
      self.out_events.push_back(
        EpisubNetworkBehaviourAction::CloseConnection {
          peer_id: peer,
          connection: libp2p::swarm::CloseConnection::All,
        },
      );
    }
  }

  pub fn message(&mut self, _peer: PeerId, _data: Vec<u8>) {
    debug!("hyparview: message");
  }
}

impl HyParView {
  fn add_node_to_active_view(
    &mut self,
    node: AddressablePeer,
    initiator: bool,
  ) {
    if self.active.insert(node.clone()) {
      debug!("Adding peer to active view: {:?}", node);
      if initiator {
        self
          .out_events
          .push_back(EpisubNetworkBehaviourAction::Dial {
            opts: DialOpts::peer_id(node.peer_id)
              .addresses(node.addresses.into_iter().collect())
              .condition(PeerCondition::Disconnected)
              .build(),
            handler: EpisubHandler::new(self.config.max_transmit_size),
          });

        self.out_events.push_back(
          EpisubNetworkBehaviourAction::NotifyHandler {
            peer_id: node.peer_id,
            handler: NotifyHandler::Any,
            event: rpc::Rpc {
              topic: self.topic.clone(),
              action: Some(rpc::rpc::Action::Neighbor(rpc::Neighbor {
                peer: self.local_node.clone().into(),
                priority: match self.active.is_empty() {
                  true => rpc::neighbor::Priority::High.into(),
                  false => rpc::neighbor::Priority::Low.into(),
                },
              })),
            },
          },
        );
      }

      self
        .out_events
        .push_back(EpisubNetworkBehaviourAction::GenerateEvent(
          EpisubEvent::ActivePeerAdded(node.peer_id),
        ));
    }
  }

  fn add_node_to_passive_view(&mut self, node: AddressablePeer) {
    if node.peer_id != self.local_node.peer_id {
      debug!("Adding peer to passive view: {:?}", node);
      self.passive.insert(node);
      if self.passive.len() > self.max_passive_view_size() {
        if let Some(random) =
          self.passive().choose(&mut rand::thread_rng()).cloned()
        {
          self.passive.remove(&random);
        }
      }
    }
  }

  /// Every `shuffle_duration` we boradcast to all our active peers
  /// a random sample of peers that we know about with active random walk length
  /// equal to the ttl of the JOIN request.
  fn send_shuffle(&mut self, peer: PeerId) {
    self
      .out_events
      .push_back(EpisubNetworkBehaviourAction::NotifyHandler {
        peer_id: peer,
        handler: NotifyHandler::Any,
        event: rpc::Rpc {
          topic: self.topic.clone(),
          action: Some(rpc::rpc::Action::Shuffle(rpc::Shuffle {
            ttl: self.config.active_walk_length as u32,
            origin: self.local_node.clone().into(),
            active: self
              .active()
              .choose_multiple(
                &mut rand::thread_rng(),
                self.config.shuffle_max_size,
              )
              .iter()
              .cloned()
              .map(|a| a.into())
              .collect(),
            passive: self
              .passive()
              .choose_multiple(
                &mut rand::thread_rng(),
                self.config.shuffle_max_size,
              )
              .iter()
              .cloned()
              .map(|p| p.into())
              .collect(),
          })),
        },
      });
  }

  fn send_shuffle_reply(
    &mut self,
    origin: AddressablePeer,
    nodes: Vec<AddressablePeer>,
  ) {
    // first establish a temporary connection with the origin peer
    // if it is not established already
    self
      .out_events
      .push_back(EpisubNetworkBehaviourAction::Dial {
        opts: DialOpts::peer_id(origin.peer_id)
          .addresses(origin.addresses.into_iter().collect())
          .condition(PeerCondition::Disconnected)
          .build(),
        handler: EpisubHandler::new(self.config.max_transmit_size),
      });

    // then send them a message
    self
      .out_events
      .push_back(EpisubNetworkBehaviourAction::NotifyHandler {
        peer_id: origin.peer_id,
        handler: NotifyHandler::Any,
        event: rpc::Rpc {
          topic: self.topic.clone(),
          action: Some(rpc::rpc::Action::ShuffleReply(rpc::ShuffleReply {
            nodes: nodes.into_iter().map(|n| n.into()).collect(),
          })),
        },
      });
  }

  fn maybe_move_random_passive_to_active(&mut self) {
    while self.active.len() < self.max_active_view_size() {
      let random = self.passive.iter().choose(&mut rand::thread_rng()).cloned();
      if let Some(random) = random {
        self.add_node_to_active_view(random, true);
      } else {
        break; // we're out of passive nodes..
      }
    }
  }
}

impl Future for HyParView {
  type Output = EpisubNetworkBehaviourAction;
  fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
    // HyParView protocol has the concept of a shuffle, that exchanges
    // periodically info about known peers between nodes, to make sure
    // that the passive view of each node has as many good backup nodes
    // as possible.
    if Instant::now().duration_since(self.last_shuffle)
      > self.config.shuffle_interval
    {
      if let Some(random) =
        self.active().choose(&mut rand::thread_rng()).cloned()
      {
        self.send_shuffle(random.peer_id);
        self.last_shuffle = Instant::now();
      }
    }

    if let Some(event) = self.out_events.pop_front() {
      return Poll::Ready(event);
    }

    Poll::Pending
  }
}

/// This struct uniquely identifies a node on the internet.
#[derive(Debug, Clone)]
pub struct AddressablePeer {
  pub peer_id: PeerId,
  pub addresses: HashSet<Multiaddr>,
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

impl Into<rpc::AddressablePeer> for &AddressablePeer {
  fn into(self) -> rpc::AddressablePeer {
    rpc::AddressablePeer {
      peer_id: self.peer_id.to_bytes(),
      addresses: self.addresses.iter().map(|a| a.to_vec()).collect(),
    }
  }
}
