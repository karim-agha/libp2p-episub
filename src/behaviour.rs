use crate::{
  error::SubscriptionError, handler::EpisubHandler, rpc, view::HyParView,
};
use futures::Future;
use libp2p::{
  core::{connection::ConnectionId, ConnectedPoint, Multiaddr, PeerId},
  swarm::{
    CloseConnection, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler,
    PollParameters,
  },
};
use std::{
  collections::{HashMap, HashSet, VecDeque},
  pin::Pin,
  task::{Context, Poll},
  time::Duration,
};
use tracing::{debug, trace, warn};

/// Configuration paramaters for Episub
#[derive(Debug, Clone)]
pub struct Config {
  /// Estimated number of online nodes joining one topic
  pub network_size: usize,
  pub active_view_factor: usize,
  pub passive_view_factor: usize,
  pub max_transmit_size: usize,
  pub active_walk_length: usize,
  pub passive_walk_length: usize,
  pub shuffle_interval: Duration,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      network_size: 10_000,
      active_view_factor: 1,
      passive_view_factor: 5,
      active_walk_length: 3,
      passive_walk_length: 4,
      shuffle_interval: Duration::from_secs(60),
      max_transmit_size: 1024 * 1024 * 100,
    }
  }
}

/// Event that can be emitted by the episub behaviour.
#[derive(Debug)]
pub enum EpisubEvent {
  Message,
  Subscribed,
  Unsubscibed,
  EpisubNotSupported,
}

pub(crate) type EpisubNetworkBehaviourAction =
  NetworkBehaviourAction<EpisubEvent, EpisubHandler, rpc::Rpc>;

/// Network behaviour that handles the Episub protocol.
///
/// This network behaviour combines three academic papers into one implementation:
///   1. HyParView: For topic-peer-membership management and node discovery
///   2. Epidemic Broadcast Trees: For constructing efficient broadcast trees and efficient content dessamination
///   3. GoCast: Gossip-Enhanced Overlay Multicast for Fast and Dependable Group Communication
///
pub struct Episub {
  /// Global behaviour configuration
  config: Config,

  /// Per-topic node membership
  topics: HashMap<String, HyParView>,

  /// A list of peers that have violated the protocol
  /// and are pemanently banned from this node. All their
  /// communication will be ignored and connections rejected.
  banned_peers: HashSet<PeerId>,

  /// Topics that we want to join, but haven't found a node
  /// to connect to.
  pending_joins: HashSet<String>,

  /// events that need to yielded to the outside when polling
  out_events: VecDeque<EpisubNetworkBehaviourAction>,
}

impl Episub {
  pub fn new() -> Self {
    Self {
      config: Config::default(),
      topics: HashMap::new(),
      banned_peers: HashSet::new(),
      pending_joins: HashSet::new(),
      out_events: VecDeque::new(),
    }
  }
}

impl Episub {
  /// Sends an rpc message to a connected peer.
  ///
  /// if the connection to the peer is dropped or otherwise the peer becomes
  /// unreachable, then this event is silently dropped.
  fn send_message(&mut self, peer_id: PeerId, message: rpc::Rpc) {
    self
      .out_events
      .push_back(NetworkBehaviourAction::NotifyHandler {
        peer_id,
        event: message,
        handler: NotifyHandler::Any,
      })
  }
}

impl Episub {
  /// Subscribes to a gossip topic.
  ///
  /// Gossip topics are isolated clusters of nodes that gossip information, each topic
  /// maintins a separate HyParView of topic member nodes and does not interact with
  /// nodes from other topics (unless two nodes are subscribed to the same topic, but
  /// even then, they are not aware of the dual subscription).
  ///
  /// The subscription process is a long-living and dynamic process that has no end, and
  /// there is no point in time where we can decide that subscription is complete. What
  /// subscribing to a topic means, is that we will not ignore messages from peers that
  /// are sent for this topic id, this includes HyParView control messages or data messages.
  ///
  /// When subscribing to a new topic, we place the topic in the pending_joins collection that
  /// will send a join request to any node we connect to, until one of the nodes responds with
  /// another JOIN or FORWARDJOIN message.
  pub fn subscribe(
    &mut self,
    topic: String,
  ) -> Result<bool, SubscriptionError> {
    if self.topics.get(&topic).is_some() {
      debug!("Already subscribed to topic {}", topic);
      Ok(false)
    } else {
      debug!("Subscribing to topic: {}", topic);
      self.topics.insert(topic.clone(), HyParView::default());
      self.pending_joins.insert(topic);
      Ok(true)
    }
  }

  /// Graceful removal from cluster.
  ///
  /// Stops responding to messages sent to this topic and informs
  /// all peers in the active and passive views that we are withdrawing
  /// from the cluster.
  pub fn unsubscibe(
    &mut self,
    topic: String,
  ) -> Result<bool, SubscriptionError> {
    if self.topics.get(&topic).is_none() {
      warn!(
        "Attempt to unsubscribe from a non-subscribed topic {}",
        topic
      );
      Ok(false)
    } else {
      debug!("unsubscribing from topic: {}", topic);
      self.pending_joins.remove(&topic);
      if let Some(view) = self.topics.remove(&topic) {
        for peer in view.all_peers_id() {
          trace!("disconnecting from peer {} on topic {}", peer, topic);
          self.send_message(
            *peer,
            rpc::Rpc {
              topic: topic.clone(),
              action: Some(rpc::rpc::Action::Disconnect(rpc::Disconnect {
                alive: false, // remove from peers passive view as well
              })),
            },
          );
        }
      }

      Ok(true)
    }
  }
}

impl NetworkBehaviour for Episub {
  type ProtocolsHandler = EpisubHandler;
  type OutEvent = EpisubEvent;

  fn new_handler(&mut self) -> Self::ProtocolsHandler {
    debug!("creating new handler");
    EpisubHandler::new(self.config.max_transmit_size)
  }

  fn inject_connection_established(
    &mut self,
    peer_id: &PeerId,
    _connection_id: &ConnectionId,
    endpoint: &ConnectedPoint,
    _failed_addresses: Option<&Vec<Multiaddr>>,
  ) {
    debug!(
      "Connection to peer {} established on endpoint {:?}",
      peer_id, endpoint
    );

    if self.banned_peers.contains(peer_id) {
      self.force_disconnect(*peer_id);
      return;
    }

    // if this is us dialing a node, usually one of bootstrap nodes
    if matches!(endpoint, ConnectedPoint::Dialer { .. }) {
      // check if we are in the process of joining a topic, if so, for
      // each topic that has not found its cluster, send a join request
      // to every node that connects with us.
      self.pending_joins.clone().into_iter().for_each(|topic| {
        self.send_message(
          peer_id.clone(),
          rpc::Rpc {
            topic: topic.clone(),
            action: Some(rpc::rpc::Action::Join(rpc::Join {
              ttl: self.config.active_walk_length as u32,
            })),
          },
        );
      });
    }
  }

  fn inject_connection_closed(
    &mut self,
    peer_id: &PeerId,
    _: &ConnectionId,
    endpoint: &ConnectedPoint,
    _: EpisubHandler,
  ) {
    debug!(
      "Connection to peer {} closed on endpoint {:?}",
      peer_id, endpoint
    );
  }

  fn inject_event(
    &mut self,
    peer_id: PeerId,
    connection: ConnectionId,
    event: rpc::Rpc,
  ) {
    if self.banned_peers.contains(&peer_id) {
      debug!(
        "rejecting event from a banned peer {}: {:?}",
        peer_id, event
      );
      self.force_disconnect(peer_id);
      return;
    }

    debug!(
      "inject_event, peerid: {}, connection: {:?}, event: {:?}",
      peer_id, connection, event
    );

    if event.action.is_none() {
      self.ban_peer(peer_id); // peer is violating the protocol
      return;
    }

    if self.pending_joins.contains(&event.topic) {
      self.handle_first_join(event.clone());
    }

    if let Some(view) = self.topics.get_mut(&event.topic) {
      // handle rpc call on an established topic.
      use rpc::rpc::Action;
      match event.action.unwrap() {
        Action::Join(rpc::Join { ttl }) => {
          view.join(peer_id, ttl);
        }
        Action::ForwardJoin(params) => {
          view.forward_join(peer_id, params);
        }
        Action::Neighbor(rpc::Neighbor { priority }) => {
          view.neighbor(peer_id, priority);
        }
        Action::Disconnect(rpc::Disconnect { alive }) => {
          view.disconnect(peer_id, alive);
        }
        Action::Shuffle(params) => {
          view.shuffle(peer_id, params);
        }
        Action::ShuffleReply(params) => {
          view.shuffle_reply(peer_id, params);
        }
        Action::Message(rpc::Message { data }) => {
          view.message(peer_id, data);
        }
      }
    }
  }

  fn poll(
    &mut self,
    cx: &mut Context<'_>,
    _: &mut impl PollParameters,
  ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>> {
    // bubble up any outstanding behaviour-level events in fifo order
    if let Some(event) = self.out_events.pop_front() {
      return Poll::Ready(event);
    }

    // next bubble up events for all topics
    // todo, randomize polling among topics, otherwise
    // some topics might be starved
    for view in self.topics.values_mut() {
      let pinned = Pin::new(view);
      if let Poll::Ready(event) = pinned.poll(cx) {
        return Poll::Ready(event);
      }
    }

    Poll::Pending
  }
}

impl Episub {
  /// Handles the first JOIN request to a topic that we are trying to subscribe to.
  /// Once we have at least one other node in the topic active set, se will stop
  /// proactively sending JOIN requests to any dialed peer.
  fn handle_first_join(&mut self, event: rpc::Rpc) {
    if let Some(rpc::rpc::Action::Join(rpc::Join { .. })) = event.action {
      self.pending_joins.remove(&event.topic);
      self.topics.insert(event.topic, HyParView::default());
    }
  }

  fn ban_peer(&mut self, peer: PeerId) {
    warn!("Banning peer {}", peer);
    self.banned_peers.insert(peer);
    self.force_disconnect(peer);
  }

  fn force_disconnect(&mut self, peer: PeerId) {
    self
      .out_events
      .push_back(EpisubNetworkBehaviourAction::CloseConnection {
        peer_id: peer,
        connection: CloseConnection::All,
      });
  }
}
