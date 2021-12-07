use crate::{
  error::{PublishError, SubscriptionError},
  handler::{EpisubHandler, HandlerEvent},
  rpc,
  view::HyParView,
};
use libp2p::{
  core::{connection::ConnectionId, ConnectedPoint, Multiaddr, PeerId},
  swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, PollParameters,
  },
};
use std::{
  collections::{HashMap, VecDeque},
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

type EpisubNetworkBehaviourAction =
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

  /// events that need to yielded to the outside when polling
  events: VecDeque<EpisubNetworkBehaviourAction>,
}

impl Episub {
  pub fn new() -> Self {
    Self {
      config: Config::default(),
      topics: HashMap::new(),
      events: VecDeque::new(),
    }
  }
}

impl Episub {
  /// Sends an rpc message to a connected peer.
  ///
  /// if the connection to the peer is dropped or otherwise the peer becomes
  /// unreachable, then this event is silently dropped.
  fn send_message(
    &mut self,
    peer_id: PeerId,
    message: rpc::Rpc,
  ) -> Result<(), PublishError> {
    Ok(
      self
        .events
        .push_back(NetworkBehaviourAction::NotifyHandler {
          peer_id,
          event: message,
          handler: NotifyHandler::Any,
        }),
    )
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
  /// When subscribing to a new topic, and the active and passive views are not full yet, then
  /// a JOIN(topic) message will be sent to any peer that is connecting or dialed by this node.
  pub fn subscribe(
    &mut self,
    topic: String,
  ) -> Result<bool, SubscriptionError> {
    if self.topics.get(&topic).is_some() {
      debug!("Already subscribed to topic {}", topic);
      Ok(false)
    } else {
      debug!("Subscribing to topic: {}", topic);
      self.topics.insert(topic, HyParView::default());
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
          )?;
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
    event: HandlerEvent,
  ) {
    debug!(
      "inject_event, peerid: {}, connection: {:?}, event: {:?}",
      peer_id, connection, event
    );
    todo!()
  }

  fn poll(
    &mut self,
    cx: &mut Context<'_>,
    params: &mut impl PollParameters,
  ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>> {

    // bubble up any outstanding events in fifo order
    if let Some(event) = self.events.pop_front() {
      return Poll::Ready(event);
    }

    Poll::Pending
  }
}
