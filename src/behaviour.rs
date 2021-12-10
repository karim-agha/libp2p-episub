use crate::{
  error::SubscriptionError,
  handler::EpisubHandler,
  rpc,
  view::{AddressablePeer, HyParView},
};
use futures::Future;
use libp2p::{
  core::{
    connection::{ConnectionId, ListenerId},
    ConnectedPoint, Multiaddr, PeerId,
  },
  multiaddr::Protocol,
  swarm::{
    CloseConnection, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler,
    PollParameters,
  },
};
use std::{
  collections::{HashMap, HashSet, VecDeque},
  net::{Ipv4Addr, Ipv6Addr},
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
      network_size: 1000,
      active_view_factor: 1,
      passive_view_factor: 5,
      active_walk_length: 3,
      passive_walk_length: 2,
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

  /// Identity of this node
  local_node: Option<AddressablePeer>,

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

  /// a mapping of known peerid to the addresses they have dialed us from
  peer_addresses: HashMap<PeerId, Multiaddr>,

  /// This is the set of peers that we have managed to dial before we started
  /// listening on an external address that is not localhost. It does not make
  /// sense to send a JOIN message to peers without telling them where are we
  /// listening, so in cases when a connection is established, but we didn't
  /// get any valid address in local_node.addresses, we keep track of those
  /// peer id, and once we get a listen address, we send a join request to
  /// them.
  early_peers: HashSet<PeerId>,
}

impl Episub {
  pub fn new() -> Self {
    Self {
      local_node: None,
      config: Config::default(),
      topics: HashMap::new(),
      peer_addresses: HashMap::new(),
      banned_peers: HashSet::new(),
      pending_joins: HashSet::new(),
      out_events: VecDeque::new(),
      early_peers: HashSet::new(),
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
      self.topics.insert(
        topic.clone(),
        HyParView::new(topic.clone(), self.config.clone()),
      );
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

    // preserve a mapping from peer id to the address that was
    // used to establish the connection.
    self.peer_addresses.insert(
      *peer_id,
      match endpoint {
        ConnectedPoint::Dialer { address } => address.clone(),
        ConnectedPoint::Listener { send_back_addr, .. } => {
          send_back_addr.clone()
        }
      },
    );

    // if this is us dialing a node, usually one of bootstrap nodes
    if matches!(endpoint, ConnectedPoint::Dialer { .. }) {
      // check if we are in the process of joining a topic, if so, for
      // each topic that has not found its cluster, send a join request
      // to every node that accepts a connection from us.

      // make sure that we know who we are and how we can be reached.
      if self.local_node.is_some() {
        self.pending_joins.clone().into_iter().for_each(|topic| {
          self.send_message(
            peer_id.clone(),
            rpc::Rpc {
              topic: topic.clone(),
              action: Some(rpc::rpc::Action::Join(rpc::Join {
                ttl: self.config.active_walk_length as u32,
                peer: self.local_node.as_ref().unwrap().clone().into(),
              })),
            },
          );
        });
      } else {
        self.early_peers.insert(peer_id.clone());
      }
    }
  }

  fn inject_new_listen_addr(&mut self, _id: ListenerId, addr: &Multiaddr) {
    if !is_local_address(addr) {
      if let Some(node) = self.local_node.as_mut() {
        node.addresses.push(addr.clone());

        // attempt to send JOIN messages to all peers that we have connected
        // to before getting a local address assigned.
        let early_peers: Vec<PeerId> = self.early_peers.drain().collect();
        let pending_joins = self.pending_joins.clone();

        for topic in pending_joins.iter() {
          for peer in early_peers.iter() {
            self.send_message(
              peer.clone(),
              rpc::Rpc {
                topic: topic.clone(),
                action: Some(rpc::rpc::Action::Join(rpc::Join {
                  ttl: self.config.active_walk_length as u32,
                  peer: self.local_node.as_ref().unwrap().clone().into(),
                })),
              },
            );
          }
        }
      }
    }
  }

  fn inject_expired_listen_addr(&mut self, _id: ListenerId, addr: &Multiaddr) {
    if let Some(node) = self.local_node.as_mut() {
      node.addresses.retain(|a| a != addr);
    }
  }

  fn inject_dial_failure(
    &mut self,
    peer_id: Option<PeerId>,
    _handler: Self::ProtocolsHandler,
    error: &libp2p::swarm::DialError,
  ) {
    if let Some(peer_id) = peer_id {
      debug!("Dialing peer {} failed: {:?}", peer_id, error);

      for (_, view) in self.topics.iter_mut() {
        view.peer_disconnected(peer_id);
      }
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

    for (_, view) in self.topics.iter_mut() {
      view.peer_disconnected(*peer_id);
    }
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
      if self.handle_join_accepted(peer_id, event.clone()) {
        return;
      }
    }

    if let Some(view) = self.topics.get_mut(&event.topic) {
      // handle rpc call on an established topic.
      use rpc::rpc::Action;
      match event.action.unwrap() {
        Action::Join(rpc::Join { ttl, peer }) => {
          if let Ok(peer) = peer.try_into() {
            view.join(peer, ttl);
          } else {
            warn!("malformed join request");
            self.ban_peer(peer_id);
          }
        }
        Action::JoinAccepted(rpc::JoinAccepted { .. }) => {
          warn!(
            "received duplicate join accepted from peer {}, banning peer",
            peer_id
          );
          self.ban_peer(peer_id);
        }
        Action::ForwardJoin(rpc::ForwardJoin { ttl, peer }) => {
          if let Some(local) = self.local_node.as_ref() {
            match peer.try_into() {
              Ok(peer) => {
                view.forward_join(peer, ttl as usize, local.peer_id, peer_id)
              }
              Err(err) => {
                warn!(
                  "banning peer {} because of protocol violation: {:?}",
                  peer_id, err
                );
                self.ban_peer(peer_id);
              }
            }
          }
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
    } else {
      // reject any messages on a topic that we're not subscribed to
      // by immediately sending a disconnect event with alive: false,
      // then closing connection to the peer.
      self
        .out_events
        .push_back(EpisubNetworkBehaviourAction::NotifyHandler {
          peer_id: peer_id,
          handler: NotifyHandler::Any,
          event: rpc::Rpc {
            topic: event.topic,
            action: Some(rpc::rpc::Action::Disconnect(rpc::Disconnect {
              alive: false,
            })),
          },
        });
      self.force_disconnect(peer_id);
    }
  }

  fn poll(
    &mut self,
    cx: &mut Context<'_>,
    params: &mut impl PollParameters,
  ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>> {
    // update local peer identity and addresses
    self.update_local_node_info(params);

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
  /// updates our own information abount ourselves.
  /// This includes our own peer id, the addresses we are listening on
  /// and any external addresses we are aware of. Excludes any localhost
  /// addresses
  fn update_local_node_info(&mut self, params: &impl PollParameters) {
    if self.local_node.is_none() {
      self.local_node.replace(AddressablePeer {
        peer_id: *params.local_peer_id(),
        addresses: params
          .external_addresses()
          .map(|ad| ad.addr)
          .chain(params.listened_addresses())
          .filter(|a| !is_local_address(a))
          .collect(),
      });
    }
  }

  /// Handles the first JOIN request to a topic that we are trying to subscribe to.
  /// Once we have at least one other node in the topic active set, se will stop
  /// proactively sending JOIN requests to any dialed peer.
  fn handle_join_accepted(&mut self, peer_id: PeerId, event: rpc::Rpc) -> bool {
    if self.local_node.is_none() {
      return false; // we still don't know who we are.
    }

    if let Some(rpc::rpc::Action::JoinAccepted(rpc::JoinAccepted { peer })) =
      event.action
    {
      if let Ok(peer) = AddressablePeer::try_from(peer) {
        if peer.peer_id != self.local_node.as_ref().unwrap().peer_id {
          warn!("Got invalid join-accept!");
          self.ban_peer(peer_id);
        } else {
          if let Some(view) = self.topics.get_mut(&event.topic) {
            self.pending_joins.remove(&event.topic);
            view.join_accepted(AddressablePeer {
              peer_id,
              addresses: vec![self
                .peer_addresses
                .get(&peer_id)
                .unwrap()
                .clone()],
            });
            return true;
          } else {
            warn!("Got join-accept for a topic we didn't request to join");
            self.ban_peer(peer_id);
          }
        }
      } else {
        warn!("malformed join-accpet message, banning peer.");
        self.ban_peer(peer_id);
      }
    }

    false
  }

  fn ban_peer(&mut self, peer: PeerId) {
    warn!("Banning peer {}", peer);
    self.peer_addresses.remove(&peer);
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

fn is_local_address(addr: &Multiaddr) -> bool {
  addr.iter().any(|p| {
    // fileter out all localhost addresses
    if let Protocol::Ip4(addr) = p {
      addr == Ipv4Addr::LOCALHOST || addr == Ipv4Addr::UNSPECIFIED
    } else if let Protocol::Ip6(addr) = p {
      addr == Ipv6Addr::LOCALHOST || addr == Ipv6Addr::UNSPECIFIED
    } else {
      false
    }
  })
}
