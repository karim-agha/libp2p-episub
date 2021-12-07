use std::task::{Context, Poll};

use libp2p_core::{
  connection::ConnectionId, ConnectedPoint, Multiaddr, PeerId,
};
use libp2p_swarm::{
  IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction,
  PollParameters, SubstreamProtocol,
};
use tracing::debug;

use crate::{
  handler::{EpisubHandler, HandlerEvent},
  protocol::EpisubProtocol,
  view::PartialView,
};

/// Configuration paramaters for Episub
#[derive(Debug, Clone)]
pub struct Config {
  pub fanout: u16,
  pub max_transmit_size: usize,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      fanout: 6,
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

/// Network behaviour that handles the Episub protocol.
///
/// This network behaviour combines three academic papers into one implementation:
///   1. HyParView: For topic-peer-membership management and node discovery
///   2. Epidemic Broadcast Trees: For constructing efficient broadcast trees and efficient content dessamination
///   3. GoCast: Gossip-Enhanced Overlay Multicast for Fast and Dependable Group Communication
///
#[derive(Debug)]
pub struct Episub {
  config: Config,
  view: PartialView,
}

impl Episub {
  pub fn new() -> Self {
    Self {
      config: Config::default(),
      view: PartialView::default(),
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
    _: <Self::ProtocolsHandler as IntoProtocolsHandler>::Handler,
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
    Poll::Pending
  }
}
