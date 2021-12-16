//! Plumtree: Epidemic Broadcast Trees
//! Leitão, João & Pereira, José & Rodrigues, Luís. (2007).
//! 301-310. 10.1109/SRDS.2007.27.

use libp2p::{swarm::NotifyHandler, PeerId};
use tracing::{debug, error, info};

use crate::{
  behaviour::EpisubNetworkBehaviourAction,
  cache::{ExpiringCache, MessageInfo, MessageRecord},
  rpc, Config, EpisubEvent,
};
use std::{
  collections::{HashSet, VecDeque},
  future::Future,
  pin::Pin,
  task::{Context, Poll},
  time::{Duration, Instant},
};

pub struct PlumTree {
  topic: String,
  local_node: PeerId,
  lazy: HashSet<PeerId>,
  eager: HashSet<PeerId>,
  last_tick: Instant,
  tick_frequency: Duration,
  observed: ExpiringCache<MessageInfo>,
  received: ExpiringCache<MessageRecord>,
  out_events: VecDeque<EpisubNetworkBehaviourAction>,
}

impl PlumTree {
  pub fn new(topic: String, config: Config, local_node: PeerId) -> Self {
    PlumTree {
      topic,
      local_node,
      lazy: HashSet::new(),
      eager: HashSet::new(),
      last_tick: Instant::now(),
      tick_frequency: config.tick_frequency,
      observed: ExpiringCache::new(
        config.history_window,
        config.tick_frequency,
      ),
      received: ExpiringCache::new(
        config.history_window,
        config.tick_frequency,
      ),
      out_events: VecDeque::new(),
    }
  }

  /// Called when the peer sampling service (HyparView) activates a peer
  pub fn inject_neighbor_up(&mut self, peer: PeerId) {
    self.eager.insert(peer);
  }

  /// Called when the peer sampling service (HyparView) deactivates a peer
  pub fn inject_neighbor_down(&mut self, peer: PeerId) {
    self.eager.remove(&peer);
    self.lazy.remove(&peer);
  }

  pub fn publish(&mut self, id: u128, payload: Vec<u8>) {
    if let Some(msg) = self.received.get(&id) {
      error!("refusing to send a message with id {}, received previously from node {}",
      id, msg.sender);
      return;
    }

    if let Some(msg) = self.observed.get(&id) {
      error!(
        "refusing to send a message with id {}, observed previously by node {}",
        id, msg.sender
      );
      return;
    }

    let message = MessageRecord {
      id,
      payload,
      hop: 1,
      sender: self.local_node,
    };

    for enode in &self.eager {
      debug!("sending message {} to peer {}", id, enode);
      self
        .out_events
        .push_back(EpisubNetworkBehaviourAction::NotifyHandler {
          peer_id: *enode,
          handler: NotifyHandler::Any,
          event: rpc::Rpc {
            topic: self.topic.clone(),
            action: Some(rpc::rpc::Action::Message(message.clone().into())),
          },
        });
    }

    // mark this message as received, so if we get it
    // again from other nodes we know that there is a
    // cycle in the broadcast tree.
    self.received.insert(MessageRecord { hop: 0, ..message });
  }

  pub fn broadcast(
    &mut self,
    peer_id: PeerId,
    id: u128,
    hop: u32,
    payload: Vec<u8>,
  ) {
    info!(
      "Plumtree broadcast message from {} with id {} [hop {}]",
      peer_id, id, hop
    );

    // if we don't have this message in the message cache
    // it means that we're seeing it for the first time,
    // then forward it to all eager push nodes.
    if !self.received.insert(MessageRecord {
      id,
      hop,
      payload: payload.clone(),
      sender: peer_id,
    }) {
      self
        .out_events
        .push_back(EpisubNetworkBehaviourAction::GenerateEvent(
          EpisubEvent::Message {
            topic: self.topic.clone(),
            id,
            payload: payload.clone(),
          },
        ));

      let message = rpc::Rpc {
        topic: self.topic.clone(),
        action: Some(rpc::rpc::Action::Message(rpc::Message {
          payload,
          id: id.to_le_bytes().to_vec(),
          hop: hop + 1,
        })),
      };

      // push message to all eager peers, except the sender
      for peer in &self.eager {
        if peer == &peer_id {
          continue;
        }
        self
          .out_events
          .push_back(EpisubNetworkBehaviourAction::NotifyHandler {
            peer_id: *peer,
            handler: NotifyHandler::Any,
            event: message.clone(),
          })
      }
    } else {
      // this is a duplicate message, it means that we are
      // having a cycle in the node connectivity graph. The
      // sender should be moved to lazy push peers and notified
      // that we have moved them to lazy nodes.
      self.inject_prune(peer_id);
      self
        .out_events
        .push_back(EpisubNetworkBehaviourAction::NotifyHandler {
          peer_id,
          handler: NotifyHandler::Any,
          event: rpc::Rpc {
            topic: self.topic.clone(),
            action: Some(rpc::rpc::Action::Prune(rpc::Prune {})),
          },
        });
    }
  }

  pub fn inject_ihave(&mut self, peer_id: PeerId, id: u128, hop: u32) {
    info!("IHAVE {:?} [hop {}] from {}", id, hop, peer_id);
    self.observed.insert(MessageInfo {
      id,
      hop,
      sender: peer_id,
    });
  }

  pub fn inject_prune(&mut self, peer_id: PeerId) {
    self.eager.remove(&peer_id);
    self.lazy.insert(peer_id);
  }

  pub fn inject_graft(&mut self, peer_id: PeerId, ids: Vec<u128>) {
    // updgrade to eager node after graft
    self.lazy.remove(&peer_id);
    self.eager.insert(peer_id);

    // and send all missing messages
    ids
      .into_iter()
      .filter_map(|id| self.received.get(&id))
      .for_each(|msg| {
        self
          .out_events
          .push_back(EpisubNetworkBehaviourAction::NotifyHandler {
            peer_id,
            handler: NotifyHandler::Any,
            event: rpc::Rpc {
              topic: self.topic.clone(),
              action: Some(rpc::rpc::Action::Message(msg.into())),
            },
          })
      });
  }
}

impl Future for PlumTree {
  type Output = EpisubNetworkBehaviourAction;

  fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
    if Instant::now().duration_since(self.last_tick) > self.tick_frequency {
      self.last_tick = Instant::now();
    }

    if let Some(event) = self.out_events.pop_front() {
      return Poll::Ready(event);
    }

    Poll::Pending
  }
}
