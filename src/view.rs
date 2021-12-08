//! [HyParView]: a membership protocol for reliable gossip-based broadcast
//! [HyParView]: http://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf

use std::collections::{HashSet, VecDeque};

use crate::error::MeshError;
use libp2p::core::{Connected, ConnectedPoint, Multiaddr, PeerId};

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
#[derive(Debug)]
pub struct HyParView {
  active: HashSet<Connected>,
  passive: HashSet<AddressablePeer>,
}

impl Default for HyParView {
  fn default() -> Self {
    Self {
      active: HashSet::new(),
      passive: HashSet::new(),
    }
  }
}

/// Access to Partial View network overlays
impl HyParView {
  /// The active views of all nodes create an overlay that is used for message dissemination.
  /// Links in the overlay are symmetric, this means that each node keeps an open TCP connection
  /// to every other node in its active view.
  ///
  /// The active view is maintained using a reactive strategy, meaning nodes are remove
  /// when they fail.
  pub fn active(&self) -> impl Iterator<Item = &Connected> {
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
      .map(|a| &a.peer_id)
      .chain(self.passive().map(|p| &p.peer_id))
  }
}

/// Manipulation of PartialView nodes
impl HyParView {
  /// Removes a node entirely from the all views, active and passive.
  /// This happens when we know that a node has died, or it has been
  /// banned for violating the protocol or other security reasons.
  pub fn remove(&mut self, peer: PeerId) -> Option<AddressablePeer> {
    todo!();
  }

  /// Removes a node from the active list and moves it to the passive list.
  /// This happens when for example the active view is full and new nodes
  /// are requesting to join the topic. Returns true if the peer was
  /// demoted to the passive view, otherwise false if the peer was not in
  /// the active view and nothing changed.
  pub async fn demote_active(&mut self, peer: PeerId) -> Result<bool, MeshError> {
    todo!();
  }

  /// Promotes a node from the passive list to the active list and removes
  /// it from the passive view. Returns true if the node was found in the
  /// passive view and was successfully moved to the active view, otherwise
  /// returns false if the node was not present in the passive view, or
  /// we failed connecting to the peer.
  pub async fn promote_passive(&mut self, peer: PeerId) -> Result<bool, MeshError> {
    todo!();
  }
}

/// This struct uniquely identifies a node on the internet.
#[derive(Debug, Clone)]
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
