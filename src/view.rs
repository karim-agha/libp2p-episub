use libp2p_core::{Multiaddr, PeerId, Connected};

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
pub struct PartialView {
  active: usize,
  passive: usize,
  total: usize,
}

impl Default for PartialView {
  fn default() -> Self {
    Self {
      active: 1,
      passive: 6,
      total: 10_000,
    }
  }
}

/// Access to Partial View network overlays
impl PartialView {
  /// The active views of all nodes create an overlay that is used for message dissemination. 
  /// Links in the overlay are symmetric, this means that each node keeps an open TCP connection 
  /// to every other node in its active view.
  /// 
  /// The active view is maintained using a reactive strategy, meaning nodes are remove
  /// when they fail.
  pub fn active() -> impl Iterator<Item = Connected> {
    std::iter::empty()
  }

  /// The goal of the passive view is to maintain a list of nodes that can be used to 
  /// replace failed members of the active view. The passive view is not used for message
  /// dissemination.
  /// 
  /// The passive view is maintained using a cyclic strategy. Periodically, each node 
  /// performs shuffle operation with one of its neighbors in order to update its passive view.
  pub fn passive() -> impl Iterator<Item = AddressablePeer> {
    std::iter::empty()
  }
}

/// Manipulation of PartialView nodes
impl PartialView {
  /// Removes a node entirely from the all views, active and passive.
  /// This happens when we know that a node has died, or it has been
  /// banned for violating the protocol or other security reasons.
  pub fn remove(peer: PeerId) -> Option<AddressablePeer> {
    todo!()
  }
}

/// This struct uniquely identifies a node on the internet.
pub struct AddressablePeer {
  pub peer_id: PeerId,
  pub address: Multiaddr
}