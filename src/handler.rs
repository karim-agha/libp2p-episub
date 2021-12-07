use crate::{
  codec::EpisubCodec, error::EpisubHandlerError, protocol::EpisubProtocol, rpc,
};
use asynchronous_codec::Framed;
use futures::StreamExt;
use libp2p::{
  core::{InboundUpgrade, OutboundUpgrade},
  swarm::{
    KeepAlive, NegotiatedSubstream, ProtocolsHandler, ProtocolsHandlerEvent,
    ProtocolsHandlerUpgrErr, SubstreamProtocol,
  },
};
use std::task::{Context, Poll};
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub enum HandlerEvent {
  Message(rpc::Rpc),
  PeerKind,
}

/// State of the inbound substream, opened either by us or by the remote.
enum InboundSubstreamState {
  /// Waiting for a message from the remote. The idle state for an inbound substream.
  WaitingInput(Framed<NegotiatedSubstream, EpisubCodec>),
  /// The substream is being closed.
  Closing(Framed<NegotiatedSubstream, EpisubCodec>),
  /// An error occurred during processing.
  Poisoned,
}

/// State of the outbound substream, opened either by us or by the remote.
enum OutboundSubstreamState {
  /// Waiting for the user to send a message. The idle state for an outbound substream.
  WaitingOutput(Framed<NegotiatedSubstream, EpisubCodec>),
  /// Waiting to send a message to the remote.
  PendingSend(Framed<NegotiatedSubstream, EpisubCodec>, crate::rpc::Rpc),
  /// Waiting to flush the substream so that the data arrives to the remote.
  PendingFlush(Framed<NegotiatedSubstream, EpisubCodec>),
  /// The substream is being closed. Used by either substream.
  _Closing(Framed<NegotiatedSubstream, EpisubCodec>),
  /// An error occurred during processing.
  Poisoned,
}

/// Protocol handler that manages a single long-lived substream with a peer
pub struct EpisubHandler {
  /// Upgrade configuration for the episub protocol.
  listen_protocol: SubstreamProtocol<EpisubProtocol, ()>,

  /// The single long-lived outbound substream.
  outbound_substream: Option<OutboundSubstreamState>,

  /// The single long-lived inbound substream.
  inbound_substream: Option<InboundSubstreamState>,
}

impl EpisubHandler {
  pub fn new(max_transmit_size: usize) -> Self {
    Self {
      listen_protocol: SubstreamProtocol::new(
        EpisubProtocol::new(max_transmit_size),
        (),
      ),
      outbound_substream: None,
      inbound_substream: None,
    }
  }
}

impl ProtocolsHandler for EpisubHandler {
  type InEvent = rpc::Rpc;
  type OutEvent = HandlerEvent;
  type Error = EpisubHandlerError;
  type InboundOpenInfo = ();
  type InboundProtocol = EpisubProtocol;
  type OutboundOpenInfo = rpc::Rpc;
  type OutboundProtocol = EpisubProtocol;

  fn listen_protocol(
    &self,
  ) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
    debug!("got a clone of listen protocol");
    self.listen_protocol.clone()
  }

  fn inject_fully_negotiated_inbound(
    &mut self,
    substream: <Self::InboundProtocol as InboundUpgrade<NegotiatedSubstream>>::Output,
    _: Self::InboundOpenInfo,
  ) {
    debug!("inbound protocol negotiated");
    self.inbound_substream =
      Some(InboundSubstreamState::WaitingInput(substream))
  }

  fn inject_fully_negotiated_outbound(
    &mut self,
    substream: <Self::OutboundProtocol as OutboundUpgrade<
      NegotiatedSubstream,
    >>::Output,
    info: Self::OutboundOpenInfo,
  ) {
    debug!("outbound protocol negotiated");
    if self.outbound_substream.is_none() {
      self.outbound_substream =
        Some(OutboundSubstreamState::PendingSend(substream, info));
    }
  }

  fn inject_event(&mut self, event: Self::InEvent) {
    info!("injecting event: {:?}", event);
  }

  fn inject_dial_upgrade_error(
    &mut self,
    info: Self::OutboundOpenInfo,
    error: ProtocolsHandlerUpgrErr<
      <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Error,
    >,
  ) {
    warn!("dial upgrade error: {:?}", error);
  }

  fn connection_keep_alive(&self) -> KeepAlive {
    KeepAlive::Yes
  }

  fn poll(
    &mut self,
    cx: &mut Context<'_>,
  ) -> Poll<
    ProtocolsHandlerEvent<
      Self::OutboundProtocol,
      Self::OutboundOpenInfo,
      Self::OutEvent,
      Self::Error,
    >,
  > {
    loop {
      match std::mem::replace(
        &mut self.inbound_substream,
        Some(InboundSubstreamState::Poisoned),
      ) {
        Some(InboundSubstreamState::WaitingInput(mut substream)) => {
          match substream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(message))) => {
              self.inbound_substream =
                Some(InboundSubstreamState::WaitingInput(substream));
              return Poll::Ready(ProtocolsHandlerEvent::Custom(message));
            }
            Poll::Ready(Some(Err(error))) => {
              warn!("inbound stream error: {:?}", error);
            }
            Poll::Ready(None) => {
              warn!("Peer closed their outbound stream");
              self.inbound_substream =
                Some(InboundSubstreamState::Closing(substream));
            }
            Poll::Pending => {
              break;
            }
          }
        }
        Some(InboundSubstreamState::Closing(_)) => info!("inbound closing"),
        Some(InboundSubstreamState::Poisoned) => info!("inbound poisoned"),
        None => {
          info!("inbound state none");
          self.inbound_substream = None;
          break;
        }
      }
    }
    Poll::Pending
  }
}
