use crate::{
  codec::EpisubCodec, error::EpisubHandlerError, protocol::EpisubProtocol, rpc,
};
use asynchronous_codec::Framed;
use futures::{Sink, StreamExt};
use libp2p::{
  core::{InboundUpgrade, OutboundUpgrade},
  swarm::{
    KeepAlive, NegotiatedSubstream, ProtocolsHandler, ProtocolsHandlerEvent,
    ProtocolsHandlerUpgrErr, SubstreamProtocol,
  },
};
use std::{
  collections::VecDeque,
  io,
  pin::Pin,
  task::{Context, Poll},
};
use tracing::{debug, error, info, warn};

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
  // upgrade requested and waiting for the upgrade to be negotiated.
  SubstreamRequested,
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
  /// Whether we want the peer to have strong live connection to us.
  /// This changes when a peer is moved from the active view to the passive view.
  keep_alive: KeepAlive,
  /// The list of messages scheduled to be sent to this peer
  outbound_queue: VecDeque<rpc::Rpc>,
}

type EpisubHandlerEvent = ProtocolsHandlerEvent<
  <EpisubHandler as ProtocolsHandler>::OutboundProtocol,
  <EpisubHandler as ProtocolsHandler>::OutboundOpenInfo,
  <EpisubHandler as ProtocolsHandler>::OutEvent,
  <EpisubHandler as ProtocolsHandler>::Error,
>;

impl EpisubHandler {
  pub fn new(max_transmit_size: usize) -> Self {
    Self {
      listen_protocol: SubstreamProtocol::new(
        EpisubProtocol::new(max_transmit_size),
        (),
      ),
      keep_alive: KeepAlive::Yes,
      outbound_substream: None,
      inbound_substream: None,
      outbound_queue: VecDeque::new(),
    }
  }
}

impl ProtocolsHandler for EpisubHandler {
  type InEvent = rpc::Rpc;
  type OutEvent = HandlerEvent;
  type Error = EpisubHandlerError;
  type InboundOpenInfo = ();
  type InboundProtocol = EpisubProtocol;
  type OutboundOpenInfo = ();
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
    _: Self::OutboundOpenInfo,
  ) {
    debug!("outbound protocol negotiated");
    self.outbound_substream =
      Some(OutboundSubstreamState::WaitingOutput(substream));
  }

  fn inject_event(&mut self, event: Self::InEvent) {
    info!("injecting event: {:?}", event);
    self.outbound_queue.push_back(event);
  }

  fn inject_dial_upgrade_error(
    &mut self,
    _: Self::OutboundOpenInfo,
    error: ProtocolsHandlerUpgrErr<
      <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Error,
    >,
  ) {
    warn!("dial upgrade error: {:?}", error);
  }

  fn connection_keep_alive(&self) -> KeepAlive {
    self.keep_alive
  }

  fn poll(&mut self, cx: &mut Context<'_>) -> Poll<EpisubHandlerEvent> {
    // process inbound stream first
    info!("will process inbound polls");
    let inbound_poll = self.process_inbound_poll(cx);
    if !matches!(inbound_poll, Poll::<EpisubHandlerEvent>::Pending) {
      return inbound_poll;
    }

    info!("will process outbound polls");
    // then process outbound steram
    let outbound_poll = self.process_outbound_poll(cx);
    if !matches!(outbound_poll, Poll::<EpisubHandlerEvent>::Pending) {
      return outbound_poll;
    }

    info!("no polls this time");
    // nothing to communicate to the runtime for this connection.
    Poll::Pending
  }
}

impl EpisubHandler {
  fn process_inbound_poll(
    &mut self,
    cx: &mut Context<'_>,
  ) -> Poll<EpisubHandlerEvent> {
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
              info!("received message on handler: {:?}", message);
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
              self.inbound_substream =
                Some(InboundSubstreamState::WaitingInput(substream));
              break;
            }
          }
        }
        Some(InboundSubstreamState::Closing(mut substream)) => {
          match Sink::poll_close(Pin::new(&mut substream), cx) {
            Poll::Ready(res) => {
              if let Err(e) = res {
                // Don't close the connection but just drop the inbound substream.
                // In case the remote has more to send, they will open up a new
                // substream.
                warn!("Inbound substream error while closing: {:?}", e);
              }
              self.inbound_substream = None;
              if self.outbound_substream.is_none() {
                self.keep_alive = KeepAlive::No;
              }
              break;
            }
            Poll::Pending => {
              self.inbound_substream =
                Some(InboundSubstreamState::Closing(substream));
              break;
            }
          }
        }
        Some(InboundSubstreamState::Poisoned) => {
          unreachable!("Error occurred during inbound stream processing");
        }
        None => {
          info!("inbound state none");
          self.inbound_substream = None;
          break;
        }
      }
    }
    Poll::Pending
  }

  fn process_outbound_poll(
    &mut self,
    cx: &mut Context<'_>,
  ) -> Poll<EpisubHandlerEvent> {
    loop {
      match std::mem::replace(
        &mut self.outbound_substream,
        Some(OutboundSubstreamState::Poisoned),
      ) {
        Some(OutboundSubstreamState::WaitingOutput(substream)) => {
          if let Some(msg) = self.outbound_queue.pop_front() {
            info!("there is an outbount event in queue: {:?}", msg);
            self.outbound_queue.shrink_to_fit();
            self.outbound_substream =
              Some(OutboundSubstreamState::PendingSend(substream, msg));
          } else {
            self.outbound_substream =
              Some(OutboundSubstreamState::WaitingOutput(substream));
            break;
          }
        }
        Some(OutboundSubstreamState::PendingSend(mut substream, message)) => {
          match Sink::poll_ready(Pin::new(&mut substream), cx) {
            Poll::Ready(Ok(())) => {
              info!("sending message {:?}", message);
              match Sink::start_send(Pin::new(&mut substream), message) {
                Ok(()) => {
                  self.outbound_substream =
                    Some(OutboundSubstreamState::PendingFlush(substream));
                }
                Err(EpisubHandlerError::MaxTransmissionSize) => {
                  error!("Message exceeds the maximum transmission size and was dropped.");
                  self.outbound_substream =
                    Some(OutboundSubstreamState::WaitingOutput(substream));
                }
                Err(e) => {
                  error!("Error sending message: {}", e);
                  return Poll::Ready(ProtocolsHandlerEvent::Close(e));
                }
              }
            }
            Poll::Ready(Err(e)) => {
              error!("outbound substream error while sending message: {:?}", e);
              return Poll::Ready(ProtocolsHandlerEvent::Close(e));
            }
            Poll::Pending => {
              self.keep_alive = KeepAlive::Yes;
              self.outbound_substream =
                Some(OutboundSubstreamState::PendingSend(substream, message));
              break;
            }
          }
        }
        Some(OutboundSubstreamState::PendingFlush(mut substream)) => {
          match Sink::poll_flush(Pin::new(&mut substream), cx) {
            Poll::Ready(Ok(())) => {
              self.outbound_substream =
                Some(OutboundSubstreamState::WaitingOutput(substream));
            }
            Poll::Ready(Err(e)) => {
              return Poll::Ready(ProtocolsHandlerEvent::Close(e))
            }
            Poll::Pending => {
              self.keep_alive = KeepAlive::Yes;
              self.outbound_substream =
                Some(OutboundSubstreamState::PendingFlush(substream));
              break;
            }
          }
        }
        Some(OutboundSubstreamState::_Closing(mut substream)) => {
          match Sink::poll_close(Pin::new(&mut substream), cx) {
            Poll::Ready(Ok(())) => {
              self.outbound_substream = None;
              if self.inbound_substream.is_none() {
                self.keep_alive = KeepAlive::No;
              }
              break;
            }
            Poll::Ready(Err(e)) => {
              warn!("Outbound substream error while closing: {:?}", e);
              return Poll::Ready(ProtocolsHandlerEvent::Close(
                io::Error::new(
                  io::ErrorKind::BrokenPipe,
                  "Failed to close outbound substream",
                )
                .into(),
              ));
            }
            Poll::Pending => {
              self.keep_alive = KeepAlive::No;
              self.outbound_substream =
                Some(OutboundSubstreamState::_Closing(substream));
              break;
            }
          }
        }
        Some(OutboundSubstreamState::SubstreamRequested) => {
          info!("awaiting substream upgrade");
          self.outbound_substream =
            Some(OutboundSubstreamState::SubstreamRequested);
          break;
        }
        Some(OutboundSubstreamState::Poisoned) => {
          unreachable!("Error occurred during outbound stream processing");
        }
        None => {
          info!("outbound state none");
          self.outbound_substream =
            Some(OutboundSubstreamState::SubstreamRequested);
          return Poll::Ready(
            ProtocolsHandlerEvent::OutboundSubstreamRequest {
              protocol: self.listen_protocol.clone(),
            },
          );
        }
      }
    }

    Poll::Pending
  }
}
