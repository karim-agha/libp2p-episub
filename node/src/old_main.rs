//!
//! P2P Protocols Lab
//!
//! This executable is the entrypoint to running the development version of libp2p-episub.
//! Using this executable you can crate and test the behavior of p2p gossip networks.
//!
//! At some point when episub implementation reaches a certain level of maturity it should
//! be packaged into a separate crate that could be used as a module in libp2p. However for
//! now, while we are still very early in the development lifecycle, its more convenient to have
//! everything in one place.
//!

use std::{
  collections::hash_map::DefaultHasher,
  hash::{Hash, Hasher},
  intrinsics::transmute,
  mem::size_of,
  time::Duration,
};

use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use libp2p::{
  gossipsub::{
    self, GossipsubMessage, IdentTopic, MessageAuthenticity, MessageId,
    ValidationMode,
  },
  identity,
  swarm::SwarmEvent,
  Multiaddr, PeerId,
};

use libp2p_episub::{NodeEvent, NodeUpdate};
use structopt::StructOpt;
use tokio::{net::UdpSocket, sync::mpsc::unbounded_channel};
use tracing::{error, info, trace, Level};

static DEFAULT_BOOTSTRAP_NODE: &str = "/dnsaddr/bootstrap.libp2p.io";

#[derive(Debug, StructOpt)]
struct CliOptions {
  #[structopt(
    short,
    long,
    parse(from_occurrences),
    help = "Use verbose output (-vv very verbose output)"
  )]
  pub verbose: u64,

  #[structopt(long, about = "gossip topic name")]
  topic: String,

  #[structopt(long, default_value=DEFAULT_BOOTSTRAP_NODE, about = "p2p bootstrap peers")]
  bootstrap: Vec<Multiaddr>,

  #[structopt(long, about = "p2p audit node")]
  audit: String,
}

async fn send_update(socket: &UdpSocket, update: NodeUpdate) -> Result<()> {
  let buf: [u8; size_of::<NodeUpdate>()] = unsafe { transmute(update) };
  socket.send(&buf).await?;
  Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
  let opts = CliOptions::from_args();

  tracing_subscriber::fmt()
    .with_max_level(match opts.verbose {
      1 => Level::DEBUG,
      2 => Level::TRACE,
      _ => Level::INFO,
    })
    .init();

  let local_key = identity::Keypair::generate_ed25519();
  let local_peer_id = PeerId::from(local_key.public());

  info!("Local peer id: {:?}", local_peer_id);
  info!("Bootstrap nodes: {:?}", opts.bootstrap);
  info!("Gossip Topic: {}", opts.topic);

  // Set up an encrypted TCP Transport over the Mplex and Yamux protocols
  let transport = libp2p::development_transport(local_key.clone()).await?;
  let topic = IdentTopic::new(&opts.topic);

  info!("audit addr: {:?}", opts.audit);
  let audit_sock = UdpSocket::bind("0.0.0.0:9000").await?;
  audit_sock.connect(opts.audit.as_str()).await?;

  send_update(
    &audit_sock,
    NodeUpdate {
      node_id: local_peer_id,
      peer_id: local_peer_id,
      event: NodeEvent::Up,
    },
  )
  .await?;

  // Create a Swarm to manage peers and events
  let mut swarm = {
    // To content-address message, we can take the hash of message and use it as an ID.
    let message_id_fn = |message: &GossipsubMessage| {
      let mut s = DefaultHasher::new();
      message.data.hash(&mut s);
      MessageId::from(s.finish().to_string())
    };

    // Set a custom gossipsub
    let gossipsub_config = gossipsub::GossipsubConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
        .validation_mode(ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
        .message_id_fn(message_id_fn) // content-address messages. No two messages of the
        .mesh_outbound_min(1)
        // same content will be propagated.
        .build()
        .expect("Valid config");
    // build a gossipsub network behaviour
    let mut gossipsub: gossipsub::Gossipsub = gossipsub::Gossipsub::new(
      MessageAuthenticity::Signed(local_key),
      gossipsub_config,
    )
    .expect("Correct configuration");

    // subscribes to our topic
    gossipsub.subscribe(&topic).unwrap();

    // build the swarm
    libp2p::Swarm::new(transport, gossipsub, local_peer_id)
  };

  // Listen on all interfaces and whatever port the OS assigns
  swarm
    .listen_on("/ip4/0.0.0.0/tcp/4001".parse().unwrap())
    .unwrap();

  // dial all bootstrap nodes
  opts
    .bootstrap
    .into_iter()
    .for_each(|addr| swarm.dial(addr).unwrap());

  let (msg_tx, mut msg_rx) = unbounded_channel::<Vec<u8>>();
  let msg_tx_clone = msg_tx.clone();
  tokio::spawn(async move {
    let local_peer_id = local_peer_id.clone();
    loop {
      // every 5 seconds send a message to the gossip topic
      tokio::time::sleep(Duration::from_secs(5)).await;
      msg_tx
        .send(
          format!(
            "I am {}, sending message at: {}",
            local_peer_id,
            Utc::now().to_rfc2822()
          )
          .into_bytes(),
        )
        .unwrap();
      info!("Broadcasted message from local peer");
    }
  });

  // run the libp2p event loop in conjunction with our send loop
  loop {
    tokio::select! {
      Some(event) = swarm.next() => {
        match event {
          SwarmEvent::Behaviour(gossipsub::GossipsubEvent::Message {
            propagation_source: peer,
            message_id: id,
            message,
          }) => {
            info!("Gossip message {} from {}: {:?}", id, peer, message);

            if let Ok(addr) = Multiaddr::try_from(message.data) {
              info!("Received address {}: {:?}", addr.clone(), swarm.dial(addr.clone()));
            }

            send_update(&audit_sock, NodeUpdate {
              node_id: local_peer_id,
              peer_id: peer,
              event: NodeEvent::Message
            }).await?;
          }
          SwarmEvent::Behaviour(gossipsub::GossipsubEvent::Subscribed {
            peer_id,
            topic
          }) => {
            info!("Peer {} subscribed to topic {}", peer_id, topic);
          }
          SwarmEvent::IncomingConnection { send_back_addr, local_addr } => {
            msg_tx_clone.send(send_back_addr.to_vec()).unwrap();
            msg_tx_clone.send(local_addr.to_vec()).unwrap();
          }
          SwarmEvent::NewListenAddr { address, .. } => {
            info!("Listening on {}", address);
            msg_tx_clone.send(address.to_vec()).unwrap();
          }
          _ => trace!("swarm event: {:?}", event),
        }
      },
      Some(sendmsg) = msg_rx.recv() => {
        for p in swarm.behaviour().all_mesh_peers() {
          info!("I know about peer {:?}", p);
        }

        if let Err(e) = swarm.behaviour_mut().publish(topic.clone(), sendmsg) {
          error!("Failed publishing Gossipsub message: {:?}", e);
        }
      }
    };
  }
}
