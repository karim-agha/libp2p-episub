use libp2p::PeerId;
use std::{
  collections::{HashMap, VecDeque},
  hash::Hash,
  sync::Arc,
  time::Duration,
};
use tracing::info;

use crate::rpc;

pub trait Keyed {
  type Key: Eq + Hash;
  fn key(&self) -> Self::Key;
}

pub struct ExpiringCache<T: Keyed> {
  data: HashMap<T::Key, Arc<T>>,
  buckets: VecDeque<Arc<T>>,
}

impl<T> ExpiringCache<T>
where
  T: Keyed,
{
  pub fn new(retention: Duration, tick: Duration) -> Self {
    assert!(retention > tick);
    assert_ne!(tick, Duration::ZERO);

    info!(
      "retention: {}, tick: {}, buckets: {}",
      retention.as_millis(),
      tick.as_millis(),
      (retention.as_millis() / tick.as_millis())
    );
    Self {
      data: HashMap::new(),
      buckets: VecDeque::new(),
    }
  }

  pub fn insert(&mut self, item: T) -> bool {
    let ptr1 = Arc::new(item);
    let ptr2 = Arc::clone(&ptr1);

    self.buckets.push_back(ptr2);
    self.data.insert(ptr1.key(), ptr1).is_some()
  }

  pub fn get(&self, key: &T::Key) -> Option<&T> {
    self.data.get(key).map(|arc| arc.as_ref())
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageRecord {
  pub id: u128,
  pub hop: u32,
  pub sender: PeerId,
  pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageInfo {
  pub id: u128,
  pub hop: u32,
  pub sender: PeerId,
}

impl Keyed for MessageRecord {
  type Key = u128;
  fn key(&self) -> Self::Key {
    self.id
  }
}

impl Keyed for MessageInfo {
  type Key = u128;
  fn key(&self) -> Self::Key {
    self.id
  }
}

impl From<MessageRecord> for rpc::Message {
  fn from(record: MessageRecord) -> Self {
    rpc::Message {
      id: record.id.to_le_bytes().to_vec(),
      hop: record.hop,
      payload: record.payload,
    }
  }
}

impl From<&MessageRecord> for rpc::Message {
  fn from(record: &MessageRecord) -> Self {
    record.clone().into()
  }
}
