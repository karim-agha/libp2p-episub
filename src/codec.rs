use crate::{error::EpisubHandlerError, handler::HandlerEvent, rpc};
use asynchronous_codec::{Bytes, BytesMut, Decoder, Encoder};
use prost::Message;
use unsigned_varint::codec;

pub struct EpisubCodec {
  /// Codec to encode/decode the Unsigned varint length prefix of the frames.
  length_codec: codec::UviBytes,
}

impl EpisubCodec {
  pub fn new(length_codec: codec::UviBytes) -> Self {
    Self { length_codec }
  }
}

impl Encoder for EpisubCodec {
  type Item = rpc::Rpc;
  type Error = EpisubHandlerError;

  fn encode(
    &mut self,
    item: Self::Item,
    dst: &mut BytesMut,
  ) -> Result<(), Self::Error> {
    let mut buf = Vec::with_capacity(item.encoded_len());
    item.encode(&mut buf).expect("buffer overrun");
    self
      .length_codec
      .encode(Bytes::from(buf), dst)
      .map_err(|_| EpisubHandlerError::MaxTransmissionSize)
  }
}

impl Decoder for EpisubCodec {
  type Item = HandlerEvent;
  type Error = EpisubHandlerError;

  fn decode(
    &mut self,
    src: &mut BytesMut,
  ) -> Result<Option<Self::Item>, Self::Error> {
    let packet = match self.length_codec.decode(src).map_err(|e| {
      if let std::io::ErrorKind::PermissionDenied = e.kind() {
        EpisubHandlerError::MaxTransmissionSize
      } else {
        EpisubHandlerError::Io(e)
      }
    })? {
      Some(p) => p,
      None => return Ok(None),
    };

    let x = rpc::Rpc::decode(&packet[..]).map_err(std::io::Error::from)?;

    todo!();
  }
}
