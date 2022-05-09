//! Handles the encoding and decoding of pubsub messages.

use crate::types::{GossipEncoding, GossipKind, GossipTopic};
use crate::TopicHash;
use libp2p::gossipsub::{DataTransform, GossipsubMessage, RawGossipsubMessage};
use snap::raw::{decompress_len, Decoder, Encoder};
use ssz::{Decode, Encode};
use std::io::{Error, ErrorKind};

#[derive(Debug, Clone, PartialEq)]
pub enum PubsubMessage {
    ExampleMessage(u64),
}

// Implements the `DataTransform` trait of gossipsub to employ snappy compression
pub struct SnappyTransform {
    /// Sets the maximum size we allow gossipsub messages to decompress to.
    max_size_per_message: usize,
}

impl SnappyTransform {
    pub fn new(max_size_per_message: usize) -> Self {
        SnappyTransform {
            max_size_per_message,
        }
    }
}

impl DataTransform for SnappyTransform {
    // Provides the snappy decompression from RawGossipsubMessages
    fn inbound_transform(
        &self,
        raw_message: RawGossipsubMessage,
    ) -> Result<GossipsubMessage, std::io::Error> {
        // check the length of the raw bytes
        let len = decompress_len(&raw_message.data)?;
        if len > self.max_size_per_message {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "ssz_snappy decoded data > GOSSIP_MAX_SIZE",
            ));
        }

        let mut decoder = Decoder::new();
        let decompressed_data = decoder.decompress_vec(&raw_message.data)?;

        // Build the GossipsubMessage struct
        Ok(GossipsubMessage {
            source: raw_message.source,
            data: decompressed_data,
            sequence_number: raw_message.sequence_number,
            topic: raw_message.topic,
        })
    }

    /// Provides the snappy compression logic to gossipsub.
    fn outbound_transform(
        &self,
        _topic: &TopicHash,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, std::io::Error> {
        // Currently we are not employing topic-based compression. Everything is expected to be
        // snappy compressed.
        if data.len() > self.max_size_per_message {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "ssz_snappy Encoded data > GOSSIP_MAX_SIZE",
            ));
        }
        let mut encoder = Encoder::new();
        encoder.compress_vec(&data).map_err(Into::into)
    }
}

impl PubsubMessage {
    /// Returns the topics that each pubsub message will be sent across, given a supported
    /// gossipsub encoding and fork version.
    pub fn topics(&self, encoding: GossipEncoding) -> Vec<GossipTopic> {
        vec![GossipTopic::new(self.kind(), encoding)]
    }

    /// Returns the kind of gossipsub topic associated with the message.
    pub fn kind(&self) -> GossipKind {
        match self {
            PubsubMessage::ExampleMessage(_) => GossipKind::Example,
        }
    }

    /// This decodes `data` into a `PubsubMessage` given a topic.
    /* Note: This is assuming we are not hashing topics. If we choose to hash topics, these will
     * need to be modified.
     */
    pub fn decode(topic: &TopicHash, data: &[u8]) -> Result<Self, String> {
        match GossipTopic::decode(topic.as_str()) {
            Err(_) => Err(format!("Unknown gossipsub topic: {:?}", topic)),
            Ok(gossip_topic) => {
                // All topics are currently expected to be compressed and decompressed with snappy.
                // This is done in the `SnappyTransform` struct.
                // Therefore compression has already been handled for us by the time we are
                // decoding the objects here.

                // the ssz decoders
                match gossip_topic.kind() {
                    GossipKind::Example => Ok(PubsubMessage::ExampleMessage(
                        u64::from_ssz_bytes(data).map_err(|e| format!("{:?}", e))?,
                    )),
                }
            }
        }
    }

    /// Encodes a `PubsubMessage` based on the topic encodings. The first known encoding is used. If
    /// no encoding is known, and error is returned.
    pub fn encode(&self, _encoding: GossipEncoding) -> Vec<u8> {
        // Currently do not employ encoding strategies based on the topic. All messages are ssz
        // encoded.
        // Also note, that the compression is handled by the `SnappyTransform` struct. Gossipsub will compress the
        // messages for us.
        match &self {
            PubsubMessage::ExampleMessage(data) => data.as_ssz_bytes(),
        }
    }
}

impl std::fmt::Display for PubsubMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PubsubMessage::ExampleMessage(msg) => {
                write!(f, "Example message: {}", msg,)
            }
        }
    }
}
