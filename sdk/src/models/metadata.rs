use crate::bytes_serializable::BytesSerializable;
use crate::error::SystemError;
use bytes::BufMut;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct Metadata {
    pub leader_id: Option<u64>,
    pub nodes: HashMap<u64, NodeInfo>,
    pub streams: HashMap<u64, StreamInfo>,
}

impl Display for Metadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Metadata {{ leader_id: {:?}, nodes: {:?}, streams: {:?} }}",
            self.leader_id, self.nodes, self.streams
        )
    }
}

#[derive(Debug)]
pub struct NodeInfo {
    pub id: u64,
    pub name: String,
    pub address: String,
}

impl Display for NodeInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NodeInfo {{ id: {}, name: {}, address: {} }}",
            self.id, self.name, self.address
        )
    }
}

impl NodeInfo {
    fn get_size_bytes(&self) -> usize {
        8 + 1 + self.name.len() + 1 + self.address.len()
    }
}

#[derive(Debug)]
pub struct StreamInfo {
    pub stream_id: u64,
    pub leader_id: u64,
}

impl Display for StreamInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StreamInfo {{ id: {}, leader_id: {} }}",
            self.stream_id, self.leader_id
        )
    }
}

impl StreamInfo {
    fn get_size_bytes(&self) -> usize {
        16
    }
}

impl BytesSerializable for NodeInfo {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.put_u64_le(self.id);
        bytes.put_u8(self.name.len() as u8);
        bytes.extend(self.name.as_bytes());
        bytes.put_u8(self.address.len() as u8);
        bytes.extend(self.address.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let name_len = bytes[8] as usize;
        let name = String::from_utf8(bytes[9..9 + name_len].to_vec()).unwrap();
        let address_len = bytes[9 + name_len] as usize;
        let address =
            String::from_utf8(bytes[10 + name_len..10 + name_len + address_len].to_vec()).unwrap();
        Ok(NodeInfo { id, name, address })
    }
}

impl BytesSerializable for StreamInfo {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.put_u64_le(self.stream_id);
        bytes.put_u64_le(self.leader_id);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let leader_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        Ok(StreamInfo {
            stream_id: id,
            leader_id,
        })
    }
}

impl BytesSerializable for Metadata {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        if let Some(leader_id) = self.leader_id {
            bytes.put_u64_le(leader_id);
        } else {
            bytes.put_u64_le(0);
        }
        bytes.put_u8(self.nodes.len() as u8);
        for node in self.nodes.values() {
            bytes.extend(node.as_bytes());
        }
        bytes.put_u8(self.streams.len() as u8);
        for stream in self.streams.values() {
            bytes.extend(stream.as_bytes());
        }
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        let leader_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let leader_id = if leader_id == 0 {
            None
        } else {
            Some(leader_id)
        };
        let mut offset = 8;
        let nodes_len = bytes[offset] as usize;
        offset += 1;
        let mut nodes = HashMap::new();
        for _ in 0..nodes_len {
            let node = NodeInfo::from_bytes(&bytes[offset..])?;
            offset += node.get_size_bytes();
            nodes.insert(node.id, node);
        }
        let streams_len = bytes[offset] as usize;
        offset += 1;
        let mut streams = HashMap::new();
        for _ in 0..streams_len {
            let stream = StreamInfo::from_bytes(&bytes[offset..])?;
            offset += stream.get_size_bytes();
            streams.insert(stream.stream_id, stream);
        }
        Ok(Metadata {
            leader_id,
            nodes,
            streams,
        })
    }
}
