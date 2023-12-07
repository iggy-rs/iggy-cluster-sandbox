use crate::bytes_serializable::BytesSerializable;
use crate::error::SystemError;
use bytes::BufMut;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct Metadata {
    pub nodes: Vec<NodeInfo>,
    pub streams: Vec<StreamInfo>,
}

impl Display for Metadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Metadata {{ nodes: {:?}, streams: {:?} }}",
            self.nodes, self.streams
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
    pub id: u64,
    pub leader_id: u64,
    pub name: String,
}

impl Display for StreamInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StreamInfo {{ id: {}, leader_id: {}, name: {} }}",
            self.id, self.leader_id, self.name
        )
    }
}

impl StreamInfo {
    fn get_size_bytes(&self) -> usize {
        8 + 8 + 1 + self.name.len()
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
        bytes.put_u64_le(self.id);
        bytes.put_u64_le(self.leader_id);
        bytes.put_u8(self.name.len() as u8);
        bytes.extend(self.name.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let leader_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let name_len = bytes[16] as usize;
        let name = String::from_utf8(bytes[17..17 + name_len].to_vec()).unwrap();
        Ok(StreamInfo {
            id,
            leader_id,
            name,
        })
    }
}

impl BytesSerializable for Metadata {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.put_u8(self.nodes.len() as u8);
        for node in &self.nodes {
            bytes.extend(&node.as_bytes());
        }
        bytes.put_u8(self.streams.len() as u8);
        for stream in &self.streams {
            bytes.extend(&stream.as_bytes());
        }
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized,
    {
        let mut offset = 0;
        let nodes_len = bytes[offset] as usize;
        offset += 1;
        let mut nodes = Vec::new();
        for _ in 0..nodes_len {
            let node = NodeInfo::from_bytes(&bytes[offset..])?;
            offset += node.get_size_bytes();
            nodes.push(node);
        }
        let streams_len = bytes[offset] as usize;
        offset += 1;
        let mut streams = Vec::new();
        for _ in 0..streams_len {
            let stream = StreamInfo::from_bytes(&bytes[offset..])?;
            offset += stream.get_size_bytes();
            streams.push(stream);
        }
        Ok(Metadata { nodes, streams })
    }
}
