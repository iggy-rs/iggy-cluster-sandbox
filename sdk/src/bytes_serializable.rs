use crate::error::SystemError;

pub trait BytesSerializable {
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Result<Self, SystemError>
    where
        Self: Sized;
}
