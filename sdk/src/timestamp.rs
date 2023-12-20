use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct TimeStamp(SystemTime);

impl Default for TimeStamp {
    fn default() -> Self {
        Self(SystemTime::now())
    }
}

impl TimeStamp {
    pub fn now() -> Self {
        TimeStamp::default()
    }

    pub fn to_secs(&self) -> u64 {
        self.0.duration_since(UNIX_EPOCH).unwrap().as_secs()
    }

    pub fn to_micros(&self) -> u64 {
        self.0.duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
    }
}

impl From<u64> for TimeStamp {
    fn from(timestamp: u64) -> Self {
        TimeStamp(UNIX_EPOCH + Duration::from_micros(timestamp))
    }
}
