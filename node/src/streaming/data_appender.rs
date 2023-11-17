use std::fmt::{Display, Formatter};
use std::fs::create_dir_all;
use std::path::Path;
use tracing::info;

#[derive(Debug)]
pub struct DataAppender {
    path: String,
}

impl DataAppender {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }

    pub fn init(&self) {
        if Path::new(&self.path).exists() {
            info!("Stream directory already exists: {}", self.path);
            return;
        }

        create_dir_all(&self.path)
            .unwrap_or_else(|_| panic!("Failed to create stream directory: {}", self.path));
        info!("Created stream directory: {}", self.path);
    }
}

impl Display for DataAppender {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataAppender {{ path: {} }}", self.path)
    }
}
