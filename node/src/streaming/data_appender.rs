use std::fmt::{Display, Formatter};

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
}

impl Display for DataAppender {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataAppender {{ path: {} }}", self.path)
    }
}
