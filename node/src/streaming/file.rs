use monoio::fs::{File, OpenOptions};

pub(crate) async fn open(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().read(true).open(path).await
}

pub(crate) async fn append(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().read(true).append(true).open(path).await
}

pub(crate) async fn write(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().create(true).write(true).open(path).await
}
