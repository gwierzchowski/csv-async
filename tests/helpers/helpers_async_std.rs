#![allow(dead_code)]
pub use async_std::test;
use async_std::fs::File;
pub use futures::stream::StreamExt;

pub type Reader = csv_async::AsyncReader<File>;
#[cfg(feature = "with_serde")]
pub type Deserializer = csv_async::AsyncDeserializer<File>;

pub async fn get_reader(path: &str) -> async_std::io::Result<Reader> {
    Ok(csv_async::AsyncReader::from_reader(File::open(path).await?))
}

#[cfg(feature = "with_serde")]
pub async fn get_deserializer(path: &str) -> async_std::io::Result<Deserializer> {
    Ok(
        csv_async::AsyncReaderBuilder::new()
            .create_deserializer(File::open(path).await?)
    )
}
