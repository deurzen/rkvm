use bincode::{DefaultOptions, Options};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{Error, ErrorKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub trait Message: Sized {
    async fn decode<R: AsyncRead + Send + Unpin>(stream: &mut R) -> Result<Self, Error>;

    async fn decode_with_buffer<R: AsyncRead + Send + Unpin>(
        stream: &mut R,
        buffer: &mut Vec<u8>,
    ) -> Result<Self, Error> {
        let _ = buffer;
        Self::decode(stream).await
    }

    async fn encode<W: AsyncWrite + Send + Unpin>(&self, stream: &mut W) -> Result<(), Error>;

    async fn encode_with_buffer<W: AsyncWrite + Send + Unpin>(
        &self,
        stream: &mut W,
        buffer: &mut Vec<u8>,
    ) -> Result<(), Error> {
        let _ = buffer;
        self.encode(stream).await
    }
}

impl<T: DeserializeOwned + Serialize + Sync> Message for T {
    async fn decode<R: AsyncRead + Send + Unpin>(stream: &mut R) -> Result<Self, Error> {
        let mut buffer = Vec::new();
        Self::decode_with_buffer(stream, &mut buffer).await
    }

    async fn decode_with_buffer<R: AsyncRead + Send + Unpin>(
        stream: &mut R,
        buffer: &mut Vec<u8>,
    ) -> Result<Self, Error> {
        let length = stream.read_u16().await?;
        let length_usize = length.into();

        buffer.resize(length_usize, 0);
        stream.read_exact(&mut buffer[..length_usize]).await?;

        let data = options()
            .deserialize(&buffer[..length_usize])
            .map_err(|err| Error::new(ErrorKind::InvalidData, err))?;

        tracing::trace!("Read {} bytes", 2 + length);

        Ok(data)
    }

    async fn encode<W: AsyncWrite + Send + Unpin>(&self, stream: &mut W) -> Result<(), Error> {
        let mut buffer = Vec::new();
        self.encode_with_buffer(stream, &mut buffer).await
    }

    async fn encode_with_buffer<W: AsyncWrite + Send + Unpin>(
        &self,
        stream: &mut W,
        buffer: &mut Vec<u8>,
    ) -> Result<(), Error> {
        buffer.clear();
        buffer.extend_from_slice(&[0, 0]);
        options()
            .serialize_into(&mut *buffer, self)
            .map_err(|err| Error::new(ErrorKind::InvalidInput, err))?;

        let length = u16::try_from(buffer.len() - 2)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "Data too large"))?;
        buffer[..2].copy_from_slice(&length.to_be_bytes());

        stream.write_all(buffer).await?;

        tracing::trace!("Wrote {} bytes", buffer.len());

        Ok(())
    }
}

fn options() -> impl Options {
    DefaultOptions::new().with_limit(u16::MAX.into())
}
