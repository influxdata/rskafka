use std::{
    collections::HashMap,
    io::Cursor,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc, RwLock,
    },
};

use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, WriteHalf},
    sync::{
        oneshot::{channel, Sender},
        Mutex,
    },
    task::JoinHandle,
};

use crate::protocol::{
    api_key::ApiKey,
    api_version::ApiVersion,
    frame::{AsyncMessageRead, AsyncMessageWrite},
    messages::{
        ReadVersionedError, ReadVersionedType, RequestBody, RequestHeader, ResponseHeader,
        WriteVersionedError, WriteVersionedType,
    },
    primitives::{Int16, Int32, NullableString, TaggedFields},
};

struct Response {
    #[allow(dead_code)]
    header: ResponseHeader,
    data: Cursor<Vec<u8>>,
}

pub struct Messenger<RW> {
    stream_write: Mutex<WriteHalf<RW>>,
    correlation_id: AtomicI32,
    version_ranges: RwLock<HashMap<ApiKey, (ApiVersion, ApiVersion)>>,
    channels: Arc<Mutex<HashMap<i32, Sender<Response>>>>,
    join_handle: JoinHandle<()>,
}

#[derive(Error, Debug)]
pub enum RequestError {
    #[error("Cannot find matching version for: {api_key:?}")]
    NoVersionMatch { api_key: ApiKey },

    #[error(transparent)]
    WriteError(#[from] WriteVersionedError),

    #[error(transparent)]
    WriteMessageError(#[from] crate::protocol::frame::WriteError),

    #[error(transparent)]
    ReadError(#[from] ReadVersionedError),

    #[error("Cannot read/write data")]
    IO(#[from] std::io::Error),
}

impl<RW> Messenger<RW>
where
    RW: AsyncRead + AsyncWrite + Send + 'static,
{
    pub fn new(stream: RW) -> Self {
        let (stream_read, stream_write) = tokio::io::split(stream);
        let channels: Arc<Mutex<HashMap<i32, Sender<Response>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let channels_captured = Arc::clone(&channels);

        let join_handle = tokio::spawn(async move {
            let mut stream_read = stream_read;

            loop {
                let msg = stream_read.read_message(1024 * 1024).await.unwrap();
                let mut cursor = Cursor::new(msg);
                let header =
                    ResponseHeader::read_versioned(&mut cursor, ApiVersion(Int16(0))).unwrap();
                if let Some(tx) = channels_captured
                    .lock()
                    .await
                    .remove(&header.correlation_id.0)
                {
                    // we don't care if the other side is gone
                    tx.send(Response {
                        header,
                        data: cursor,
                    })
                    .ok();
                }
            }
        });
        Self {
            stream_write: Mutex::new(stream_write),
            correlation_id: AtomicI32::new(0),
            version_ranges: RwLock::new(HashMap::new()),
            channels,
            join_handle,
        }
    }

    pub fn set_version_ranges(&self, ranges: HashMap<ApiKey, (ApiVersion, ApiVersion)>) {
        *self.version_ranges.write().expect("lock poissened") = ranges;
    }

    pub async fn request<R>(&self, msg: R) -> Result<R::ResponseBody, RequestError>
    where
        R: RequestBody + WriteVersionedType<Vec<u8>>,
        R::ResponseBody: ReadVersionedType<Cursor<Vec<u8>>>,
    {
        let body_api_version = self
            .version_ranges
            .read()
            .expect("lock poissened")
            .get(&R::API_KEY)
            .map(|range_server| match_versions(*range_server, R::API_VERSION_RANGE))
            .flatten()
            .ok_or(RequestError::NoVersionMatch {
                api_key: R::API_KEY,
            })?;
        let correlation_id = self.correlation_id.fetch_add(1, Ordering::SeqCst);

        let header = RequestHeader {
            request_api_key: ApiKey::ApiVersions,
            request_api_version: body_api_version,
            correlation_id: Int32(correlation_id),
            client_id: NullableString(None),
            tagged_fields: TaggedFields::default(),
        };

        let mut buf = Vec::new();
        header
            .write_versioned(&mut buf, ApiVersion(Int16(2)))
            .unwrap();
        msg.write_versioned(&mut buf, body_api_version)?;

        let (tx, rx) = channel();
        self.channels.lock().await.insert(correlation_id, tx);

        {
            let mut stream_write = self.stream_write.lock().await;
            stream_write.write_message(&buf).await?;
            stream_write.flush().await?;
        }

        let mut response = rx.await.unwrap();
        let body = R::ResponseBody::read_versioned(&mut response.data, body_api_version)?;

        Ok(body)
    }
}

impl<RW> Drop for Messenger<RW> {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

fn match_versions(
    range_a: (ApiVersion, ApiVersion),
    range_b: (ApiVersion, ApiVersion),
) -> Option<ApiVersion> {
    assert!(range_a.0 <= range_a.1);
    assert!(range_b.0 <= range_b.1);

    if range_a.0 <= range_b.1 && range_b.0 <= range_a.1 {
        Some(range_a.1.min(range_b.1))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_versions() {
        assert_eq!(
            match_versions(
                (ApiVersion(Int16(10)), ApiVersion(Int16(20))),
                (ApiVersion(Int16(10)), ApiVersion(Int16(20))),
            ),
            Some(ApiVersion(Int16(20))),
        );

        assert_eq!(
            match_versions(
                (ApiVersion(Int16(10)), ApiVersion(Int16(15))),
                (ApiVersion(Int16(13)), ApiVersion(Int16(20))),
            ),
            Some(ApiVersion(Int16(15))),
        );

        assert_eq!(
            match_versions(
                (ApiVersion(Int16(10)), ApiVersion(Int16(15))),
                (ApiVersion(Int16(15)), ApiVersion(Int16(20))),
            ),
            Some(ApiVersion(Int16(15))),
        );

        assert_eq!(
            match_versions(
                (ApiVersion(Int16(10)), ApiVersion(Int16(14))),
                (ApiVersion(Int16(15)), ApiVersion(Int16(20))),
            ),
            None,
        );
    }
}
