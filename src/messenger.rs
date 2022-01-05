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

use crate::protocol::primitives::CompactString;
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
use crate::protocol::{messages::ApiVersionsRequest, traits::ReadType};

struct Response {
    #[allow(dead_code)]
    header: ResponseHeader,
    data: Cursor<Vec<u8>>,
}

struct ActiveRequest {
    channel: Sender<Response>,
    use_tagged_fields: bool,
}

/// A connection to a single broker
///
/// Note: Requests to the same [`Messenger`] will be pipelined by Kafka
///
pub struct Messenger<RW> {
    stream_write: Mutex<WriteHalf<RW>>,
    correlation_id: AtomicI32,
    version_ranges: RwLock<HashMap<ApiKey, (ApiVersion, ApiVersion)>>,
    active_requests: Arc<Mutex<HashMap<i32, ActiveRequest>>>,
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
        let active_requests: Arc<Mutex<HashMap<i32, ActiveRequest>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let active_requests_captured = Arc::clone(&active_requests);

        let join_handle = tokio::spawn(async move {
            let mut stream_read = stream_read;

            loop {
                // TODO: don't unwrap and silently kill the background worker
                let msg = stream_read.read_message(1024 * 1024).await.unwrap();
                let mut cursor = Cursor::new(msg);

                // read header as version 0 (w/o tagged fields) first since this is a strict prefix or the more advanced
                // header version
                let mut header =
                    ResponseHeader::read_versioned(&mut cursor, ApiVersion(Int16(0))).unwrap();

                if let Some(active_request) = active_requests_captured
                    .lock()
                    .await
                    .remove(&header.correlation_id.0)
                {
                    // optionally read tagged fields from the header as well
                    if active_request.use_tagged_fields {
                        header.tagged_fields = TaggedFields::read(&mut cursor).unwrap();
                    }

                    // we don't care if the other side is gone
                    active_request
                        .channel
                        .send(Response {
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
            active_requests,
            join_handle,
        }
    }

    fn set_version_ranges(&self, ranges: HashMap<ApiKey, (ApiVersion, ApiVersion)>) {
        *self.version_ranges.write().expect("lock poisoned") = ranges;
    }

    pub async fn request<R>(&self, msg: R) -> Result<R::ResponseBody, RequestError>
    where
        R: RequestBody + WriteVersionedType<Vec<u8>>,
        R::ResponseBody: ReadVersionedType<Cursor<Vec<u8>>>,
    {
        let body_api_version = self
            .version_ranges
            .read()
            .expect("lock poisoned")
            .get(&R::API_KEY)
            .map(|range_server| match_versions(*range_server, R::API_VERSION_RANGE))
            .flatten()
            .ok_or(RequestError::NoVersionMatch {
                api_key: R::API_KEY,
            })?;

        // determine if our request and response headers shall contain tagged fields. This system is borrowed from
        // rdkafka ("flexver"), see:
        // - https://github.com/edenhill/librdkafka/blob/2b76b65212e5efda213961d5f84e565038036270/src/rdkafka_request.c#L973
        // - https://github.com/edenhill/librdkafka/blob/2b76b65212e5efda213961d5f84e565038036270/src/rdkafka_buf.c#L167-L174
        let use_tagged_fields = body_api_version >= R::FIRST_TAGGED_FIELD_VERSION;

        // Correlcation ID so that we can de-multiplex the responses.
        let correlation_id = self.correlation_id.fetch_add(1, Ordering::SeqCst);

        let header = RequestHeader {
            request_api_key: R::API_KEY,
            request_api_version: body_api_version,
            correlation_id: Int32(correlation_id),
            client_id: NullableString(None),
            tagged_fields: TaggedFields::default(),
        };
        let header_version = if use_tagged_fields {
            ApiVersion(Int16(2))
        } else {
            ApiVersion(Int16(1))
        };

        let mut buf = Vec::new();
        header
            .write_versioned(&mut buf, header_version)
            .expect("Writing header to buffer should always work");
        msg.write_versioned(&mut buf, body_api_version)?;

        let (tx, rx) = channel();
        self.active_requests.lock().await.insert(
            correlation_id,
            ActiveRequest {
                channel: tx,
                use_tagged_fields,
            },
        );

        {
            let mut stream_write = self.stream_write.lock().await;
            stream_write.write_message(&buf).await?;
            stream_write.flush().await?;
        }

        let mut response = rx.await.expect("Who closed this channel?!");
        let body = R::ResponseBody::read_versioned(&mut response.data, body_api_version)?;

        Ok(body)
    }

    pub async fn sync_versions(&self) {
        for upper_bound in (ApiVersionsRequest::API_VERSION_RANGE.0 .0 .0
            ..=ApiVersionsRequest::API_VERSION_RANGE.1 .0 .0)
            .rev()
        {
            self.set_version_ranges(HashMap::from([(
                ApiKey::ApiVersions,
                (
                    ApiVersionsRequest::API_VERSION_RANGE.0,
                    ApiVersion(Int16(upper_bound)),
                ),
            )]));

            let body = ApiVersionsRequest {
                client_software_name: CompactString(String::from("")),
                client_software_version: CompactString(String::from("")),
                tagged_fields: TaggedFields::default(),
            };

            if let Ok(response) = self.request(body).await {
                if response.error_code.is_some() {
                    continue;
                }

                // TODO: check min and max are sane
                let ranges = response
                    .api_keys
                    .into_iter()
                    .map(|x| (x.api_key, (x.min_version, x.max_version)))
                    .collect();
                self.set_version_ranges(ranges);
                return;
            }
            // TODO: don't ignore all errors (e.g. IO errors)
        }

        // TODO: don't panic
        panic!("cannot sync")
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
