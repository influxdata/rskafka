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

    #[error(
        "Data left at the end of the message. Got {message_size} bytes but only read {read} bytes"
    )]
    TooMuchData { message_size: u64, read: u64 },
}

#[derive(Error, Debug)]
pub enum SyncVersionsError {
    #[error("Did not found a version for ApiVersion that works with that broker")]
    NoWorkingVersion,

    #[error(transparent)]
    RequestError(#[from] RequestError),

    #[error("Got flipped version from server for API key {api_key:?}: min={min:?} max={max:?}")]
    FlippedVersionRange {
        api_key: ApiKey,
        min: ApiVersion,
        max: ApiVersion,
    },
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
                        header.tagged_fields = Some(TaggedFields::read(&mut cursor).unwrap());
                    }

                    // we don't care if the other side is gone
                    active_request
                        .channel
                        .send(Response {
                            header,
                            data: cursor,
                        })
                        .ok();
                } else {
                    println!(
                        "Got response for unknown request: {}",
                        header.correlation_id.0
                    )
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

        // Correlation ID so that we can de-multiplex the responses.
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

        // check if we fully consumed the message, otherwise there might be a bug in our protocol code
        let read_bytes = response.data.position();
        let message_bytes = response.data.into_inner().len() as u64;
        if read_bytes != message_bytes {
            return Err(RequestError::TooMuchData {
                message_size: message_bytes,
                read: read_bytes,
            });
        }

        Ok(body)
    }

    pub async fn sync_versions(&self) -> Result<(), SyncVersionsError> {
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

            match self.request(body).await {
                Ok(response) => {
                    if let Some(e) = response.error_code {
                        println!(
                            "Got error during version sync, cannot use version {} for ApiVersionRequest: {}",
                            upper_bound,
                            e,
                        );
                        continue;
                    }

                    // check range sanity
                    for api_key in &response.api_keys {
                        if api_key.min_version.0 > api_key.max_version.0 {
                            return Err(SyncVersionsError::FlippedVersionRange {
                                api_key: api_key.api_key,
                                min: api_key.min_version,
                                max: api_key.max_version,
                            });
                        }
                    }

                    let ranges = response
                        .api_keys
                        .into_iter()
                        .map(|x| (x.api_key, (x.min_version, x.max_version)))
                        .collect();
                    self.set_version_ranges(ranges);
                    return Ok(());
                }
                Err(RequestError::NoVersionMatch { .. }) => {
                    unreachable!("Just set to version range to a non-empty range")
                }
                Err(RequestError::ReadError(e)) => {
                    println!(
                        "Cannot read ApiVersionResponse for version {}: {}",
                        upper_bound, e,
                    );
                    continue;
                }
                Err(e) => {
                    return Err(SyncVersionsError::RequestError(e));
                }
            }
        }

        Err(SyncVersionsError::NoWorkingVersion)
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
    use std::ops::Deref;

    use assert_matches::assert_matches;
    use tokio::io::DuplexStream;

    use super::*;

    use crate::protocol::{
        error::Error as ApiError,
        messages::{ApiVersionsResponse, ApiVersionsResponseApiKey},
    };

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

    #[tokio::test]
    async fn test_sync_versions_ok() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx);

        // construct response
        let mut msg = vec![];
        ResponseHeader {
            correlation_id: Int32(0),
            tagged_fields: Default::default(),
        }
        .write_versioned(&mut msg, ApiVersion(Int16(1)))
        .unwrap();
        ApiVersionsResponse {
            error_code: None,
            api_keys: vec![ApiVersionsResponseApiKey {
                api_key: ApiKey::Produce,
                min_version: ApiVersion(Int16(1)),
                max_version: ApiVersion(Int16(5)),
                tagged_fields: Default::default(),
            }],
            throttle_time_ms: None,
            tagged_fields: None,
        }
        .write_versioned(&mut msg, ApiVersionsRequest::API_VERSION_RANGE.1)
        .unwrap();
        sim.push(msg);

        // sync versions
        messenger.sync_versions().await.unwrap();
        let expected = HashMap::from([(
            (ApiKey::Produce),
            (ApiVersion(Int16(1)), ApiVersion(Int16(5))),
        )]);
        assert_eq!(messenger.version_ranges.read().unwrap().deref(), &expected);
    }

    #[tokio::test]
    async fn test_sync_versions_ignores_error_code() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx);

        // construct error response
        let mut msg = vec![];
        ResponseHeader {
            correlation_id: Int32(0),
            tagged_fields: Default::default(),
        }
        .write_versioned(&mut msg, ApiVersion(Int16(1)))
        .unwrap();
        ApiVersionsResponse {
            error_code: Some(ApiError::CorruptMessage),
            api_keys: vec![ApiVersionsResponseApiKey {
                api_key: ApiKey::Produce,
                min_version: ApiVersion(Int16(2)),
                max_version: ApiVersion(Int16(3)),
                tagged_fields: Default::default(),
            }],
            throttle_time_ms: None,
            tagged_fields: None,
        }
        .write_versioned(&mut msg, ApiVersionsRequest::API_VERSION_RANGE.1)
        .unwrap();
        sim.push(msg);

        // construct good response
        let mut msg = vec![];
        ResponseHeader {
            correlation_id: Int32(1),
            tagged_fields: Default::default(),
        }
        .write_versioned(&mut msg, ApiVersion(Int16(0)))
        .unwrap();
        ApiVersionsResponse {
            error_code: None,
            api_keys: vec![ApiVersionsResponseApiKey {
                api_key: ApiKey::Produce,
                min_version: ApiVersion(Int16(1)),
                max_version: ApiVersion(Int16(5)),
                tagged_fields: Default::default(),
            }],
            throttle_time_ms: None,
            tagged_fields: None,
        }
        .write_versioned(
            &mut msg,
            ApiVersion(Int16(ApiVersionsRequest::API_VERSION_RANGE.1 .0 .0 - 1)),
        )
        .unwrap();
        sim.push(msg);

        // sync versions
        messenger.sync_versions().await.unwrap();
        let expected = HashMap::from([(
            (ApiKey::Produce),
            (ApiVersion(Int16(1)), ApiVersion(Int16(5))),
        )]);
        assert_eq!(messenger.version_ranges.read().unwrap().deref(), &expected);
    }

    #[tokio::test]
    async fn test_sync_versions_ignores_read_code() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx);

        // construct error response
        let mut msg = vec![];
        ResponseHeader {
            correlation_id: Int32(0),
            tagged_fields: Default::default(),
        }
        .write_versioned(&mut msg, ApiVersion(Int16(1)))
        .unwrap();
        msg.push(b'\0');
        sim.push(msg);

        // construct good response
        let mut msg = vec![];
        ResponseHeader {
            correlation_id: Int32(1),
            tagged_fields: Default::default(),
        }
        .write_versioned(&mut msg, ApiVersion(Int16(0)))
        .unwrap();
        ApiVersionsResponse {
            error_code: None,
            api_keys: vec![ApiVersionsResponseApiKey {
                api_key: ApiKey::Produce,
                min_version: ApiVersion(Int16(1)),
                max_version: ApiVersion(Int16(5)),
                tagged_fields: Default::default(),
            }],
            throttle_time_ms: None,
            tagged_fields: None,
        }
        .write_versioned(
            &mut msg,
            ApiVersion(Int16(ApiVersionsRequest::API_VERSION_RANGE.1 .0 .0 - 1)),
        )
        .unwrap();
        sim.push(msg);

        // sync versions
        messenger.sync_versions().await.unwrap();
        let expected = HashMap::from([(
            (ApiKey::Produce),
            (ApiVersion(Int16(1)), ApiVersion(Int16(5))),
        )]);
        assert_eq!(messenger.version_ranges.read().unwrap().deref(), &expected);
    }

    #[tokio::test]
    async fn test_sync_versions_err_flipped_range() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx);

        // construct response
        let mut msg = vec![];
        ResponseHeader {
            correlation_id: Int32(0),
            tagged_fields: Default::default(),
        }
        .write_versioned(&mut msg, ApiVersion(Int16(1)))
        .unwrap();
        ApiVersionsResponse {
            error_code: None,
            api_keys: vec![ApiVersionsResponseApiKey {
                api_key: ApiKey::Produce,
                min_version: ApiVersion(Int16(2)),
                max_version: ApiVersion(Int16(1)),
                tagged_fields: Default::default(),
            }],
            throttle_time_ms: None,
            tagged_fields: None,
        }
        .write_versioned(&mut msg, ApiVersionsRequest::API_VERSION_RANGE.1)
        .unwrap();
        sim.push(msg);

        // sync versions
        let err = messenger.sync_versions().await.unwrap_err();
        assert_matches!(err, SyncVersionsError::FlippedVersionRange { .. });
    }

    #[tokio::test]
    async fn test_sync_versions_err_garbage() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx);

        // construct response
        let mut msg = vec![];
        ResponseHeader {
            correlation_id: Int32(0),
            tagged_fields: Default::default(),
        }
        .write_versioned(&mut msg, ApiVersion(Int16(1)))
        .unwrap();
        ApiVersionsResponse {
            error_code: None,
            api_keys: vec![ApiVersionsResponseApiKey {
                api_key: ApiKey::Produce,
                min_version: ApiVersion(Int16(1)),
                max_version: ApiVersion(Int16(2)),
                tagged_fields: Default::default(),
            }],
            throttle_time_ms: None,
            tagged_fields: None,
        }
        .write_versioned(&mut msg, ApiVersionsRequest::API_VERSION_RANGE.1)
        .unwrap();
        msg.push(b'\0');
        sim.push(msg);

        // sync versions
        let err = messenger.sync_versions().await.unwrap_err();
        assert_matches!(err, SyncVersionsError::RequestError(..));
    }

    #[tokio::test]
    async fn test_sync_versions_err_no_working_version() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx);

        // construct error response
        for (i, v) in ((ApiVersionsRequest::API_VERSION_RANGE.0 .0 .0)
            ..=(ApiVersionsRequest::API_VERSION_RANGE.1 .0 .0))
            .rev()
            .enumerate()
        {
            let mut msg = vec![];
            ResponseHeader {
                correlation_id: Int32(i as i32),
                tagged_fields: Default::default(),
            }
            .write_versioned(&mut msg, ApiVersion(Int16(0)))
            .unwrap();
            ApiVersionsResponse {
                error_code: Some(ApiError::CorruptMessage),
                api_keys: vec![ApiVersionsResponseApiKey {
                    api_key: ApiKey::Produce,
                    min_version: ApiVersion(Int16(1)),
                    max_version: ApiVersion(Int16(5)),
                    tagged_fields: Default::default(),
                }],
                throttle_time_ms: None,
                tagged_fields: None,
            }
            .write_versioned(&mut msg, ApiVersion(Int16(v)))
            .unwrap();
            sim.push(msg);
        }

        // sync versions
        let err = messenger.sync_versions().await.unwrap_err();
        assert_matches!(err, SyncVersionsError::NoWorkingVersion);
    }

    struct MessageSimulator {
        messages: Arc<RwLock<Vec<Vec<u8>>>>,
        join_handle: JoinHandle<()>,
    }

    impl MessageSimulator {
        fn new() -> (Self, DuplexStream) {
            let (mut tx, rx) = tokio::io::duplex(1_000);
            let messages: Arc<RwLock<Vec<Vec<u8>>>> = Arc::new(RwLock::new(vec![]));

            let messages_captured = Arc::clone(&messages);
            let join_handle = tokio::task::spawn(async move {
                loop {
                    tx.read_message(1_000).await.unwrap();

                    let message = {
                        let mut messages = messages_captured.write().unwrap();
                        messages.remove(0)
                    };
                    tx.write_message(&message).await.unwrap();
                }
            });

            let this = Self {
                messages,
                join_handle,
            };
            (this, rx)
        }

        fn push(&self, msg: Vec<u8>) {
            self.messages.write().unwrap().push(msg);
        }
    }

    impl Drop for MessageSimulator {
        fn drop(&mut self) {
            // this will drop the future and therefore tx which will close th streams
            self.join_handle.abort();
        }
    }
}
