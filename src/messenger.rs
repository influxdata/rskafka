use std::{
    collections::HashMap,
    io::Cursor,
    ops::DerefMut,
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

#[derive(Debug)]
struct Response {
    #[allow(dead_code)]
    header: ResponseHeader,
    data: Cursor<Vec<u8>>,
}

#[derive(Debug)]
struct ActiveRequest {
    channel: Sender<Result<Response, RequestError>>,
    use_tagged_fields: bool,
}

#[derive(Debug)]
enum MessengerState {
    /// Currently active requests by correlation ID.
    ///
    /// An active request is one that got prepared or send but the response wasn't received yet.
    RequestMap(HashMap<i32, ActiveRequest>),

    /// One or our streams died and we are unable to process any more requests.
    Poisson(Arc<RequestError>),
}

impl MessengerState {
    async fn poisson(&mut self, err: RequestError) -> Arc<RequestError> {
        match self {
            Self::RequestMap(map) => {
                let err = Arc::new(err);

                // inform all active requests
                for (_correlation_id, active_request) in map.drain() {
                    // it's OK if the other side is gone
                    active_request
                        .channel
                        .send(Err(RequestError::Poisoned(Arc::clone(&err))))
                        .ok();
                }

                *self = Self::Poisson(Arc::clone(&err));
                err
            }
            Self::Poisson(e) => {
                // already poisoned, used existing error
                Arc::clone(e)
            }
        }
    }
}

/// A connection to a single broker
///
/// Note: Requests to the same [`Messenger`] will be pipelined by Kafka
///
#[derive(Debug)]
pub struct Messenger<RW> {
    /// The half of the stream that we use to send data TO the broker.
    ///
    /// This will be used by [`request`](Self::request) to queue up messages.
    stream_write: Mutex<WriteHalf<RW>>,

    /// The next correction ID.
    ///
    /// This is used to map responses to active requests.
    correlation_id: AtomicI32,

    /// Version ranges that we think are supported by the broker.
    ///
    /// This needs to be bootstrapped by [`sync_versions`](Self::sync_versions).
    version_ranges: RwLock<HashMap<ApiKey, (ApiVersion, ApiVersion)>>,

    /// Current stream state.
    ///
    /// Note that this and `stream_write` are separate struct to allow sending and receiving data concurrently.
    state: Arc<Mutex<MessengerState>>,

    /// Join handle for the background worker that fetches responses.
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
    ReadError(#[from] crate::protocol::traits::ReadError),

    #[error(transparent)]
    ReadVersionedError(#[from] ReadVersionedError),

    #[error("Cannot read/write data")]
    IO(#[from] std::io::Error),

    #[error(
        "Data left at the end of the message. Got {message_size} bytes but only read {read} bytes"
    )]
    TooMuchData { message_size: u64, read: u64 },

    #[error(transparent)]
    ReadMessageError(#[from] crate::protocol::frame::ReadError),

    #[error(transparent)]
    Poisoned(Arc<RequestError>),
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
    pub fn new(stream: RW, max_message_size: usize) -> Self {
        let (stream_read, stream_write) = tokio::io::split(stream);
        let state = Arc::new(Mutex::new(MessengerState::RequestMap(HashMap::default())));
        let state_captured = Arc::clone(&state);

        let join_handle = tokio::spawn(async move {
            let mut stream_read = stream_read;

            loop {
                match stream_read.read_message(max_message_size).await {
                    Ok(msg) => {
                        // message was read, so all subsequent errors should not poisson the whole stream
                        let mut cursor = Cursor::new(msg);

                        // read header as version 0 (w/o tagged fields) first since this is a strict prefix or the more advanced
                        // header version
                        let mut header =
                            match ResponseHeader::read_versioned(&mut cursor, ApiVersion(Int16(0)))
                            {
                                Ok(header) => header,
                                Err(e) => {
                                    println!("Cannot read message header, ignoring message: {}", e);
                                    continue;
                                }
                            };

                        let active_request = match state_captured.lock().await.deref_mut() {
                            MessengerState::RequestMap(map) => {
                                if let Some(active_request) = map.remove(&header.correlation_id.0) {
                                    active_request
                                } else {
                                    println!(
                                        "Got response for unknown request: {}",
                                        header.correlation_id.0
                                    );
                                    continue;
                                }
                            }
                            MessengerState::Poisson(_) => {
                                // stream is poisoned, no need to anything
                                return;
                            }
                        };

                        // optionally read tagged fields from the header as well
                        if active_request.use_tagged_fields {
                            header.tagged_fields = match TaggedFields::read(&mut cursor) {
                                Ok(fields) => Some(fields),
                                Err(e) => {
                                    // we don't care if the other side is gone
                                    active_request
                                        .channel
                                        .send(Err(RequestError::ReadError(e)))
                                        .ok();
                                    continue;
                                }
                            };
                        }

                        // we don't care if the other side is gone
                        active_request
                            .channel
                            .send(Ok(Response {
                                header,
                                data: cursor,
                            }))
                            .ok();
                    }
                    Err(e) => {
                        state_captured
                            .lock()
                            .await
                            .poisson(RequestError::ReadMessageError(e))
                            .await;
                        return;
                    }
                }
            }
        });

        Self {
            stream_write: Mutex::new(stream_write),
            correlation_id: AtomicI32::new(0),
            version_ranges: RwLock::new(HashMap::new()),
            state,
            join_handle,
        }
    }

    fn set_version_ranges(&self, ranges: HashMap<ApiKey, (ApiVersion, ApiVersion)>) {
        *self.version_ranges.write().expect("lock poisoned") = ranges;
    }

    pub async fn request<R>(&self, msg: R) -> Result<R::ResponseBody, RequestError>
    where
        R: RequestBody + Send + WriteVersionedType<Vec<u8>>,
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

        match self.state.lock().await.deref_mut() {
            MessengerState::RequestMap(map) => {
                map.insert(
                    correlation_id,
                    ActiveRequest {
                        channel: tx,
                        use_tagged_fields,
                    },
                );
            }
            MessengerState::Poisson(e) => {
                return Err(RequestError::Poisoned(Arc::clone(e)));
            }
        }

        self.send_message(&buf).await?;

        let mut response = rx.await.expect("Who closed this channel?!")?;
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

    async fn send_message(&self, msg: &[u8]) -> Result<(), RequestError> {
        match self.send_message_inner(msg).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // need to poisson the stream because message framing might be out-of-sync
                let mut state = self.state.lock().await;
                Err(RequestError::Poisoned(state.poisson(e).await))
            }
        }
    }

    async fn send_message_inner(&self, msg: &[u8]) -> Result<(), RequestError> {
        let mut stream_write = self.stream_write.lock().await;
        stream_write.write_message(msg).await?;
        stream_write.flush().await?;
        Ok(())
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
                Err(RequestError::ReadVersionedError(e)) => {
                    println!(
                        "Cannot read ApiVersionResponse for version {}: {}",
                        upper_bound, e,
                    );
                    continue;
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
    use tokio::{io::DuplexStream, sync::mpsc::UnboundedSender};

    use super::*;

    use crate::protocol::{
        error::Error as ApiError,
        messages::{ApiVersionsResponse, ApiVersionsResponseApiKey, ListOffsetsRequest},
        traits::WriteType,
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
        let messenger = Messenger::new(rx, 1_000);

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
        let messenger = Messenger::new(rx, 1_000);

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
        let messenger = Messenger::new(rx, 1_000);

        // construct error response
        let mut msg = vec![];
        ResponseHeader {
            correlation_id: Int32(0),
            tagged_fields: Default::default(),
        }
        .write_versioned(&mut msg, ApiVersion(Int16(1)))
        .unwrap();
        msg.push(b'\0'); // malformed message body which can happen if the server doesn't really support this version
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
        let messenger = Messenger::new(rx, 1_000);

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
        let messenger = Messenger::new(rx, 1_000);

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
        msg.push(b'\0'); // add junk to the end of the message to trigger `TooMuchData`
        sim.push(msg);

        // sync versions
        let err = messenger.sync_versions().await.unwrap_err();
        assert_matches!(
            err,
            SyncVersionsError::RequestError(RequestError::TooMuchData { .. })
        );
    }

    #[tokio::test]
    async fn test_sync_versions_err_no_working_version() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx, 1_000);

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

    #[tokio::test]
    async fn test_poisson_hangup() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx, 1_000);
        messenger.set_version_ranges(HashMap::from([(
            ApiKey::ListOffsets,
            ListOffsetsRequest::API_VERSION_RANGE,
        )]));

        sim.hang_up();

        let err = messenger
            .request(ListOffsetsRequest {
                replica_id: Int32(-1),
                isolation_level: None,
                topics: vec![],
            })
            .await
            .unwrap_err();
        assert_matches!(err, RequestError::Poisoned(_));
    }

    #[tokio::test]
    async fn test_poisson_negative_message_size() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx, 1_000);
        messenger.set_version_ranges(HashMap::from([(
            ApiKey::ListOffsets,
            ListOffsetsRequest::API_VERSION_RANGE,
        )]));

        sim.negative_message_size();

        let err = messenger
            .request(ListOffsetsRequest {
                replica_id: Int32(-1),
                isolation_level: None,
                topics: vec![],
            })
            .await
            .unwrap_err();
        assert_matches!(err, RequestError::Poisoned(_));

        // follow-up message is broken as well
        let err = messenger
            .request(ListOffsetsRequest {
                replica_id: Int32(-1),
                isolation_level: None,
                topics: vec![],
            })
            .await
            .unwrap_err();
        assert_matches!(err, RequestError::Poisoned(_));
    }

    #[tokio::test]
    async fn test_broken_msg_header_does_not_poisson() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx, 1_000);
        messenger.set_version_ranges(HashMap::from([(
            ApiKey::ApiVersions,
            ApiVersionsRequest::API_VERSION_RANGE,
        )]));

        // garbage
        sim.send(b"foo".to_vec());

        // construct good response
        let mut msg = vec![];
        ResponseHeader {
            correlation_id: Int32(0),
            tagged_fields: Default::default(),
        }
        .write_versioned(&mut msg, ApiVersion(Int16(1)))
        .unwrap();
        let resp = ApiVersionsResponse {
            error_code: Some(ApiError::CorruptMessage),
            api_keys: vec![],
            throttle_time_ms: Some(Int32(1)),
            tagged_fields: Some(TaggedFields::default()),
        };
        resp.write_versioned(&mut msg, ApiVersionsRequest::API_VERSION_RANGE.1)
            .unwrap();
        sim.push(msg);

        let actual = messenger
            .request(ApiVersionsRequest {
                client_software_name: CompactString(String::new()),
                client_software_version: CompactString(String::new()),
                tagged_fields: TaggedFields::default(),
            })
            .await
            .unwrap();
        assert_eq!(actual, resp);
    }

    #[derive(Debug)]
    enum Message {
        Send(Vec<u8>),
        Consume,
        NegativeMessageSize,
        HangUp,
    }

    struct MessageSimulator {
        messages: UnboundedSender<Message>,
        join_handle: JoinHandle<()>,
    }

    impl MessageSimulator {
        fn new() -> (Self, DuplexStream) {
            let (mut tx, rx) = tokio::io::duplex(1_000);
            let (msg_tx, mut msg_rx) = tokio::sync::mpsc::unbounded_channel();

            let join_handle = tokio::task::spawn(async move {
                loop {
                    let message = match msg_rx.recv().await {
                        Some(msg) => msg,
                        None => return,
                    };

                    match message {
                        Message::Consume => {
                            tx.read_message(1_000).await.unwrap();
                        }
                        Message::Send(data) => {
                            tx.write_message(&data).await.unwrap();
                        }
                        Message::NegativeMessageSize => {
                            let mut buf = vec![];
                            Int32(-1).write(&mut buf).unwrap();
                            tx.write_all(&buf).await.unwrap()
                        }
                        Message::HangUp => {
                            return;
                        }
                    }
                }
            });

            let this = Self {
                messages: msg_tx,
                join_handle,
            };
            (this, rx)
        }

        fn push(&self, msg: Vec<u8>) {
            // Must wait for the request message before reading the response, otherwise `Messenger` might read
            // our response that doesn't have a correlated request yet and throws it away. This is because
            // servers never send data without being asked to do so.
            self.consume();
            self.send(msg);
        }

        fn consume(&self) {
            self.messages.send(Message::Consume).unwrap();
        }

        fn send(&self, msg: Vec<u8>) {
            self.messages.send(Message::Send(msg)).unwrap();
        }

        fn negative_message_size(&self) {
            self.messages.send(Message::NegativeMessageSize).unwrap();
        }

        fn hang_up(&self) {
            self.messages.send(Message::HangUp).unwrap();
        }
    }

    impl Drop for MessageSimulator {
        fn drop(&mut self) {
            // this will drop the future and therefore tx which will close th streams
            self.join_handle.abort();
        }
    }
}
