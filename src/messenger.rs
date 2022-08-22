use std::{
    collections::HashMap,
    future::Future,
    io::Cursor,
    ops::DerefMut,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc, RwLock,
    },
    task::Poll,
};

use futures::future::BoxFuture;
use parking_lot::Mutex;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, WriteHalf},
    sync::{
        oneshot::{channel, Sender},
        Mutex as AsyncMutex,
    },
    task::JoinHandle,
};
use tracing::{debug, warn};

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
use crate::protocol::{api_version::ApiVersionRange, primitives::CompactString};
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
    use_tagged_fields_in_response: bool,
}

#[derive(Debug)]
enum MessengerState {
    /// Currently active requests by correlation ID.
    ///
    /// An active request is one that got prepared or send but the response wasn't received yet.
    RequestMap(HashMap<i32, ActiveRequest>),

    /// One or our streams died and we are unable to process any more requests.
    Poison(Arc<RequestError>),
}

impl MessengerState {
    fn poison(&mut self, err: RequestError) -> Arc<RequestError> {
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

                *self = Self::Poison(Arc::clone(&err));
                err
            }
            Self::Poison(e) => {
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
    stream_write: Arc<AsyncMutex<WriteHalf<RW>>>,

    /// The next correlation ID.
    ///
    /// This is used to map responses to active requests.
    correlation_id: AtomicI32,

    /// Version ranges that we think are supported by the broker.
    ///
    /// This needs to be bootstrapped by [`sync_versions`](Self::sync_versions).
    version_ranges: RwLock<HashMap<ApiKey, ApiVersionRange>>,

    /// Current stream state.
    ///
    /// Note that this and `stream_write` are separate struct to allow sending and receiving data concurrently.
    state: Arc<Mutex<MessengerState>>,

    /// Join handle for the background worker that fetches responses.
    join_handle: JoinHandle<()>,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum RequestError {
    #[error("Cannot find matching version for: {api_key:?}")]
    NoVersionMatch { api_key: ApiKey },

    #[error("Cannot write data: {0}")]
    WriteError(#[from] WriteVersionedError),

    #[error("Cannot write versioned data: {0}")]
    WriteMessageError(#[from] crate::protocol::frame::WriteError),

    #[error("Cannot read data: {0}")]
    ReadError(#[from] crate::protocol::traits::ReadError),

    #[error("Cannot read versioned data: {0}")]
    ReadVersionedError(#[from] ReadVersionedError),

    #[error("Cannot read/write data: {0}")]
    IO(#[from] std::io::Error),

    #[error(
        "Data left at the end of the message. Got {message_size} bytes but only read {read} bytes. api_key={api_key:?} api_version={api_version}"
    )]
    TooMuchData {
        message_size: u64,
        read: u64,
        api_key: ApiKey,
        api_version: ApiVersion,
    },

    #[error("Cannot read framed message: {0}")]
    ReadFramedMessageError(#[from] crate::protocol::frame::ReadError),

    #[error("Connection is poisoned: {0}")]
    Poisoned(Arc<RequestError>),
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum SyncVersionsError {
    #[error("Did not found a version for ApiVersion that works with that broker")]
    NoWorkingVersion,

    #[error("Request error: {0}")]
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
                        // message was read, so all subsequent errors should not poison the whole stream
                        let mut cursor = Cursor::new(msg);

                        // read header as version 0 (w/o tagged fields) first since this is a strict prefix or the more advanced
                        // header version
                        let mut header =
                            match ResponseHeader::read_versioned(&mut cursor, ApiVersion(Int16(0)))
                            {
                                Ok(header) => header,
                                Err(e) => {
                                    warn!(%e, "Cannot read message header, ignoring message");
                                    continue;
                                }
                            };

                        let active_request = match state_captured.lock().deref_mut() {
                            MessengerState::RequestMap(map) => {
                                if let Some(active_request) = map.remove(&header.correlation_id.0) {
                                    active_request
                                } else {
                                    warn!(
                                        correlation_id = header.correlation_id.0,
                                        "Got response for unknown request",
                                    );
                                    continue;
                                }
                            }
                            MessengerState::Poison(_) => {
                                // stream is poisoned, no need to anything
                                return;
                            }
                        };

                        // optionally read tagged fields from the header as well
                        if active_request.use_tagged_fields_in_response {
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
                            .poison(RequestError::ReadFramedMessageError(e));
                        return;
                    }
                }
            }
        });

        Self {
            stream_write: Arc::new(AsyncMutex::new(stream_write)),
            correlation_id: AtomicI32::new(0),
            version_ranges: RwLock::new(HashMap::new()),
            state,
            join_handle,
        }
    }

    #[cfg(feature = "unstable-fuzzing")]
    pub fn override_version_ranges(&self, ranges: HashMap<ApiKey, ApiVersionRange>) {
        self.set_version_ranges(ranges);
    }

    fn set_version_ranges(&self, ranges: HashMap<ApiKey, ApiVersionRange>) {
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
            .and_then(|range_server| match_versions(*range_server, R::API_VERSION_RANGE))
            .ok_or(RequestError::NoVersionMatch {
                api_key: R::API_KEY,
            })?;

        // determine if our request and response headers shall contain tagged fields. This system is borrowed from
        // rdkafka ("flexver"), see:
        // - https://github.com/edenhill/librdkafka/blob/2b76b65212e5efda213961d5f84e565038036270/src/rdkafka_request.c#L973
        // - https://github.com/edenhill/librdkafka/blob/2b76b65212e5efda213961d5f84e565038036270/src/rdkafka_buf.c#L167-L174
        let use_tagged_fields_in_request =
            body_api_version >= R::FIRST_TAGGED_FIELD_IN_REQUEST_VERSION;
        let use_tagged_fields_in_response =
            body_api_version >= R::FIRST_TAGGED_FIELD_IN_RESPONSE_VERSION;

        // Correlation ID so that we can de-multiplex the responses.
        let correlation_id = self.correlation_id.fetch_add(1, Ordering::SeqCst);

        let header = RequestHeader {
            request_api_key: R::API_KEY,
            request_api_version: body_api_version,
            correlation_id: Int32(correlation_id),
            // Technically we don't need to send a client_id, but newer redpanda version fail to parse the message
            // without it. See https://github.com/influxdata/rskafka/issues/169 .
            client_id: Some(NullableString(Some(String::from(env!("CARGO_PKG_NAME"))))),
            tagged_fields: Some(TaggedFields::default()),
        };
        let header_version = if use_tagged_fields_in_request {
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

        // to prevent stale data in inner state, ensure that we would remove the request again if we are cancelled while
        // sending the request
        let cleanup_on_cancel =
            CleanupRequestStateOnCancel::new(Arc::clone(&self.state), correlation_id);

        match self.state.lock().deref_mut() {
            MessengerState::RequestMap(map) => {
                map.insert(
                    correlation_id,
                    ActiveRequest {
                        channel: tx,
                        use_tagged_fields_in_response,
                    },
                );
            }
            MessengerState::Poison(e) => {
                return Err(RequestError::Poisoned(Arc::clone(e)));
            }
        }

        self.send_message(buf).await?;
        cleanup_on_cancel.message_sent();

        let mut response = rx.await.expect("Who closed this channel?!")?;
        let body = R::ResponseBody::read_versioned(&mut response.data, body_api_version)?;

        // check if we fully consumed the message, otherwise there might be a bug in our protocol code
        let read_bytes = response.data.position();
        let message_bytes = response.data.into_inner().len() as u64;
        if read_bytes != message_bytes {
            return Err(RequestError::TooMuchData {
                message_size: message_bytes,
                read: read_bytes,
                api_key: R::API_KEY,
                api_version: body_api_version,
            });
        }

        Ok(body)
    }

    async fn send_message(&self, msg: Vec<u8>) -> Result<(), RequestError> {
        match self.send_message_inner(msg).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // need to poison the stream because message framing might be out-of-sync
                let mut state = self.state.lock();
                Err(RequestError::Poisoned(state.poison(e)))
            }
        }
    }

    async fn send_message_inner(&self, msg: Vec<u8>) -> Result<(), RequestError> {
        let mut stream_write = Arc::clone(&self.stream_write).lock_owned().await;

        // use a wrapper so that cancelation doesn't cancel the send operation and leaves half-send messages on the wire
        let fut = CancellationSafeFuture::new(async move {
            stream_write.write_message(&msg).await?;
            stream_write.flush().await?;
            Ok(())
        });

        fut.await
    }

    pub async fn sync_versions(&self) -> Result<(), SyncVersionsError> {
        for upper_bound in (ApiVersionsRequest::API_VERSION_RANGE.min().0 .0
            ..=ApiVersionsRequest::API_VERSION_RANGE.max().0 .0)
            .rev()
        {
            self.set_version_ranges(HashMap::from([(
                ApiKey::ApiVersions,
                ApiVersionRange::new(
                    ApiVersionsRequest::API_VERSION_RANGE.min(),
                    ApiVersion(Int16(upper_bound)),
                ),
            )]));

            let body = ApiVersionsRequest {
                client_software_name: Some(CompactString(String::from(env!("CARGO_PKG_NAME")))),
                client_software_version: Some(CompactString(String::from(env!(
                    "CARGO_PKG_VERSION"
                )))),
                tagged_fields: Some(TaggedFields::default()),
            };

            match self.request(body).await {
                Ok(response) => {
                    if let Some(e) = response.error_code {
                        debug!(
                            %e,
                            version=upper_bound,
                            "Got error during version sync, cannot use version for ApiVersionRequest",
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
                        .map(|x| {
                            (
                                x.api_key,
                                ApiVersionRange::new(x.min_version, x.max_version),
                            )
                        })
                        .collect();
                    debug!(
                        versions=%sorted_ranges_repr(&ranges),
                        "Detected supported broker versions",
                    );
                    self.set_version_ranges(ranges);
                    return Ok(());
                }
                Err(RequestError::NoVersionMatch { .. }) => {
                    unreachable!("Just set to version range to a non-empty range")
                }
                Err(RequestError::ReadVersionedError(e)) => {
                    debug!(
                        %e,
                        version=upper_bound,
                        "Cannot read ApiVersionResponse for version",
                    );
                    continue;
                }
                Err(RequestError::ReadError(e)) => {
                    debug!(
                        %e,
                        version=upper_bound,
                        "Cannot read ApiVersionResponse for version",
                    );
                    continue;
                }
                Err(e @ RequestError::TooMuchData { .. }) => {
                    debug!(
                        %e,
                        version=upper_bound,
                        "Cannot read ApiVersionResponse for version",
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

fn sorted_ranges_repr(ranges: &HashMap<ApiKey, ApiVersionRange>) -> String {
    let mut ranges: Vec<_> = ranges.iter().map(|(key, range)| (*key, *range)).collect();
    ranges.sort_by_key(|(key, _range)| *key);
    let ranges: Vec<_> = ranges
        .into_iter()
        .map(|(key, range)| format!("{:?}: {}", key, range))
        .collect();
    ranges.join(", ")
}

fn match_versions(range_a: ApiVersionRange, range_b: ApiVersionRange) -> Option<ApiVersion> {
    if range_a.min() <= range_b.max() && range_b.min() <= range_a.max() {
        Some(range_a.max().min(range_b.max()))
    } else {
        None
    }
}

/// Helper that ensures that a request is removed when a request is cancelled before it was actually sent out.
struct CleanupRequestStateOnCancel {
    state: Arc<Mutex<MessengerState>>,
    correlation_id: i32,
    message_sent: bool,
}

impl CleanupRequestStateOnCancel {
    /// Create new helper.
    ///
    /// You must call [`message_sent`](Self::message_sent) when the request was sent.
    fn new(state: Arc<Mutex<MessengerState>>, correlation_id: i32) -> Self {
        Self {
            state,
            correlation_id,
            message_sent: false,
        }
    }

    /// Request was sent. Do NOT clean the state any longer.
    fn message_sent(mut self) {
        self.message_sent = true;
    }
}

impl Drop for CleanupRequestStateOnCancel {
    fn drop(&mut self) {
        if !self.message_sent {
            if let MessengerState::RequestMap(map) = self.state.lock().deref_mut() {
                map.remove(&self.correlation_id);
            }
        }
    }
}

/// Wrapper around a future that cannot be cancelled.
///
/// When the future is dropped/cancelled, we'll spawn a tokio task to _rescue_ it.
struct CancellationSafeFuture<F>
where
    F: Future + Send + 'static,
{
    /// Mark if the inner future finished. If not, we must spawn a helper task on drop.
    done: bool,

    /// Inner future.
    ///
    /// Wrapped in an `Option` so we can extract it during drop. Inside that option however we also need a pinned
    /// box because once this wrapper is polled, it will be pinned in memory -- even during drop. Now the inner
    /// future does not necessarily implement `Unpin`, so we need a heap allocation to pin it in memory even when we
    /// move it out of this option.
    inner: Option<BoxFuture<'static, F::Output>>,
}

impl<F> Drop for CancellationSafeFuture<F>
where
    F: Future + Send + 'static,
{
    fn drop(&mut self) {
        if !self.done {
            let inner = self.inner.take().expect("Double-drop?");
            tokio::task::spawn(async move {
                inner.await;
            });
        }
    }
}

impl<F> CancellationSafeFuture<F>
where
    F: Future + Send,
{
    fn new(fut: F) -> Self {
        Self {
            done: false,
            inner: Some(Box::pin(fut)),
        }
    }
}

impl<F> Future for CancellationSafeFuture<F>
where
    F: Future + Send,
{
    type Output = F::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        match self.inner.as_mut().expect("no dropped").as_mut().poll(cx) {
            Poll::Ready(res) => {
                self.done = true;
                Poll::Ready(res)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, time::Duration};

    use assert_matches::assert_matches;
    use futures::{pin_mut, FutureExt};
    use tokio::{
        io::{AsyncReadExt, DuplexStream},
        sync::{mpsc::UnboundedSender, Barrier},
    };

    use super::*;

    use crate::protocol::{
        error::Error as ApiError,
        messages::{
            ApiVersionsResponse, ApiVersionsResponseApiKey, ListOffsetsRequest, NORMAL_CONSUMER,
        },
        traits::WriteType,
    };

    #[test]
    fn test_match_versions() {
        assert_eq!(
            match_versions(
                ApiVersionRange::new(ApiVersion(Int16(10)), ApiVersion(Int16(20))),
                ApiVersionRange::new(ApiVersion(Int16(10)), ApiVersion(Int16(20))),
            ),
            Some(ApiVersion(Int16(20))),
        );

        assert_eq!(
            match_versions(
                ApiVersionRange::new(ApiVersion(Int16(10)), ApiVersion(Int16(15))),
                ApiVersionRange::new(ApiVersion(Int16(13)), ApiVersion(Int16(20))),
            ),
            Some(ApiVersion(Int16(15))),
        );

        assert_eq!(
            match_versions(
                ApiVersionRange::new(ApiVersion(Int16(10)), ApiVersion(Int16(15))),
                ApiVersionRange::new(ApiVersion(Int16(15)), ApiVersion(Int16(20))),
            ),
            Some(ApiVersion(Int16(15))),
        );

        assert_eq!(
            match_versions(
                ApiVersionRange::new(ApiVersion(Int16(10)), ApiVersion(Int16(14))),
                ApiVersionRange::new(ApiVersion(Int16(15)), ApiVersion(Int16(20))),
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
            tagged_fields: Default::default(), // NOT serialized for ApiVersion!
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
        .write_versioned(&mut msg, ApiVersionsRequest::API_VERSION_RANGE.max())
        .unwrap();
        sim.push(msg);

        // sync versions
        messenger.sync_versions().await.unwrap();
        let expected = HashMap::from([(
            (ApiKey::Produce),
            ApiVersionRange::new(ApiVersion(Int16(1)), ApiVersion(Int16(5))),
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
            tagged_fields: Default::default(), // NOT serialized for ApiVersion!
        }
        .write_versioned(&mut msg, ApiVersion(Int16(0)))
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
        .write_versioned(&mut msg, ApiVersionsRequest::API_VERSION_RANGE.max())
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
            ApiVersion(Int16(ApiVersionsRequest::API_VERSION_RANGE.max().0 .0 - 1)),
        )
        .unwrap();
        sim.push(msg);

        // sync versions
        messenger.sync_versions().await.unwrap();
        let expected = HashMap::from([(
            (ApiKey::Produce),
            ApiVersionRange::new(ApiVersion(Int16(1)), ApiVersion(Int16(5))),
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
            tagged_fields: Default::default(), // NOT serialized for ApiVersion!
        }
        .write_versioned(&mut msg, ApiVersion(Int16(0)))
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
            ApiVersion(Int16(ApiVersionsRequest::API_VERSION_RANGE.max().0 .0 - 1)),
        )
        .unwrap();
        sim.push(msg);

        // sync versions
        messenger.sync_versions().await.unwrap();
        let expected = HashMap::from([(
            (ApiKey::Produce),
            ApiVersionRange::new(ApiVersion(Int16(1)), ApiVersion(Int16(5))),
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
            tagged_fields: Default::default(), // NOT serialized for ApiVersion!
        }
        .write_versioned(&mut msg, ApiVersion(Int16(0)))
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
        .write_versioned(&mut msg, ApiVersionsRequest::API_VERSION_RANGE.max())
        .unwrap();
        sim.push(msg);

        // sync versions
        let err = messenger.sync_versions().await.unwrap_err();
        assert_matches!(err, SyncVersionsError::FlippedVersionRange { .. });
    }

    #[tokio::test]
    async fn test_sync_versions_ignores_garbage() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx, 1_000);

        // construct response
        let mut msg = vec![];
        ResponseHeader {
            correlation_id: Int32(0),
            tagged_fields: Default::default(), // NOT serialized for ApiVersion!
        }
        .write_versioned(&mut msg, ApiVersion(Int16(0)))
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
        .write_versioned(&mut msg, ApiVersionsRequest::API_VERSION_RANGE.max())
        .unwrap();
        msg.push(b'\0'); // add junk to the end of the message to trigger `TooMuchData`
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
            ApiVersion(Int16(ApiVersionsRequest::API_VERSION_RANGE.max().0 .0 - 1)),
        )
        .unwrap();
        sim.push(msg);

        // sync versions
        messenger.sync_versions().await.unwrap();
        let expected = HashMap::from([(
            (ApiKey::Produce),
            ApiVersionRange::new(ApiVersion(Int16(1)), ApiVersion(Int16(5))),
        )]);
        assert_eq!(messenger.version_ranges.read().unwrap().deref(), &expected);
    }

    #[tokio::test]
    async fn test_sync_versions_err_no_working_version() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx, 1_000);

        // construct error response
        for (i, v) in ((ApiVersionsRequest::API_VERSION_RANGE.min().0 .0)
            ..=(ApiVersionsRequest::API_VERSION_RANGE.max().0 .0))
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
    async fn test_poison_hangup() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx, 1_000);
        messenger.set_version_ranges(HashMap::from([(
            ApiKey::ListOffsets,
            ListOffsetsRequest::API_VERSION_RANGE,
        )]));

        sim.hang_up();

        let err = messenger
            .request(ListOffsetsRequest {
                replica_id: NORMAL_CONSUMER,
                isolation_level: None,
                topics: vec![],
            })
            .await
            .unwrap_err();
        assert_matches!(err, RequestError::Poisoned(_));
    }

    #[tokio::test]
    async fn test_poison_negative_message_size() {
        let (sim, rx) = MessageSimulator::new();
        let messenger = Messenger::new(rx, 1_000);
        messenger.set_version_ranges(HashMap::from([(
            ApiKey::ListOffsets,
            ListOffsetsRequest::API_VERSION_RANGE,
        )]));

        sim.negative_message_size();

        let err = messenger
            .request(ListOffsetsRequest {
                replica_id: NORMAL_CONSUMER,
                isolation_level: None,
                topics: vec![],
            })
            .await
            .unwrap_err();
        assert_matches!(err, RequestError::Poisoned(_));

        // follow-up message is broken as well
        let err = messenger
            .request(ListOffsetsRequest {
                replica_id: NORMAL_CONSUMER,
                isolation_level: None,
                topics: vec![],
            })
            .await
            .unwrap_err();
        assert_matches!(err, RequestError::Poisoned(_));
    }

    #[tokio::test]
    async fn test_broken_msg_header_does_not_poison() {
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
            tagged_fields: Default::default(), // NOT serialized for ApiVersion!
        }
        .write_versioned(&mut msg, ApiVersion(Int16(0)))
        .unwrap();
        let resp = ApiVersionsResponse {
            error_code: Some(ApiError::CorruptMessage),
            api_keys: vec![],
            throttle_time_ms: Some(Int32(1)),
            tagged_fields: Some(TaggedFields::default()),
        };
        resp.write_versioned(&mut msg, ApiVersionsRequest::API_VERSION_RANGE.max())
            .unwrap();
        sim.push(msg);

        let actual = messenger
            .request(ApiVersionsRequest {
                client_software_name: Some(CompactString(String::new())),
                client_software_version: Some(CompactString(String::new())),
                tagged_fields: Some(TaggedFields::default()),
            })
            .await
            .unwrap();
        assert_eq!(actual, resp);
    }

    #[tokio::test]
    async fn test_cancel_request() {
        // Use a "virtual" network between a simulated broker and a client. The network is intercepted in the middle to
        // pause it after 3 bytes are sent by the client.
        let (tx_front, rx_middle) = tokio::io::duplex(1);
        let (tx_middle, mut rx_back) = tokio::io::duplex(1);

        let messenger = Messenger::new(tx_front, 1_000);

        // create two barriers:
        // - pause: will be passed after 3 bytes were sent by the client
        // - continue: will be passed to continue client->broker traffic
        //
        // The barriers do NOT affect broker->client traffic.
        //
        // The sizes of the barriers is 2: one for the network simulation task and one for the main/control thread.
        let network_pause = Arc::new(Barrier::new(2));
        let network_pause_captured = Arc::clone(&network_pause);
        let network_continue = Arc::new(Barrier::new(2));
        let network_continue_captured = Arc::clone(&network_continue);
        let handle_network = tokio::spawn(async move {
            // Need to split both directions into read and write halfs so we can run full-duplex in two loops. Otherwise
            // the test might deadlock even though the code is just fine (TCP is full-duplex).
            let (mut rx_middle_read, mut rx_middle_write) = tokio::io::split(rx_middle);
            let (mut tx_middle_read, mut tx_middle_write) = tokio::io::split(tx_middle);

            let direction_client_broker = async {
                for i in 0.. {
                    let mut buf = [0; 1];
                    rx_middle_read.read_exact(&mut buf).await.unwrap();
                    tx_middle_write.write_all(&buf).await.unwrap();

                    if i == 3 {
                        network_pause_captured.wait().await;
                        network_continue_captured.wait().await;
                    }
                }
            };

            let direction_broker_client = async {
                loop {
                    let mut buf = [0; 1];
                    tx_middle_read.read_exact(&mut buf).await.unwrap();
                    rx_middle_write.write_all(&buf).await.unwrap();
                }
            };

            tokio::select! {
                _ = direction_client_broker => {}
                _ = direction_broker_client => {}
            }
        });

        // simulated broker, just reads messages and answers w/ "api versions" responses
        let handle_broker = tokio::spawn(async move {
            for correlation_id in 0.. {
                let data = rx_back.read_message(1_000).await.unwrap();
                let mut data = Cursor::new(data);
                let header =
                    RequestHeader::read_versioned(&mut data, ApiVersion(Int16(1))).unwrap();
                assert_eq!(
                    header,
                    RequestHeader {
                        request_api_key: ApiKey::ApiVersions,
                        request_api_version: ApiVersion(Int16(0)),
                        correlation_id: Int32(correlation_id),
                        client_id: Some(NullableString(Some(String::from(env!("CARGO_PKG_NAME"))))),
                        tagged_fields: None,
                    }
                );
                let body =
                    ApiVersionsRequest::read_versioned(&mut data, ApiVersion(Int16(0))).unwrap();
                assert_eq!(
                    body,
                    ApiVersionsRequest {
                        client_software_name: None,
                        client_software_version: None,
                        tagged_fields: None,
                    }
                );
                assert_eq!(data.position() as usize, data.get_ref().len());

                let mut msg = vec![];
                ResponseHeader {
                    correlation_id: Int32(correlation_id),
                    tagged_fields: Default::default(), // NOT serialized for ApiVersion!
                }
                .write_versioned(&mut msg, ApiVersion(Int16(0)))
                .unwrap();
                let resp = ApiVersionsResponse {
                    error_code: Some(ApiError::CorruptMessage),
                    api_keys: vec![],
                    throttle_time_ms: Some(Int32(1)),
                    tagged_fields: Some(TaggedFields::default()),
                };
                resp.write_versioned(&mut msg, ApiVersionsRequest::API_VERSION_RANGE.min())
                    .unwrap();
                rx_back.write_message(&msg).await.unwrap();
            }
        });

        messenger.set_version_ranges(HashMap::from([(
            ApiKey::ApiVersions,
            ApiVersionRange::new(ApiVersion(Int16(0)), ApiVersion(Int16(0))),
        )]));

        // send first message, this task will be canceled after 3 bytes got sent.
        let task_to_cancel = (async {
            messenger
                .request(ApiVersionsRequest {
                    client_software_name: Some(CompactString(String::from("foo"))),
                    client_software_version: Some(CompactString(String::from("bar"))),
                    tagged_fields: Some(TaggedFields::default()),
                })
                .await
                .unwrap();
        })
        .fuse();

        {
            // cancel when we exit this block
            pin_mut!(task_to_cancel);

            // write exactly 3 bytes via the client and then cancel the task.
            futures::select_biased! {
                _ = &mut task_to_cancel => panic!("should not have finished"),
                _ = network_pause.wait().fuse() => {},
            }
        }

        // continue client->broker traffic
        network_continue.wait().await;

        // IF the original bug in https://github.com/influxdata/rskafka/issues/103 exists, then the following statement
        // will timeout because the broker got garbage and will wait forever to read the message.
        tokio::time::timeout(Duration::from_millis(100), async {
            messenger
                .request(ApiVersionsRequest {
                    client_software_name: Some(CompactString(String::from("foo"))),
                    client_software_version: Some(CompactString(String::from("bar"))),
                    tagged_fields: Some(TaggedFields::default()),
                })
                .await
                .unwrap();
        })
        .await
        .unwrap();

        // clean up helper tasks
        handle_broker.abort();
        handle_network.abort();
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
