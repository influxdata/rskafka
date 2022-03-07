#![no_main]
use std::{collections::HashMap, io::Cursor, time::Duration};

use libfuzzer_sys::fuzz_target;
use pin_project_lite::pin_project;
use rskafka::{
    messenger::Messenger,
    protocol::{
        api_key::ApiKey,
        api_version::{ApiVersion, ApiVersionRange},
        frame::AsyncMessageWrite,
        messages::{
            ApiVersionsRequest, CreateTopicsRequest, FetchRequest, ListOffsetsRequest,
            MetadataRequest, ProduceRequest, ReadVersionedType, RequestBody, WriteVersionedType,
        },
        primitives::{CompactString, Int16, Int32, NullableString, TaggedFields},
        traits::ReadType,
    },
};
use tokio::io::{AsyncRead, AsyncWrite, Sink};

fuzz_target!(|data: &[u8]| {
    driver(data).ok();
});

type Error = Box<dyn std::error::Error>;

fn driver(data: &[u8]) -> Result<(), Error> {
    let mut cursor = Cursor::new(data);

    let api_key = ApiKey::from(Int16::read(&mut cursor)?);
    let api_version = ApiVersion(Int16::read(&mut cursor)?);

    match api_key {
        ApiKey::ApiVersions => send_recv(
            ApiVersionsRequest {
                client_software_name: Some(CompactString(String::new())),
                client_software_version: Some(CompactString(String::new())),
                tagged_fields: Some(TaggedFields::default()),
            },
            cursor,
            api_key,
            api_version,
        ),
        ApiKey::CreateTopics => send_recv(
            CreateTopicsRequest {
                topics: vec![],
                timeout_ms: Int32(0),
                validate_only: None,
                tagged_fields: None,
            },
            cursor,
            api_key,
            api_version,
        ),
        ApiKey::Fetch => send_recv(
            FetchRequest {
                replica_id: Int32(0),
                max_wait_ms: Int32(0),
                min_bytes: Int32(0),
                max_bytes: None,
                isolation_level: None,
                topics: vec![],
            },
            cursor,
            api_key,
            api_version,
        ),
        ApiKey::ListOffsets => send_recv(
            ListOffsetsRequest {
                replica_id: Int32(0),
                isolation_level: None,
                topics: vec![],
            },
            cursor,
            api_key,
            api_version,
        ),
        ApiKey::Metadata => send_recv(
            MetadataRequest {
                topics: None,
                allow_auto_topic_creation: None,
            },
            cursor,
            api_key,
            api_version,
        ),
        ApiKey::Produce => send_recv(
            ProduceRequest {
                transactional_id: NullableString(None),
                acks: Int16(0),
                timeout_ms: Int32(0),
                topic_data: vec![],
            },
            cursor,
            api_key,
            api_version,
        ),
        _ => Err(format!("Fuzzing not implemented for: {:?}", api_key).into()),
    }
}

fn send_recv<T>(
    request: T,
    cursor: Cursor<&[u8]>,
    api_key: ApiKey,
    api_version: ApiVersion,
) -> Result<(), Error>
where
    T: RequestBody + Send + WriteVersionedType<Vec<u8>>,
    T::ResponseBody: ReadVersionedType<Cursor<Vec<u8>>>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("tokio RT setup");

    rt.block_on(async move {
        // determine actual message size
        let pos = cursor.position() as usize;
        let data = cursor.into_inner();
        let message_size = data.len() - pos;

        // setup transport
        // Note: allocate a 32bits more to fit the message size marker in
        // Note: write message and let rskafka generate the size marker to help the fuzzer a bit
        let mut transport_data = Vec::with_capacity(message_size + 4);
        tokio::time::timeout(
            Duration::from_secs(1),
            transport_data.write_message(&data[pos..]),
        )
        .await
        .expect("no timeout while writing data")
        .expect("write transport data");
        let transport = MockTransport::new(transport_data);

        // setup messenger
        let messenger = Messenger::new(transport, message_size);
        messenger.override_version_ranges(HashMap::from([(
            api_key,
            ApiVersionRange::new(api_version, api_version),
        )]));

        // the actual request
        tokio::time::timeout(Duration::from_millis(1), messenger.request(request))
            .await
            .expect("request timeout")?;

        Ok(())
    })
}

pin_project! {
    /// One-way mock transport with limited data.
    ///
    /// Can only be read. Writes go to `/dev/null`.
    struct MockTransport {
        #[pin]
        data: Cursor<Vec<u8>>,
        #[pin]
        sink: Sink,
    }
}

impl MockTransport {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data: Cursor::new(data),
            sink: tokio::io::sink(),
        }
    }
}

impl AsyncWrite for MockTransport {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        self.project().sink.poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        self.project().sink.poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        self.project().sink.poll_shutdown(cx)
    }
}

impl AsyncRead for MockTransport {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.project().data.poll_read(cx, buf)
    }
}
