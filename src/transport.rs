use std::io::Cursor;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpStream, ToSocketAddrs},
};

use crate::protocol::{
    primitives::Int32,
    traits::{ReadType, WriteType}, frame::MessageFramer,
};

pub struct Client {
    stream: MessageFramer<BufStream<TcpStream>>,
}

impl Client {
    pub async fn new<A>(addr: A) -> Self
    where
        A: ToSocketAddrs,
    {
        let stream = TcpStream::connect(addr).await.unwrap();
        let stream = BufStream::new(stream);
        let stream = MessageFramer::new(stream, 1024 * 1024 * 1024);

        Self { stream }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use tokio::pin;

    use crate::protocol::{messages::{RequestHeader, ApiVersionsRequest, WriteVersionedType, ResponseHeader, ReadVersionedType, ApiVersionsResponse}, api_key::{ApiKey, self}, api_version::{ApiVersion, self}, primitives::{Int16, NullableString, TaggedFields, CompactString}};

    use super::*;

    #[tokio::test]
    async fn test() {
        let mut client = Client::new("localhost:9093").await;
        let body_api_version = ApiVersion(Int16(3));
        let header = RequestHeader {
            request_api_key: ApiKey::ApiVersions,
            request_api_version: body_api_version,
            correlation_id: Int32(0),
            client_id: NullableString(None),
            tagged_fields: TaggedFields::default(),
        };
        let body = ApiVersionsRequest {
            client_software_name: CompactString(String::from("")),
            client_software_version: CompactString(String::from("")),
            tagged_fields: TaggedFields::default(),
        };

        let mut buf = Vec::new();
        header.write_versioned(&mut buf, ApiVersion(Int16(2))).unwrap();
        body.write_versioned(&mut buf, ApiVersion(Int16(3))).unwrap();

        Pin::new(&mut client.stream).write_message(&buf).await.unwrap();
        client.stream.inner_mut().flush().await.unwrap();

        let mut buf = Cursor::new(Pin::new(&mut client.stream).read_message().await.unwrap());
        let header = ResponseHeader::read_versioned(&mut buf, ApiVersion(Int16(0))).unwrap();
        dbg!(header);
        dbg!(buf.position());
        let body = ApiVersionsResponse::read_versioned(&mut buf, body_api_version).unwrap();
        dbg!(body);
    }
}
