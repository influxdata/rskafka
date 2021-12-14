use std::collections::HashMap;

use tokio::{
    io::BufStream,
    net::{TcpStream, ToSocketAddrs},
};

use crate::{
    messenger::Messenger,
    protocol::{
        api_key::ApiKey,
        api_version::ApiVersion,
        messages::{ApiVersionsRequest, RequestBody},
        primitives::{CompactString, Int16, TaggedFields},
    },
};

pub struct Client {
    messenger: Messenger<BufStream<TcpStream>>,
}

impl Client {
    pub async fn new<A>(addr: A) -> Self
    where
        A: ToSocketAddrs,
    {
        let stream = TcpStream::connect(addr).await.unwrap();
        let stream = BufStream::new(stream);
        let messenger = Messenger::new(stream);
        sync_versions(&messenger).await;

        Self { messenger }
    }
}

async fn sync_versions(messenger: &Messenger<BufStream<TcpStream>>) {
    for upper_bound in (ApiVersionsRequest::API_VERSION_RANGE.0 .0 .0
        ..=ApiVersionsRequest::API_VERSION_RANGE.1 .0 .0)
        .rev()
    {
        messenger.set_version_ranges(HashMap::from([(
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

        if let Ok(response) = messenger.request(body).await {
            if response.error_code.is_some() {
                continue;
            }

            // TODO: check min and max are sane
            let ranges = response
                .api_keys
                .into_iter()
                .map(|x| (x.api_key, (x.min_version, x.max_version)))
                .collect();
            messenger.set_version_ranges(ranges);
            return;
        }
    }

    panic!("cannot sync")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test() {
        let mut client = Client::new("localhost:9093").await;
    }
}
