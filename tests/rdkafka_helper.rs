use std::time::Duration;

use chrono::{TimeZone, Utc};
use futures::{StreamExt, TryStreamExt};
use rdkafka::{
    ClientConfig, Message, TopicPartitionList,
    consumer::{Consumer, StreamConsumer},
    message::{Header, Headers, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use rskafka::{
    client::partition::Compression,
    record::{Record, RecordAndOffset},
};

/// Produce.
pub async fn produce(
    connection: &[String],
    records: Vec<(String, i32, Record)>,
    compression: Compression,
) -> Vec<i64> {
    // create client
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", bootstrap_servers(connection));
    match compression {
        Compression::NoCompression => {}
        #[cfg(feature = "compression-gzip")]
        Compression::Gzip => {
            cfg.set("compression.codec", "gzip");
        }
        #[cfg(feature = "compression-lz4")]
        Compression::Lz4 => {
            cfg.set("compression.codec", "lz4");
        }
        #[cfg(feature = "compression-snappy")]
        Compression::Snappy => {
            cfg.set("compression.codec", "snappy");
        }
        #[cfg(feature = "compression-zstd")]
        Compression::Zstd => {
            cfg.set("compression.codec", "zstd");
        }
    }
    let client: FutureProducer<_> = cfg.create().unwrap();

    // create record
    let mut offsets = vec![];
    for (topic_name, partition_index, record) in records {
        let mut headers = OwnedHeaders::new();
        for (k, v) in record.headers {
            headers = headers.insert(Header {
                key: &k,
                value: Some(&v),
            });
        }

        let mut f_record = FutureRecord::to(&topic_name)
            .partition(partition_index)
            .headers(headers)
            .timestamp(record.timestamp.timestamp_millis());
        let key_ref: Option<&Vec<u8>> = record.key.as_ref();
        let value_ref: Option<&Vec<u8>> = record.value.as_ref();
        if let Some(key) = key_ref {
            f_record = f_record.key(key);
        }
        if let Some(value) = value_ref {
            f_record = f_record.payload(value);
        }
        let delivery = client.send(f_record, Timeout::Never).await.unwrap();
        offsets.push(delivery.offset);
    }

    offsets
}

/// Consume
pub async fn consume(
    connection: &[String],
    topic_name: &str,
    partition_index: i32,
    n: usize,
) -> Vec<RecordAndOffset> {
    tokio::time::timeout(Duration::from_secs(10), async move {
        loop {
            // create client
            let mut cfg = ClientConfig::new();
            cfg.set("bootstrap.servers", bootstrap_servers(connection));
            cfg.set("message.timeout.ms", "5000");
            cfg.set("group.id", "foo");
            cfg.set("auto.offset.reset", "smallest");
            let client: StreamConsumer<_> = cfg.create().unwrap();

            let mut assignment = TopicPartitionList::new();
            assignment.add_partition(topic_name, partition_index);
            client.assign(&assignment).unwrap();

            let res = client
                .stream()
                .take(n)
                .map_ok(|msg| RecordAndOffset {
                    record: Record {
                        key: msg.key().map(|k| k.to_vec()),
                        value: msg.payload().map(|v| v.to_vec()),
                        headers: msg
                            .headers()
                            .map(|headers| {
                                (0..headers.count())
                                    .map(|i| {
                                        let header = headers.get(i);
                                        (header.key.to_owned(), header.value.unwrap().to_vec())
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                        timestamp: Utc
                            .timestamp_millis_opt(msg.timestamp().to_millis().unwrap_or_default())
                            .unwrap(),
                    },
                    offset: msg.offset(),
                })
                .try_collect()
                .await;

            match res {
                Ok(records) => {
                    return records;
                }
                Err(e) => {
                    // this might happen on a fresh rdkafka node
                    // (e.g. "KafkaError (Message consumption error: NotCoordinator (Broker: Not coordinator))")
                    println!("Encountered rdkafka error while consuming, try again: {e:?}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }
    })
    .await
    .unwrap()
}

fn bootstrap_servers(connection: &[String]) -> String {
    // remove "invalid" server because rdkafka isn't really fault tolerant and will sometimes (but not
    // deterministically) fail to connect
    //
    // Also see https://github.com/influxdata/rskafka/pull/260
    let connection = connection
        .iter()
        .map(|s| s.as_str())
        .filter(|s| !s.contains("invalid"))
        .collect::<Vec<_>>();

    connection.join(",")
}
