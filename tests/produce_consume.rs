use std::sync::Arc;

use rskafka::{
    client::{
        partition::{Compression, PartitionClient},
        ClientBuilder,
    },
    record::{Record, RecordAndOffset},
};

mod java_helper;
mod rdkafka_helper;
mod test_helpers;

use test_helpers::{maybe_start_logging, now, random_topic_name, record};

#[tokio::test]
async fn test_produce_java_consume_java_nocompression() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_java, consume_java, Compression::NoCompression).await;
}

#[tokio::test]
async fn test_produce_java_consume_rskafka_nocompression() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_java, consume_rskafka, Compression::NoCompression).await;
}

#[tokio::test]
async fn test_produce_rskafka_consume_java_nocompression() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_rskafka, consume_java, Compression::NoCompression).await;
}

#[tokio::test]
async fn test_produce_rdkafka_consume_rdkafka_nocompression() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_rdkafka, consume_rdkafka, Compression::NoCompression).await;
}

#[tokio::test]
async fn test_produce_rskafka_consume_rdkafka_nocompression() {
    assert_produce_consume(produce_rskafka, consume_rdkafka, Compression::NoCompression).await;
}

#[tokio::test]
async fn test_produce_rdkafka_consume_rskafka_nocompression() {
    assert_produce_consume(produce_rdkafka, consume_rskafka, Compression::NoCompression).await;
}

#[tokio::test]
async fn test_produce_rskafka_consume_rskafka_nocompression() {
    assert_produce_consume(produce_rskafka, consume_rskafka, Compression::NoCompression).await;
}

#[cfg(feature = "compression-gzip")]
#[tokio::test]
async fn test_produce_java_consume_java_gzip() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_java, consume_java, Compression::Gzip).await;
}

#[cfg(feature = "compression-gzip")]
#[tokio::test]
async fn test_produce_java_consume_rskafka_gzip() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_java, consume_rskafka, Compression::Gzip).await;
}

#[cfg(feature = "compression-gzip")]
#[tokio::test]
async fn test_produce_rskafka_consume_java_gzip() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_rskafka, consume_java, Compression::Gzip).await;
}

#[cfg(feature = "compression-gzip")]
#[tokio::test]
async fn test_produce_rdkafka_consume_rdkafka_gzip() {
    assert_produce_consume(produce_rdkafka, consume_rdkafka, Compression::Gzip).await;
}

#[cfg(feature = "compression-gzip")]
#[tokio::test]
async fn test_produce_rskafka_consume_rdkafka_gzip() {
    assert_produce_consume(produce_rskafka, consume_rdkafka, Compression::Gzip).await;
}

#[cfg(feature = "compression-gzip")]
#[tokio::test]
async fn test_produce_rdkafka_consume_rskafka_gzip() {
    assert_produce_consume(produce_rdkafka, consume_rskafka, Compression::Gzip).await;
}

#[cfg(feature = "compression-gzip")]
#[tokio::test]
async fn test_produce_rskafka_consume_rskafka_gzip() {
    assert_produce_consume(produce_rskafka, consume_rskafka, Compression::Gzip).await;
}

#[cfg(feature = "compression-lz4")]
#[tokio::test]
async fn test_produce_java_consume_java_lz4() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_java, consume_java, Compression::Lz4).await;
}

#[cfg(feature = "compression-lz4")]
#[tokio::test]
async fn test_produce_java_consume_rskafka_lz4() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_java, consume_rskafka, Compression::Lz4).await;
}

#[cfg(feature = "compression-lz4")]
#[tokio::test]
async fn test_produce_rskafka_consume_java_lz4() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_rskafka, consume_java, Compression::Lz4).await;
}

#[cfg(feature = "compression-lz4")]
#[tokio::test]
async fn test_produce_rdkafka_consume_rdkafka_lz4() {
    assert_produce_consume(produce_rdkafka, consume_rdkafka, Compression::Lz4).await;
}

#[cfg(feature = "compression-lz4")]
#[tokio::test]
async fn test_produce_rskafka_consume_rdkafka_lz4() {
    assert_produce_consume(produce_rskafka, consume_rdkafka, Compression::Lz4).await;
}

#[cfg(feature = "compression-lz4")]
#[tokio::test]
async fn test_produce_rdkafka_consume_rskafka_lz4() {
    assert_produce_consume(produce_rdkafka, consume_rskafka, Compression::Lz4).await;
}

#[cfg(feature = "compression-lz4")]
#[tokio::test]
async fn test_produce_rskafka_consume_rskafka_lz4() {
    assert_produce_consume(produce_rskafka, consume_rskafka, Compression::Lz4).await;
}

#[cfg(feature = "compression-snappy")]
#[tokio::test]
async fn test_produce_java_consume_java_snappy() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_java, consume_java, Compression::Snappy).await;
}

#[cfg(feature = "compression-snappy")]
#[tokio::test]
async fn test_produce_java_consume_rskafka_snappy() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_java, consume_rskafka, Compression::Snappy).await;
}

#[cfg(feature = "compression-snappy")]
#[tokio::test]
async fn test_produce_rskafka_consume_java_snappy() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_rskafka, consume_java, Compression::Snappy).await;
}

#[cfg(feature = "compression-snappy")]
#[tokio::test]
async fn test_produce_rdkafka_consume_rdkafka_snappy() {
    assert_produce_consume(produce_rdkafka, consume_rdkafka, Compression::Snappy).await;
}

#[cfg(feature = "compression-snappy")]
#[tokio::test]
async fn test_produce_rskafka_consume_rdkafka_snappy() {
    assert_produce_consume(produce_rskafka, consume_rdkafka, Compression::Snappy).await;
}

#[cfg(feature = "compression-snappy")]
#[tokio::test]
async fn test_produce_rdkafka_consume_rskafka_snappy() {
    assert_produce_consume(produce_rdkafka, consume_rskafka, Compression::Snappy).await;
}

#[cfg(feature = "compression-snappy")]
#[tokio::test]
async fn test_produce_rskafka_consume_rskafka_snappy() {
    assert_produce_consume(produce_rskafka, consume_rskafka, Compression::Snappy).await;
}

#[cfg(feature = "compression-zstd")]
#[tokio::test]
async fn test_produce_java_consume_java_zstd() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_java, consume_java, Compression::Zstd).await;
}

#[cfg(feature = "compression-zstd")]
#[tokio::test]
async fn test_produce_java_consume_rskafka_zstd() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_java, consume_rskafka, Compression::Zstd).await;
}

#[cfg(feature = "compression-zstd")]
#[tokio::test]
async fn test_produce_rskafka_consume_java_zstd() {
    maybe_skip_java_interopt!();
    assert_produce_consume(produce_rskafka, consume_java, Compression::Zstd).await;
}

#[cfg(feature = "compression-zstd")]
#[tokio::test]
async fn test_produce_rdkafka_consume_rdkafka_zstd() {
    assert_produce_consume(produce_rdkafka, consume_rdkafka, Compression::Zstd).await;
}

#[cfg(feature = "compression-zstd")]
#[tokio::test]
async fn test_produce_rskafka_consume_rdkafka_zstd() {
    assert_produce_consume(produce_rskafka, consume_rdkafka, Compression::Zstd).await;
}

#[cfg(feature = "compression-zstd")]
#[tokio::test]
async fn test_produce_rdkafka_consume_rskafka_zstd() {
    assert_produce_consume(produce_rdkafka, consume_rskafka, Compression::Zstd).await;
}

#[cfg(feature = "compression-zstd")]
#[tokio::test]
async fn test_produce_rskafka_consume_rskafka_zstd() {
    assert_produce_consume(produce_rskafka, consume_rskafka, Compression::Zstd).await;
}

async fn assert_produce_consume<F1, G1, F2, G2>(
    f_produce: F1,
    f_consume: F2,
    compression: Compression,
) where
    F1: Fn(Arc<PartitionClient>, String, String, i32, Vec<Record>, Compression) -> G1,
    G1: std::future::Future<Output = Vec<i64>>,
    F2: Fn(Arc<PartitionClient>, String, String, i32, usize) -> G2,
    G2: std::future::Future<Output = Vec<RecordAndOffset>>,
{
    maybe_start_logging();

    let connection = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 2;

    let client = ClientBuilder::new(vec![connection.clone()])
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();
    controller_client
        .create_topic(&topic_name, n_partitions, 1, 5_000)
        .await
        .unwrap();
    let partition_client = Arc::new(client.partition_client(topic_name.clone(), 1).unwrap());

    let record_1 = {
        let record = record(b"");
        match compression {
            Compression::NoCompression => record,
            #[allow(unreachable_patterns)]
            _ => {
                // add a bit more data to encourage rdkafka to actually use compression, otherwise the compressed data
                // is larger than the uncompressed version and rdkafka will not use compression at all
                Record {
                    key: Some(vec![b'x'; 100]),
                    ..record
                }
            }
        }
    };
    let record_2 = Record {
        value: Some(b"some value".to_vec()),
        timestamp: now(),
        ..record_1.clone()
    };
    let record_3 = Record {
        value: Some(b"more value".to_vec()),
        timestamp: now(),
        ..record_1.clone()
    };

    // produce
    let mut offsets = vec![];
    offsets.append(
        &mut f_produce(
            Arc::clone(&partition_client),
            connection.clone(),
            topic_name.clone(),
            1,
            vec![record_1.clone(), record_2.clone()],
            compression,
        )
        .await,
    );
    offsets.append(
        &mut f_produce(
            Arc::clone(&partition_client),
            connection.clone(),
            topic_name.clone(),
            1,
            vec![record_3.clone()],
            compression,
        )
        .await,
    );
    assert_eq!(offsets.len(), 3);
    assert_ne!(offsets[0], offsets[1]);
    assert_ne!(offsets[1], offsets[2]);
    assert_ne!(offsets[2], offsets[0]);

    // consume
    let actual = f_consume(partition_client, connection, topic_name, 1, 3).await;
    let expected: Vec<_> = offsets
        .into_iter()
        .zip([record_1, record_2, record_3])
        .map(|(offset, record)| RecordAndOffset { record, offset })
        .collect();
    assert_eq!(actual, expected);
}

async fn produce_java(
    _partition_client: Arc<PartitionClient>,
    connection: String,
    topic_name: String,
    partition_index: i32,
    records: Vec<Record>,
    compression: Compression,
) -> Vec<i64> {
    java_helper::produce(
        &connection,
        records
            .into_iter()
            .map(|record| (topic_name.clone(), partition_index, record))
            .collect(),
        compression,
    )
    .await
}

async fn produce_rdkafka(
    _partition_client: Arc<PartitionClient>,
    connection: String,
    topic_name: String,
    partition_index: i32,
    records: Vec<Record>,
    compression: Compression,
) -> Vec<i64> {
    rdkafka_helper::produce(
        &connection,
        records
            .into_iter()
            .map(|record| (topic_name.clone(), partition_index, record))
            .collect(),
        compression,
    )
    .await
}

async fn produce_rskafka(
    partition_client: Arc<PartitionClient>,
    _connection: String,
    _topic_name: String,
    _partition_index: i32,
    records: Vec<Record>,
    compression: Compression,
) -> Vec<i64> {
    partition_client
        .produce(records, compression)
        .await
        .unwrap()
}

async fn consume_java(
    _partition_client: Arc<PartitionClient>,
    connection: String,
    topic_name: String,
    partition_index: i32,
    n: usize,
) -> Vec<RecordAndOffset> {
    java_helper::consume(&connection, &topic_name, partition_index, n).await
}

async fn consume_rdkafka(
    _partition_client: Arc<PartitionClient>,
    connection: String,
    topic_name: String,
    partition_index: i32,
    n: usize,
) -> Vec<RecordAndOffset> {
    rdkafka_helper::consume(&connection, &topic_name, partition_index, n).await
}

async fn consume_rskafka(
    partition_client: Arc<PartitionClient>,
    _connection: String,
    _topic_name: String,
    _partition_index: i32,
    n: usize,
) -> Vec<RecordAndOffset> {
    // TODO: use a proper stream here
    let mut records = vec![];
    let mut offset = 0;
    while records.len() < n {
        let res = partition_client
            .fetch_records(offset, 0..1_000_000, 1_000)
            .await
            .unwrap()
            .0;
        assert!(!res.is_empty());
        for record in res {
            offset = offset.max(record.offset);
            records.push(record);
        }
    }
    records.into_iter().take(n).collect()
}
