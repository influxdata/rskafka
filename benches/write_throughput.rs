use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, Criterion, SamplingMode,
};
use futures::{stream::FuturesUnordered, StreamExt};
use rdkafka::producer::FutureProducer;
use rskafka::{
    client::{
        partition::{Compression, PartitionClient},
        producer::{aggregator::RecordAggregator, BatchProducerBuilder},
        ClientBuilder,
    },
    record::Record,
};
use time::OffsetDateTime;
use tokio::runtime::Runtime;

const PARALLEL_BATCH_SIZE: usize = 1_000_000;
const PARALLEL_LINGER_MS: u64 = 10;

pub fn criterion_benchmark(c: &mut Criterion) {
    let connection = maybe_skip_kafka_integration!();

    let key = vec![b'k'; 10];
    let value = vec![b'x'; 10_000];

    {
        let mut group_sequential = benchark_group(c, "sequential");

        group_sequential.bench_function("rdkafka", |b| {
            let connection = connection.clone();
            let key = key.clone();
            let value = value.clone();

            b.to_async(runtime()).iter_custom(|iters| {
                let connection = connection.clone();
                let key = key.clone();
                let value = value.clone();

                async move {
                    use rdkafka::{producer::FutureRecord, util::Timeout};

                    let (client, topic) = setup_rdkafka(connection, false).await;

                    let start = Instant::now();

                    for _ in 0..iters {
                        let f_record = FutureRecord::to(&topic).key(&key).payload(&value);
                        client.send(f_record, Timeout::Never).await.unwrap();
                    }

                    start.elapsed()
                }
            });
        });

        group_sequential.bench_function("rskafka", |b| {
            let connection = connection.clone();
            let key = key.clone();
            let value = value.clone();

            b.to_async(runtime()).iter_custom(|iters| {
                let connection = connection.clone();
                let key = key.clone();
                let value = value.clone();

                async move {
                    let client = setup_rskafka(connection).await;
                    let record = Record {
                        key: Some(key),
                        value: Some(value),
                        headers: BTreeMap::default(),
                        timestamp: OffsetDateTime::now_utc(),
                    };

                    let start = Instant::now();

                    for _ in 0..iters {
                        client
                            .produce(vec![record.clone()], Compression::NoCompression)
                            .await
                            .unwrap();
                    }

                    start.elapsed()
                }
            });
        });
    }

    {
        let mut group_parallel = benchark_group(c, "parallel");

        group_parallel.bench_function("rdkafka", |b| {
            let connection = connection.clone();
            let key = key.clone();
            let value = value.clone();

            b.to_async(runtime()).iter_custom(|iters| {
                let connection = connection.clone();
                let key = key.clone();
                let value = value.clone();

                async move {
                    use rdkafka::{producer::FutureRecord, util::Timeout};

                    let (client, topic) = setup_rdkafka(connection, true).await;

                    let start = Instant::now();

                    let mut tasks: FuturesUnordered<_> = (0..iters)
                        .map(|_| async {
                            let f_record = FutureRecord::to(&topic).key(&key).payload(&value);
                            client.send(f_record, Timeout::Never).await.unwrap();
                        })
                        .collect();
                    while tasks.next().await.is_some() {}

                    start.elapsed()
                }
            });
        });

        group_parallel.bench_function("rskafka", |b| {
            let connection = connection.clone();
            let key = key.clone();
            let value = value.clone();

            b.to_async(runtime()).iter_custom(|iters| {
                let connection = connection.clone();
                let key = key.clone();
                let value = value.clone();

                async move {
                    let client = setup_rskafka(connection).await;
                    let record = Record {
                        key: Some(key),
                        value: Some(value),
                        headers: BTreeMap::default(),
                        timestamp: OffsetDateTime::now_utc(),
                    };
                    let producer = BatchProducerBuilder::new(Arc::new(client))
                        .with_linger(Duration::from_millis(PARALLEL_LINGER_MS))
                        .build(RecordAggregator::new(PARALLEL_BATCH_SIZE));

                    let start = Instant::now();

                    let mut tasks: FuturesUnordered<_> = (0..iters)
                        .map(|_| async {
                            producer.produce(record.clone()).await.unwrap();
                        })
                        .collect();
                    while tasks.next().await.is_some() {}

                    start.elapsed()
                }
            });
        });
    }
}

/// Get the testing Kafka connection string or return current scope.
///
/// If `TEST_INTEGRATION` and `KAFKA_CONNECT` are set, return the Kafka connection URL to the
/// caller.
///
/// If `TEST_INTEGRATION` is set but `KAFKA_CONNECT` is not set, fail the tests and provide
/// guidance for setting `KAFKA_CONNECTION`.
///
/// If `TEST_INTEGRATION` is not set, skip the calling test by returning early.
#[macro_export]
macro_rules! maybe_skip_kafka_integration {
    () => {{
        use std::env;
        dotenv::dotenv().ok();

        match (
            env::var("TEST_INTEGRATION").is_ok(),
            env::var("KAFKA_CONNECT").ok(),
        ) {
            (true, Some(kafka_connection)) => kafka_connection,
            (true, None) => {
                panic!(
                    "TEST_INTEGRATION is set which requires running integration tests, but \
                    KAFKA_CONNECT is not set. Please run Kafka or Redpanda then \
                    set KAFKA_CONNECT as directed in README.md."
                )
            }
            (false, Some(_)) => {
                eprintln!("skipping Kafka integration tests - set TEST_INTEGRATION to run");
                return;
            }
            (false, None) => {
                eprintln!(
                    "skipping Kafka integration tests - set TEST_INTEGRATION and KAFKA_CONNECT to \
                    run"
                );
                return;
            }
        }
    }};
}

fn benchark_group<'a>(c: &'a mut Criterion, name: &str) -> BenchmarkGroup<'a, WallTime> {
    let mut group = c.benchmark_group(name);
    group.measurement_time(Duration::from_secs(60));
    group.sample_size(15);
    group.sampling_mode(SamplingMode::Linear);
    group
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
}

/// Generated random topic name for testing.
fn random_topic_name() -> String {
    format!("test_topic_{}", uuid::Uuid::new_v4())
}

async fn setup_rdkafka(connection: String, buffering: bool) -> (FutureProducer, String) {
    use rdkafka::{
        admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
        producer::FutureRecord,
        util::Timeout,
        ClientConfig,
    };

    let topic_name = random_topic_name();

    // configure clients
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", connection);
    cfg.set("message.timeout.ms", "5000");
    if buffering {
        cfg.set("batch.num.messages", PARALLEL_BATCH_SIZE.to_string()); // = loads
        cfg.set("batch.size", 1_000_000.to_string());
        cfg.set("queue.buffering.max.ms", PARALLEL_LINGER_MS.to_string());
    } else {
        cfg.set("batch.num.messages", "1");
        cfg.set("queue.buffering.max.ms", "0");
    }

    // create topic
    let admin_client: AdminClient<_> = cfg.create().unwrap();
    let topic = NewTopic::new(&topic_name, 1, TopicReplication::Fixed(1));
    let opts = AdminOptions::default();
    let mut results = admin_client.create_topics([&topic], &opts).await.unwrap();
    assert_eq!(results.len(), 1, "created exactly one topic");
    let result = results.pop().expect("just checked the vector length");
    result.unwrap();

    let producer_client: FutureProducer = cfg.create().unwrap();

    // warm up connection
    let key = vec![b'k'; 1];
    let payload = vec![b'x'; 10];
    let f_record = FutureRecord::to(&topic_name).key(&key).payload(&payload);
    producer_client
        .send(f_record, Timeout::Never)
        .await
        .unwrap();

    (producer_client, topic_name)
}

async fn setup_rskafka(connection: String) -> PartitionClient {
    let topic_name = random_topic_name();

    let client = ClientBuilder::new(vec![connection]).build().await.unwrap();
    client
        .controller_client()
        .await
        .unwrap()
        .create_topic(topic_name.clone(), 1, 1, 5_000)
        .await
        .unwrap();

    client.partition_client(topic_name, 0).await.unwrap()
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
