use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};

use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, Criterion, SamplingMode,
};
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use parking_lot::Once;
use pin_project_lite::pin_project;
use rdkafka::{
    consumer::{Consumer, StreamConsumer as RdStreamConsumer},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig, TopicPartitionList,
};
use rskafka::{
    client::{
        consumer::{StartOffset, StreamConsumerBuilder as RsStreamConsumerBuilder},
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
    maybe_start_logging();
    let connection = maybe_skip_kafka_integration!();

    let record = Record {
        key: Some(vec![b'k'; 10]),
        value: Some(vec![b'x'; 10_000]),
        headers: BTreeMap::default(),
        timestamp: OffsetDateTime::now_utc(),
    };

    {
        let mut group = benchark_group(c, "read");

        group.bench_function("rdkafka", |b| {
            b.to_async(runtime()).iter_custom(|iters| {
                let connection = connection.clone();
                let record = record.clone();

                async move {
                    let rd = RdKafka::setup(connection, false).await;
                    let producer = rd.producer(false).await;

                    exec_sequential(
                        || async {
                            let f_record = record.to_rdkafka(&rd.topic_name);
                            producer.send(f_record, Timeout::Never).await.unwrap();
                        },
                        iters,
                    )
                    .await;

                    async {
                        // rdkafka fetches data in the background, so we need include the client setup into the
                        // measurement
                        let consumer = rd.consumer().await;
                        let messages = consumer
                            .stream()
                            .take(iters as usize)
                            .try_collect::<Vec<_>>()
                            .await
                            .unwrap();
                        assert_eq!(messages.len(), iters as usize);
                    }
                    .time_it()
                    .await
                }
            });
        });

        group.bench_function("rskafka", |b| {
            b.to_async(runtime()).iter_custom(|iters| {
                let connection = connection.clone();
                let record = record.clone();

                async move {
                    let client = setup_rskafka(connection).await;

                    exec_sequential(
                        || async {
                            client
                                .produce(vec![record.clone()], Compression::NoCompression)
                                .await
                                .unwrap();
                        },
                        iters,
                    )
                    .await;

                    async {
                        let consumer =
                            RsStreamConsumerBuilder::new(Arc::new(client), StartOffset::Earliest)
                                .build();
                        let messages = consumer
                            .take(iters as usize)
                            .try_collect::<Vec<_>>()
                            .await
                            .unwrap();
                        assert_eq!(messages.len(), iters as usize);
                    }
                    .time_it()
                    .await
                }
            });
        });
    }

    {
        let mut group = benchark_group(c, "write_sequential");

        group.bench_function("rdkafka", |b| {
            b.to_async(runtime()).iter_custom(|iters| {
                let connection = connection.clone();
                let record = record.clone();

                async move {
                    let rd = RdKafka::setup(connection, false).await;
                    let producer = rd.producer(true).await;

                    exec_sequential(
                        || async {
                            let f_record = record.to_rdkafka(&rd.topic_name);
                            producer.send(f_record, Timeout::Never).await.unwrap();
                        },
                        iters,
                    )
                    .time_it()
                    .await
                }
            });
        });

        group.bench_function("rskafka", |b| {
            b.to_async(runtime()).iter_custom(|iters| {
                let connection = connection.clone();
                let record = record.clone();

                async move {
                    let client = setup_rskafka(connection).await;

                    exec_sequential(
                        || async {
                            client
                                .produce(vec![record.clone()], Compression::NoCompression)
                                .await
                                .unwrap();
                        },
                        iters,
                    )
                    .time_it()
                    .await
                }
            });
        });
    }

    {
        let mut group = benchark_group(c, "write_parallel");

        group.bench_function("rdkafka", |b| {
            b.to_async(runtime()).iter_custom(|iters| {
                let connection = connection.clone();
                let record = record.clone();

                async move {
                    let rd = RdKafka::setup(connection, true).await;
                    let producer = rd.producer(true).await;

                    exec_parallel(
                        || async {
                            let f_record = record.to_rdkafka(&rd.topic_name);
                            producer.send(f_record, Timeout::Never).await.unwrap();
                        },
                        iters,
                    )
                    .time_it()
                    .await
                }
            });
        });

        group.bench_function("rskafka", |b| {
            b.to_async(runtime()).iter_custom(|iters| {
                let connection = connection.clone();
                let record = record.clone();

                async move {
                    let client = setup_rskafka(connection).await;
                    let producer = BatchProducerBuilder::new(Arc::new(client))
                        .with_linger(Duration::from_millis(PARALLEL_LINGER_MS))
                        .build(RecordAggregator::new(PARALLEL_BATCH_SIZE));

                    exec_parallel(
                        || async {
                            producer.produce(record.clone()).await.unwrap();
                        },
                        iters,
                    )
                    .time_it()
                    .await
                }
            });
        });
    }
}

async fn exec_sequential<F, Fut>(f: F, iters: u64)
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()>,
{
    for _ in 0..iters {
        f().await;
    }
}

async fn exec_parallel<F, Fut>(f: F, iters: u64)
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()>,
{
    let mut tasks: FuturesUnordered<_> = (0..iters).map(|_| f()).collect();
    while tasks.next().await.is_some() {}
}

/// "Time it" extension for futures.
trait FutureTimeItExt {
    type TimeItFut: Future<Output = Duration>;

    /// Measures time it takes to execute given async block once
    fn time_it(self) -> Self::TimeItFut;
}

impl<F> FutureTimeItExt for F
where
    F: Future<Output = ()>,
{
    type TimeItFut = TimeIt<F>;

    fn time_it(self) -> Self::TimeItFut {
        TimeIt {
            t_start: Instant::now(),
            inner: self,
        }
    }
}

pin_project! {
    struct TimeIt<F> {
        t_start: Instant,
        #[pin]
        inner: F,
    }
}

impl<F> Future for TimeIt<F>
where
    F: Future<Output = ()>,
{
    type Output = Duration;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Ready(_) => Poll::Ready(this.t_start.elapsed()),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Extension to convert rdkafka to rskafka records.
trait RecordExt {
    fn to_rdkafka<'a>(&'a self, topic: &'a str) -> FutureRecord<'a, Vec<u8>, Vec<u8>>;
}

impl RecordExt for Record {
    fn to_rdkafka<'a>(&'a self, topic: &'a str) -> FutureRecord<'a, Vec<u8>, Vec<u8>> {
        let mut record = FutureRecord::to(topic);
        if let Some(key) = self.key.as_ref() {
            record = record.key(key);
        }
        if let Some(value) = self.value.as_ref() {
            record = record.payload(value);
        }
        record
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
        dotenvy::dotenv().ok();

        match (
            env::var("TEST_INTEGRATION").is_ok(),
            env::var("KAFKA_CONNECT").ok(),
        ) {
            (true, Some(kafka_connection)) => {
                let kafka_connection: Vec<String> =
                    kafka_connection.split(",").map(|s| s.to_owned()).collect();
                kafka_connection
            }
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

struct RdKafka {
    cfg: ClientConfig,
    topic_name: String,
}

impl RdKafka {
    async fn setup(connection: Vec<String>, buffering: bool) -> Self {
        use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};

        let topic_name = random_topic_name();

        // configure clients
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", connection.join(","));
        cfg.set("message.timeout.ms", "5000");
        cfg.set("group.id", "foo");
        cfg.set("auto.offset.reset", "smallest");
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

        Self { cfg, topic_name }
    }

    async fn producer(&self, warmup: bool) -> FutureProducer {
        let producer_client: FutureProducer = self.cfg.create().unwrap();

        // warm up connection
        if warmup {
            let key = vec![b'k'; 1];
            let payload = vec![b'x'; 10];
            let f_record = FutureRecord::to(&self.topic_name)
                .key(&key)
                .payload(&payload);
            producer_client
                .send(f_record, Timeout::Never)
                .await
                .unwrap();
        }

        producer_client
    }

    async fn consumer(&self) -> RdStreamConsumer {
        let consumer_client: RdStreamConsumer = self.cfg.create().unwrap();

        let mut assignment = TopicPartitionList::new();
        assignment.add_partition(&self.topic_name, 0);
        consumer_client.assign(&assignment).unwrap();

        consumer_client
    }
}

async fn setup_rskafka(connection: Vec<String>) -> PartitionClient {
    let topic_name = random_topic_name();

    let client = ClientBuilder::new(connection).build().await.unwrap();
    client
        .controller_client()
        .unwrap()
        .create_topic(topic_name.clone(), 1, 1, 5_000)
        .await
        .unwrap();

    client.partition_client(topic_name, 0).unwrap()
}

static LOG_SETUP: Once = Once::new();

/// Enables debug logging if the `RUST_LOG` environment variable is
/// set. Does nothing if `RUST_LOG` is not set.
pub fn maybe_start_logging() {
    if std::env::var("RUST_LOG").is_ok() {
        start_logging()
    }
}

/// Start logging.
pub fn start_logging() {
    use tracing_log::LogTracer;
    use tracing_subscriber::{filter::EnvFilter, FmtSubscriber};

    LOG_SETUP.call_once(|| {
        LogTracer::init().unwrap();

        let subscriber = FmtSubscriber::builder()
            .with_env_filter(EnvFilter::from_default_env())
            .with_test_writer()
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
