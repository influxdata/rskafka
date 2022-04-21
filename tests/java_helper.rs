use std::collections::BTreeMap;

use j4rs::{Instance, InvocationArg, Jvm, JvmBuilder, MavenArtifact};
use once_cell::sync::Lazy;
use rskafka::{
    client::partition::Compression,
    record::{Record, RecordAndOffset},
};
use time::OffsetDateTime;

/// If `TEST_JAVA_INTEROPT` is not set, skip the calling test by returning early.
#[macro_export]
macro_rules! maybe_skip_java_interopt {
    () => {{
        use std::env;
        dotenv::dotenv().ok();

        if env::var("TEST_JAVA_INTEROPT").is_err() {
            return;
        }
    }};
}

/// Produce.
pub async fn produce(
    connection: &str,
    records: Vec<(String, i32, Record)>,
    compression: Compression,
) -> Vec<i64> {
    let jvm = setup_jvm();

    let compression = match compression {
        Compression::NoCompression => "none",
        #[cfg(feature = "compression-gzip")]
        Compression::Gzip => "gzip",
        #[cfg(feature = "compression-lz4")]
        Compression::Lz4 => "lz4",
        #[cfg(feature = "compression-snappy")]
        Compression::Snappy => "snappy",
        #[cfg(feature = "compression-zstd")]
        Compression::Zstd => "zstd",
    };

    let props = create_properties(
        &jvm,
        &[
            ("bootstrap.servers", connection),
            (
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer",
            ),
            (
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer",
            ),
            ("linger.ms", "100000"),
            ("compression.type", compression),
        ],
    );

    // https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#%3Cinit%3E(java.util.Map)
    let producer = jvm
        .create_instance(
            "org.apache.kafka.clients.producer.KafkaProducer",
            &[InvocationArg::from(props)],
        )
        .expect("creating KafkaProducer");

    let mut futures = vec![];
    for (topic_name, partition_index, record) in records {
        let ts = (record.timestamp.unix_timestamp_nanos() / 1_000_000) as i64;
        let k = String::from_utf8(record.key.unwrap()).unwrap();
        let v = String::from_utf8(record.value.unwrap()).unwrap();

        let headers = jvm
            .create_instance(
                "org.apache.kafka.common.header.internals.RecordHeaders",
                &[],
            )
            .expect("creating KafkaProducer");
        for (k, v) in record.headers {
            jvm.invoke(
                &headers,
                "add",
                &[
                    InvocationArg::try_from(k).expect("key arg"),
                    InvocationArg::from(to_java_bytes(&jvm, &v)),
                ],
            )
            .expect("add header");
        }

        // https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html#%3Cinit%3E(java.lang.String,java.lang.Integer,K,V,java.lang.Iterable)
        let jvm_record = jvm
            .create_instance(
                "org.apache.kafka.clients.producer.ProducerRecord",
                &[
                    InvocationArg::try_from(topic_name).expect("topic arg"),
                    InvocationArg::try_from(partition_index).expect("partition arg"),
                    InvocationArg::try_from(ts).expect("ts arg"),
                    InvocationArg::try_from(k).expect("key arg"),
                    InvocationArg::try_from(v).expect("value arg"),
                    InvocationArg::from(headers),
                ],
            )
            .expect("creating KafkaProducer");

        // https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#send(org.apache.kafka.clients.producer.ProducerRecord)
        let fut = jvm
            .invoke(&producer, "send", &[InvocationArg::from(jvm_record)])
            .expect("flush");
        futures.push(fut);
    }

    // https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#flush()
    jvm.invoke(&producer, "flush", &[]).expect("flush");

    let mut offsets = vec![];
    for fut in futures {
        // https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/Future.html#get()
        let record_metadata = jvm.invoke(&fut, "get", &[]).expect("polling future");
        let record_metadata = jvm
            .cast(
                &record_metadata,
                "org.apache.kafka.clients.producer.RecordMetadata",
            )
            .expect("cast to RecordMetadata");
        // https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/producer/RecordMetadata.html#offset()
        let offset = jvm
            .invoke(&record_metadata, "offset", &[])
            .expect("getting offset");
        let offset: i64 = jvm.to_rust(offset).expect("converting offset to Rust");
        offsets.push(offset);
    }

    // https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#close()
    jvm.invoke(&producer, "close", &[]).expect("close");

    offsets
}

/// Consume
pub async fn consume(
    connection: &str,
    topic_name: &str,
    partition_index: i32,
    n: usize,
) -> Vec<RecordAndOffset> {
    let jvm = setup_jvm();

    let props = create_properties(
        &jvm,
        &[
            ("bootstrap.servers", connection),
            (
                "key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
            ),
            (
                "value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
            ),
            ("auto.offset.reset", "earliest"),
        ],
    );

    // https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#%3Cinit%3E(java.util.Map)
    let consumer = jvm
        .create_instance(
            "org.apache.kafka.clients.consumer.KafkaConsumer",
            &[InvocationArg::from(props)],
        )
        .expect("creating KafkaConsumer");

    // https://kafka.apache.org/31/javadoc/org/apache/kafka/common/TopicPartition.html#%3Cinit%3E(java.lang.String,int)
    let topic_partition = jvm
        .create_instance(
            "org.apache.kafka.common.TopicPartition",
            &[
                InvocationArg::try_from(topic_name).expect("topic arg"),
                InvocationArg::try_from(partition_index)
                    .expect("partition arg")
                    .into_primitive()
                    .expect("partition arg to int"),
            ],
        )
        .expect("creating TopicPartition");
    let partitions = jvm
        .create_java_array(
            "org.apache.kafka.common.TopicPartition",
            &[InvocationArg::from(topic_partition)],
        )
        .expect("creating partitions array");
    let partitions = jvm
        .invoke_static(
            "java.util.Arrays",
            "asList",
            &[InvocationArg::from(partitions)],
        )
        .expect("partitions asList");
    // https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#assign(java.util.Collection)
    jvm.invoke(&consumer, "assign", &[InvocationArg::from(partitions)])
        .expect("assign");

    let mut results = vec![];
    while results.len() < n {
        // https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#poll(long)
        let consumer_records = jvm
            .invoke(
                &consumer,
                "poll",
                &[InvocationArg::try_from(1_000i64)
                    .expect("timeout arg")
                    .into_primitive()
                    .expect("timeout into primitive")],
            )
            .expect("poll");

        // https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/consumer/ConsumerRecords.html#iterator()
        let it = jvm
            .invoke(&consumer_records, "iterator", &[])
            .expect("iterator");
        for consumer_record in JavaIterator::new(&jvm, it) {
            let consumer_record = jvm
                .cast(
                    &consumer_record,
                    "org.apache.kafka.clients.consumer.ConsumerRecord",
                )
                .expect("cast to ConsumerRecord");

            // https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/consumer/ConsumerRecord.html
            let key = jvm.invoke(&consumer_record, "key", &[]).expect("key");
            let key: String = jvm.to_rust(key).expect("key to Rust");

            let offset = jvm.invoke(&consumer_record, "offset", &[]).expect("offset");
            let offset: i64 = jvm.to_rust(offset).expect("offset to Rust");

            let timestamp = jvm
                .invoke(&consumer_record, "timestamp", &[])
                .expect("timestamp");
            let timestamp: i64 = jvm.to_rust(timestamp).expect("timestamp to Rust");

            let value = jvm.invoke(&consumer_record, "value", &[]).expect("value");
            let value: String = jvm.to_rust(value).expect("value to Rust");

            let headers = jvm
                .invoke(&consumer_record, "headers", &[])
                .expect("headers");
            let headers = jvm.invoke(&headers, "toArray", &[]).expect("toArray");
            let headers = jvm
                .invoke_static(
                    "java.util.Arrays",
                    "asList",
                    &[InvocationArg::from(headers)],
                )
                .expect("headers asList");
            let headers_it = jvm.invoke(&headers, "iterator", &[]).expect("iterator");
            let mut headers = BTreeMap::new();
            for header in JavaIterator::new(&jvm, headers_it) {
                let header = jvm
                    .cast(&header, "org.apache.kafka.common.header.Header")
                    .expect("cast to Header");

                let key = jvm.invoke(&header, "key", &[]).expect("key");
                let key: String = jvm.to_rust(key).expect("key to Rust");

                let value = jvm.invoke(&header, "value", &[]).expect("value");
                let value = from_java_bytes(&jvm, value);

                headers.insert(key, value);
            }

            let record = Record {
                key: Some(key.as_bytes().to_vec()),
                value: Some(value.as_bytes().to_vec()),
                headers,
                timestamp: OffsetDateTime::from_unix_timestamp_nanos(timestamp as i128 * 1_000_000)
                    .unwrap(),
            };
            let record_and_offset = RecordAndOffset { record, offset };
            results.push(record_and_offset);
        }
    }

    // https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#close()
    jvm.invoke(&consumer, "close", &[]).expect("close");

    results
}

/// Lazy static that tracks if we already installed all JVM dependencies.
static JVM_SETUP: Lazy<()> = Lazy::new(|| {
    // The way JVM is hooked up via JNI is kinda weird. We have process-wide VMs that are always cached. On first
    // startup j4rs sets up the class path based on what's already installed. If we now run the installation VM in the
    // same process as our tests, we can never consume the freshly installed libraries. So we use a subprocess to run
    // the actual dependency installation and drop that process (including its VM) once its completed.
    procspawn::init();

    let handle = procspawn::spawn((), |_| {
        let jvm_installation = JvmBuilder::new().build().expect("setup JVM");

        for artifact_name in [
            // Kafka client
            // Note that j4rs does NOT pull dependencies, so we need to add compression libs (except for gzip, which is
            // built into Java) manually.
            "org.apache.kafka:kafka-clients:3.1.0",
            // Helper used in `from_java_bytes`
            "org.apache.commons:commons-lang3:3.12.0",
            // LZ4 compression support
            "org.lz4:lz4-java:1.8.0",
            // snappy compression support
            "org.xerial.snappy:snappy-java:1.1.8.4",
            // zstd compression support
            "com.github.luben:zstd-jni:1.5.0-4",
        ] {
            let artifact = MavenArtifact::from(artifact_name);
            jvm_installation
                .deploy_artifact(&artifact)
                .unwrap_or_else(|_| panic!("Artifact deployment failed ({artifact_name})"));
        }
    });

    handle.join().unwrap();
});

fn setup_jvm() -> Jvm {
    Lazy::force(&JVM_SETUP);

    let jvm = JvmBuilder::new().build().expect("setup JVM");
    jvm
}

fn create_properties(jvm: &Jvm, properties: &[(&str, &str)]) -> Instance {
    let props = jvm
        .create_instance("java.util.Properties", &[])
        .expect("creating Properties");

    for (k, v) in properties {
        jvm.invoke(
            &props,
            "put",
            &[
                InvocationArg::try_from(*k).expect("convert str to java"),
                InvocationArg::try_from(*v).expect("convert str to java"),
            ],
        )
        .expect("put property");
    }

    props
}

fn to_java_bytes(jvm: &Jvm, bytes: &[u8]) -> Instance {
    let mut args = vec![];
    for b in bytes {
        args.push(
            InvocationArg::try_from(*b as i8)
                .expect("byte arg")
                .into_primitive()
                .expect("to byte primitive"),
        );
    }
    jvm.create_java_array("byte", &args).expect("create array")
}

fn from_java_bytes(jvm: &Jvm, bytes: Instance) -> Vec<u8> {
    let bytes = jvm
        .invoke_static(
            "org.apache.commons.lang3.ArrayUtils",
            "toObject",
            &[InvocationArg::from(bytes)],
        )
        .expect("toObject");
    let bytes = jvm
        .invoke_static("java.util.Arrays", "asList", &[InvocationArg::from(bytes)])
        .expect("bytes asList");

    let it = jvm.invoke(&bytes, "iterator", &[]).expect("iterator");

    let mut res = vec![];
    for byte in JavaIterator::new(jvm, it) {
        let byte: i8 = jvm.to_rust(byte).expect("byte to Rust");

        res.push(byte as u8);
    }

    res
}

struct JavaIterator<'a> {
    jvm: &'a Jvm,
    it: Instance,
}

impl<'a> JavaIterator<'a> {
    fn new(jvm: &'a Jvm, it: Instance) -> Self {
        Self { jvm, it }
    }
}

impl<'a> Iterator for JavaIterator<'a> {
    type Item = Instance;

    fn next(&mut self) -> Option<Self::Item> {
        // https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Iterator.html#hasNext()
        let has_next = self.jvm.invoke(&self.it, "hasNext", &[]).expect("hasNext");
        let has_next: bool = self.jvm.to_rust(has_next).expect("hasNext to Rust");
        if has_next {
            // https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Iterator.html#next()
            Some(self.jvm.invoke(&self.it, "next", &[]).expect("next"))
        } else {
            None
        }
    }
}
