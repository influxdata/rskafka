# RSKafka

[![CircleCI](https://circleci.com/gh/influxdata/rskafka/tree/main.svg?style=shield&circle-token=531ba1f38035a10da6dbf7cc71e6f55eff496c70)](https://circleci.com/gh/influxdata/rskafka/tree/main)
[![Crates.io](https://img.shields.io/crates/v/rskafka)](https://crates.io/crates/rskafka)
[![Documentation](https://img.shields.io/docsrs/rskafka)](https://docs.rs/crate/rskafka/latest)
[![License](https://img.shields.io/crates/l/rskafka)](#license)

This crate aims to be a minimal Kafka implementation for simple workloads that wish to use Kafka as a distributed
write-ahead log.

It is **not** a general-purpose Kafka implementation, instead it is heavily optimised for simplicity, both in terms of
implementation and its emergent operational characteristics. In particular, it aims to meet the needs
of [IOx].

This crate has:

* No support for offset tracking, consumer groups, transactions, etc...
* No built-in buffering, aggregation, linger timeouts, etc...
* Independent write streams per partition

It will be a good fit for workloads that:

* Perform offset tracking independently of Kafka
* Read/Write reasonably sized payloads per-partition
* Have a low number of high-throughput partitions [^1]


## Usage

```rust,no_run
# async fn test() {
use rskafka::{
    client::{
        ClientBuilder,
        partition::{Compression, UnknownTopicHandling},
    },
    record::Record,
};
use time::OffsetDateTime;
use std::collections::BTreeMap;

// setup client
let connection = "localhost:9093".to_owned();
let client = ClientBuilder::new(vec![connection]).build().await.unwrap();

// create a topic
let topic = "my_topic";
let controller_client = client.controller_client().unwrap();
controller_client.create_topic(
    topic,
    2,      // partitions
    1,      // replication factor
    5_000,  // timeout (ms)
).await.unwrap();

// get a partition-bound client
let partition_client = client
    .partition_client(
        topic.to_owned(),
        0,  // partition
        UnknownTopicHandling::Retry,
     )
     .await
    .unwrap();

// produce some data
let record = Record {
    key: None,
    value: Some(b"hello kafka".to_vec()),
    headers: BTreeMap::from([
        ("foo".to_owned(), b"bar".to_vec()),
    ]),
    timestamp: OffsetDateTime::now_utc(),
};
partition_client.produce(vec![record], Compression::default()).await.unwrap();

// consume data
let (records, high_watermark) = partition_client
    .fetch_records(
        0,  // offset
        1..1_000_000,  // min..max bytes
        1_000,  // max wait time
    )
   .await
   .unwrap();
# }
```

For more advanced production and consumption, see [`crate::client::producer`] and [`crate::client::consumer`].


## Features

- **`compression-gzip` (default):** Support compression and decompression of messages using [gzip].
- **`compression-lz4` (default):** Support compression and decompression of messages using [LZ4].
- **`compression-snappy` (default):** Support compression and decompression of messages using [Snappy].
- **`compression-zstd` (default):** Support compression and decompression of messages using [zstd].
- **`full`:** Includes all stable features (`compression-gzip`, `compression-lz4`, `compression-snappy`,
  `compression-zstd`, `transport-socks5`, `transport-tls`).
- **`transport-socks5`:** Allow transport via SOCKS5 proxy.
- **`transport-tls`:** Allows TLS transport via [rustls].
- **`unstable-fuzzing`:** Exposes some internal data structures so that they can be used by our fuzzers. This is NOT a stable
  feature / API!

## Testing

### Redpanda

To run integration tests against [Redpanda], run:

```console
$ docker-compose -f docker-compose-redpanda.yml up
```

in one session, and then run:

```console
$ TEST_INTEGRATION=1 TEST_BROKER_IMPL=redpanda KAFKA_CONNECT=0.0.0.0:9011 cargo test
```

in another session.

### Apache Kafka

To run integration tests against [Apache Kafka], run:

```console
$ docker-compose -f docker-compose-kafka.yml up
```

in one session, and then run:

```console
$ TEST_INTEGRATION=1 TEST_BROKER_IMPL=kafka KAFKA_CONNECT=localhost:9011 cargo test
```

in another session. Note that Apache Kafka supports a different set of features then redpanda, so we pass other
environment variables.

### Using a SOCKS5 Proxy

To run the integration test via a SOCKS5 proxy, you need to set the environment variable `SOCKS_PROXY`. The following
command requires a running proxy on the local machine.

```console
$ KAFKA_CONNECT=0.0.0.0:9011,kafka-1:9021,redpanda-1:9021 SOCKS_PROXY=localhost:1080 cargo test --features full
```

The SOCKS5 proxy will automatically be started by the docker compose files. Note that `KAFKA_CONNECT` was extended by
addresses that are reachable via the proxy.

### Java Interopt
To test if RSKafka can produce/consume records to/from the official Java client, you need to have Java installed and the
`TEST_JAVA_INTEROPT=1` environment variable set.

### Fuzzing
RSKafka offers fuzz targets for certain protocol parsing steps. To build them make sure you have [cargo-fuzz] installed.
Select one of the following fuzzers:

- **`protocol_reader`:** Selects an API key and API version and then reads message frames and tries to decode the
  response object. The message frames are read w/o the length marker for more efficient fuzzing.
- **`record_batch_body_reader`:** Reads the inner part of a record batch (w/o the prefix that contains length and CRC)
  and tries to decode it. In theory this is covered by `protocol_reader` as well but the length fields and CRC make it
  hard for the fuzzer to traverse this data structure.

Then run the fuzzer with:

```console
$ cargo +nightly fuzz run protocol_reader
...
```

Let it running for how long you wish or until it finds a crash:

```text
...
Failing input:

        fuzz/artifacts/protocol_reader/crash-369f9787d35767c47431161d455aa696a71c23e3

Output of `std::fmt::Debug`:

        [0, 18, 0, 3, 0, 0, 0, 0, 71, 88, 0, 0, 0, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 0, 0, 0, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 0, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 164, 18, 18, 0, 164, 0, 164, 164, 164, 30, 164, 164, 0, 0, 0, 0, 63]

Reproduce with:

        cargo fuzz run protocol_reader fuzz/artifacts/protocol_reader/crash-369f9787d35767c47431161d455aa696a71c23e3

Minimize test case with:

        cargo fuzz tmin protocol_reader fuzz/artifacts/protocol_reader/crash-369f9787d35767c47431161d455aa696a71c23e3
```

Sadly the backtraces that you might get are not really helpful and you need a debugger to detect the exact source
locations:

```console
$ rust-lldb ./target/x86_64-unknown-linux-gnu/release/protocol_reader fuzz/artifacts/protocol_reader/crash-7b824dad6e26002e5488e8cc84ce16728222dcf5
...

(lldb) r
...
Process 177543 launched: '/home/mneumann/src/rskafka/target/x86_64-unknown-linux-gnu/release/protocol_reader' (x86_64)
INFO: Running with entropic power schedule (0xFF, 100).
INFO: Seed: 3549747846
...
==177543==ABORTING
(lldb) AddressSanitizer report breakpoint hit. Use 'thread info -s' to get extended information about the report.
Process 177543 stopped
...

(lldb) bt
* thread #1, name = 'protocol_reader', stop reason = AddressSanitizer detected: allocation-size-too-big
  * frame #0: 0x0000555556c04f20 protocol_reader`::AsanDie() at asan_rtl.cpp:45:7
    frame #1: 0x0000555556c1a33c protocol_reader`__sanitizer::Die() at sanitizer_termination.cpp:55:7
    frame #2: 0x0000555556c01471 protocol_reader`::~ScopedInErrorReport() at asan_report.cpp:190:7
    frame #3: 0x0000555556c021f4 protocol_reader`::ReportAllocationSizeTooBig() at asan_report.cpp:313:1
...
```

Then create a unit test and fix the bug.

For out-of-memory errors [LLDB] does not stop automatically. You can however set a breakpoint before starting the
execution that hooks right into the place where it is about to exit:

```console
(lldb) b fuzzer::PrintStackTrace()
```

### Benchmarks
Install [cargo-criterion], make sure you have some Kafka cluster running, and then you can run all benchmarks with:

```console
$ TEST_INTEGRATION=1 TEST_BROKER_IMPL=kafka KAFKA_CONNECT=localhost:9011 cargo criterion --all-features
```

If you find a benchmark that is too slow, you can may want to profile it. Get [cargo-with], and [perf], then run (here
for the `parallel/rskafka` benchmark):

```console
$ TEST_INTEGRATION=1 TEST_BROKER_IMPL=kafka KAFKA_CONNECT=localhost:9011 cargo with 'perf record --call-graph dwarf -- {bin}' -- \
    bench --all-features --bench write_throughput -- \
    --bench --noplot parallel/rskafka
```

Have a look at the report:

```console
$ perf report
```


## License

Licensed under either of these:

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
 * MIT License ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

### Contributing

Unless you explicitly state otherwise, any contribution you intentionally submit for inclusion in the work, as defined
in the Apache-2.0 license, shall be dual-licensed as above, without any additional terms or conditions.


[^1]: Kafka's design makes it hard for any client to support the converse, as ultimately each partition is an
independent write stream within the broker. However, this crate makes no attempt to mitigate per-partition overheads
e.g. by batching writes to multiple partitions in a single ProduceRequest


[Apache Kafka]: https://kafka.apache.org/
[cargo-criterion]: https://github.com/bheisler/cargo-criterion
[cargo-fuzz]: https://github.com/rust-fuzz/cargo-fuzz
[cargo-with]: https://github.com/cbourjau/cargo-with
[gzip]: https://en.wikipedia.org/wiki/Gzip
[IOx]: https://github.com/influxdata/influxdb_iox/
[LLDB]: https://lldb.llvm.org/
[LZ4]: https://lz4.github.io/lz4/
[perf]: https://perf.wiki.kernel.org/index.php/Main_Page
[Redpanda]: https://vectorized.io/redpanda
[rustls]: https://github.com/rustls/rustls
[Snappy]: https://github.com/google/snappy
[zstd]: https://github.com/facebook/zstd
