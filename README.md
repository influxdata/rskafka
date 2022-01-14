# RSKafka

This crate aims to be a minimal Kafka implementation for simple workloads that wish to use Kafka as a distributed
write-ahead log.

It is **not** a general-purpose Kafka implementation, instead it is heavily optimised for simplicity, both in terms of
implementation and its emergent operational characteristics. In particular, it aims to meet the needs
of [IOx](https://github.com/influxdata/influxdb_iox/).

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
    client::Client,
    record::Record,
};
use time::OffsetDateTime;
use std::collections::BTreeMap;

// setup client
let connection = "localhost:9093".to_owned();
let client = Client::new_plain(vec![connection]).await.unwrap();

// create a topic
let topic = "my_topic";
client.create_topic(
    topic,
    2,  // partitions
    1,  // replication factor
).await.unwrap();

// get a partition-bound client
let partition_client = client
    .partition_client(
        topic.to_owned(),
        0,  // partition
     )
    .await
    .unwrap();

// produce some data
let record = Record {
    key: b"".to_vec(),
    value: b"hello kafka".to_vec(),
    headers: BTreeMap::from([
        ("foo".to_owned(), b"bar".to_vec()),
    ]),
    timestamp: OffsetDateTime::now_utc(),
};
partition_client.produce(vec![record]).await.unwrap();

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

## Testing

### Redpanda

To run integration tests against Redpanda, run:

```console
$ docker-compose -f docker-compose-redpanda.yml up
```

in one session, and then run:

```console
$ TEST_INTEGRATION=1 KAFKA_CONNECT=0.0.0.0:9093 cargo test
```

in another session.

### Kafka

To run integration tests against Kafka, run:

```console
$ docker-compose -f docker-compose-kafka.yml up
```

in one session, and then run:

```console
$ TEST_INTEGRATION=1 KAFKA_CONNECT=localhost:9094 cargo test
```

in another session.

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
