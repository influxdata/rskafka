# MiniKafka

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
* Have a low number of high-throughput partitions <sup>1.</sup>

<sup>1.</sup> Kafka's design makes it hard for any client to support the converse, as ultimately each partition is an
independent write stream within the broker. However, this crate makes no attempt to mitigate per-partition overheads
e.g. by batching writes to multiple partitions in a single ProduceRequest
