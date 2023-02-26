# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## 0.3.0 -- Bug Fixes, Small API Improvements

### Improvements
- assorted DX and infrastructure improvements (#116, #123, #127, #128, #135, #138, #145, #146)
- convert some `as` casts to proper conversion (#117, #118, #120, #121)
- make `Client::{controller,partition}_client` sync (#136)

### Dependency Updates
- `rustls-pemfile` to 1.0 (#126)
- `uuid` to 1.0 (#131)

### Bug Fixes
- multiple cancellation and panic safety fixes (#113, #115)
- Java interopt, esp. for Snappy-compressed messages (#108)
- fix potential OOM cases (#138, #141)
- remove potentially buggy Redpanda quirk (#150)
- documentation (#151)


## 0.2.0 -- Essential Bug Fixes, Compression

### Breaking Changes
- `Record::{key,value}` are now optional, following the underlying Kafka protocol (#93)
- compression support, `PartitionClient::produce` requires `compression` parameter (#82, #91, #92, #94)
- `PartitionClient::get_high_watermark` was replaced by `PartitionClient::get_offset` (#100)
- `StreamConsumer::new` `start_offset` parameter changed from `i64` to `StartOffset` type (#104)
- rework features (#107)

### Features
- record deletion (#97)

### Bug Fixes
- ignore `InvalidReplicationFactor` (#106)
- fix rare panic in `BatchProducer` (#105)
- filter out records that were not requested (#99)
- terminate consumer stream on `OffsetOutOfRange` (#96)

### Performance
- faster CRC calculation (#85)


## 0.1.0 -- Initial Release
This is the first release featuring:

- tokio-based async connection handling
- TLS and SOCKS5 support
- listing topics
- create a topic
- produce records (w/o compression)
- consume records (w/o compression)
- basic consumer streams
- framework to set up record batching
