#![no_main]
use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use rskafka::protocol::{record::RecordBatchBody, traits::ReadType};

fuzz_target!(|data: &[u8]| {
    RecordBatchBody::read(&mut Cursor::new(data)).ok();
});
