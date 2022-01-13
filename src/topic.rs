use std::collections::BTreeSet;

#[derive(Debug)]
pub struct Topic {
    pub name: String,
    pub partitions: BTreeSet<i32>,
}
