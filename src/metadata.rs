//! Cluster-wide Kafka metadata.

/// Metadata container for the entire cluster.
#[derive(Debug, PartialEq)]
pub struct Metadata {
    /// Brokers.
    pub brokers: Vec<MetadataBroker>,

    /// The ID of the controller broker.
    pub controller_id: Option<i32>,

    /// Topics.
    pub topics: Vec<MetadataTopic>,
}

/// Metadata for a certain broker.
#[derive(Debug, PartialEq)]
pub struct MetadataBroker {
    /// The broker ID
    pub node_id: i32,

    /// The broker hostname
    pub host: String,

    /// The broker port
    pub port: i32,

    /// Rack.
    pub rack: Option<String>,
}

/// Metadata for a certain topic.
#[derive(Debug, PartialEq)]
pub struct MetadataTopic {
    /// The topic name
    pub name: String,

    /// True if the topic is internal
    pub is_internal: Option<bool>,

    /// Each partition in the topic
    pub partitions: Vec<MetadataPartition>,
}

/// Metadata for a certain partition.
#[derive(Debug, PartialEq)]
pub struct MetadataPartition {
    /// The partition index
    pub partition_index: i32,

    /// The ID of the leader broker
    pub leader_id: i32,

    /// The set of all nodes that host this partition
    pub replica_nodes: Vec<i32>,

    /// The set of all nodes that are in sync with the leader for this partition
    pub isr_nodes: Vec<i32>,
}
