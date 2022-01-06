use crate::protocol::messages::MetadataResponseBroker;
use parking_lot::RwLock;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

#[derive(Debug, Default)]
pub struct BrokerTopology {
    topology: RwLock<HashMap<i32, Broker>>,
}

#[derive(Debug, Clone)]
struct Broker {
    host: String,
    port: i32,
}

impl Display for Broker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl<'a> From<&'a MetadataResponseBroker> for Broker {
    fn from(b: &'a MetadataResponseBroker) -> Self {
        Broker {
            host: b.host.0.clone(),
            port: b.port.0,
        }
    }
}

impl BrokerTopology {
    pub fn is_empty(&self) -> bool {
        self.topology.read().is_empty()
    }

    /// Returns the broker URL for the provided broker
    pub async fn get_broker_url(&self, broker_id: i32) -> Option<String> {
        self.topology
            .read()
            .get(&broker_id)
            .map(ToString::to_string)
    }

    /// Returns a list of all broker URLs
    pub fn get_broker_urls(&self) -> Vec<String> {
        self.topology
            .read()
            .values()
            .map(ToString::to_string)
            .collect()
    }

    /// Updates with the provided broker metadata
    pub fn update(&self, brokers: &[MetadataResponseBroker]) {
        let mut topology = self.topology.write();
        for broker in brokers {
            match topology.entry(broker.node_id.0) {
                Entry::Occupied(mut o) => {
                    let current = o.get_mut();
                    if current.host != broker.host.0 || current.port != broker.port.0 {
                        let new = Broker::from(broker);
                        println!(
                            "Broker {} update from {} to {}",
                            broker.node_id.0, current, new
                        );
                        *current = new;
                    }
                }
                Entry::Vacant(v) => {
                    let new = Broker::from(broker);
                    println!("New broker {}: {}", broker.node_id.0, new);
                    v.insert(new);
                }
            }
        }
    }
}
