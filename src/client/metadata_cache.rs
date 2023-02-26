use std::ops::Deref;

use parking_lot::Mutex;
use tracing::{debug, info};

use crate::protocol::messages::MetadataResponse;

/// Cache generation for [`MetadataCache`].
///
/// This is used to avoid double-invalidating a cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataCacheGeneration(usize);

/// A [`MetadataCache`] provides look-aside caching of [`MetadataResponse`]
/// instances.
#[derive(Debug)]
pub(crate) struct MetadataCache {
    cache: Mutex<(Option<MetadataResponse>, MetadataCacheGeneration)>,
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self {
            cache: Mutex::new((None, MetadataCacheGeneration(0))),
        }
    }
}

impl MetadataCache {
    /// Grab a copy of the cached metadata.
    ///
    /// If `topics` is `Some` the returned metadata contains topics that are
    /// filtered to match by name. If a topic name is specified that doesn't
    /// exist in the cached metadata, the cache is invalidated.
    pub(crate) fn get(
        &self,
        topics: &Option<Vec<String>>,
    ) -> Option<(MetadataResponse, MetadataCacheGeneration)> {
        let (mut m, gen) = match self.cache.lock().deref() {
            (Some(m), gen) => (m.clone(), *gen),
            (None, _) => {
                return None;
            }
        };

        // If the caller requested a subset of topics, filter the cached result
        // to ensure only the expected topics are present.
        if let Some(want) = topics {
            // Filter out any topics the caller did not ask for.
            m.topics.retain(|t| want.contains(&t.name.0));

            // Validate the resulting number of topics in the metadata response.
            if m.topics.len() != want.len() {
                // The caller requested more topics than the cached entry
                // contains. This may indicate the cached entry is stale.
                //
                // In order to maximise correctness, do not use the cached entry
                // and invalidate this cache, at the expense of cache thrashing
                // if a caller keeps requesting metadata for a non-existent
                // topic.
                debug!("cached metadata query for unknown topic");
                self.invalidate("get from metadata cache: unknown topic", gen);
                return None;
            }
        }

        debug!(?m, "using cached metadata response");

        Some((m, gen))
    }

    pub(crate) fn invalidate(&self, reason: &'static str, gen: MetadataCacheGeneration) {
        let mut guard = self.cache.lock();
        if guard.1 != gen {
            // stale request
            debug!(
                reason,
                current_gen = guard.1 .0,
                request_gen = gen.0,
                "stale invalidation request for metadata cache",
            );
            return;
        }

        guard.0 = None;
        info!(reason, "invalidated metadata cache",);
    }

    pub(crate) fn update(&self, m: MetadataResponse) {
        let mut guard = self.cache.lock();
        guard.0 = Some(m);
        guard.1 .0 += 1;
        debug!("updated metadata cache");
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::{
        messages::MetadataResponseTopic,
        primitives::{Int32, String_},
    };

    use super::*;

    /// Generate a MetadataResponse with the specified topics.
    fn response_with_topics(topics: Option<&'static [&'static str]>) -> MetadataResponse {
        let topics = topics
            .into_iter()
            .flatten()
            .map(|t| MetadataResponseTopic {
                name: String_(t.to_string()),
                error: Default::default(),
                is_internal: Default::default(),
                partitions: Default::default(),
            })
            .collect();

        MetadataResponse {
            throttle_time_ms: Some(Int32(42)),
            brokers: Default::default(),
            cluster_id: Default::default(),
            controller_id: Default::default(),
            topics,
        }
    }

    #[test]
    fn test_get() {
        let cache = MetadataCache::default();
        assert!(cache.get(&None).is_none());

        let m = response_with_topics(None);
        cache.update(m.clone());

        let (got, _gen) = cache.get(&None).expect("should have cached entry");
        assert_eq!(m, got);
    }

    #[test]
    fn test_get_topic_subset_filtered() {
        let cache = MetadataCache::default();
        cache.update(response_with_topics(Some(&["bananas", "platanos"])));

        // Request a subset of the topics
        let (got, _gen) = cache
            .get(&Some(vec!["bananas".to_string()]))
            .expect("should have cached entry");
        assert_eq!(response_with_topics(Some(&["bananas"])), got);

        let (got, _gen) = cache.get(&Some(vec![])).expect("should have cached entry");
        assert_eq!(response_with_topics(Some(&[])), got);

        // A request for "None" actually means "all of them".
        let (got, _gen) = cache.get(&None).expect("should have cached entry");
        assert_eq!(response_with_topics(Some(&["bananas", "platanos"])), got);
    }

    #[test]
    fn test_get_missing_topic_invalidate() {
        let cache = MetadataCache::default();
        cache.update(response_with_topics(Some(&["bananas", "platanos"])));

        assert!(cache.get(&Some(vec!["bananas".to_string()])).is_some());

        // Request an unknown topic and assert the cache was invalidated as a
        // result
        assert!(cache.get(&Some(vec!["goats".to_string()])).is_none());
        // The previously successful get should now return no cached entry
        assert!(cache.get(&Some(vec!["bananas".to_string()])).is_none());
    }

    #[test]
    fn test_explicit_invalidate() {
        let cache = MetadataCache::default();
        cache.update(MetadataResponse {
            throttle_time_ms: Default::default(),
            brokers: Default::default(),
            cluster_id: Default::default(),
            controller_id: Default::default(),
            topics: Default::default(),
        });

        let (_data, gen1) = cache.get(&None).unwrap();
        cache.invalidate("test", gen1);
        assert!(cache.get(&None).is_none());

        cache.update(MetadataResponse {
            throttle_time_ms: Default::default(),
            brokers: Default::default(),
            cluster_id: Default::default(),
            controller_id: Default::default(),
            topics: Default::default(),
        });

        let (_data, gen2) = cache.get(&None).unwrap();

        // outdated gen
        cache.invalidate("test", gen1);
        assert!(cache.get(&None).is_some());

        // the actual gen
        cache.invalidate("test", gen2);
        assert!(cache.get(&None).is_none());
    }
}
