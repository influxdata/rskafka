use rand::prelude::*;
use std::time::Duration;

/// Exponential backoff with jitter
///
/// See <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    pub init_backoff: Duration,
    pub max_backoff: Duration,
    pub base: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            init_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(500),
            base: 3.,
        }
    }
}

/// [`Backoff`] can be created from a [`BackoffConfig`]
///
/// Consecutive calls to [`Backoff::next`] will return the next backoff interval
///
#[derive(Debug)]
pub struct Backoff<R> {
    init_backoff: f64,
    next_backoff_secs: f64,
    max_backoff_secs: f64,
    base: f64,
    rng: R,
}

impl Backoff<ThreadRng> {
    /// Create a new [`Backoff`] from the provided [`BackoffConfig`]
    pub fn new(config: &BackoffConfig) -> Self {
        Self::new_with_rng(config, thread_rng())
    }
}

impl<R: Rng> Backoff<R> {
    pub fn new_with_rng(config: &BackoffConfig, rng: R) -> Self {
        let init_backoff = config.init_backoff.as_secs_f64();
        Self {
            init_backoff,
            next_backoff_secs: init_backoff,
            max_backoff_secs: config.max_backoff.as_secs_f64(),
            base: config.base,
            rng,
        }
    }

    /// Returns the next backoff duration to wait for
    pub fn next(&mut self) -> Duration {
        let next_backoff = self.max_backoff_secs.min(
            self.rng
                .gen_range(self.init_backoff..(self.next_backoff_secs * self.base)),
        );
        Duration::from_secs_f64(std::mem::replace(&mut self.next_backoff_secs, next_backoff))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::mock::StepRng;

    #[test]
    fn test_backoff() {
        let init_backoff_secs = 1.;
        let max_backoff_secs = 500.;
        let base = 3.;

        let config = BackoffConfig {
            init_backoff: Duration::from_secs_f64(init_backoff_secs),
            max_backoff: Duration::from_secs_f64(max_backoff_secs),
            base,
        };

        let assert_fuzzy_eq = |a: f64, b: f64| assert!((b - a).abs() < 0.0001, "{} != {}", a, b);

        // Create a static rng that takes the minimum of the range
        let rng = StepRng::new(0, 0);
        let mut backoff = Backoff::new_with_rng(&config, rng);

        for _ in 0..20 {
            assert_eq!(backoff.next().as_secs_f64(), init_backoff_secs);
        }

        // Create a static rng that takes the maximum of the range
        let rng = StepRng::new(u64::MAX, 0);
        let mut backoff = Backoff::new_with_rng(&config, rng);

        for i in 0..20 {
            let value = (base.powi(i) * init_backoff_secs).min(max_backoff_secs);
            assert_fuzzy_eq(backoff.next().as_secs_f64(), value);
        }

        // Create a static rng that takes the mid point of the range
        let rng = StepRng::new(u64::MAX / 2, 0);
        let mut backoff = Backoff::new_with_rng(&config, rng);

        let mut value = init_backoff_secs;
        for _ in 0..20 {
            assert_fuzzy_eq(backoff.next().as_secs_f64(), value);
            value =
                (init_backoff_secs + (value * base - init_backoff_secs) / 2.).min(max_backoff_secs);
        }
    }
}
