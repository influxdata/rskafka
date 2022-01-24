//! Helpers for common checks for the validity of data.

/// Validates a collection contains exactly one item when expected to.
pub trait ExactlyOne<T> {
    fn exactly_one(self) -> Result<T, usize>;
}

impl<T> ExactlyOne<T> for Vec<T> {
    /// Returns the one item contained in the vector if there is exactly one
    /// item; returns an error if there is more than one item in the vector.
    fn exactly_one(mut self) -> Result<T, usize> {
        let v = self.pop().ok_or(0_usize)?;
        if self.is_empty() {
            Ok(v)
        } else {
            Err(self.len() + 1)
        }
    }
}
