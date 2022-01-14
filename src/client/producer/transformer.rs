//! Transforms user types into [Record]s.
//!
//! Note that we need custom types for this instead of `Fn` due to <https://github.com/rust-lang/rust/issues/29625>.
use crate::record::Record;

/// The error returned by [`Transformer`] implementations
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Transforms user types into [Record]s.
pub trait Transformer: Send {
    /// User type.
    type Input;

    /// Transform RSKafka result (e.g. offset) into another use type.
    type StatusBuilder: StatusBuilder;

    /// Perform transformation.
    ///
    /// Returns record and status transformer.
    fn transform(&self, input: Self::Input) -> Result<(Vec<Record>, Self::StatusBuilder), Error>;
}

/// Converts RSKafka result (for successful `produce` operations) into user type.
pub trait StatusBuilder: Send {
    /// User-defined status.
    type Status: Clone + Send + std::fmt::Debug + 'static;

    /// Build user-definied type.
    fn build(&self, res: Vec<i64>) -> Result<Self::Status, Error>;
}

/// Helper trait to access the status of an [`Transformer`].
pub trait TransformerStatus {
    type Status;
}

impl<T> TransformerStatus for T
where
    T: Transformer,
{
    type Status = <<Self as Transformer>::StatusBuilder as StatusBuilder>::Status;
}

/// Simple default transformer that does nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdentityTransformer {}

impl Transformer for IdentityTransformer {
    type Input = Vec<Record>;

    type StatusBuilder = IdentityTransformerStatusBuilder;

    fn transform(&self, input: Self::Input) -> Result<(Vec<Record>, Self::StatusBuilder), Error> {
        Ok((input, IdentityTransformerStatusBuilder {}))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IdentityTransformerStatusBuilder {}

impl StatusBuilder for IdentityTransformerStatusBuilder {
    type Status = Vec<i64>;

    fn build(&self, res: Vec<i64>) -> Result<Self::Status, Error> {
        Ok(res)
    }
}
