/// Vector of results.
///
/// This type is marked as `must_use`, in contrast to a simple `Vec<Result<T, E>>`.
///
/// Note: The only we to consume this is by calling [`into_iter`}(Self::into_iter) or by using
/// [`unpack`](Self::unpack), because every other interaction (like checking the size) would bypass `must_use`.
#[derive(Debug)]
#[must_use]
pub struct ResultVec<T, E>(Vec<Result<T, E>>);

impl<T, E> ResultVec<T, E> {
    /// Unpacks results if all are `Ok` or returns first error.
    pub fn unpack(self) -> Result<Vec<T>, E> {
        self.0.into_iter().collect()
    }
}

impl<T, E> From<Vec<Result<T, E>>> for ResultVec<T, E> {
    fn from(v: Vec<Result<T, E>>) -> Self {
        Self(v)
    }
}

impl<T, E> IntoIterator for ResultVec<T, E> {
    type Item = Result<T, E>;

    type IntoIter = std::vec::IntoIter<Result<T, E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
