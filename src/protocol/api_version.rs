use super::primitives::Int16;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct ApiVersion(pub Int16);
