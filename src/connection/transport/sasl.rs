#[derive(Debug, Clone)]
pub enum SaslConfig {
    /// SASL - PLAIN
    ///
    /// # References
    /// - <https://datatracker.ietf.org/doc/html/rfc4616>
    Plain(Credentials),
    /// SASL - SCRAM-SHA-256
    ///
    /// # References
    /// - <https://datatracker.ietf.org/doc/html/rfc7677>
    ScramSha256(Credentials),
    /// SASL - SCRAM-SHA-512
    ///
    /// # References
    /// - <https://datatracker.ietf.org/doc/html/draft-melnikov-scram-sha-512-04>
    ScramSha512(Credentials),
}

#[derive(Debug, Clone)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

impl Credentials {
    pub fn new(username: String, password: String) -> Self {
        Self { username, password }
    }
}

impl SaslConfig {
    pub(crate) fn credentials(&self) -> Credentials {
        match self {
            Self::Plain(credentials) => credentials.clone(),
            Self::ScramSha256(credentials) => credentials.clone(),
            Self::ScramSha512(credentials) => credentials.clone(),
        }
    }

    pub(crate) fn mechanism(&self) -> &str {
        match self {
            Self::Plain { .. } => "PLAIN",
            Self::ScramSha256 { .. } => "SCRAM-SHA-256",
            Self::ScramSha512 { .. } => "SCRAM-SHA-512",
        }
    }
}
