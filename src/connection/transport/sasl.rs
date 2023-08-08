#[derive(Debug, Clone)]
pub enum SaslConfig {
    /// SASL - PLAIN
    ///
    /// # References
    /// - <https://datatracker.ietf.org/doc/html/rfc4616>
    Plain { username: String, password: String },
}

impl SaslConfig {
    pub(crate) fn auth_bytes(&self) -> Vec<u8> {
        match self {
            Self::Plain { username, password } => {
                let mut auth: Vec<u8> = vec![0];
                auth.extend(username.bytes());
                auth.push(0);
                auth.extend(password.bytes());
                auth
            }
        }
    }

    pub(crate) fn mechanism(&self) -> &str {
        match self {
            Self::Plain { .. } => "PLAIN",
        }
    }
}
