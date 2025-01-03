use std::{fmt::Debug, sync::Arc};

use futures::future::BoxFuture;
use rsasl::{
    callback::SessionCallback,
    config::SASLConfig,
    property::{AuthzId, OAuthBearerKV, OAuthBearerToken},
};

use crate::messenger::SaslError;

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
    /// SASL - OAUTHBEARER
    ///
    /// # References
    /// - <https://datatracker.ietf.org/doc/html/rfc7628>
    Oauthbearer(OauthBearerCredentials),
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
    pub(crate) async fn get_sasl_config(&self) -> Result<Arc<SASLConfig>, SaslError> {
        match self {
            Self::Plain(credentials)
            | Self::ScramSha256(credentials)
            | Self::ScramSha512(credentials) => Ok(SASLConfig::with_credentials(
                None,
                credentials.username.clone(),
                credentials.password.clone(),
            )?),
            Self::Oauthbearer(credentials) => {
                // Fetch the token first, since that's an async call.
                let token = (*credentials.callback)()
                    .await
                    .map_err(SaslError::Callback)?;

                struct OauthProvider {
                    authz_id: Option<String>,
                    bearer_kvs: Vec<(String, String)>,
                    token: String,
                }

                // Define a callback that is called while stepping through the SASL client
                // to provide necessary data for oauth.
                // Since this callback is synchronous, we fetch the token first. Generally
                // speaking the SASL process should not take long enough for the token to
                // expire, but we do need to check for token expiry each time we authenticate.
                impl SessionCallback for OauthProvider {
                    fn callback(
                        &self,
                        _session_data: &rsasl::callback::SessionData,
                        _context: &rsasl::callback::Context<'_>,
                        request: &mut rsasl::callback::Request<'_>,
                    ) -> Result<(), rsasl::prelude::SessionError> {
                        request
                            .satisfy::<OAuthBearerKV>(
                                &self
                                    .bearer_kvs
                                    .iter()
                                    .map(|(k, v)| (k.as_str(), v.as_str()))
                                    .collect::<Vec<_>>(),
                            )?
                            .satisfy::<OAuthBearerToken>(&self.token)?;
                        if let Some(authz_id) = &self.authz_id {
                            request.satisfy::<AuthzId>(authz_id)?;
                        }
                        Ok(())
                    }
                }

                Ok(SASLConfig::builder()
                    .with_default_mechanisms()
                    .with_callback(OauthProvider {
                        authz_id: credentials.authz_id.clone(),
                        bearer_kvs: credentials.bearer_kvs.clone(),
                        token,
                    })?)
            }
        }
    }

    pub(crate) fn mechanism(&self) -> &str {
        use rsasl::mechanisms::*;
        match self {
            Self::Plain { .. } => plain::PLAIN.mechanism.as_str(),
            Self::ScramSha256 { .. } => scram::SCRAM_SHA256.mechanism.as_str(),
            Self::ScramSha512 { .. } => scram::SCRAM_SHA512.mechanism.as_str(),
            Self::Oauthbearer { .. } => oauthbearer::OAUTHBEARER.mechanism.as_str(),
        }
    }
}

type DynError = Box<dyn std::error::Error + Send + Sync>;

/// Callback for fetching an OAUTH token. This should cache tokens and only request a new token
/// when the old is close to expiring.
pub type OauthCallback =
    Arc<dyn Fn() -> BoxFuture<'static, Result<String, DynError>> + Send + Sync>;

#[derive(Clone)]
pub struct OauthBearerCredentials {
    /// Callback that should return a token that is valid and will remain valid for
    /// long enough to complete authentication. This should cache the token and only request
    /// a new one when the old is close to expiring.
    /// The token must be on [RFC 6750](https://www.rfc-editor.org/rfc/rfc6750) format.
    pub callback: OauthCallback,
    /// ID of a user to impersonate. Can be left as `None` to authenticate using
    /// the user for the token returned by `callback`.
    pub authz_id: Option<String>,
    /// Custom key-value pairs sent as part of the SASL request. Most normal usage
    /// can let this be an empty list.
    pub bearer_kvs: Vec<(String, String)>,
}

impl Debug for OauthBearerCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OauthBearerCredentials")
            .field("authz_id", &self.authz_id)
            .field("bearer_kvs", &self.bearer_kvs)
            .finish_non_exhaustive()
    }
}
