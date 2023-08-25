use pin_project_lite::pin_project;
use std::pin::Pin;
#[cfg(feature = "transport-tls")]
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

#[cfg(feature = "transport-tls")]
use tokio_rustls::{client::TlsStream, TlsConnector};

mod sasl;
pub use sasl::SaslConfig;

#[cfg(feature = "transport-tls")]
pub type TlsConfig = Option<Arc<rustls::ClientConfig>>;

#[cfg(not(feature = "transport-tls"))]
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone, Default)]
pub struct TlsConfig();

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Invalid host-port string: {0}")]
    InvalidHostPort(String),

    #[error("Invalid port: {0}")]
    InvalidPort(#[from] std::num::ParseIntError),

    #[cfg(feature = "transport-tls")]
    #[error("Invalid Hostname: {0}")]
    BadHostname(#[from] rustls::client::InvalidDnsNameError),

    #[cfg(feature = "transport-socks5")]
    #[error("Cannot establish SOCKS5 connection: {0}")]
    Socks5(#[from] async_socks5::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(feature = "transport-tls")]
pin_project! {
    #[project = TransportProj]
    #[derive(Debug)]
    pub enum Transport {
        Plain{
            #[pin]
            inner: TcpStream,
        },

        Tls{
            #[pin]
            inner: Box<TlsStream<TcpStream>>,
        },
    }
}

#[cfg(not(feature = "transport-tls"))]
pin_project! {
    #[project = TransportProj]
    #[derive(Debug)]
    pub enum Transport {
        Plain{
            #[pin]
            inner: TcpStream,
        },
    }
}

impl AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.project() {
            TransportProj::Plain { inner } => inner.poll_read(cx, buf),

            #[cfg(feature = "transport-tls")]
            TransportProj::Tls { inner } => inner.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.project() {
            TransportProj::Plain { inner } => inner.poll_write(cx, buf),

            #[cfg(feature = "transport-tls")]
            TransportProj::Tls { inner } => inner.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            TransportProj::Plain { inner } => inner.poll_flush(cx),

            #[cfg(feature = "transport-tls")]
            TransportProj::Tls { inner } => inner.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            TransportProj::Plain { inner } => inner.poll_shutdown(cx),

            #[cfg(feature = "transport-tls")]
            TransportProj::Tls { inner } => inner.poll_shutdown(cx),
        }
    }
}

impl Transport {
    pub async fn connect(
        broker: &str,
        tls_config: TlsConfig,
        socks5_proxy: Option<String>,
    ) -> Result<Self> {
        let tcp_stream = Self::connect_tcp(broker, socks5_proxy).await?;
        Self::wrap_tls(tcp_stream, broker, tls_config).await
    }

    #[cfg(feature = "transport-socks5")]
    async fn connect_tcp(broker: &str, socks5_proxy: Option<String>) -> Result<TcpStream> {
        use async_socks5::connect;

        match socks5_proxy {
            Some(proxy) => {
                let mut stream = TcpStream::connect(proxy).await?;

                let mut broker_iter = broker.split(':');
                let broker_host = broker_iter
                    .next()
                    .ok_or_else(|| Error::InvalidHostPort(broker.to_owned()))?;
                let broker_port: u16 = broker_iter
                    .next()
                    .ok_or_else(|| Error::InvalidHostPort(broker.to_owned()))?
                    .parse()?;

                connect(&mut stream, (broker_host, broker_port), None).await?;

                Ok(stream)
            }
            None => Ok(TcpStream::connect(broker).await?),
        }
    }

    #[cfg(not(feature = "transport-socks5"))]
    async fn connect_tcp(broker: &str, _socks5_proxy: Option<String>) -> Result<TcpStream> {
        Ok(TcpStream::connect(broker).await?)
    }

    #[cfg(feature = "transport-tls")]
    async fn wrap_tls(tcp_stream: TcpStream, broker: &str, tls_config: TlsConfig) -> Result<Self> {
        match tls_config {
            Some(config) => {
                // Strip port if any
                let host = broker
                    .split(':')
                    .next()
                    .ok_or_else(|| Error::InvalidHostPort(broker.to_owned()))?;
                let server_name = rustls::ServerName::try_from(host)?;

                let connector = TlsConnector::from(config);
                let tls_stream = connector.connect(server_name, tcp_stream).await?;
                Ok(Self::Tls {
                    inner: Box::new(tls_stream),
                })
            }
            None => Ok(Self::Plain { inner: tcp_stream }),
        }
    }

    #[cfg(not(feature = "transport-tls"))]
    async fn wrap_tls(
        tcp_stream: TcpStream,
        _broker: &str,
        _tls_config: TlsConfig,
    ) -> Result<Self> {
        Ok(Self::Plain { inner: tcp_stream })
    }
}
