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

#[cfg(feature = "transport-tls")]
pub type TlsConfig = Option<Arc<rustls::ClientConfig>>;

#[cfg(not(feature = "transport-tls"))]
pub type TlsConfig = ();

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),

    #[cfg(feature = "transport-tls")]
    #[error("Invalid Hostname: {0}")]
    BadHostname(#[from] rustls::client::InvalidDnsNameError),
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
    pub async fn connect(broker: &str, tls_config: TlsConfig) -> Result<Self> {
        let tcp_stream = TcpStream::connect(&broker).await?;
        Self::wrap_tls(tcp_stream, broker, tls_config).await
    }

    #[cfg(feature = "transport-tls")]
    async fn wrap_tls(tcp_stream: TcpStream, broker: &str, tls_config: TlsConfig) -> Result<Self> {
        match tls_config {
            Some(config) => {
                // Strip port if any
                let host = broker.split(':').next().unwrap();
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
