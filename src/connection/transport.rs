use pin_project::pin_project;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Invalid Hostname: {0}")]
    BadHostname(#[from] rustls::client::InvalidDnsNameError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[pin_project]
pub enum Transport {
    Plain(#[pin] TcpStream),
    Tls(#[pin] Box<TlsStream<TcpStream>>),
}

impl AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.project() {
            __TransportProjection::Plain(p) => p.poll_read(cx, buf),
            __TransportProjection::Tls(p) => p.poll_read(cx, buf),
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
            __TransportProjection::Plain(p) => p.poll_write(cx, buf),
            __TransportProjection::Tls(p) => p.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            __TransportProjection::Plain(p) => p.poll_flush(cx),
            __TransportProjection::Tls(p) => p.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            __TransportProjection::Plain(p) => p.poll_shutdown(cx),
            __TransportProjection::Tls(p) => p.poll_shutdown(cx),
        }
    }
}

impl Transport {
    pub async fn connect(
        broker: &str,
        tls_config: Option<Arc<rustls::ClientConfig>>,
    ) -> Result<Self> {
        let tcp_stream = TcpStream::connect(&broker).await?;
        match tls_config {
            Some(config) => {
                // Strip port if any
                let host = broker.split(':').next().unwrap();
                let server_name = rustls::ServerName::try_from(host)?;

                let connector = TlsConnector::from(config);
                let tls_stream = connector.connect(server_name, tcp_stream).await?;
                Ok(Self::Tls(Box::new(tls_stream)))
            }
            None => Ok(Self::Plain(tcp_stream)),
        }
    }
}
