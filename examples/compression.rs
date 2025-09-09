/// Example how to limit blob requests by hash and node id, and to add
/// throttling or limiting the maximum number of connections.
///
/// Limiting is done via a fn that returns an EventSender and internally
/// makes liberal use of spawn to spawn background tasks.
///
/// This is fine, since the tasks will terminate as soon as the [BlobsProtocol]
/// instance holding the [EventSender] will be dropped. But for production
/// grade code you might nevertheless put the tasks into a [tokio::task::JoinSet] or
/// [n0_future::FuturesUnordered].
mod common;
use std::{io, path::PathBuf, time::Instant};

use anyhow::Result;
use async_compression::tokio::{bufread::Lz4Decoder, write::Lz4Encoder};
use bao_tree::blake3;
use clap::Parser;
use common::setup_logging;
use iroh::protocol::ProtocolHandler;
use iroh_blobs::{
    api::Store,
    get::fsm::{AtConnected, ConnectedNext, EndBlobNext},
    protocol::{ChunkRangesSeq, GetRequest, Request},
    provider::{
        events::{ClientConnected, EventSender, HasErrorCode},
        handle_get, AsyncReadRecvStream, AsyncWriteSendStream, StreamPair,
    },
    store::mem::MemStore,
    ticket::BlobTicket,
};
use tokio::io::BufReader;
use tracing::debug;

use crate::common::get_or_generate_secret_key;

#[derive(Debug, Parser)]
#[command(version, about)]
pub enum Args {
    /// Limit requests by node id
    Provide {
        /// Path for files to add.
        path: PathBuf,
    },
    /// Get a blob. Just for completeness sake.
    Get {
        /// Ticket for the blob to download
        ticket: BlobTicket,
        /// Path to save the blob to
        #[clap(long)]
        target: Option<PathBuf>,
    },
}

struct CompressedWriter(async_compression::tokio::write::Lz4Encoder<iroh::endpoint::SendStream>);
struct CompressedReader(
    async_compression::tokio::bufread::Lz4Decoder<BufReader<iroh::endpoint::RecvStream>>,
);

impl iroh_blobs::provider::SendStream for CompressedWriter {
    async fn send_bytes(&mut self, bytes: bytes::Bytes) -> io::Result<()> {
        AsyncWriteSendStream::new(self).send_bytes(bytes).await
    }

    async fn send<const L: usize>(&mut self, buf: &[u8; L]) -> io::Result<()> {
        AsyncWriteSendStream::new(self).send(buf).await
    }

    async fn sync(&mut self) -> io::Result<()> {
        AsyncWriteSendStream::new(self).sync().await
    }
}

impl iroh_blobs::provider::RecvStream for CompressedReader {
    async fn recv_bytes(&mut self, len: usize) -> io::Result<bytes::Bytes> {
        AsyncReadRecvStream::new(self).recv_bytes(len).await
    }

    async fn recv_bytes_exact(&mut self, len: usize) -> io::Result<bytes::Bytes> {
        AsyncReadRecvStream::new(self).recv_bytes_exact(len).await
    }

    async fn recv<const L: usize>(&mut self) -> io::Result<[u8; L]> {
        AsyncReadRecvStream::new(self).recv::<L>().await
    }
}

impl tokio::io::AsyncRead for CompressedReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for CompressedWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl iroh_blobs::provider::SendStreamSpecific for CompressedWriter {
    fn reset(&mut self, code: quinn::VarInt) -> io::Result<()> {
        self.0.get_mut().reset(code)?;
        Ok(())
    }

    async fn stopped(&mut self) -> io::Result<Option<quinn::VarInt>> {
        let res = self.0.get_mut().stopped().await?;
        Ok(res)
    }
}

impl iroh_blobs::provider::RecvStreamSpecific for CompressedReader {
    fn stop(&mut self, code: quinn::VarInt) -> io::Result<()> {
        self.0.get_mut().get_mut().stop(code)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct CompressedBlobsProtocol {
    store: Store,
    events: EventSender,
}

impl CompressedBlobsProtocol {
    fn new(store: &Store, events: EventSender) -> Self {
        Self {
            store: store.clone(),
            events,
        }
    }
}

impl ProtocolHandler for CompressedBlobsProtocol {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> std::result::Result<(), iroh::protocol::AcceptError> {
        let connection_id = connection.stable_id() as u64;
        let node_id = connection.remote_node_id()?;
        if let Err(cause) = self
            .events
            .client_connected(|| ClientConnected {
                connection_id,
                node_id,
            })
            .await
        {
            connection.close(cause.code(), cause.reason());
            debug!("closing connection: {cause}");
            return Ok(());
        }
        while let Ok((send, recv)) = connection.accept_bi().await {
            let stream_id = send.id().index();
            let send = CompressedWriter(Lz4Encoder::new(send));
            let recv = CompressedReader(Lz4Decoder::new(BufReader::new(recv)));
            let store = self.store.clone();
            let mut pair =
                StreamPair::new(connection_id, stream_id, recv, send, self.events.clone());
            tokio::spawn(async move {
                let request = pair.read_request().await?;
                if let Request::Get(request) = request {
                    handle_get(pair, store, request).await?;
                }
                anyhow::Ok(())
            });
        }
        Ok(())
    }
}

const ALPN: &[u8] = b"iroh-blobs-compressed/0.1.0";

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging();
    let args = Args::parse();
    let secret = get_or_generate_secret_key()?;
    let endpoint = iroh::Endpoint::builder()
        .secret_key(secret)
        .discovery_n0()
        .bind()
        .await?;
    match args {
        Args::Provide { path } => {
            let store = MemStore::new();
            let tag = store.add_path(path).await?;
            let blobs = CompressedBlobsProtocol::new(&store, EventSender::DEFAULT);
            let router = iroh::protocol::Router::builder(endpoint.clone())
                .accept(ALPN, blobs)
                .spawn();
            let ticket = BlobTicket::new(endpoint.node_id().into(), tag.hash, tag.format);
            println!("Serving blob with hash {}", tag.hash);
            println!("Ticket: {ticket}");
            println!("Node is running. Press Ctrl-C to exit.");
            tokio::signal::ctrl_c().await?;
            println!("Shutting down.");
            router.shutdown().await?;
        }
        Args::Get { ticket, target } => {
            let conn = endpoint.connect(ticket.node_addr().clone(), ALPN).await?;
            let (send, recv) = conn.open_bi().await?;
            let send = CompressedWriter(Lz4Encoder::new(send));
            let recv = CompressedReader(Lz4Decoder::new(BufReader::new(recv)));
            let request = GetRequest {
                hash: ticket.hash(),
                ranges: ChunkRangesSeq::root(),
            };
            let connected =
                AtConnected::new(Instant::now(), recv, send, request, Default::default());
            let ConnectedNext::StartRoot(start) = connected.next().await? else {
                unreachable!("expected start root");
            };
            let (end, data) = start.next().concatenate_into_vec().await?;
            let EndBlobNext::Closing(closing) = end.next() else {
                unreachable!("expected closing");
            };
            let stats = closing.next().await?;
            if let Some(target) = target {
                tokio::fs::write(&target, &data).await?;
                println!(
                    "Wrote {} bytes to {}",
                    stats.payload_bytes_read,
                    target.display()
                );
            } else {
                let hash = blake3::hash(&data);
                println!("Hash: {hash}");
            }
        }
    }
    Ok(())
}
