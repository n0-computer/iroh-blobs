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
use std::{io, path::PathBuf};

use anyhow::Result;
use async_compression::tokio::{bufread::Lz4Decoder, write::Lz4Encoder};
use clap::Parser;
use common::setup_logging;
use iroh::{endpoint::VarInt, protocol::ProtocolHandler};
use iroh_blobs::{
    api::Store,
    provider::{
        events::{ClientConnected, EventSender, HasErrorCode},
        handle_stream, AsyncReadRecvStream, AsyncWriteSendStream, RecvStreamSpecific,
        SendStreamSpecific, StreamPair,
    },
    store::mem::MemStore,
    ticket::BlobTicket,
};
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
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

struct CompressedWriteStream(Lz4Encoder<iroh::endpoint::SendStream>);

impl SendStreamSpecific for CompressedWriteStream {
    fn inner(&mut self) -> &mut (impl AsyncWrite + Unpin + Send) {
        &mut self.0
    }

    fn reset(&mut self, code: VarInt) -> io::Result<()> {
        Ok(self.0.get_mut().reset(code)?)
    }

    async fn stopped(&mut self) -> io::Result<Option<VarInt>> {
        Ok(self.0.get_mut().stopped().await?)
    }
}

struct CompressedReadStream(Lz4Decoder<BufReader<iroh::endpoint::RecvStream>>);

impl RecvStreamSpecific for CompressedReadStream {
    fn inner(&mut self) -> &mut (impl AsyncRead + Unpin + Send) {
        &mut self.0
    }

    fn stop(&mut self, code: VarInt) -> io::Result<()> {
        Ok(self.0.get_mut().get_mut().stop(code)?)
    }

    fn id(&self) -> u64 {
        self.0.get_ref().get_ref().id().index()
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
            let send = AsyncWriteSendStream::new(CompressedWriteStream(Lz4Encoder::new(send)));
            let recv = AsyncReadRecvStream::new(CompressedReadStream(Lz4Decoder::new(
                BufReader::new(recv),
            )));
            let store = self.store.clone();
            let pair = StreamPair::new(connection_id, recv, send, self.events.clone());
            tokio::spawn(handle_stream(pair, store));
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
            let store = MemStore::new();
            let conn = endpoint.connect(ticket.node_addr().clone(), ALPN).await?;
            let connection_id = conn.stable_id() as u64;
            let (send, recv) = conn.open_bi().await?;
            let send = AsyncWriteSendStream::new(CompressedWriteStream(Lz4Encoder::new(send)));
            let recv = AsyncReadRecvStream::new(CompressedReadStream(Lz4Decoder::new(
                BufReader::new(recv),
            )));
            let sp = StreamPair::new(connection_id, recv, send, EventSender::DEFAULT);
            let stats = store.remote().fetch(sp, ticket.hash_and_format()).await?;
            if let Some(target) = target {
                let size = store.export(ticket.hash(), &target).await?;
                println!("Wrote {} bytes to {}", size, target.display());
            } else {
                println!("Hash: {}", ticket.hash());
            }
        }
    }
    Ok(())
}
