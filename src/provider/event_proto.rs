use std::fmt::Debug;

use iroh::NodeId;
use irpc::{
    channel::{none::NoSender, oneshot},
    rpc_requests,
};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use crate::{protocol::ChunkRangesSeq, provider::TransferStats, Hash};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum EventMode {
    /// We don't get these kinds of events at all
    #[default]
    None,
    /// We get a notification for these kinds of events
    Notify,
    /// We can respond to these kinds of events, either by aborting or by
    /// e.g. introducing a delay for throttling.
    Request,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum EventMode2 {
    /// We don't get these kinds of events at all
    #[default]
    None,
    /// We get a notification for these kinds of events
    Notify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AbortReason {
    RateLimited,
    Permission,
}

#[derive(Debug, Snafu)]
pub enum ClientError {
    RateLimited,
    Permission,
    #[snafu(transparent)]
    Irpc {
        source: irpc::Error,
    },
}

impl From<AbortReason> for ClientError {
    fn from(value: AbortReason) -> Self {
        match value {
            AbortReason::RateLimited => ClientError::RateLimited,
            AbortReason::Permission => ClientError::Permission,
        }
    }
}

pub type EventResult = Result<(), AbortReason>;
pub type ClientResult = Result<(), ClientError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EventMask {
    connected: EventMode,
    get: EventMode,
    get_many: EventMode,
    push: EventMode,
    transfer: EventMode,
    transfer_complete: EventMode2,
    transfer_aborted: EventMode2,
}

/// Newtype wrapper that wraps an event so that it is a distinct type for the notify variant.
#[derive(Debug, Serialize, Deserialize)]
pub struct Notify<T>(T);

#[derive(Debug, Default)]
pub struct Client {
    mask: EventMask,
    inner: Option<irpc::Client<ProviderProto>>,
}

/// A new get request was received from the provider.
#[derive(Debug, Serialize, Deserialize)]
pub struct GetRequestReceived {
    /// The connection id. Multiple requests can be sent over the same connection.
    pub connection_id: u64,
    /// The request id. There is a new id for each request.
    pub request_id: u64,
    /// The root hash of the request.
    pub hash: Hash,
    /// The exact query ranges of the request.
    pub ranges: ChunkRangesSeq,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetManyRequestReceived {
    /// The connection id. Multiple requests can be sent over the same connection.
    pub connection_id: u64,
    /// The request id. There is a new id for each request.
    pub request_id: u64,
    /// The root hash of the request.
    pub hashes: Vec<Hash>,
    /// The exact query ranges of the request.
    pub ranges: ChunkRangesSeq,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PushRequestReceived {
    /// The connection id. Multiple requests can be sent over the same connection.
    pub connection_id: u64,
    /// The request id. There is a new id for each request.
    pub request_id: u64,
    /// The root hash of the request.
    pub hash: Hash,
    /// The exact query ranges of the request.
    pub ranges: ChunkRangesSeq,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransferProgress {
    /// The connection id. Multiple requests can be sent over the same connection.
    pub connection_id: u64,
    /// The request id. There is a new id for each request.
    pub request_id: u64,
    /// The index of the blob in the request. 0 for the first blob or for raw blob requests.
    pub index: u64,
    /// The end offset of the chunk that was sent.
    pub end_offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransferStarted {
    /// The connection id. Multiple requests can be sent over the same connection.
    pub connection_id: u64,
    /// The request id. There is a new id for each request.
    pub request_id: u64,
    /// The index of the blob in the request. 0 for the first blob or for raw blob requests.
    pub index: u64,
    /// The hash of the blob. This is the hash of the request for the first blob, the child hash (index-1) for subsequent blobs.
    pub hash: Hash,
    /// The size of the blob. This is the full size of the blob, not the size we are sending.
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransferCompleted {
    /// The connection id. Multiple requests can be sent over the same connection.
    pub connection_id: u64,
    /// The request id. There is a new id for each request.
    pub request_id: u64,
    /// Statistics about the transfer.
    pub stats: Box<TransferStats>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransferAborted {
    /// The connection id. Multiple requests can be sent over the same connection.
    pub connection_id: u64,
    /// The request id. There is a new id for each request.
    pub request_id: u64,
    /// Statistics about the part of the transfer that was aborted.
    pub stats: Option<Box<TransferStats>>,
}

/// Client for progress notifications.
///
/// For most event types, the client can be configured to either send notifications or requests that
/// can have a response.
impl Client {
    /// A client that does not send anything.
    pub const NONE: Self = Self {
        mask: EventMask {
            connected: EventMode::None,
            get: EventMode::None,
            get_many: EventMode::None,
            push: EventMode::None,
            transfer: EventMode::None,
            transfer_complete: EventMode2::None,
            transfer_aborted: EventMode2::None,
        },
        inner: None,
    };

    pub async fn client_connected(&self, f: impl Fn() -> ClientConnected) -> ClientResult {
        Ok(if let Some(client) = &self.inner {
            match self.mask.connected {
                EventMode::None => {}
                EventMode::Notify => client.notify(Notify(f())).await?,
                EventMode::Request => client.rpc(f()).await??,
            }
        })
    }

    pub async fn get_request(&self, f: impl Fn() -> GetRequestReceived) -> ClientResult {
        Ok(if let Some(client) = &self.inner {
            match self.mask.get {
                EventMode::None => {}
                EventMode::Notify => client.notify(Notify(f())).await?,
                EventMode::Request => client.rpc(f()).await??,
            }
        })
    }

    pub async fn push_request(&self, f: impl Fn() -> PushRequestReceived) -> ClientResult {
        Ok(if let Some(client) = &self.inner {
            match self.mask.push {
                EventMode::None => {}
                EventMode::Notify => client.notify(Notify(f())).await?,
                EventMode::Request => client.rpc(f()).await??,
            }
        })
    }

    pub async fn send_get_many_request(
        &self,
        f: impl Fn() -> GetManyRequestReceived,
    ) -> ClientResult {
        Ok(if let Some(client) = &self.inner {
            match self.mask.get_many {
                EventMode::None => {}
                EventMode::Notify => client.notify(Notify(f())).await?,
                EventMode::Request => client.rpc(f()).await??,
            }
        })
    }

    pub async fn transfer_progress(&self, f: impl Fn() -> TransferProgress) -> ClientResult {
        Ok(if let Some(client) = &self.inner {
            match self.mask.transfer {
                EventMode::None => {}
                EventMode::Notify => client.notify(Notify(f())).await?,
                EventMode::Request => client.rpc(f()).await??,
            }
        })
    }
}

#[rpc_requests(message = ProviderMessage)]
#[derive(Debug, Serialize, Deserialize)]
pub enum ProviderProto {
    /// A new client connected to the provider.
    #[rpc(tx = oneshot::Sender<EventResult>)]
    #[wrap(ClientConnected)]
    ClientConnected { connection_id: u64, node_id: NodeId },
    /// A new client connected to the provider. Notify variant.
    #[rpc(tx = NoSender)]
    ClientConnectedNotify(Notify<ClientConnected>),
    /// A client disconnected from the provider.
    #[rpc(tx = NoSender)]
    #[wrap(ConnectionClosed)]
    ConnectionClosed { connection_id: u64 },

    #[rpc(tx = oneshot::Sender<EventResult>)]
    /// A new get request was received from the provider.
    GetRequestReceived(GetRequestReceived),

    #[rpc(tx = NoSender)]
    /// A new get request was received from the provider.
    GetRequestReceivedNotify(Notify<GetRequestReceived>),
    /// A new get request was received from the provider.
    #[rpc(tx = oneshot::Sender<EventResult>)]
    GetManyRequestReceived(GetManyRequestReceived),
    /// A new get request was received from the provider.
    #[rpc(tx = NoSender)]
    GetManyRequestReceivedNotify(Notify<GetManyRequestReceived>),
    /// A new get request was received from the provider.
    #[rpc(tx = oneshot::Sender<EventResult>)]
    PushRequestReceived(PushRequestReceived),
    /// A new get request was received from the provider.
    #[rpc(tx = NoSender)]
    PushRequestReceivedNotify(Notify<PushRequestReceived>),
    /// Transfer for the nth blob started.
    #[rpc(tx = NoSender)]
    TransferStarted(TransferStarted),
    /// Progress of the transfer.
    #[rpc(tx = oneshot::Sender<EventResult>)]
    TransferProgress(TransferProgress),
    /// Progress of the transfer.
    #[rpc(tx = NoSender)]
    TransferProgressNotify(Notify<TransferProgress>),
    /// Entire transfer completed.
    #[rpc(tx = NoSender)]
    TransferCompleted(TransferCompleted),
    /// Entire transfer aborted.
    #[rpc(tx = NoSender)]
    TransferAborted(TransferAborted),
}
