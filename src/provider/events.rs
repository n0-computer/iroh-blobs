use std::fmt::Debug;

use irpc::{
    channel::{mpsc, none::NoSender, oneshot},
    rpc_requests, Channels, WithChannels,
};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use crate::{provider::{events::irpc_ext::IrpcClientExt, TransferStats}, Hash};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ConnectMode {
    /// We don't get notification of connect events at all.
    #[default]
    None,
    /// We get a notification for connect events.
    Notify,
    /// We get a request for connect events and can reject incoming connections.
    Request,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum RequestMode {
    /// We don't get request events at all.
    #[default]
    None,
    /// We get a notification for each request.
    Notify,
    /// We get a request for each request, and can reject incoming requests.
    Request,
    /// We get a notification for each request as well as detailed transfer events.
    NotifyLog,
    /// We get a request for each request, and can reject incoming requests.
    /// We also get detailed transfer events.
    RequestLog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ThrottleMode {
    /// We don't get these kinds of events at all
    #[default]
    None,
    /// We call throttle to give the event handler a way to throttle requests
    Throttle,
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

impl From<irpc::channel::RecvError> for ClientError {
    fn from(value: irpc::channel::RecvError) -> Self {
        ClientError::Irpc {
            source: value.into(),
        }
    }
}

impl From<irpc::channel::SendError> for ClientError {
    fn from(value: irpc::channel::SendError) -> Self {
        ClientError::Irpc {
            source: value.into(),
        }
    }
}

pub type EventResult = Result<(), AbortReason>;
pub type ClientResult = Result<(), ClientError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EventMask {
    connected: ConnectMode,
    get: RequestMode,
    get_many: RequestMode,
    push: RequestMode,
    /// throttling is somewhat costly, so you can disable it completely
    throttle: ThrottleMode,
}

impl EventMask {
    /// Everything is disabled. You won't get any events, but there is also no runtime cost.
    pub const NONE: Self = Self {
        connected: ConnectMode::None,
        get: RequestMode::None,
        get_many: RequestMode::None,
        push: RequestMode::None,
        throttle: ThrottleMode::None,
    };

    /// You get asked for every single thing that is going on and can intervene/throttle.
    pub const ALL: Self = Self {
        connected: ConnectMode::Request,
        get: RequestMode::RequestLog,
        get_many: RequestMode::RequestLog,
        push: RequestMode::RequestLog,
        throttle: ThrottleMode::Throttle,
    };

    /// You get notified for every single thing that is going on, but can't intervene.
    pub const NOTIFY_ALL: Self = Self {
        connected: ConnectMode::Notify,
        get: RequestMode::NotifyLog,
        get_many: RequestMode::NotifyLog,
        push: RequestMode::NotifyLog,
        throttle: ThrottleMode::None,
    };
}

/// Newtype wrapper that wraps an event so that it is a distinct type for the notify variant.
#[derive(Debug, Serialize, Deserialize)]
pub struct Notify<T>(T);

#[derive(Debug, Default, Clone)]
pub struct EventSender {
    mask: EventMask,
    inner: Option<irpc::Client<ProviderProto>>,
}

#[derive(Debug, Default)]
enum RequestUpdates {
    /// Request tracking was not configured, all ops are no-ops
    #[default]
    None,
    /// Active request tracking, all ops actually send
    Active(mpsc::Sender<RequestUpdate>),
    /// Disabled request tracking, we just hold on to the sender so it drops
    /// once the request is completed or aborted.
    Disabled(#[allow(dead_code)] mpsc::Sender<RequestUpdate>),
}

#[derive(Debug)]
pub struct RequestTracker {
    updates: RequestUpdates,
    throttle: Option<(irpc::Client<ProviderProto>, u64, u64)>,
}

impl RequestTracker {
    fn new(
        updates: RequestUpdates,
        throttle: Option<(irpc::Client<ProviderProto>, u64, u64)>,
    ) -> Self {
        Self { updates, throttle }
    }

    /// A request tracker that doesn't track anything.
    const NONE: Self = Self {
        updates: RequestUpdates::None,
        throttle: None,
    };

    /// Transfer for index `index` started, size `size`
    pub async fn transfer_started(&self, index: u64, hash: &Hash, size: u64) -> irpc::Result<()> {
        if let RequestUpdates::Active(tx) = &self.updates {
            tx.send(RequestUpdate::Started(TransferStarted { index, hash: *hash, size }))
                .await?;
        }
        Ok(())
    }

    /// Transfer progress for the previously reported blob, end_offset is the new end offset in bytes.
    pub async fn transfer_progress(&mut self, end_offset: u64) -> ClientResult {
        if let RequestUpdates::Active(tx) = &mut self.updates {
            tx.try_send(RequestUpdate::Progress(TransferProgress { end_offset }))
                .await?;
        }
        if let Some((throttle, connection_id, request_id)) = &self.throttle {
            throttle
                .rpc(Throttle {
                    connection_id: *connection_id,
                    request_id: *request_id,
                })
                .await??;
        }
        Ok(())
    }

    /// Transfer completed for the previously reported blob.
    pub async fn transfer_completed(
        &self,
        f: impl Fn() -> Box<TransferStats>,
    ) -> irpc::Result<()> {
        if let RequestUpdates::Active(tx) = &self.updates {
            tx.send(RequestUpdate::Completed(TransferCompleted { stats: f() }))
                .await?;
        }
        Ok(())
    }

    /// Transfer aborted for the previously reported blob.
    pub async fn transfer_aborted(
        &self,
        f: impl Fn() -> Option<Box<TransferStats>>,
    ) -> irpc::Result<()> {
        if let RequestUpdates::Active(tx) = &self.updates {
            tx.send(RequestUpdate::Aborted(TransferAborted { stats: f() }))
                .await?;
        }
        Ok(())
    }
}

/// Client for progress notifications.
///
/// For most event types, the client can be configured to either send notifications or requests that
/// can have a response.
impl EventSender {
    /// A client that does not send anything.
    pub const NONE: Self = Self {
        mask: EventMask::NONE,
        inner: None,
    };

    pub fn new(client: tokio::sync::mpsc::Sender<ProviderMessage>, mask: EventMask) -> Self {
        Self { mask, inner: Some(irpc::Client::from(client)) }
    }

    /// A new client has been connected.
    pub async fn client_connected(&self, f: impl Fn() -> ClientConnected) -> ClientResult {
        Ok(if let Some(client) = &self.inner {
            match self.mask.connected {
                ConnectMode::None => {}
                ConnectMode::Notify => client.notify(Notify(f())).await?,
                ConnectMode::Request => client.rpc(f()).await??,
            }
        })
    }

    /// A new client has been connected.
    pub async fn connection_closed(&self, f: impl Fn() -> ConnectionClosed) -> ClientResult {
        Ok(if let Some(client) = &self.inner {
            client.notify(f()).await?;
        })
    }

    /// Start a get request. You will get back either an error if the request should not proceed, or a
    /// [`RequestTracker`] that you can use to log progress for this particular request.
    ///
    /// Depending on the event sender config, the returned tracker might be a no-op.
    pub async fn get_request(
        &self,
        f: impl FnOnce() -> GetRequestReceived,
    ) -> Result<RequestTracker, ClientError> {
        self.request(f).await
    }

    // Start a get_many request. You will get back either an error if the request should not proceed, or a
    /// [`RequestTracker`] that you can use to log progress for this particular request.
    ///
    /// Depending on the event sender config, the returned tracker might be a no-op.
    pub async fn get_many_request(
        &self,
        f: impl FnOnce() -> GetManyRequestReceived,
    ) -> Result<RequestTracker, ClientError> {
        self.request(f).await
    }

    // Start a push request. You will get back either an error if the request should not proceed, or a
    /// [`RequestTracker`] that you can use to log progress for this particular request.
    ///
    /// Depending on the event sender config, the returned tracker might be a no-op.
    pub async fn push_request(
        &self,
        f: impl FnOnce() -> PushRequestReceived,
    ) -> Result<RequestTracker, ClientError> {
        self.request(f).await
    }

    /// Abstract request, to DRY the 3 to 4 request types.
    ///
    /// DRYing stuff with lots of bounds is no fun at all...
    async fn request<Req>(&self, f: impl FnOnce() -> Req) -> Result<RequestTracker, ClientError>
    where
        Req: Request,
        ProviderProto: From<Req>,
        ProviderMessage: From<WithChannels<Req, ProviderProto>>,
        Req: Channels<
            ProviderProto,
            Tx = oneshot::Sender<EventResult>,
            Rx = mpsc::Receiver<RequestUpdate>,
        >,
        ProviderProto: From<Notify<Req>>,
        ProviderMessage: From<WithChannels<Notify<Req>, ProviderProto>>,
        Notify<Req>: Channels<ProviderProto, Tx = NoSender, Rx = mpsc::Receiver<RequestUpdate>>,
    {
        Ok(self.into_tracker(if let Some(client) = &self.inner {
            match self.mask.get {
                RequestMode::None => {
                    if self.mask.throttle == ThrottleMode::Throttle {
                        // if throttling is enabled, we need to call f to get connection_id and request_id
                        let msg = f();
                        (RequestUpdates::None, msg.id())
                    } else {
                        (RequestUpdates::None, (0, 0))
                    }
                }
                RequestMode::Notify => {
                    let msg = f();
                    let id = msg.id();
                    (
                        RequestUpdates::Disabled(client.notify_streaming(Notify(msg), 32).await?),
                        id,
                    )
                }
                RequestMode::Request => {
                    let msg = f();
                    let id = msg.id();
                    let (tx, rx) = client.client_streaming(msg, 32).await?;
                    // bail out if the request is not allowed
                    rx.await??;
                    (RequestUpdates::Disabled(tx), id)
                }
                RequestMode::NotifyLog => {
                    let msg = f();
                    let id = msg.id();
                    (
                        RequestUpdates::Active(client.notify_streaming(Notify(msg), 32).await?),
                        id,
                    )
                }
                RequestMode::RequestLog => {
                    let msg = f();
                    let id = msg.id();
                    let (tx, rx) = client.client_streaming(msg, 32).await?;
                    // bail out if the request is not allowed
                    rx.await??;
                    (RequestUpdates::Active(tx), id)
                }
            }
        } else {
            (RequestUpdates::None, (0, 0))
        }))
    }

    fn into_tracker(
        &self,
        (updates, (connection_id, request_id)): (RequestUpdates, (u64, u64)),
    ) -> RequestTracker {
        let throttle = match self.mask.throttle {
            ThrottleMode::None => None,
            ThrottleMode::Throttle => self
                .inner
                .clone()
                .map(|client| (client, connection_id, request_id)),
        };
        RequestTracker::new(updates, throttle)
    }
}

#[rpc_requests(message = ProviderMessage)]
#[derive(Debug, Serialize, Deserialize)]
pub enum ProviderProto {
    /// A new client connected to the provider.
    #[rpc(tx = oneshot::Sender<EventResult>)]
    ClientConnected(ClientConnected),
    /// A new client connected to the provider. Notify variant.
    #[rpc(tx = NoSender)]
    ClientConnectedNotify(Notify<ClientConnected>),

    /// A client disconnected from the provider.
    #[rpc(tx = NoSender)]
    ConnectionClosed(ConnectionClosed),

    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = oneshot::Sender<EventResult>)]
    /// A new get request was received from the provider.
    GetRequestReceived(GetRequestReceived),

    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = NoSender)]
    /// A new get request was received from the provider.
    GetRequestReceivedNotify(Notify<GetRequestReceived>),

    /// A new get request was received from the provider.
    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = oneshot::Sender<EventResult>)]
    GetManyRequestReceived(GetManyRequestReceived),

    /// A new get request was received from the provider.
    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = NoSender)]
    GetManyRequestReceivedNotify(Notify<GetManyRequestReceived>),

    /// A new get request was received from the provider.
    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = oneshot::Sender<EventResult>)]
    PushRequestReceived(PushRequestReceived),

    /// A new get request was received from the provider.
    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = NoSender)]
    PushRequestReceivedNotify(Notify<PushRequestReceived>),

    #[rpc(tx = oneshot::Sender<EventResult>)]
    Throttle(Throttle),
}

trait Request {
    fn id(&self) -> (u64, u64);
}

mod proto {
    use iroh::NodeId;
    use serde::{Deserialize, Serialize};

    use super::Request;
    use crate::{protocol::{ChunkRangesSeq, GetRequest}, provider::TransferStats, Hash};

    #[derive(Debug, Serialize, Deserialize)]
    pub struct ClientConnected {
        pub connection_id: u64,
        pub node_id: NodeId,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct ConnectionClosed {
        pub connection_id: u64,
    }

    /// A new get request was received from the provider.
    #[derive(Debug, Serialize, Deserialize)]
    pub struct GetRequestReceived {
        /// The connection id. Multiple requests can be sent over the same connection.
        pub connection_id: u64,
        /// The request id. There is a new id for each request.
        pub request_id: u64,
        /// The request
        pub request: GetRequest,
    }

    impl Request for GetRequestReceived {
        fn id(&self) -> (u64, u64) {
            (self.connection_id, self.request_id)
        }
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

    impl Request for GetManyRequestReceived {
        fn id(&self) -> (u64, u64) {
            (self.connection_id, self.request_id)
        }
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

    impl Request for PushRequestReceived {
        fn id(&self) -> (u64, u64) {
            (self.connection_id, self.request_id)
        }
    }

    /// Request to throttle sending for a specific request.
    #[derive(Debug, Serialize, Deserialize)]
    pub struct Throttle {
        /// The connection id. Multiple requests can be sent over the same connection.
        pub connection_id: u64,
        /// The request id. There is a new id for each request.
        pub request_id: u64,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct TransferProgress {
        /// The end offset of the chunk that was sent.
        pub end_offset: u64,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct TransferStarted {
        pub index: u64,
        pub hash: Hash,
        pub size: u64,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct TransferCompleted {
        pub stats: Box<TransferStats>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct TransferAborted {
        pub stats: Option<Box<TransferStats>>,
    }

    /// Stream of updates for a single request
    #[derive(Debug, Serialize, Deserialize)]
    pub enum RequestUpdate {
        /// Start of transfer for a blob, mandatory event
        Started(TransferStarted),
        /// Progress for a blob - optional event
        Progress(TransferProgress),
        /// Successful end of transfer
        Completed(TransferCompleted),
        /// Aborted end of transfer
        Aborted(TransferAborted),
    }
}
pub use proto::*;

mod irpc_ext {
    use std::future::Future;

    use irpc::{
        channel::{mpsc, none::NoSender, oneshot},
        Channels, RpcMessage, Service, WithChannels,
    };

    pub trait IrpcClientExt<S: Service> {
        fn notify_streaming<Req, Update>(
            &self,
            msg: Req,
            local_update_cap: usize,
        ) -> impl Future<Output = irpc::Result<mpsc::Sender<Update>>>
        where
            S: From<Req>,
            S::Message: From<WithChannels<Req, S>>,
            Req: Channels<S, Tx = NoSender, Rx = mpsc::Receiver<Update>>,
            Update: RpcMessage;
    }

    impl<S: Service> IrpcClientExt<S> for irpc::Client<S> {
        fn notify_streaming<Req, Update>(
            &self,
            msg: Req,
            local_update_cap: usize,
        ) -> impl Future<Output = irpc::Result<mpsc::Sender<Update>>>
        where
            S: From<Req>,
            S::Message: From<WithChannels<Req, S>>,
            Req: Channels<S, Tx = NoSender, Rx = mpsc::Receiver<Update>>,
            Update: RpcMessage,
        {
            let client = self.clone();
            async move {
                let request = client.request().await?;
                match request {
                    irpc::Request::Local(local) => {
                        let (req_tx, req_rx) = mpsc::channel(local_update_cap);
                        local
                            .send((msg, NoSender, req_rx))
                            .await
                            .map_err(irpc::Error::from)?;
                        Ok(req_tx)
                    }
                    irpc::Request::Remote(remote) => {
                        let (s, r) = remote.write(msg).await?;
                        Ok(s.into())
                    }
                }
            }
        }
    }
}
