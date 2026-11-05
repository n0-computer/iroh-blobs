use std::{fmt::Debug, io, ops::Deref};

use iroh::endpoint::VarInt;
use irpc::{
    channel::{mpsc, none::NoSender, oneshot},
    rpc_requests, Channels, WithChannels,
};
use n0_error::{e, stack_error};
use serde::{Deserialize, Serialize};

use crate::{
    protocol::{
        GetManyRequest, GetRequest, ObserveRequest, PushRequest, ERR_INTERNAL, ERR_LIMIT,
        ERR_PERMISSION,
    },
    provider::{events::irpc_ext::IrpcClientExt, TransferStats},
    Hash,
};

/// Mode for connect events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ConnectMode {
    /// We don't get notification of connect events at all.
    #[default]
    None,
    /// We get a notification for connect events.
    Notify,
    /// We get a request for connect events and can reject incoming connections.
    Intercept,
}

/// Request mode for observe requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ObserveMode {
    /// We don't get observe request events at all.
    None,
    /// We get a notification for each observe request.
    ///
    /// There are no `*Log` variants, since an observe request has no transfer events.
    Notify,
    /// We get a request for each observe request, and can reject it.
    Intercept,
    /// Observe requests are completely disabled. All requests will be rejected.
    ///
    /// This is the default, since observe requests are long running
    /// subscriptions, and are not needed for normal blob transfers.
    #[default]
    Disabled,
}

/// Request mode for all data related requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum RequestMode {
    /// We don't get request events at all.
    #[default]
    None,
    /// We get a notification for each request, but no transfer events.
    Notify,
    /// We get a request for each request, and can reject incoming requests, but no transfer events.
    Intercept,
    /// We get a notification for each request as well as detailed transfer events.
    NotifyLog,
    /// We get a request for each request, and can reject incoming requests.
    /// We also get detailed transfer events.
    InterceptLog,
    /// This request type is completely disabled. All requests will be rejected.
    ///
    /// This means that requests of this kind will always be rejected, whereas
    /// None means that we don't get any events, but requests will be processed normally.
    Disabled,
}

/// Throttling mode for requests that support throttling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ThrottleMode {
    /// We don't get these kinds of events at all
    #[default]
    None,
    /// We call throttle to give the event handler a way to throttle requests
    Intercept,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AbortReason {
    /// The request was aborted because a limit was exceeded. It is OK to try again later.
    RateLimited,
    /// The request was aborted because the client does not have permission to perform the operation.
    Permission,
}

/// Errors that can occur when sending progress updates.
#[stack_error(derive, add_meta, from_sources)]
pub enum ProgressError {
    #[error("limit")]
    Limit {},
    #[error("permission")]
    Permission {},
    #[error(transparent)]
    Internal { source: irpc::Error },
}

impl From<ProgressError> for io::Error {
    fn from(value: ProgressError) -> Self {
        match value {
            ProgressError::Limit { .. } => io::ErrorKind::QuotaExceeded.into(),
            ProgressError::Permission { .. } => io::ErrorKind::PermissionDenied.into(),
            ProgressError::Internal { source, .. } => source.into(),
        }
    }
}

pub trait HasErrorCode {
    fn code(&self) -> VarInt;
}

impl HasErrorCode for ProgressError {
    fn code(&self) -> VarInt {
        match self {
            ProgressError::Limit { .. } => ERR_LIMIT,
            ProgressError::Permission { .. } => ERR_PERMISSION,
            ProgressError::Internal { .. } => ERR_INTERNAL,
        }
    }
}

impl ProgressError {
    pub fn reason(&self) -> &'static [u8] {
        match self {
            ProgressError::Limit { .. } => b"limit",
            ProgressError::Permission { .. } => b"permission",
            ProgressError::Internal { .. } => b"internal",
        }
    }
}

impl From<AbortReason> for ProgressError {
    fn from(value: AbortReason) -> Self {
        match value {
            AbortReason::RateLimited => n0_error::e!(ProgressError::Limit),
            AbortReason::Permission => n0_error::e!(ProgressError::Permission),
        }
    }
}

impl From<irpc::channel::mpsc::RecvError> for ProgressError {
    fn from(value: irpc::channel::mpsc::RecvError) -> Self {
        n0_error::e!(ProgressError::Internal, value.into())
    }
}

impl From<irpc::channel::oneshot::RecvError> for ProgressError {
    fn from(value: irpc::channel::oneshot::RecvError) -> Self {
        n0_error::e!(ProgressError::Internal, value.into())
    }
}

impl From<irpc::channel::SendError> for ProgressError {
    fn from(value: irpc::channel::SendError) -> Self {
        n0_error::e!(ProgressError::Internal, value.into())
    }
}

pub type EventResult = Result<(), AbortReason>;
pub type ClientResult = Result<(), ProgressError>;

/// Event mask to configure which events are sent to the event handler.
///
/// This can also be used to completely disable certain request types. By
/// default, get and get many requests are allowed, while push requests, which
/// can write to the local store, and observe requests, which are long running
/// subscriptions, are disabled.
///
/// The modes either send a notification (`Notify`, `NotifyLog`) or ask the
/// event handler for a decision (`Intercept`, `InterceptLog`). If the event
/// handler is gone (it dropped its receiver), a request with notifications
/// goes ahead without them, while a request that needs a decision is rejected,
/// and a transfer that is being throttled is aborted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventMask {
    /// Connection event mask
    pub connected: ConnectMode,
    /// Get request event mask
    pub get: RequestMode,
    /// Get many request event mask
    pub get_many: RequestMode,
    /// Push request event mask
    pub push: RequestMode,
    /// Observe request event mask
    pub observe: ObserveMode,
    /// throttling is somewhat costly, so you can disable it completely
    pub throttle: ThrottleMode,
}

impl Default for EventMask {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl EventMask {
    /// All event notifications are fully disabled. Push and observe requests
    /// are disabled by default.
    pub const DEFAULT: Self = Self {
        connected: ConnectMode::None,
        get: RequestMode::None,
        get_many: RequestMode::None,
        push: RequestMode::Disabled,
        throttle: ThrottleMode::None,
        observe: ObserveMode::Disabled,
    };

    /// All event notifications for read-only requests are fully enabled.
    ///
    /// If you want to enable push requests, which can write to the local store, you
    /// need to do it manually. Providing constants that have push enabled would
    /// risk misuse.
    pub const ALL_READONLY: Self = Self {
        connected: ConnectMode::Intercept,
        get: RequestMode::InterceptLog,
        get_many: RequestMode::InterceptLog,
        push: RequestMode::Disabled,
        throttle: ThrottleMode::Intercept,
        observe: ObserveMode::Intercept,
    };
}

/// Selects the [`EventMask`] field that governs a request type.
pub(crate) trait RequestKind {
    /// Returns the request mode configured for this request type in `mask`.
    fn mode(mask: &EventMask) -> RequestMode;
}

impl RequestKind for GetRequest {
    fn mode(mask: &EventMask) -> RequestMode {
        mask.get
    }
}

impl RequestKind for GetManyRequest {
    fn mode(mask: &EventMask) -> RequestMode {
        mask.get_many
    }
}

impl RequestKind for PushRequest {
    fn mode(mask: &EventMask) -> RequestMode {
        mask.push
    }
}

impl RequestKind for ObserveRequest {
    fn mode(mask: &EventMask) -> RequestMode {
        match mask.observe {
            ObserveMode::None => RequestMode::None,
            ObserveMode::Notify => RequestMode::Notify,
            ObserveMode::Intercept => RequestMode::Intercept,
            ObserveMode::Disabled => RequestMode::Disabled,
        }
    }
}

/// Newtype wrapper that wraps an event so that it is a distinct type for the notify variant.
#[derive(Debug, Serialize, Deserialize)]
pub struct Notify<T>(T);

impl<T> Deref for Notify<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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
    pub const NONE: Self = Self {
        updates: RequestUpdates::None,
        throttle: None,
    };

    /// Transfer for index `index` started, size `size` in bytes.
    pub async fn transfer_started(&self, index: u64, hash: &Hash, size: u64) -> irpc::Result<()> {
        if let RequestUpdates::Active(tx) = &self.updates {
            tx.send(
                TransferStarted {
                    index,
                    hash: *hash,
                    size,
                }
                .into(),
            )
            .await?;
        }
        Ok(())
    }

    /// Transfer progress for the previously reported blob, end_offset is the new end offset in bytes.
    pub async fn transfer_progress(&mut self, len: u64, end_offset: u64) -> ClientResult {
        if let RequestUpdates::Active(tx) = &mut self.updates {
            // a notification, so a handler that is gone does not abort the transfer
            tx.try_send(TransferProgress { end_offset }.into())
                .await
                .ok();
        }
        if let Some((throttle, connection_id, request_id)) = &self.throttle {
            throttle
                .rpc(Throttle {
                    connection_id: *connection_id,
                    request_id: *request_id,
                    size: len,
                })
                .await??;
        }
        Ok(())
    }

    /// Transfer completed for the previously reported blob.
    pub async fn transfer_completed(&self, f: impl Fn() -> Box<TransferStats>) -> irpc::Result<()> {
        if let RequestUpdates::Active(tx) = &self.updates {
            tx.send(TransferCompleted { stats: f() }.into()).await?;
        }
        Ok(())
    }

    /// Transfer aborted for the previously reported blob.
    pub async fn transfer_aborted(&self, f: impl Fn() -> Box<TransferStats>) -> irpc::Result<()> {
        if let RequestUpdates::Active(tx) = &self.updates {
            tx.send(TransferAborted { stats: f() }.into()).await?;
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
    pub const DEFAULT: Self = Self {
        mask: EventMask::DEFAULT,
        inner: None,
    };

    pub fn new(client: tokio::sync::mpsc::Sender<ProviderMessage>, mask: EventMask) -> Self {
        Self {
            mask,
            inner: Some(irpc::Client::from(client)),
        }
    }

    pub fn channel(
        capacity: usize,
        mask: EventMask,
    ) -> (Self, tokio::sync::mpsc::Receiver<ProviderMessage>) {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);
        (Self::new(tx, mask), rx)
    }

    /// Log all events for read-only requests at trace level, and allow every request.
    ///
    /// This uses [`EventMask::ALL_READONLY`], so push requests stay disabled.
    /// Use [`Self::tracing_rw`] to also allow and log push requests.
    pub fn tracing() -> Self {
        Self::tracing_with(EventMask::ALL_READONLY)
    }

    /// Log all events at trace level, and allow every request, including push.
    ///
    /// Push requests write to the local store, so with this any peer that can
    /// connect can add data. Only use it if that is intended.
    pub fn tracing_rw() -> Self {
        Self::tracing_with(EventMask {
            push: RequestMode::InterceptLog,
            ..EventMask::ALL_READONLY
        })
    }

    /// Log events at trace level and approve every decision the mask asks for.
    fn tracing_with(mask: EventMask) -> Self {
        use tracing::trace;
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);
        n0_future::task::spawn(async move {
            fn log_request_events(
                mut rx: irpc::channel::mpsc::Receiver<RequestUpdate>,
                connection_id: u64,
                request_id: u64,
            ) {
                n0_future::task::spawn(async move {
                    while let Ok(Some(update)) = rx.recv().await {
                        trace!(%connection_id, %request_id, "{update:?}");
                    }
                });
            }
            while let Some(msg) = rx.recv().await {
                match msg {
                    ProviderMessage::ClientConnected(msg) => {
                        trace!("{:?}", msg.inner);
                        msg.tx.send(Ok(())).await.ok();
                    }
                    ProviderMessage::ClientConnectedNotify(msg) => {
                        trace!("{:?}", msg.inner);
                    }
                    ProviderMessage::ConnectionClosed(msg) => {
                        trace!("{:?}", msg.inner);
                    }
                    ProviderMessage::GetRequestReceived(msg) => {
                        trace!("{:?}", msg.inner);
                        msg.tx.send(Ok(())).await.ok();
                        log_request_events(msg.rx, msg.inner.connection_id, msg.inner.request_id);
                    }
                    ProviderMessage::GetRequestReceivedNotify(msg) => {
                        trace!("{:?}", msg.inner);
                        log_request_events(msg.rx, msg.inner.connection_id, msg.inner.request_id);
                    }
                    ProviderMessage::GetManyRequestReceived(msg) => {
                        trace!("{:?}", msg.inner);
                        msg.tx.send(Ok(())).await.ok();
                        log_request_events(msg.rx, msg.inner.connection_id, msg.inner.request_id);
                    }
                    ProviderMessage::GetManyRequestReceivedNotify(msg) => {
                        trace!("{:?}", msg.inner);
                        log_request_events(msg.rx, msg.inner.connection_id, msg.inner.request_id);
                    }
                    ProviderMessage::PushRequestReceived(msg) => {
                        trace!("{:?}", msg.inner);
                        msg.tx.send(Ok(())).await.ok();
                        log_request_events(msg.rx, msg.inner.connection_id, msg.inner.request_id);
                    }
                    ProviderMessage::PushRequestReceivedNotify(msg) => {
                        trace!("{:?}", msg.inner);
                        log_request_events(msg.rx, msg.inner.connection_id, msg.inner.request_id);
                    }
                    ProviderMessage::ObserveRequestReceived(msg) => {
                        trace!("{:?}", msg.inner);
                        msg.tx.send(Ok(())).await.ok();
                        log_request_events(msg.rx, msg.inner.connection_id, msg.inner.request_id);
                    }
                    ProviderMessage::ObserveRequestReceivedNotify(msg) => {
                        trace!("{:?}", msg.inner);
                        log_request_events(msg.rx, msg.inner.connection_id, msg.inner.request_id);
                    }
                    ProviderMessage::Throttle(msg) => {
                        trace!("{:?}", msg.inner);
                        msg.tx.send(Ok(())).await.ok();
                    }
                }
            }
        });
        Self {
            mask,
            inner: Some(irpc::Client::from(tx)),
        }
    }

    /// A new client has been connected.
    pub async fn client_connected(&self, f: impl Fn() -> ClientConnected) -> ClientResult {
        match (self.mask.connected, &self.inner) {
            (ConnectMode::None, _) | (ConnectMode::Notify, None) => {}
            (ConnectMode::Notify, Some(client)) => {
                client.notify(Notify(f())).await.ok();
            }
            (ConnectMode::Intercept, Some(client)) => client.rpc(f()).await??,
            (ConnectMode::Intercept, None) => return Err(e!(ProgressError::Permission)),
        }
        Ok(())
    }

    /// A connection has been closed.
    ///
    /// Only sent if connect events are enabled, so the handler knows about the connection.
    pub async fn connection_closed(&self, f: impl Fn() -> ConnectionClosed) -> ClientResult {
        if let (ConnectMode::Notify | ConnectMode::Intercept, Some(client)) =
            (self.mask.connected, &self.inner)
        {
            client.notify(f()).await.ok();
        }
        Ok(())
    }

    /// Abstract request, to DRY the 3 to 4 request types.
    ///
    /// DRYing stuff with lots of bounds is no fun at all...
    pub(crate) async fn request<Req>(
        &self,
        f: impl FnOnce() -> Req,
        connection_id: u64,
        request_id: u64,
    ) -> Result<RequestTracker, ProgressError>
    where
        Req: RequestKind,
        ProviderProto: From<RequestReceived<Req>>,
        ProviderMessage: From<WithChannels<RequestReceived<Req>, ProviderProto>>,
        RequestReceived<Req>: Channels<
            ProviderProto,
            Tx = oneshot::Sender<EventResult>,
            Rx = mpsc::Receiver<RequestUpdate>,
        >,
        ProviderProto: From<Notify<RequestReceived<Req>>>,
        ProviderMessage: From<WithChannels<Notify<RequestReceived<Req>>, ProviderProto>>,
        Notify<RequestReceived<Req>>:
            Channels<ProviderProto, Tx = NoSender, Rx = mpsc::Receiver<RequestUpdate>>,
    {
        let msg = || RequestReceived {
            request: f(),
            connection_id,
            request_id,
        };
        let updates = match (Req::mode(&self.mask), &self.inner) {
            (RequestMode::None, _) => RequestUpdates::None,
            (RequestMode::Disabled, _) => return Err(e!(ProgressError::Permission)),
            // A notification needs no answer, so without a handler, or if it
            // is gone, the request goes ahead.
            (RequestMode::Notify | RequestMode::NotifyLog, None) => RequestUpdates::None,
            (RequestMode::Notify, Some(client)) => {
                match client.notify_streaming(Notify(msg()), 32).await {
                    Ok(tx) => RequestUpdates::Disabled(tx),
                    Err(_) => RequestUpdates::None,
                }
            }
            (RequestMode::NotifyLog, Some(client)) => {
                match client.notify_streaming(Notify(msg()), 32).await {
                    Ok(tx) => RequestUpdates::Active(tx),
                    Err(_) => RequestUpdates::None,
                }
            }
            // A decision needs an answer, so without a handler, or if it is
            // gone, the request is rejected.
            (RequestMode::Intercept | RequestMode::InterceptLog, None) => {
                return Err(e!(ProgressError::Permission));
            }
            (RequestMode::Intercept, Some(client)) => {
                let (tx, rx) = client.client_streaming(msg(), 32).await?;
                // bail out if the request is not allowed
                rx.await??;
                RequestUpdates::Disabled(tx)
            }
            (RequestMode::InterceptLog, Some(client)) => {
                let (tx, rx) = client.client_streaming(msg(), 32).await?;
                // bail out if the request is not allowed
                rx.await??;
                RequestUpdates::Active(tx)
            }
        };
        if self.mask.throttle == ThrottleMode::Intercept && self.inner.is_none() {
            return Err(e!(ProgressError::Permission));
        }
        Ok(self.create_tracker((updates, connection_id, request_id)))
    }

    fn create_tracker(
        &self,
        (updates, connection_id, request_id): (RequestUpdates, u64, u64),
    ) -> RequestTracker {
        let throttle = match self.mask.throttle {
            ThrottleMode::None => None,
            ThrottleMode::Intercept => self
                .inner
                .clone()
                .map(|client| (client, connection_id, request_id)),
        };
        RequestTracker::new(updates, throttle)
    }
}

#[rpc_requests(message = ProviderMessage, rpc_feature = "rpc")]
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

    /// A new get request was received from the provider.
    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = oneshot::Sender<EventResult>)]
    GetRequestReceived(RequestReceived<GetRequest>),

    /// A new get request was received from the provider (notify variant).
    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = NoSender)]
    GetRequestReceivedNotify(Notify<RequestReceived<GetRequest>>),

    /// A new get many request was received from the provider.
    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = oneshot::Sender<EventResult>)]
    GetManyRequestReceived(RequestReceived<GetManyRequest>),

    /// A new get many request was received from the provider (notify variant).
    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = NoSender)]
    GetManyRequestReceivedNotify(Notify<RequestReceived<GetManyRequest>>),

    /// A new push request was received from the provider.
    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = oneshot::Sender<EventResult>)]
    PushRequestReceived(RequestReceived<PushRequest>),

    /// A new push request was received from the provider (notify variant).
    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = NoSender)]
    PushRequestReceivedNotify(Notify<RequestReceived<PushRequest>>),

    /// A new observe request was received from the provider.
    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = oneshot::Sender<EventResult>)]
    ObserveRequestReceived(RequestReceived<ObserveRequest>),

    /// A new observe request was received from the provider (notify variant).
    #[rpc(rx = mpsc::Receiver<RequestUpdate>, tx = NoSender)]
    ObserveRequestReceivedNotify(Notify<RequestReceived<ObserveRequest>>),

    /// Request to throttle sending for a specific data request.
    #[rpc(tx = oneshot::Sender<EventResult>)]
    Throttle(Throttle),
}

mod proto {
    use iroh::EndpointId;
    use serde::{Deserialize, Serialize};

    use crate::{provider::TransferStats, Hash};

    #[derive(Debug, Serialize, Deserialize)]
    pub struct ClientConnected {
        pub connection_id: u64,
        pub endpoint_id: Option<EndpointId>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct ConnectionClosed {
        pub connection_id: u64,
    }

    /// A new get request was received from the provider.
    #[derive(Debug, Serialize, Deserialize)]
    pub struct RequestReceived<R> {
        /// The connection id. Multiple requests can be sent over the same connection.
        pub connection_id: u64,
        /// The request id. There is a new id for each request.
        pub request_id: u64,
        /// The request
        pub request: R,
    }

    /// Request to throttle sending for a specific request.
    #[derive(Debug, Serialize, Deserialize)]
    pub struct Throttle {
        /// The connection id. Multiple requests can be sent over the same connection.
        pub connection_id: u64,
        /// The request id. There is a new id for each request.
        pub request_id: u64,
        /// Size of the chunk to be throttled. This will usually be 16 KiB.
        pub size: u64,
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
        pub stats: Box<TransferStats>,
    }

    /// Stream of updates for a single request
    #[derive(Debug, Serialize, Deserialize, derive_more::From)]
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
        channel::{mpsc, none::NoSender},
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
                    #[cfg(feature = "rpc")]
                    irpc::Request::Remote(remote) => {
                        let (s, _) = remote.write(msg).await?;
                        Ok(s.into())
                    }
                    #[cfg(not(feature = "rpc"))]
                    irpc::Request::Remote(_) => {
                        unreachable!()
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use testresult::TestResult;

    use super::*;

    fn get() -> GetRequest {
        GetRequest::blob(Hash::new(b"a"))
    }

    fn observe() -> ObserveRequest {
        ObserveRequest::new(Hash::new(b"a"))
    }

    /// An event sender whose handler is already gone.
    fn dead(mask: EventMask) -> EventSender {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        EventSender::new(tx, mask)
    }

    fn get_mask(get: RequestMode) -> EventMask {
        EventMask {
            get,
            ..EventMask::DEFAULT
        }
    }

    fn observe_mask(observe: ObserveMode) -> EventMask {
        EventMask {
            observe,
            ..EventMask::DEFAULT
        }
    }

    #[tokio::test]
    async fn notifications_survive_a_dead_handler() -> TestResult {
        for mode in [RequestMode::Notify, RequestMode::NotifyLog] {
            let mut tracker = dead(get_mask(mode)).request(get, 0, 0).await?;
            tracker.transfer_progress(1024, 1024).await?;
        }
        dead(observe_mask(ObserveMode::Notify))
            .request(observe, 0, 0)
            .await?;
        let events = dead(EventMask {
            connected: ConnectMode::Notify,
            ..EventMask::DEFAULT
        });
        events
            .client_connected(|| ClientConnected {
                connection_id: 0,
                endpoint_id: None,
            })
            .await?;
        events
            .connection_closed(|| ConnectionClosed { connection_id: 0 })
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn progress_notifications_survive_a_dropped_receiver() -> TestResult {
        let (events, mut rx) = EventSender::channel(1, get_mask(RequestMode::NotifyLog));
        let mut tracker = events.request(get, 0, 0).await?;
        let Some(ProviderMessage::GetRequestReceivedNotify(mut msg)) = rx.recv().await else {
            panic!("expected a get notification");
        };
        // First establish that this request actually delivers progress updates.
        tracker.transfer_progress(1024, 1024).await?;
        let update =
            tokio::time::timeout(std::time::Duration::from_secs(5), msg.rx.recv()).await??;
        assert!(matches!(
            update,
            Some(RequestUpdate::Progress(TransferProgress {
                end_offset: 1024
            }))
        ));
        // Losing the handler after tracking starts must not abort the transfer.
        drop(msg);
        drop(rx);
        tracker.transfer_progress(1024, 2048).await?;
        Ok(())
    }

    #[tokio::test]
    async fn decisions_fail_closed_with_a_dead_handler() {
        for mode in [RequestMode::Intercept, RequestMode::InterceptLog] {
            assert!(dead(get_mask(mode)).request(get, 0, 0).await.is_err());
        }
        assert!(dead(observe_mask(ObserveMode::Intercept))
            .request(observe, 0, 0)
            .await
            .is_err());
        let events = dead(EventMask {
            connected: ConnectMode::Intercept,
            ..EventMask::DEFAULT
        });
        assert!(events
            .client_connected(|| ClientConnected {
                connection_id: 0,
                endpoint_id: None,
            })
            .await
            .is_err());
    }

    #[tokio::test]
    async fn decisions_fail_closed_without_a_handler() {
        let without = |mask| EventSender { mask, inner: None };
        for mode in [RequestMode::Intercept, RequestMode::InterceptLog] {
            let error = without(get_mask(mode)).request(get, 0, 0).await;
            assert!(matches!(error, Err(ProgressError::Permission { .. })));
        }
        let throttled = without(EventMask {
            throttle: ThrottleMode::Intercept,
            ..EventMask::DEFAULT
        });
        let error = throttled.request(get, 0, 0).await;
        assert!(matches!(error, Err(ProgressError::Permission { .. })));
        let events = without(EventMask {
            connected: ConnectMode::Intercept,
            ..EventMask::DEFAULT
        });
        let error = events
            .client_connected(|| ClientConnected {
                connection_id: 0,
                endpoint_id: None,
            })
            .await;
        assert!(matches!(error, Err(ProgressError::Permission { .. })));
    }

    #[tokio::test]
    async fn disabled_rejects_even_with_a_handler() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let events = EventSender::new(
            tx,
            EventMask {
                get: RequestMode::Disabled,
                observe: ObserveMode::Disabled,
                ..EventMask::DEFAULT
            },
        );
        let error = events.request(get, 0, 0).await;
        assert!(matches!(error, Err(ProgressError::Permission { .. })));
        let error = events.request(observe, 0, 0).await;
        assert!(matches!(error, Err(ProgressError::Permission { .. })));
    }

    #[tokio::test]
    async fn only_tracing_rw_allows_push() -> TestResult {
        let push = || PushRequest::new(Hash::new(b"a"), crate::protocol::ChunkRangesSeq::root());
        let error = EventSender::tracing().request(push, 0, 0).await;
        assert!(matches!(error, Err(ProgressError::Permission { .. })));
        EventSender::tracing_rw().request(push, 0, 0).await?;
        EventSender::tracing().request(get, 0, 0).await?;
        EventSender::tracing().request(observe, 0, 0).await?;
        Ok(())
    }

    #[tokio::test]
    async fn connection_closed_only_with_connect_events() -> TestResult {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let events = EventSender::new(tx, EventMask::DEFAULT);
        events
            .connection_closed(|| ConnectionClosed { connection_id: 0 })
            .await?;
        assert!(rx.try_recv().is_err());
        Ok(())
    }
}
