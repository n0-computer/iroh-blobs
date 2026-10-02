use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc};

use irpc::channel::{mpsc::DynSender, SendError};
use n0_error::{e, AnyError};
use n0_future::future::Boxed;
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::api::{
    proto::{Command, ShutdownRequest},
    ApiClient,
};

use super::{meta, RtWrapper};

/// Original failures encountered while joining a file store's resources.
///
/// This error is shared by all shutdown waiters, including after cancellation.
/// The RPC receive error carries it in a [`std::io::Error`]; use
/// [`std::io::Error::get_ref`] to downcast the inner error to this type.
#[derive(Clone, Debug)]
pub struct ShutdownError(Arc<[AnyError]>);

impl ShutdownError {
    /// All original failures, with the resource being joined as context.
    pub fn causes(&self) -> &[AnyError] {
        &self.0
    }

    pub(super) fn from_failures(failures: Vec<AnyError>) -> Self {
        Self(failures.into())
    }

    pub(super) fn result(failures: Vec<AnyError>) -> Result<(), Self> {
        if failures.is_empty() {
            Ok(())
        } else {
            Err(Self::from_failures(failures))
        }
    }
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "file-store shutdown failed")?;
        for failure in self.0.iter() {
            write!(formatter, "; {failure:#}")?;
        }
        Ok(())
    }
}

impl Error for ShutdownError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.first().map(|error| error as &(dyn Error + 'static))
    }
}

enum State {
    Closing(Boxed<Result<(), ShutdownError>>),
    Closed(Result<(), ShutdownError>),
}

pub(super) struct Shutdown {
    stop: CancellationToken,
    state: Mutex<State>,
    #[cfg(test)]
    pub(super) actors: [tokio::task::AbortHandle; 2],
}

impl fmt::Debug for Shutdown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Shutdown")
            .field("closing", &self.stop.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl Shutdown {
    pub(super) fn new(
        commands: mpsc::WeakSender<Command>,
        runtime: RtWrapper,
        actor: JoinHandle<n0_error::Result<()>>,
        database: JoinHandle<meta::ActorResult<()>>,
        gc: Option<JoinHandle<()>>,
        stop: CancellationToken,
    ) -> Self {
        #[cfg(test)]
        let actors = [actor.abort_handle(), database.abort_handle()];
        let gc_stop = stop.clone();
        let close = async move {
            gc_stop.cancel();
            let mut failures = Vec::new();
            if let Some(gc) = gc {
                if let Err(error) = gc.await {
                    failures.push(AnyError::from_std(error).context("garbage collector task"));
                }
            }
            // Only this raw client bypasses the admission fence. It must not
            // capture an anchored Store, which would retain this future's owner.
            let request = async {
                let Some(sender) = commands.upgrade() else {
                    return Err(e!(SendError::ReceiverClosed).into());
                };
                ApiClient::from(sender).rpc(ShutdownRequest).await
            };
            let (request, actor, database) = tokio::join!(request, actor, database);
            match database {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(AnyError::from(error).context("database actor")),
                Err(error) => {
                    failures.push(AnyError::from_std(error).context("database actor task"))
                }
            }
            match actor {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(error.context("main actor")),
                Err(error) => failures.push(AnyError::from_std(error).context("main actor task")),
            }
            if let Err(error) = request {
                failures.push(AnyError::from(error).context("shutdown request"));
            }
            // Runtime::drop joins its blocking pool as well as its async
            // workers. The job's actual JoinHandle stays in this same future.
            if let Err(error) = runtime.shutdown().await {
                failures.push(AnyError::from_std(error).context("dedicated runtime shutdown"));
            }
            ShutdownError::result(failures)
        };
        Self {
            stop,
            state: Mutex::new(State::Closing(Box::pin(close))),
            #[cfg(test)]
            actors,
        }
    }

    pub(super) fn sender<T: Send + 'static>(
        self: &Arc<Self>,
        sender: mpsc::Sender<T>,
    ) -> irpc::channel::mpsc::Sender<T> {
        irpc::channel::mpsc::Sender::Boxed(Arc::new(OwnedSender {
            sender,
            shutdown: self.clone(),
        }))
    }

    pub(super) async fn wait(&self) -> irpc::Result<()> {
        let mut state = self.state.lock().await;
        let result = match &mut *state {
            State::Closing(future) => {
                let result = future.await;
                *state = State::Closed(result.clone());
                result
            }
            State::Closed(result) => result.clone(),
        };
        result.map_err(|error| {
            e!(
                irpc::channel::oneshot::RecvError::Io,
                std::io::Error::other(error)
            )
            .into()
        })
    }
}

/// Both generic Store clients and FS-specific clients retain the same owner.
/// Internal completion messages and GC use raw senders, without this anchor.
struct OwnedSender<T> {
    sender: mpsc::Sender<T>,
    shutdown: Arc<Shutdown>,
}

impl<T> fmt::Debug for OwnedSender<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnedSender")
            .field("shutdown", &self.shutdown)
            .finish_non_exhaustive()
    }
}

impl<T: Send + 'static> DynSender<T> for OwnedSender<T> {
    fn send(&self, value: T) -> Pin<Box<dyn Future<Output = Result<(), SendError>> + Send + '_>> {
        Box::pin(async move {
            tokio::select! {
                biased;
                _ = self.shutdown.stop.cancelled() => Err(e!(SendError::ReceiverClosed)),
                result = self.sender.send(value) => result.map_err(|_| e!(SendError::ReceiverClosed)),
            }
        })
    }

    fn try_send(
        &self,
        value: T,
    ) -> Pin<Box<dyn Future<Output = Result<bool, SendError>> + Send + '_>> {
        Box::pin(async move {
            if self.shutdown.stop.is_cancelled() {
                return Err(e!(SendError::ReceiverClosed));
            }
            match self.sender.try_send(value) {
                Ok(()) => Ok(true),
                Err(mpsc::error::TrySendError::Full(_)) => Ok(false),
                Err(mpsc::error::TrySendError::Closed(_)) => Err(e!(SendError::ReceiverClosed)),
            }
        })
    }

    fn closed(&self) -> Pin<Box<dyn Future<Output = ()> + Send + Sync + '_>> {
        Box::pin(async move {
            tokio::select! {
                _ = self.shutdown.stop.cancelled() => {}
                _ = self.sender.closed() => {}
            }
        })
    }

    fn is_rpc(&self) -> bool {
        false
    }
}
