//! [`Getter`] implementation that performs requests over [`Connection`]s.
//!
//! [`Connection`]: iroh::endpoint::Connection

use futures_lite::FutureExt;
use iroh::endpoint;

use super::{progress::BroadcastProgressSender, DownloadKind, FailureAction, GetStartFut, Getter};
use crate::{
    get::{db::get_to_db_in_steps, error::GetError},
    store::Store,
};

impl From<GetError> for FailureAction {
    fn from(e: GetError) -> Self {
        match e {
            e @ GetError::NotFound(_) => FailureAction::AbortRequest(e),
            e @ GetError::RemoteReset(_) => FailureAction::RetryLater(e.into()),
            e @ GetError::NoncompliantNode(_) => FailureAction::DropPeer(e.into()),
            e @ GetError::Io(_) => FailureAction::RetryLater(e.into()),
            e @ GetError::BadRequest(_) => FailureAction::AbortRequest(e),
            // TODO: what do we want to do on local failures?
            e @ GetError::LocalFailure(_) => FailureAction::AbortRequest(e),
        }
    }
}

/// [`Getter`] implementation that performs requests over [`Connection`]s.
///
/// [`Connection`]: iroh::endpoint::Connection
pub(crate) struct IoGetter<S: Store> {
    pub store: S,
}

impl<S: Store> Getter for IoGetter<S> {
    type Connection = endpoint::Connection;
    type NeedsConn = crate::get::db::GetStateNeedsConn;

    fn get(
        &mut self,
        kind: DownloadKind,
        progress_sender: BroadcastProgressSender,
    ) -> GetStartFut<Self::NeedsConn> {
        let store = self.store.clone();
        async move {
            match get_to_db_in_steps(store, kind.hash_and_format(), progress_sender).await {
                Err(err) => Err(err.into()),
                Ok(crate::get::db::GetState::Complete(stats)) => {
                    Ok(super::GetOutput::Complete(stats))
                }
                Ok(crate::get::db::GetState::NeedsConn(needs_conn)) => {
                    Ok(super::GetOutput::NeedsConn(needs_conn))
                }
            }
        }
        .boxed_local()
    }
}

impl super::NeedsConn<endpoint::Connection> for crate::get::db::GetStateNeedsConn {
    fn proceed(self, conn: endpoint::Connection) -> super::GetProceedFut {
        async move {
            let res = self.proceed(conn).await;
            match res {
                Ok(stats) => Ok(stats),
                Err(err) => Err(err.into()),
            }
        }
        .boxed_local()
    }
}

pub(super) fn track_metrics(
    res: &Result<crate::get::Stats, FailureAction>,
    metrics: &crate::metrics::Metrics,
) {
    match res {
        Ok(stats) => {
            let crate::get::Stats {
                bytes_written,
                bytes_read: _,
                elapsed,
            } = stats;

            metrics.downloads_success.inc();
            metrics.download_bytes_total.inc_by(*bytes_written);
            metrics
                .download_time_total
                .inc_by(elapsed.as_millis() as u64);
        }
        Err(e) => match &e {
            FailureAction::AbortRequest(GetError::NotFound(_)) => {
                metrics.downloads_notfound.inc();
            }
            _ => {
                metrics.downloads_error.inc();
            }
        },
    }
}
