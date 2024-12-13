//! [`Getter`] implementation that performs requests over [`Connection`]s.
//!
//! [`Connection`]: iroh::endpoint::Connection

use futures_lite::FutureExt;
use iroh::endpoint;

use super::{progress::BroadcastProgressSender, DownloadKind, FailureAction, GetStartFut, Getter};
use crate::{
    fetch::{db::fetch_to_db_in_steps, Error},
    store::Store,
};

impl From<Error> for FailureAction {
    fn from(e: Error) -> Self {
        match e {
            e @ Error::NotFound(_) => FailureAction::AbortRequest(e.into()),
            e @ Error::RemoteReset(_) => FailureAction::RetryLater(e.into()),
            e @ Error::NoncompliantNode(_) => FailureAction::DropPeer(e.into()),
            e @ Error::Io(_) => FailureAction::RetryLater(e.into()),
            e @ Error::BadRequest(_) => FailureAction::AbortRequest(e.into()),
            // TODO: what do we want to do on local failures?
            e @ Error::LocalFailure(_) => FailureAction::AbortRequest(e.into()),
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
    type NeedsConn = crate::fetch::db::FetchStateNeedsConn;

    fn get(
        &mut self,
        kind: DownloadKind,
        progress_sender: BroadcastProgressSender,
    ) -> GetStartFut<Self::NeedsConn> {
        let store = self.store.clone();
        async move {
            match fetch_to_db_in_steps(store, kind.hash_and_format(), progress_sender).await {
                Err(err) => Err(err.into()),
                Ok(crate::fetch::db::FetchState::Complete(stats)) => {
                    Ok(super::GetOutput::Complete(stats))
                }
                Ok(crate::fetch::db::FetchState::NeedsConn(needs_conn)) => {
                    Ok(super::GetOutput::NeedsConn(needs_conn))
                }
            }
        }
        .boxed_local()
    }
}

impl super::NeedsConn<endpoint::Connection> for crate::fetch::db::FetchStateNeedsConn {
    fn proceed(self, conn: endpoint::Connection) -> super::GetProceedFut {
        async move {
            let res = self.proceed(conn).await;
            #[cfg(feature = "metrics")]
            track_metrics(&res);
            match res {
                Ok(stats) => Ok(stats),
                Err(err) => Err(err.into()),
            }
        }
        .boxed_local()
    }
}

#[cfg(feature = "metrics")]
fn track_metrics(res: &Result<crate::fetch::Stats, Error>) {
    use iroh_metrics::{inc, inc_by};

    use crate::metrics::Metrics;
    match res {
        Ok(stats) => {
            let crate::fetch::Stats {
                bytes_written,
                bytes_read: _,
                elapsed,
            } = stats;

            inc!(Metrics, downloads_success);
            inc_by!(Metrics, download_bytes_total, *bytes_written);
            inc_by!(Metrics, download_time_total, elapsed.as_millis() as u64);
        }
        Err(e) => match &e {
            Error::NotFound(_) => inc!(Metrics, downloads_notfound),
            _ => inc!(Metrics, downloads_error),
        },
    }
}
