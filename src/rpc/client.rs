//! Iroh blobs and tags client
use anyhow::Result;
use futures_util::{Stream, StreamExt};
use quic_rpc::transport::flume::FlumeConnector;

pub mod blobs;
pub mod tags;

/// Type alias for a memory-backed client.
pub(crate) type MemConnector =
    FlumeConnector<crate::rpc::proto::Response, crate::rpc::proto::Request>;

fn flatten<T, E1, E2>(
    s: impl Stream<Item = Result<Result<T, E1>, E2>>,
) -> impl Stream<Item = Result<T>>
where
    E1: std::error::Error + Send + Sync + 'static,
    E2: std::error::Error + Send + Sync + 'static,
{
    s.map(|res| match res {
        Ok(Ok(res)) => Ok(res),
        Ok(Err(err)) => Err(err.into()),
        Err(err) => Err(err.into()),
    })
}
