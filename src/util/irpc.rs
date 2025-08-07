use std::{future::Future, io};

use genawaiter::sync::Gen;
use irpc::{channel::mpsc, RpcMessage};
use n0_future::{Stream, StreamExt};

/// Trait for an enum that has three variants, item, error, and done.
///
/// This is very common for irpc stream items if you want to provide an explicit
/// end of stream marker to make sure unsuccessful termination is not mistaken
/// for successful end of stream.
pub(crate) trait IrpcStreamItem: RpcMessage {
    /// The error case of the item enum.
    type Error;
    /// The item case of the item enum.
    type Item;
    /// Converts the stream item into either None for end of stream, or a Result
    /// containing the item or an error. Error is assumed as a termination, so
    /// if you get error you won't get an additional end of stream marker.
    fn into_result_opt(self) -> Option<Result<Self::Item, Self::Error>>;
    /// Converts a result into the item enum.
    fn from_result(item: std::result::Result<Self::Item, Self::Error>) -> Self;
    /// Produces a done marker for the item enum.
    fn done() -> Self;
}

pub(crate) trait MpscSenderExt<T: IrpcStreamItem>: Sized {
    /// Forward a stream of items to the sender.
    ///
    /// This will convert items and errors into the item enum type, and add
    /// a done marker if the stream ends without an error.
    async fn forward_stream(
        self,
        stream: impl Stream<Item = std::result::Result<T::Item, T::Error>>,
    ) -> std::result::Result<(), irpc::channel::SendError>;

    /// Forward an iterator of items to the sender.
    ///
    /// This will convert items and errors into the item enum type, and add
    /// a done marker if the iterator ends without an error.
    async fn forward_iter(
        self,
        iter: impl Iterator<Item = std::result::Result<T::Item, T::Error>>,
    ) -> std::result::Result<(), irpc::channel::SendError>;
}

impl<T> MpscSenderExt<T> for mpsc::Sender<T>
where
    T: IrpcStreamItem,
{
    async fn forward_stream(
        self,
        stream: impl Stream<Item = std::result::Result<T::Item, T::Error>>,
    ) -> std::result::Result<(), irpc::channel::SendError> {
        tokio::pin!(stream);
        while let Some(item) = stream.next().await {
            let done = item.is_err();
            self.send(T::from_result(item)).await?;
            if done {
                return Ok(());
            };
        }
        self.send(T::done()).await
    }

    async fn forward_iter(
        self,
        iter: impl Iterator<Item = std::result::Result<T::Item, T::Error>>,
    ) -> std::result::Result<(), irpc::channel::SendError> {
        for item in iter {
            let done = item.is_err();
            self.send(T::from_result(item)).await?;
            if done {
                return Ok(());
            };
        }
        self.send(T::done()).await
    }
}

pub(crate) trait IrpcReceiverFutExt<T: IrpcStreamItem> {
    /// Collects the receiver returned by this future into a collection,
    /// provided that we get a receiver and draining the receiver does not
    /// produce any error items.
    ///
    /// The collection must implement Default and Extend<T::Item>.
    /// Note that using this with a very large stream might use a lot of memory.
    async fn try_collect<C, E>(self) -> std::result::Result<C, E>
    where
        C: Default + Extend<T::Item>,
        E: From<T::Error>,
        E: From<irpc::Error>,
        E: From<irpc::channel::RecvError>;

    /// Converts the receiver returned by this future into a stream of items,
    /// where each item is either a successful item or an error.
    ///
    /// There will be at most one error item, which will terminate the stream.
    /// If the future returns an error, the stream will yield that error as the
    /// first item and then terminate.
    fn into_stream<E>(self) -> impl Stream<Item = std::result::Result<T::Item, E>>
    where
        E: From<T::Error>,
        E: From<irpc::Error>,
        E: From<irpc::channel::RecvError>;
}

impl<T, F> IrpcReceiverFutExt<T> for F
where
    T: IrpcStreamItem,
    F: Future<Output = std::result::Result<irpc::channel::mpsc::Receiver<T>, irpc::Error>>,
{
    async fn try_collect<C, E>(self) -> std::result::Result<C, E>
    where
        C: Default + Extend<T::Item>,
        E: From<T::Error>,
        E: From<irpc::Error>,
        E: From<irpc::channel::RecvError>,
    {
        let mut items = C::default();
        let mut stream = self.into_stream::<E>();
        while let Some(item) = stream.next().await {
            match item {
                Ok(i) => items.extend(Some(i)),
                Err(e) => return Err(E::from(e)),
            }
        }
        Ok(items)
    }

    fn into_stream<E>(self) -> impl Stream<Item = std::result::Result<T::Item, E>>
    where
        E: From<T::Error>,
        E: From<irpc::Error>,
        E: From<irpc::channel::RecvError>,
    {
        Gen::new(move |co| async move {
            let mut rx = match self.await {
                Ok(rx) => rx,
                Err(e) => {
                    co.yield_(Err(E::from(e))).await;
                    return;
                }
            };
            loop {
                match rx.recv().await {
                    Ok(Some(item)) => match item.into_result_opt() {
                        Some(Ok(i)) => co.yield_(Ok(i)).await,
                        Some(Err(e)) => {
                            co.yield_(Err(E::from(e))).await;
                            break;
                        }
                        None => break,
                    },
                    Ok(None) => {
                        co.yield_(Err(E::from(irpc::channel::RecvError::Io(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "unexpected end of stream",
                        )))))
                        .await;
                        break;
                    }
                    Err(e) => {
                        co.yield_(Err(E::from(e))).await;
                        break;
                    }
                }
            }
        })
    }
}
