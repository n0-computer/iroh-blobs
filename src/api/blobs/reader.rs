use std::{
    io::{self, ErrorKind, SeekFrom},
    ops::DerefMut,
    sync::{Arc, Mutex},
    task::Poll,
};

use n0_future::FutureExt;

use crate::api::{
    blobs::{Blobs, ReaderOptions},
    RequestResult,
};

pub struct Reader {
    blobs: Blobs,
    options: ReaderOptions,
    state: Arc<Mutex<ReaderState>>,
}

#[derive(Default)]
enum ReaderState {
    Idle {
        position: u64,
    },
    Seeking {
        position: u64,
    },
    Reading {
        position: u64,
        op: n0_future::boxed::BoxFuture<RequestResult<Vec<u8>>>,
    },
    #[default]
    Poisoned,
}

impl Reader {
    pub fn new(blobs: Blobs, options: ReaderOptions) -> Self {
        Self {
            blobs,
            options,
            state: Arc::new(Mutex::new(ReaderState::Idle { position: 0 })),
        }
    }
}

impl tokio::io::AsyncRead for Reader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        match std::mem::take(this.state.lock().unwrap().deref_mut()) {
            ReaderState::Idle { position } => {
                // todo: read until next page boundary instead of fixed size
                let len = buf.remaining().min(1024 * 16);
                let end = position.checked_add(len as u64).ok_or_else(|| {
                    io::Error::new(ErrorKind::InvalidInput, "Position overflow when reading")
                })?;
                let hash = this.options.hash;
                let blobs = this.blobs.clone();
                let ranges = position..end;
                let op = async move { blobs.export_ranges(hash, ranges).concatenate().await };
                *this.state.lock().unwrap() = ReaderState::Reading {
                    position,
                    op: Box::pin(op),
                };
            }
            ReaderState::Reading { position, mut op } => {
                match op.poll(cx) {
                    Poll::Ready(Ok(data)) => {
                        let len = data.len();
                        if len > buf.remaining() {
                            return Poll::Ready(Err(io::Error::new(
                                ErrorKind::UnexpectedEof,
                                "Read more data than buffer can hold",
                            )));
                        }
                        buf.put_slice(&data);
                        let position = position + len as u64;
                        *this.state.lock().unwrap() = ReaderState::Idle { position };
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(e)) => {
                        *this.state.lock().unwrap() = ReaderState::Idle { position };
                        let e = io::Error::new(ErrorKind::Other, e.to_string());
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        // Put back the state
                        *this.state.lock().unwrap() = ReaderState::Reading {
                            position,
                            op: Box::pin(op),
                        };
                        return Poll::Pending;
                    }
                }
            }
            state @ ReaderState::Seeking { .. } => {
                *this.state.lock().unwrap() = state;
                return Poll::Ready(Err(io::Error::new(
                    ErrorKind::Other,
                    "Can't read while seeking",
                )));
            }
            ReaderState::Poisoned => {
                return Poll::Ready(Err(io::Error::other("Reader is poisoned")));
            }
        };
        todo!()
    }
}

impl tokio::io::AsyncSeek for Reader {
    fn start_seek(
        self: std::pin::Pin<&mut Self>,
        seek_from: tokio::io::SeekFrom,
    ) -> std::io::Result<()> {
        let this = self.get_mut();
        match std::mem::take(this.state.lock().unwrap().deref_mut()) {
            ReaderState::Idle { position } => {
                let position1 = match seek_from {
                    SeekFrom::Start(pos) => pos,
                    SeekFrom::Current(offset) => {
                        position.checked_add_signed(offset).ok_or_else(|| {
                            io::Error::new(
                                ErrorKind::InvalidInput,
                                "Position overflow when seeking",
                            )
                        })?
                    }
                    SeekFrom::End(_offset) => {
                        // todo: support seeking from end if we know the size
                        return Err(io::Error::new(
                            ErrorKind::InvalidInput,
                            "Seeking from end is not supported yet",
                        ))?;
                    }
                };
                *this.state.lock().unwrap() = ReaderState::Seeking {
                    position: position1,
                };
                Ok(())
            }
            ReaderState::Reading { .. } => Err(io::Error::other("Can't seek while reading")),
            ReaderState::Seeking { .. } => Err(io::Error::other("Already seeking")),
            ReaderState::Poisoned => Err(io::Error::other("Reader is poisoned")),
        }
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        let this = self.get_mut();
        Poll::Ready(
            match std::mem::take(this.state.lock().unwrap().deref_mut()) {
                ReaderState::Seeking { position } => {
                    // we only put the state back if we are in the right state
                    *this.state.lock().unwrap() = ReaderState::Idle { position };
                    Ok(position)
                }
                ReaderState::Idle { .. } => Err(io::Error::other("No seek operation in progress")),
                ReaderState::Reading { .. } => Err(io::Error::other("Can't seek while reading")),
                ReaderState::Poisoned => Err(io::Error::other("Reader is poisoned")),
            },
        )
    }
}
