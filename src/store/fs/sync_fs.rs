//! Flush the data filesystem when opening a PowerLoss store, where supported.

use std::{io, path::Path};

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(super) fn sync(path: &Path) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let dir = std::fs::File::open(path)?;
    // SAFETY: `dir` owns a valid descriptor for the duration of the call.
    if unsafe { libc::syncfs(dir.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
pub(super) fn sync(path: &Path) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    // Available since macOS 10.8 / iOS 6, but not exposed by the libc crate.
    unsafe extern "C" {
        fn fsync_volume_np(fd: libc::c_int, flags: libc::c_int) -> libc::c_int;
    }
    const SYNC_VOLUME_FULLSYNC: libc::c_int = 0x01;
    const SYNC_VOLUME_WAIT: libc::c_int = 0x02;

    let dir = std::fs::File::open(path)?;
    // SAFETY: `dir` owns a valid descriptor and both flags are documented.
    let error =
        unsafe { fsync_volume_np(dir.as_raw_fd(), SYNC_VOLUME_FULLSYNC | SYNC_VOLUME_WAIT) };
    if error == 0 {
        Ok(())
    } else {
        // Unlike syncfs, this API returns the error number directly.
        Err(io::Error::from_raw_os_error(error))
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
)))]
pub(super) fn sync(_path: &Path) -> io::Result<()> {
    // Preserve existing behavior on other platforms. In particular, Windows'
    // volume-wide FlushFileBuffers requires administrator privileges.
    Ok(())
}
