//! Identification of the current boot session of the machine.
//!
//! The store records the boot ID while it is open. If it finds a recorded ID
//! from a different boot, the machine went down while the store was open, so
//! an OS crash or power loss may have lost data that was never synced.

/// Returns an ID that is the same for the whole boot session and new after every boot.
///
/// Returns `None` where the platform has no such ID, or it cannot be read.
pub(super) fn current() -> Option<String> {
    let id = platform::current()?;
    let id = id.trim();
    (!id.is_empty()).then(|| id.to_owned())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
mod platform {
    /// The kernel creates a new random UUID on every boot.
    pub(super) fn current() -> Option<String> {
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
mod platform {
    use std::ffi::CStr;

    /// The kernel creates a new UUID for every boot session.
    pub(super) fn current() -> Option<String> {
        let mut buf = [0u8; 64];
        let mut len = buf.len();
        // SAFETY: the name is a nul terminated string, `buf` is a writable
        // buffer of `len` bytes, and `len` carries its size in and the length
        // of the value out. No new value is set.
        let res = unsafe {
            libc::sysctlbyname(
                c"kern.bootsessionuuid".as_ptr(),
                buf.as_mut_ptr().cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if res != 0 {
            return None;
        }
        let value = CStr::from_bytes_until_nul(buf.get(..len)?).ok()?;
        value.to_str().ok().map(str::to_owned)
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
)))]
mod platform {
    /// No boot ID on this platform yet.
    pub(super) fn current() -> Option<String> {
        None
    }
}

#[cfg(test)]
mod tests {
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[test]
    fn boot_id_is_known_and_stable() {
        let first = super::current().expect("boot id is available on this platform");
        assert_eq!(super::current(), Some(first));
    }
}
