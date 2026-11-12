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

#[cfg(windows)]
mod platform {
    use windows_sys::Win32::{
        Foundation::ERROR_SUCCESS,
        System::Registry::{RegGetValueW, HKEY_LOCAL_MACHINE, RRF_RT_REG_DWORD},
    };

    /// The prefetcher counts boots in the registry, once per boot.
    pub(super) fn current() -> Option<String> {
        let key = wide(
            r"SYSTEM\CurrentControlSet\Control\Session Manager\Memory Management\PrefetchParameters",
        );
        let name = wide("BootId");
        let mut value = 0u32;
        let mut size = std::mem::size_of::<u32>() as u32;
        // SAFETY: key and name are nul terminated wide strings, and `value` is
        // a writable DWORD whose size `size` carries in and out.
        let res = unsafe {
            RegGetValueW(
                HKEY_LOCAL_MACHINE,
                key.as_ptr(),
                name.as_ptr(),
                RRF_RT_REG_DWORD,
                std::ptr::null_mut(),
                std::ptr::addr_of_mut!(value).cast(),
                &mut size,
            )
        };
        (res == ERROR_SUCCESS).then(|| value.to_string())
    }

    /// Encodes `s` as a nul terminated UTF-16 string.
    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    windows
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
        target_os = "ios",
        windows
    ))]
    #[test]
    fn boot_id_is_known_and_stable() {
        let first = super::current().expect("boot id is available on this platform");
        assert_eq!(super::current(), Some(first));
    }
}
