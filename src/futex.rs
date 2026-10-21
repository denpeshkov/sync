//! See [Basic futex operation and ordering guarantees](https://github.com/torvalds/linux/blob/master/kernel/futex/waitwake.c)

use std::io::Error;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

/// Waits for a [wake_one] or [wake_all] operation to wake us.
///
/// Returns directly if the futex doesn't hold the expected value.
/// Returns false on timeout, and true in all other cases.
pub fn wait(a: &AtomicU32, expected: u32, timeout: Option<Duration>) -> bool {
    let timespec = timeout.and_then(|d| {
        Some(libc::timespec {
            // Sleep forever if the timeout is longer than fits in a timespec.
            tv_sec: d.as_secs().try_into().ok()?,
            // This conversion never truncates, as subsec_nanos is always <1e9.
            tv_nsec: d.subsec_nanos() as _,
        })
    });
    let timeout_ptr = timespec
        .as_ref()
        .map_or(std::ptr::null(), |d| d as *const libc::timespec);

    let r = unsafe {
        libc::syscall(
            libc::SYS_futex,                             // The futex syscall.
            a as *const AtomicU32 as *mut u32,           // The atomic to operate on.
            libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG, // The futex operation.
            expected,                                    // The expected value.
            timeout_ptr,                                 // Timeout.
        )
    };

    // Return false ONLY if the syscall timed out.
    // Spurious wakeups (EAGAIN, EINTR) will return true.
    !(r < 0 && Error::last_os_error().raw_os_error().unwrap() == libc::ETIMEDOUT)
}

/// Wakes up one thread that's blocked on [wait] on this futex.
///
/// Returns true if this actually woke up such a thread,
/// or false if no thread was waiting on this futex.
pub fn wake_one(a: &AtomicU32) -> bool {
    let r = unsafe {
        libc::syscall(
            libc::SYS_futex,                             // The futex syscall.
            a as *const AtomicU32 as *const u32,         // The atomic to operate on.
            libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG, // The futex operation.
            1,                                           // The number of threads to wake up.
        )
    };
    r > 0
}

/// Wakes up all threads that are waiting on [wait] on this futex.4
pub fn wake_all(a: &AtomicU32) -> bool {
    let r = unsafe {
        libc::syscall(
            libc::SYS_futex,                             // The futex syscall.
            a as *const AtomicU32 as *const u32,         // The atomic to operate on.
            libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG, // The futex operation.
            i32::MAX,                                    // The number of threads to wake up.
        )
    };
    r > 0
}
