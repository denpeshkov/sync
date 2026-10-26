#[cfg(target_os = "linux")]
pub mod condvar;
#[cfg(target_os = "linux")]
pub mod futex;
pub mod linked_list_set;
#[cfg(target_os = "linux")]
pub mod mutex;
pub mod spinlock;
