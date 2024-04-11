//! Bindings to Twizzler.

use std::collections::HashMap;
use std::io::{self};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use twizzler_abi::syscall::ThreadSync;
use twizzler_futures::TwizzlerWaitable;

use crate::{Event, PollMode};

/// Interface to twizzler's thread sync.
#[derive(Debug)]
pub struct Poller {
    /// Pollable objects.
    wps: Mutex<Wps>,
    /// Notification for waking up the poller.
    notify: Pin<Box<notify::Notify>>,
    /// The number of operations (`add`, `modify` or `delete`) that are currently waiting on the
    /// mutex to become free. When this is nonzero, `wait` must be suspended until it reaches zero
    /// again.
    waiting_operations: AtomicUsize,
    /// Whether `wait` has been notified by the user.
    notified: AtomicBool,
    /// The condition variable that gets notified when `waiting_operations` reaches zero or
    /// `notified` becomes true.
    ///
    /// This is used with the `fds` mutex.
    operations_complete: Condvar,
}

/// The file descriptors to poll in a `Poller`.
#[derive(Debug)]
struct Wps {
    /// The list of ThreadSync operations to call the kernel with.
    ///
    /// The first one is always present and is used to notify the poller.
    polls: Vec<ThreadSync>,
    /// The map of each waitable object to data associated with it.
    wp_data: HashMap<HashKey, WpData>,
    /// Keys for the hash map, in order in polls.
    polls_keys: Vec<HashKey>,
    /// A buffer for cleanup, stored here to avoid allocating memory for it every time.
    cleanup_buffer: Vec<HashKey>,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
#[repr(transparent)]
struct HashKey(usize);

impl HashKey {
    fn new(key: usize, write: bool) -> Self {
        Self(key * 2 | if write { 1 } else { 0 })
    }
}

impl Wps {
    fn push_item(&mut self, source: &BorrowedTwizzlerWaitable<'static>, mut data: WpData) {
        // Push onto the polls list, and update meta data.
        let index = self.polls.len();
        data.poll_wps_index = index;
        data.bw_key = source.key();
        // Get a new ThreadSync.
        let sleep = if data.write {
            source.waitable.wait_item_write()
        } else {
            source.waitable.wait_item_read()
        };
        self.polls.push(ThreadSync::new_sleep(sleep));
        self.polls_keys.push(data.hash_key());
        self.wp_data.insert(data.hash_key(), data);
        // Reserve space for cleanup, if necessary.
        if self.cleanup_buffer.capacity() < self.polls.len() {
            self.cleanup_buffer
                .reserve(self.polls.len() - self.cleanup_buffer.capacity());
        }
    }

    fn remove_item_dir(&mut self, source: &BorrowedTwizzlerWaitable<'static>, write: bool) {
        self.remove_item_key(&HashKey::new(source.key(), write));
    }

    fn remove_item_key(&mut self, key: &HashKey) {
        if let Some(data) = self.wp_data.remove(key) {
            // Unwrap-Ok: this vec is never empty.
            let swapped_key = *self.polls_keys.last().unwrap();
            self.polls.swap_remove(data.poll_wps_index);
            self.polls_keys.swap_remove(data.poll_wps_index);

            if let Some(swapped_data) = self.wp_data.get_mut(&swapped_key) {
                swapped_data.poll_wps_index = data.poll_wps_index;
            }
        }
    }
}

/// Data associated with a file descriptor in a poller.
#[derive(Debug)]
struct WpData {
    /// The index into `poll_fds` this waitpoint.
    poll_wps_index: usize,
    /// The key of the `Event` associated with this waitpoint.
    key: usize,
    /// Whether to remove this waitpoint from the poller on the next call to `wait`.
    remove: bool,
    /// If this is for write or for read.
    write: bool,
    /// The unique key for the borrow waitable, for making a HashKey.
    bw_key: usize,
}

impl WpData {
    fn hash_key(&self) -> HashKey {
        HashKey::new(self.bw_key, self.write)
    }
}

bitflags::bitflags! {
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct PollFlags: u32 {
    const READ = 1;
    const WRITE = 2;
    const ERR = 4;
}
}

impl Poller {
    /// Creates a new poller.
    pub fn new() -> io::Result<Poller> {
        let notify = Box::pin(notify::Notify::new()?);

        tracing::trace!(?notify, "new");

        let sleep = ThreadSync::new_sleep(notify.wait_item_read());
        Ok(Self {
            wps: Mutex::new(Wps {
                polls: vec![sleep],
                wp_data: HashMap::new(),
                polls_keys: vec![HashKey::new(0, false)],
                cleanup_buffer: vec![],
            }),
            notify,
            waiting_operations: AtomicUsize::new(0),
            operations_complete: Condvar::new(),
            notified: AtomicBool::new(false),
        })
    }

    /// Whether this poller supports level-triggered events.
    pub fn supports_level(&self) -> bool {
        true
    }

    /// Whether the poller supports edge-triggered events.
    pub fn supports_edge(&self) -> bool {
        false
    }

    /// Adds a new waitable object.
    pub fn add(
        &self,
        wp: &BorrowedTwizzlerWaitable<'static>,
        ev: Event,
        mode: PollMode,
    ) -> io::Result<()> {
        let span = tracing::trace_span!(
            "add",
            notify_read = ?self.notify,
            ?wp,
            ?ev,
        );
        let _enter = span.enter();
        if ev.readable && ev.writable == false {
            tracing::warn!("unsupported event readable writable combo");
        }

        self.modify_wps(|wps| {
            // Don't add twice.
            if ev.writable && wps.wp_data.contains_key(&HashKey::new(wp.key(), true))
                || ev.readable && wps.wp_data.contains_key(&HashKey::new(wp.key(), false))
            {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists));
            }
            if ev.readable {
                wps.push_item(
                    &wp,
                    WpData {
                        key: ev.key,
                        remove: cvt_mode_as_remove(mode)?,
                        write: false,
                        // These get set in push.
                        poll_wps_index: 0,
                        bw_key: 0,
                    },
                );
            }
            if ev.writable {
                wps.push_item(
                    &wp,
                    WpData {
                        key: ev.key,
                        remove: cvt_mode_as_remove(mode)?,
                        write: true,
                        // These get set in push.
                        poll_wps_index: 0,
                        bw_key: 0,
                    },
                );
            }

            Ok(())
        })
    }

    /// Modifies an existing waitable object.
    pub fn modify(
        &self,
        source: &BorrowedTwizzlerWaitable<'static>,
        ev: Event,
        mode: PollMode,
    ) -> io::Result<()> {
        let span = tracing::trace_span!(
            "modify",
            notify_read = ?self.notify,
            ?source,
            ?ev,
        );
        let _enter = span.enter();

        self.modify_wps(|wps| {
            // If we're interested, make sure we've got it registered. Otherwise, remove anything present.
            if ev.readable {
                if let Some(data) = wps.wp_data.get_mut(&HashKey::new(source.key(), false)) {
                    data.key = ev.key;
                    data.remove = cvt_mode_as_remove(mode)?;
                    wps.polls[data.poll_wps_index] =
                        ThreadSync::new_sleep(source.waitable.wait_item_read());
                } else {
                    wps.push_item(
                        source,
                        WpData {
                            key: ev.key,
                            remove: cvt_mode_as_remove(mode)?,
                            write: false,
                            // These get set in push.
                            poll_wps_index: 0,
                            bw_key: 0,
                        },
                    );
                }
            } else {
                wps.remove_item_dir(source, false);
            }

            if ev.writable {
                if let Some(data) = wps.wp_data.get_mut(&HashKey::new(source.key(), true)) {
                    data.key = ev.key;
                    data.remove = cvt_mode_as_remove(mode)?;
                    wps.polls[data.poll_wps_index] =
                        ThreadSync::new_sleep(source.waitable.wait_item_write());
                } else {
                    wps.push_item(
                        source,
                        WpData {
                            key: ev.key,
                            remove: cvt_mode_as_remove(mode)?,
                            write: true,
                            // These get set in push.
                            poll_wps_index: 0,
                            bw_key: 0,
                        },
                    );
                }
            } else {
                wps.remove_item_dir(source, true);
            }

            Ok(())
        })
    }

    /// Deletes a waitable object.
    pub fn delete(&self, source: &BorrowedTwizzlerWaitable<'static>) -> io::Result<()> {
        let span = tracing::trace_span!(
            "delete",
            notify_read = ?self.notify,
            ?source,
        );
        let _enter = span.enter();

        self.modify_wps(|wps| {
            wps.remove_item_dir(source, false);
            wps.remove_item_dir(source, true);
            Ok(())
        })
    }

    /// Waits for I/O events with an optional timeout.
    pub fn wait(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        let span = tracing::trace_span!(
            "wait",
            notify_read = ?self.notify,
            ?timeout,
        );
        let _enter = span.enter();

        let deadline = timeout.and_then(|t| Instant::now().checked_add(t));

        events.inner.clear();

        let mut wps = self.wps.lock().unwrap();

        loop {
            // Complete all current operations.
            loop {
                if self.notified.swap(false, Ordering::SeqCst) {
                    // `notify` will have sent a notification in case we were polling. We weren't,
                    // so remove it.
                    return self.notify.pop_notification();
                } else if self.waiting_operations.load(Ordering::SeqCst) == 0 {
                    break;
                }

                wps = self.operations_complete.wait(wps).unwrap();
            }

            // Perform the poll.
            let timeout =
                deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
            let _res = twizzler_abi::syscall::sys_thread_sync(&mut wps.polls, timeout);

            let notified = wps.polls[0].ready();
            tracing::trace!(?notified, "new events",);

            // Read all notifications.
            if notified {
                self.notify.pop_all_notifications()?;
            }

            let wps = &mut *wps;
            // Store the events if there were any.
            for wp_data in wps.wp_data.values() {
                let poll_wp = &mut wps.polls[wp_data.poll_wps_index];
                if poll_wp.ready() {
                    // Store event
                    events.inner.push(Event {
                        key: wp_data.key,
                        readable: !wp_data.write,
                        writable: wp_data.write,
                        extra: EventExtra {
                            flags: if wp_data.write {
                                PollFlags::WRITE
                            } else {
                                PollFlags::READ
                            },
                        },
                    });
                    // Remove interest if necessary
                    if wp_data.remove {
                        wps.cleanup_buffer.push(wp_data.hash_key());
                    }
                }
            }

            for hk in wps.cleanup_buffer.drain(..) {
                if let Some(data) = wps.wp_data.remove(&hk) {
                    // Unwrap-Ok: this vec is never empty.
                    let swapped_key = *wps.polls_keys.last().unwrap();
                    wps.polls.swap_remove(data.poll_wps_index);
                    wps.polls_keys.swap_remove(data.poll_wps_index);

                    if let Some(swapped_data) = wps.wp_data.get_mut(&swapped_key) {
                        swapped_data.poll_wps_index = data.poll_wps_index;
                    }
                }
            }

            break;
        }

        Ok(())
    }

    /// Sends a notification to wake up the current or next `wait()` call.
    pub fn notify(&self) -> io::Result<()> {
        let span = tracing::trace_span!(
            "notify",
            notify_read = ?self.notify,
        );
        let _enter = span.enter();

        if !self.notified.swap(true, Ordering::SeqCst) {
            self.notify.notify()?;
            self.operations_complete.notify_one();
        }

        Ok(())
    }

    /// Perform a modification on `wps`, interrupting the current caller of `wait` if it's running.
    fn modify_wps(&self, f: impl FnOnce(&mut Wps) -> io::Result<()>) -> io::Result<()> {
        self.waiting_operations.fetch_add(1, Ordering::SeqCst);

        // Wake up the current caller of `wait` if there is one.
        let sent_notification = self.notify.notify().is_ok();

        let mut fds = self.wps.lock().unwrap();

        // If there was no caller of `wait` our notification was not removed.
        if sent_notification {
            let _ = self.notify.pop_notification();
        }

        let res = f(&mut fds);

        if self.waiting_operations.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.operations_complete.notify_one();
        }

        res
    }
}

/// A list of reported I/O events.
pub struct Events {
    inner: Vec<Event>,
}

impl Events {
    /// Creates an empty list.
    pub fn with_capacity(cap: usize) -> Events {
        Self {
            inner: Vec::with_capacity(cap),
        }
    }

    /// Iterates over I/O events.
    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        self.inner.iter().copied()
    }

    /// Clear the list.
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Get the capacity of the list.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}

/// Extra information associated with an event.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct EventExtra {
    /// Flags associated with this event.
    flags: PollFlags,
}

impl EventExtra {
    /// Creates an empty set of extra information.
    #[inline]
    pub const fn empty() -> Self {
        Self {
            flags: PollFlags::empty(),
        }
    }

    #[inline]
    pub fn is_err(&self) -> Option<bool> {
        Some(self.flags.contains(PollFlags::ERR))
    }

    /// Set the interrupt flag.
    #[inline]
    pub fn set_hup(&mut self, _value: bool) {}

    /// Set the priority flag.
    #[inline]
    pub fn set_pri(&mut self, _value: bool) {}

    /// Is this an interrupt event?
    #[inline]
    pub fn is_hup(&self) -> bool {
        false
    }

    /// Is this a priority event?
    #[inline]
    pub fn is_pri(&self) -> bool {
        false
    }

    #[inline]
    pub fn is_connect_failed(&self) -> Option<bool> {
        Some(self.flags.contains(PollFlags::ERR))
    }
}

fn cvt_mode_as_remove(mode: PollMode) -> io::Result<bool> {
    match mode {
        PollMode::Oneshot => Ok(true),
        PollMode::Level => Ok(false),
        _ => Err(crate::unsupported_error(
            "edge-triggered I/O events are not supported in poll()",
        )),
    }
}

mod notify {
    use twizzler_abi::syscall::ThreadSync;
    use twizzler_abi::syscall::ThreadSyncFlags;
    use twizzler_abi::syscall::ThreadSyncOp;
    use twizzler_abi::syscall::ThreadSyncReference;
    use twizzler_abi::syscall::ThreadSyncSleep;
    use twizzler_abi::syscall::ThreadSyncWake;
    use twizzler_futures::TwizzlerWaitable;

    use std::io;
    use std::marker::PhantomPinned;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    /// A notification pipe.
    ///
    /// This implementation uses thread sync.
    #[derive(Debug)]
    pub(super) struct Notify {
        pub(super) event: AtomicU64,
        // We can't move, since we build the threadsync once.
        _pin: PhantomPinned,
    }

    impl TwizzlerWaitable for Notify {
        fn wait_item_read(&self) -> twizzler_abi::syscall::ThreadSyncSleep {
            ThreadSyncSleep::new(
                ThreadSyncReference::Virtual(&self.event),
                0,
                ThreadSyncOp::Equal,
                ThreadSyncFlags::empty(),
            )
        }

        fn wait_item_write(&self) -> twizzler_abi::syscall::ThreadSyncSleep {
            ThreadSyncSleep::new(
                ThreadSyncReference::Virtual(&self.event),
                u64::MAX,
                ThreadSyncOp::Equal,
                ThreadSyncFlags::empty(),
            )
        }
    }

    impl Notify {
        /// Creates a new notification object.
        pub(super) fn new() -> io::Result<Self> {
            Ok(Self {
                event: AtomicU64::new(0),
                _pin: PhantomPinned,
            })
        }

        /// Notifies the `Poller` instance.
        pub(super) fn notify(&self) -> Result<(), io::Error> {
            if self.event.load(Ordering::SeqCst) == u64::MAX {
                tracing::warn!("notify event count overflow");
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            self.event.fetch_add(1, Ordering::SeqCst);
            let _ = twizzler_abi::syscall::sys_thread_sync(
                &mut [ThreadSync::new_wake(ThreadSyncWake::new(
                    ThreadSyncReference::Virtual(&self.event),
                    usize::MAX,
                ))],
                None,
            );

            Ok(())
        }

        /// Pops a notification (if any).
        pub(super) fn pop_notification(&self) -> Result<(), io::Error> {
            if self.event.load(Ordering::SeqCst) == 0 {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            self.event.store(0, Ordering::SeqCst);

            Ok(())
        }

        /// Pops all notifications. Equivalent to pop_notification.
        pub(super) fn pop_all_notifications(&self) -> Result<(), io::Error> {
            let _ = self.pop_notification();

            Ok(())
        }
    }
}

/// A Twizzler waitable object, with lifetime 'a.
///
/// Used to refresh wait commands for TwizzlerWaitable objects that
/// the poller is waiting on.
pub struct BorrowedTwizzlerWaitable<'a> {
    pub(crate) waitable: &'a (dyn TwizzlerWaitable + Sync),
    key: usize,
}

struct UniqueIds {
    counter: usize,
    reuse: Vec<usize>,
}

impl UniqueIds {
    fn next(&mut self) -> usize {
        match self.reuse.pop() {
            Some(v) => v,
            None => {
                self.counter += 1;
                self.counter
            }
        }
    }

    fn release(&mut self, v: usize) {
        if v == 0 {
            return;
        }
        if self.counter == v {
            self.counter -= 1;
        } else {
            self.reuse.push(v);
        }
    }
}

static UNIQUE_IDS: Mutex<UniqueIds> = std::sync::Mutex::new(UniqueIds {
    counter: 1,
    reuse: Vec::new(),
});

impl<'a> BorrowedTwizzlerWaitable<'a> {
    /// Build a new BorrowedTwizzlerWaitable.
    pub fn new(waitable: &'a (dyn TwizzlerWaitable + Sync)) -> Self {
        Self {
            waitable,
            key: UNIQUE_IDS.lock().unwrap().next(),
        }
    }

    pub(crate) fn key(&self) -> usize {
        self.key
    }
}

impl<'a> Drop for BorrowedTwizzlerWaitable<'a> {
    fn drop(&mut self) {
        UNIQUE_IDS.lock().unwrap().release(self.key());
    }
}

impl<'a> core::fmt::Debug for BorrowedTwizzlerWaitable<'a> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BorrowedTwizzlerWaitable")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl<'a> TwizzlerWaitable for BorrowedTwizzlerWaitable<'a> {
    fn wait_item_read(&self) -> twizzler_abi::syscall::ThreadSyncSleep {
        self.waitable.wait_item_read()
    }

    fn wait_item_write(&self) -> twizzler_abi::syscall::ThreadSyncSleep {
        self.waitable.wait_item_write()
    }
}
