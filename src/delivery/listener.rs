//! A subscription drained by a thread of its own, calling back for each
//! Event: the callback variant of ADR-0065 clause 2.
//!
//! The callback runs on the listener's thread, never the publisher's, so a
//! slow callback fills its own queue and nothing else. Dropping the
//! [`Listener`] closes the queue, which wakes the thread; it finishes the
//! drain it is in, unsubscribes, and is joined, so the close record is
//! handed over before the drop returns. Inside the callback itself the
//! thread cannot wait for itself, and is never made to: the drop
//! unsubscribes there and then, the close record handed to audit's keeper
//! on that thread before the drop returns, and the thread calls back
//! nothing more and ends once the callback returns (ADR-0065, amendment
//! 2026-11-20).

use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::delivery::hub::{EventSubscription, Inner, Slot};
use xevent::Event;

/// How long the thread sleeps between looks when nothing arrives; closing
/// wakes it at once, so this bounds nothing an operator waits for.
const IDLE: Duration = Duration::from_secs(60);

/// How many Events one drain hands the callback before it looks again.
const BATCH: usize = 256;

/// A subscription and the thread calling back for it.
pub struct Listener {
    slot: Arc<Slot>,
    hub: Weak<Inner>,
    thread: Option<JoinHandle<()>>,
}

impl EventSubscription {
    /// Call `each` for every Event, on a thread of its own, until the
    /// returned [`Listener`] is dropped.
    ///
    /// # Errors
    /// The operating system would not start the thread.
    pub fn listen(
        self,
        mut each: impl FnMut(&Event) + Send + 'static,
    ) -> std::io::Result<Listener> {
        let slot = self.slot();
        let hub = self.hub.clone();
        let thread = thread::Builder::new()
            .name("xmip-event-listener".to_string())
            .spawn(move || {
                let subscription = self;
                let slot = subscription.slot();
                while !slot.queue.is_closed() {
                    for event in subscription.next(IDLE, BATCH).events {
                        // Dropped in the callback before: nothing more.
                        if slot.unsubscribed() {
                            break;
                        }
                        each(&event);
                    }
                }
            })?;

        Ok(Listener {
            slot,
            hub,
            thread: Some(thread),
        })
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        let Some(thread) = self.thread.take() else {
            return;
        };
        if thread.thread().id() == thread::current().id() {
            // Inside its own callback: unsubscribed here, so the close
            // record is the keeper's before this drop returns.
            self.slot.unsubscribe(&self.hub);
        } else {
            self.slot.queue.close();
            // A callback that panicked has already said so on its thread.
            let _ = thread.join();
        }
    }
}
