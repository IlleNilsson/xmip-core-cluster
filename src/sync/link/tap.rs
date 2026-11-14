//! The answering side of a link, inside the hub: what another node's
//! subscriptions want of this node's own Events, and the bounded queue they
//! wait in to cross.
//!
//! A tap is fed by [`crate::delivery::hub::Hub::publish`] only — the Events raised on
//! this node — never by what this node heard from another, so an Event
//! crosses one hop and never comes back. It matches by the filters the
//! asking node pushed down, so what no subscriber there wants never leaves
//! here. A full queue refuses the Event and counts it for each subscription
//! it matched, and the count crosses instead (`link.rs`, *Missed*).
//!
//! What crosses is decided here and carried by the sync plane
//! (`xmip-core-cluster`): what the asking node said is answered by
//! [`Tap::asked`], and what to write next is [`Tap::next`].

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use xcore::lock::{Locked, ReadWriteLocked};

use crate::delivery::hub::Hub;
use crate::delivery::queue::Queue;
use crate::sync::link::Frame;
use xevent::Event;
use xevent::filter::Filter;

/// What one asking node wants, by its subscriptions' numbers there.
pub(crate) type Wants = Arc<Vec<(u64, Filter)>>;

/// How many Events a link's queue holds while the link writes.
pub(crate) const CAPACITY: usize = 4096;

/// One link's filters and queue: what one node listening here wants of
/// this node's own Events.
pub struct Tap {
    wants: RwLock<Wants>,
    queue: Queue,
    missed: Mutex<BTreeMap<u64, u64>>,
}

impl Tap {
    /// What the asking node said, answered: what it wants is held from now
    /// on, and the answer is the generation it asked at.
    ///
    /// # Errors
    /// It said what only an answering node says, in words.
    pub fn asked(&self, frame: Frame) -> Result<Frame, String> {
        match frame {
            Frame::Wants(generation, wants) => {
                *self.wants.write_locked() = Arc::new(wants);
                Ok(Frame::Wanted(generation))
            }
            _ => Err("the node said what only an answering node says".to_string()),
        }
    }

    /// What to write next, waiting up to `beat` for it: up to `most`
    /// Events as they were raised and what the queue refused since, or a
    /// beat when nothing came. `None` once the tap is closed.
    #[must_use]
    pub fn next(&self, beat: Duration, most: usize) -> Option<Vec<Frame>> {
        let delivery = self.queue.take(beat, most);
        if self.queue.is_closed() {
            return None;
        }
        let mut frames: Vec<Frame> = delivery.events.into_iter().map(Frame::Event).collect();
        let missed = self.take_missed();
        if !missed.is_empty() {
            frames.push(Frame::Missed(missed));
        }
        if frames.is_empty() {
            frames.push(Frame::Beat);
        }
        Some(frames)
    }

    /// Queue `event` where any filter wants it; a full queue counts it for
    /// each subscription it matched.
    pub(crate) fn offer(&self, event: &Arc<Event>) {
        let wants = Arc::clone(&self.wants.read_locked());
        if !wants.iter().any(|(_, filter)| filter.matches(event)) {
            return;
        }
        if self.queue.offer(event) || self.queue.is_closed() {
            return;
        }
        let mut missed = self.missed.locked();
        for (id, filter) in wants.iter() {
            if filter.matches(event) {
                *missed.entry(*id).or_default() += 1;
            }
        }
    }

    /// What was refused since the last time asked, by subscription.
    fn take_missed(&self) -> Vec<(u64, u64)> {
        std::mem::take(&mut *self.missed.locked())
            .into_iter()
            .collect()
    }
}

impl Hub {
    /// A new link's tap, fed from now on, wanting nothing yet.
    #[must_use]
    pub fn tap(&self) -> Arc<Tap> {
        let tap = Arc::new(Tap {
            wants: RwLock::new(Arc::new(Vec::new())),
            queue: Queue::new(CAPACITY),
            missed: Mutex::new(BTreeMap::new()),
        });
        self.change_taps(|taps| taps.push(Arc::clone(&tap)));
        tap
    }

    /// A link's tap closed and gone.
    pub fn untap(&self, tap: &Arc<Tap>) {
        tap.queue.close();
        self.change_taps(|taps| taps.retain(|held| !Arc::ptr_eq(held, tap)));
    }

    fn change_taps(&self, edit: impl FnOnce(&mut Vec<Arc<Tap>>)) {
        let mut held = self.inner.taps.write_locked();
        let mut next = held.as_ref().clone();
        edit(&mut next);
        *held = Arc::new(next);
    }
}
