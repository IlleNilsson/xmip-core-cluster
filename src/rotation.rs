//! The one turn every round robin of the cluster takes (ADR-0056 and
//! `deployment-model.md` section 3, amendments 2026-11-13).
//!
//! Two things go round the cluster's nodes: a node reaching Xmip Storage
//! goes round the Storage nodes it lists, one statement after another
//! (ADR-0056, amendments 2026-10-01 and 2026-10-03), and placement goes
//! round the nodes that can take a piece of work ([`crate::place`]). Both
//! take their turns here. A turn asks every place once, starting one
//! further than the turn before started, so the load spreads over the
//! places and a caller that passes one over still reaches every other.
//! What a caller passes over, and in what order it asks the rest, is the
//! caller's.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Where the next turn starts. Shared by whoever goes round the same
/// places — every clone of a Storage client, every placement of one kind —
/// and taken without a lock: a turn is one atomic step.
#[derive(Debug, Default)]
pub struct Rotation {
    next: AtomicUsize,
}

impl Rotation {
    /// A rotation whose first turn starts at the first place.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next: AtomicUsize::new(0),
        }
    }

    /// The next turn over `count` places: each place once, from the one
    /// after where the turn before started, round to the one before it.
    /// Empty where there are no places; the turn is taken all the same.
    pub fn turn(&self, count: usize) -> impl Iterator<Item = usize> + use<> {
        let first = self.next.fetch_add(1, Ordering::Relaxed);
        let start = first.checked_rem(count).unwrap_or_default();
        (0..count).map(move |step| (start + step) % count)
    }

    /// The place the next turn starts at, or `None` where there are no
    /// places.
    #[must_use]
    pub fn next(&self, count: usize) -> Option<usize> {
        self.turn(count).next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn each_turn_asks_every_place_once_starting_one_further() {
        let rotation = Rotation::new();
        assert_eq!(rotation.turn(3).collect::<Vec<_>>(), [0, 1, 2]);
        assert_eq!(rotation.turn(3).collect::<Vec<_>>(), [1, 2, 0]);
        assert_eq!(rotation.turn(3).collect::<Vec<_>>(), [2, 0, 1]);
        assert_eq!(rotation.turn(3).collect::<Vec<_>>(), [0, 1, 2]);
    }

    #[test]
    fn no_places_is_an_empty_turn_and_none_next() {
        let rotation = Rotation::new();
        assert_eq!(rotation.turn(0).count(), 0);
        assert_eq!(rotation.next(0), None);
        assert_eq!(rotation.next(3), Some(2), "both empty turns were taken");
        assert_eq!(rotation.next(3), Some(0));
    }

    #[test]
    fn a_rotation_wrapping_round_still_asks_every_place_once() {
        let rotation = Rotation {
            next: AtomicUsize::new(usize::MAX),
        };
        let mut turn: Vec<usize> = rotation.turn(3).collect();
        turn.sort_unstable();
        assert_eq!(turn, [0, 1, 2]);
        let mut after: Vec<usize> = rotation.turn(3).collect();
        after.sort_unstable();
        assert_eq!(after, [0, 1, 2]);
    }

    #[test]
    fn callers_sharing_a_rotation_spread_over_the_places() {
        let rotation = Arc::new(Rotation::new());
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let rotation = Arc::clone(&rotation);
                std::thread::spawn(move || {
                    (0..300)
                        .map(|_| rotation.next(3).expect("three places"))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut counts = [0_usize; 3];
        for thread in threads {
            for place in thread.join().expect("a thread") {
                counts[place] += 1;
            }
        }
        assert_eq!(counts, [400, 400, 400]);
    }
}
