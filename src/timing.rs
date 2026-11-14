//! How the sync plane keeps time: every bound its links run by, each a
//! `[tuning]` key a node reads from its cluster's file, per cluster and per
//! node (ADR-0031, amendment 2026-10-03: *All these outwards, hardware
//! assumptions and calculations should be configurable per cluster and
//! node*; ADR-0065, amendment 2026-11-13). The constants here are the
//! defaults and never the only value: a node whose configuration says
//! nothing runs by them.
//!
//! The pure part keeps them, so a node reads its `[tuning]` without the
//! network; the sync plane that runs by them is behind the feature `sync`.

use std::time::Duration;

/// How often an idle link says it is alive: `[tuning] sync_beat`.
pub const BEAT: Duration = Duration::from_secs(1);

/// How long a link may say nothing before it is taken for broken:
/// `[tuning] sync_silence`.
pub const SILENCE: Duration = Duration::from_secs(3);

/// The most a TLS handshake waits on the peer: `[tuning] sync_handshake`.
pub const HANDSHAKE: Duration = Duration::from_secs(5);

/// How often a read waiting on the socket looks whether its side is
/// ending: `[tuning] sync_watch`. An arriving byte wakes it at once; this
/// bounds only how long an end waits for an idle connection.
pub const WATCH: Duration = Duration::from_millis(50);

/// The most a connect to a member waits: `[tuning] sync_connect`.
pub const CONNECT: Duration = Duration::from_secs(2);

/// The most a write waits on a node that does not read: `[tuning]
/// sync_write`.
pub const WRITE: Duration = Duration::from_secs(10);

/// The first wait before a member that could not be reached is tried
/// again, doubled each time it is not: `[tuning] sync_retry`.
pub const RETRY: Duration = Duration::from_millis(50);

/// The longest wait before a member that could not be reached is tried
/// again: `[tuning] sync_retry_most`.
pub const RETRY_MOST: Duration = Duration::from_secs(1);

/// How often the members are read again when nothing says they changed:
/// `[tuning] sync_follow`. A node told they changed reads them at once.
pub const FOLLOW: Duration = Duration::from_secs(60);

/// Every bound the sync plane runs by.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Timing {
    pub beat: Duration,
    pub silence: Duration,
    pub handshake: Duration,
    pub watch: Duration,
    pub connect: Duration,
    pub write: Duration,
    pub retry: Duration,
    pub retry_most: Duration,
    pub follow: Duration,
}

impl Default for Timing {
    /// Every default: what a node whose `[tuning]` says nothing of the sync
    /// plane runs by.
    fn default() -> Self {
        Self {
            beat: BEAT,
            silence: SILENCE,
            handshake: HANDSHAKE,
            watch: WATCH,
            connect: CONNECT,
            write: WRITE,
            retry: RETRY,
            retry_most: RETRY_MOST,
            follow: FOLLOW,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_are_the_constants() {
        let timing = Timing::default();
        assert_eq!(
            [timing.beat, timing.silence, timing.handshake, timing.watch],
            [BEAT, SILENCE, HANDSHAKE, WATCH]
        );
        assert_eq!(
            [
                timing.connect,
                timing.write,
                timing.retry,
                timing.retry_most
            ],
            [CONNECT, WRITE, RETRY, RETRY_MOST]
        );
        assert_eq!(timing.follow, FOLLOW);
        assert!(
            timing.beat < timing.silence,
            "a beat comes before silence is taken"
        );
        assert!(timing.retry <= timing.retry_most);
    }
}
