//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): Events delivered and the sync plane held to the millisecond rule
//! (`CONTRIBUTING.md`, *Rust is here for one reason*). A plain `cargo test`
//! leaves it out; the landing runs it after every other test of the
//! module, alone, one test at a time
//! (`cargo test --test measured --all-features -- --test-threads=1`), so
//! what is measured is Xmip's work and not its neighbors':
//!
//! - `delivery`: an Event reaches an in-process subscriber within about a
//!   millisecond, a publish costs microseconds, and a resume wakes a
//!   waiting drain at once;
//! - `latency.rs`: an Event crossing to another node adds well under a
//!   millisecond, over the test cluster the sync tests stand up
//!   (`tests/sync/test_cluster.rs`, shared, not copied).

#[path = "../sync/test_cluster.rs"]
mod test_cluster;

mod delivery;
mod latency;
