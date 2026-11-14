//! Delivery in this process, held to the millisecond rule:
//!
//! - `latency.rs`: an Event reaches a waiting subscriber and a listener
//!   within about a millisecond of being published;
//! - `hub.rs`: a publish costs microseconds;
//! - `act.rs`: a resume wakes a waiting drain at once.

mod act;
mod hub;
mod latency;
