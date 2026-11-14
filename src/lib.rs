//! The Cluster: the nodes of one Xmip installation, who its members are
//! now, which of them takes a piece of work, and the one rotation every
//! round robin of the cluster takes.
//!
//! What a node declares and what work requires of it are `xmip-core-node`'s
//! (`Capability`, `Requirement`; ADR-0056). This crate holds what is said
//! of the nodes together: the members and where the cluster keeps them
//! ([`Membership`]), the placement that matches a requirement against the
//! members' capabilities and refuses where none satisfies it ([`place`]),
//! and the [`Rotation`] both Xmip Storage's round robin and placement take
//! their turns from, and the [`Timing`] the sync plane keeps. None of it
//! opens a socket, starts a thread or reads a clock, so
//! `xmip-core-configure` and `xmip-core-persist` take it without the
//! network, with `default-features = false` (ADR-0058, amendment
//! 2026-11-13).
//!
//! Delivering Events is this crate's too ([`delivery`], the feature
//! `delivery`, on by default): the hub every subscriber is admitted to and
//! every Event is published through, the subscriber and its gate, an
//! operator's acts and the forwarder that carries a subscription over the
//! wire (ADR-0065, amendment 2026-11-13). It opens no socket. The Event,
//! its filter and its wire form are `xmip-core-event`'s.
//!
//! The running part is the sync plane ([`sync`], the default feature
//! `sync`, which includes `delivery`): the sync listener, a link to each
//! member and the following of who they are, carrying the cluster's Events
//! (ADR-0065 and ADR-0067). What crosses a link, `sync::link`, is compiled
//! with `delivery`, since the hub keeps its taps and carriers by it.

#![forbid(unsafe_code)]

#[cfg(feature = "delivery")]
pub mod delivery;
pub mod member;
pub mod placement;
pub mod rotation;
#[cfg(feature = "delivery")]
pub mod sync;
pub mod timing;

pub use member::{Member, Membership};
pub use placement::{Candidate, Unplaced, every, place};
pub use rotation::Rotation;
pub use timing::Timing;

xcore::declare_error!(
    /// What the cluster could not do: its members could not be read, or
    /// a node could not join the sync plane.
    ClusterError
);
