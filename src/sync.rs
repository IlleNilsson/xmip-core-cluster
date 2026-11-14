//! The sync plane (ADR-0067): a node's sync listener answering the other
//! members, a link to each of them, and the following of who they are —
//! the running part of the cluster, behind the feature `sync` (ADR-0058,
//! amendment 2026-11-13). It carries the cluster's Events, so any node is
//! the cluster's door (ADR-0065, amendments 2026-09-26 and 2026-10-02): a
//! subscriber on whichever node it reaches receives the matching Events of
//! every node in the cluster, exactly as it would from any other.
//!
//! **What crosses is the Event conversation** ([`link`]): the frames, what
//! each side's hub does with them, who is unheard and why. It is compiled
//! with the feature `delivery`, which this one includes, because the hub
//! keeps its taps and carriers by it, and it opens nothing. The plane
//! ([`Plane`], `plane.rs`) makes and holds the connections, presents and
//! verifies the node's certificate, and keeps time by
//! [`Timing`](crate::Timing), every bound a `[tuning]` key.
//!
//! **One hop, pushed-down filters, no loops.** A node holds one link to
//! each other member of the cluster (`upstream.rs`) over Xmip's mutual TLS
//! (ADR-0063), on the member's sync listener (`serving.rs`), and asks it
//! for what its subscriptions' filters match. The member answers from its
//! own Events only ([`link::Tap`]): what it heard from a third node
//! never crosses again, so every Event crosses at most one hop, comes once,
//! and never returns. What no subscriber's filter wants never leaves the
//! node that raised it. An Event that crosses is pushed the moment it is
//! raised — no node looks for Events, nothing waits a round to send.
//!
//! **Authorized where it subscribed.** A Party is authorized by the node it
//! connected to, at subscribe, as every subscriber is; a link presents the
//! node's own certificate and the member authorizes a node of the cluster,
//! never a Party.
//!
//! **Followed, retried, said.** Which nodes are members, and where their
//! sync listeners answer, is the cluster's record
//! ([`Membership`](crate::Membership)); a
//! member joining gets a link, a member leaving or moving loses it, and one
//! naming no sync listener is unheard. A member that is down is tried again
//! on its own link's thread, so no other member waits for it, and until it
//! is heard again it is unheard — answered by the hub and by each
//! subscription, and recorded in the node's audit as `event.link` — so no
//! Event is missing silently.
//!
//! **Internal.** A link is the cluster's, not a Party's: it is not an Event
//! subscription, is not listed with them and is not paused, resumed or
//! removed by an operator, who would otherwise silence a node for every
//! subscriber at once.

pub mod link;
#[cfg(feature = "sync")]
mod plane;
#[cfg(feature = "sync")]
mod serving;
#[cfg(feature = "sync")]
mod upstream;

#[cfg(feature = "sync")]
pub use plane::{Joining, Plane};
