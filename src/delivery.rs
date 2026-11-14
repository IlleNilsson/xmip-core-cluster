//! Events delivered to the subscribers that asked for them (runtime-model
//! section 17, ADR-0065), behind the feature `delivery`.
//!
//! An Event, its taxonomy, the one subscription rule
//! (`xevent::filter::Filter::matches`) and the wire form are
//! `xmip-core-event`'s; delivering one is a node's work, started, held and
//! stopped with it (ADR-0058, amendment 2026-11-13). A subscriber is a
//! Party ([`subscriber::Subscriber`]) whom the authorization gate
//! ([`gate`]) admits or refuses; the in-process [`hub::Hub`] fans an Event
//! out through a bounded queue per subscription and never waits for one;
//! a [`listener::Listener`] calls back instead of being drained. Over the
//! wire a subscription is carried at least once by a
//! [`forward::Forwarder`], through a transport's `xevent::wire::Wire`.
//! Every subscription, delivery and refusal is audited (ADR-0062;
//! [`audit_trail`]).
//!
//! The C boundary is `xmip_operate.h` section 11, forwarded by the
//! runtime's library to [`hub::Hub::process`]; every language binds that.
//!
//! Any node is the cluster's door: a subscriber on any node hears the
//! matching Events of all of them, each crossing one hop, once, only where
//! a filter wants it (ADR-0065, amendment 2026-10-02). What crosses the
//! Event link between two nodes, and what each side's hub does with it, is
//! `sync::link`'s; the link itself is the sync plane's.
//!
//! An operator lists what a hub holds ([`hub::Hub::standing`]) and pauses,
//! resumes or removes an Event subscription ([`hub::Hub::act`], by
//! `observe::Act`; ADR-0065, amendment 2026-09-29); a surface that reads a
//! node's publication only never acts on it (ADR-0014, amendment
//! 2026-11-13).

pub mod act;
pub mod audit_trail;
pub mod forward;
pub mod gate;
pub mod hub;
pub mod listener;
pub(crate) mod queue;
pub mod subscriber;
