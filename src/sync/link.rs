//! The Event link between two nodes: what it says, frame by frame, and
//! what each side's hub does with it — the Event conversation, compiled
//! with the feature `delivery` because the hub keeps its taps and carriers
//! by it (ADR-0065, amendment 2026-11-13). Nothing here opens a connection:
//! the link itself — the sync listener, the connection to each member, the
//! following of who they are, Xmip's mutual TLS (ADR-0063) — is the sync
//! plane's (ADR-0067, `plane.rs`, behind the feature `sync`), which carries
//! these frames and decides nothing about them.
//! The protocol is agreed in the TLS handshake as [`ALPN`].
//!
//! Two sides, each the hub's. The **answering** side is a [`Tap`], one per
//! node listening here: it holds what the asking node's subscriptions
//! want, queues this node's own Events that match, and says what to write
//! next ([`Tap::next`]). The **asking** side is the hub's links to the
//! other members (`carriers.rs`): what to ask ([`Hub::asked`]), how far
//! each link carries the subscriptions, and what an answer means
//! ([`Hub::answered`]). A member that cannot be heard is said to be
//! (`unheard.rs`), never silently missing.
//!
//! The node a subscriber is on asks, and the node it hears answers:
//!
//! - **Wants** (asking): every filter its subscriptions hold, each by the
//!   subscription's number there, and the generation of the subscriptions
//!   it was read at. It replaces what was asked before; nothing is the
//!   empty list.
//! - **Wanted** (answering): the generation now carried — from here on
//!   what those filters match crosses.
//! - **Event** (answering): one Event raised on the answering node, in the
//!   Event wire form's JSON format — the form every other wire carries.
//! - **Missed** (answering): how many matching Events the link's queue
//!   refused for each subscription, so no Event is missing silently.
//! - **Beat** (answering): nothing to carry for a while, and alive.
//!
//! Each is one frame (`net::frame`): its kind in the first byte, then
//! JSON. Nothing here opens a connection.

pub(crate) mod carriers;
pub(crate) mod tap;
mod unheard;

pub use tap::Tap;

use std::io::{Read, Write};
use std::sync::Arc;

use serde_json::{Value, json};

#[cfg(doc)]
use crate::delivery::hub::Hub;
use xevent::filter::Filter;
use xevent::wire::WireEvent;
use xevent::{Event, EventError};

/// The application protocol two nodes agree in the TLS handshake: the
/// Event link, its first version.
pub const ALPN: &[u8] = b"xmip-event/1";

/// One frame of the link.
#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    /// Asking: the generation of the subscriptions, and the number and
    /// filter of each.
    Wants(u64, Vec<(u64, Filter)>),
    /// Answering: the generation now carried.
    Wanted(u64),
    /// Answering: one Event raised on the answering node.
    Event(Arc<Event>),
    /// Answering: how many matching Events the queue refused, by
    /// subscription.
    Missed(Vec<(u64, u64)>),
    /// Answering: nothing to carry for a while, and alive.
    Beat,
}

const WANTS: u8 = 1;
const WANTED: u8 = 2;
const EVENT: u8 = 3;
const MISSED: u8 = 4;
const BEAT_KIND: u8 = 5;

impl Frame {
    /// Appended to `out` as one frame.
    ///
    /// # Errors
    /// An Event the wire form refuses, or a frame over the ceiling.
    pub fn append(&self, out: &mut Vec<u8>) -> Result<(), EventError> {
        let (kind, body) = match self {
            Self::Wants(generation, wants) => {
                let wants: Vec<Value> = wants
                    .iter()
                    .map(|(id, filter)| json!({ "id": id, "filter": filter.json() }))
                    .collect();
                (WANTS, json!({ "generation": generation, "wants": wants }))
            }
            Self::Wanted(generation) => (WANTED, json!(generation)),
            Self::Event(event) => (EVENT, WireEvent::from_event(event).json()?),
            Self::Missed(missed) => (MISSED, json!(missed)),
            Self::Beat => (BEAT_KIND, Value::Null),
        };
        let mut bytes = vec![kind];
        if !body.is_null() {
            bytes.extend_from_slice(body.to_string().as_bytes());
        }
        net::frame::append(out, &bytes).map_err(|failed| EventError::new(failed.message))
    }

    /// The frame `bytes` hold.
    ///
    /// # Errors
    /// No kind this link knows, or a body that is not the kind's.
    pub fn read(bytes: &[u8]) -> Result<Self, EventError> {
        let Some((&kind, body)) = bytes.split_first() else {
            return Err(EventError::new("an empty frame on the Event link"));
        };
        if kind == BEAT_KIND {
            return Ok(Self::Beat);
        }
        let value: Value = serde_json::from_slice(body)
            .map_err(|error| EventError::new(format!("an Event link frame: {error}")))?;
        let refused = || {
            EventError::new(format!(
                "an Event link frame of kind {kind} that is not one"
            ))
        };
        match kind {
            WANTS => {
                let generation = value["generation"].as_u64().ok_or_else(refused)?;
                let wants = value["wants"]
                    .as_array()
                    .ok_or_else(refused)?
                    .iter()
                    .map(|want| {
                        let id = want["id"].as_u64().ok_or_else(refused)?;
                        Ok((id, Filter::read_json(&want["filter"])?))
                    })
                    .collect::<Result<_, EventError>>()?;
                Ok(Self::Wants(generation, wants))
            }
            WANTED => value.as_u64().map(Self::Wanted).ok_or_else(refused),
            EVENT => Ok(Self::Event(Arc::new(
                WireEvent::read_json(&value)?.event()?,
            ))),
            MISSED => serde_json::from_value(value)
                .map(Self::Missed)
                .map_err(|_| refused()),
            _ => Err(EventError::new(format!(
                "an Event link frame of kind {kind}"
            ))),
        }
    }
}

/// The next frame off `reading`, `None` where the peer closed between
/// frames.
///
/// # Errors
/// The connection failed, or the frame is not one, in words.
pub fn receive(reading: &mut impl Read) -> Result<Option<Frame>, String> {
    match net::frame::read(reading) {
        Ok(Some(bytes)) => Frame::read(&bytes)
            .map(Some)
            .map_err(|error| error.to_string()),
        Ok(None) => Ok(None),
        Err(failed) => Err(failed.message),
    }
}

/// `frames` written to `out` in one write, and flushed.
///
/// # Errors
/// A frame the wire form refuses, or the write failed, in words.
pub fn send(out: &mut impl Write, frames: &[Frame]) -> Result<(), String> {
    let mut bytes = Vec::new();
    for frame in frames {
        frame
            .append(&mut bytes)
            .map_err(|error| error.to_string())?;
    }
    out.write_all(&bytes)
        .and_then(|()| out.flush())
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use node::Stage;
    use xcore::PartyId;

    use super::*;
    use xevent::outcome::Outcome;

    fn crossed(frame: &Frame) -> Frame {
        let mut out = Vec::new();
        frame.append(&mut out).expect("written");
        let bytes = net::frame::read(&mut out.as_slice())
            .expect("read")
            .expect("one");
        Frame::read(&bytes).expect("a frame")
    }

    #[test]
    fn every_frame_crosses_as_it_was() {
        let cluster = configure::fixture::test_cluster();
        let scope = format!("{}/send/x", cluster.node_scope(0));
        let event = Event::completed(Stage::Send, Outcome::Failure, scope)
            .about(PartyId::new(7))
            .saying("status", "503");
        let wants = vec![
            (1, Filter::everything()),
            (
                4,
                Filter::everything()
                    .ending(Outcome::Failure)
                    .beneath(cluster.scope()),
            ),
        ];

        for frame in [
            Frame::Wants(9, wants),
            Frame::Wants(10, Vec::new()),
            Frame::Wanted(9),
            Frame::Event(Arc::new(event)),
            Frame::Missed(vec![(1, 3), (4, 1)]),
            Frame::Beat,
        ] {
            assert_eq!(crossed(&frame), frame);
        }
    }

    #[test]
    fn what_is_not_a_frame_is_refused_in_words() {
        assert!(Frame::read(&[]).is_err());
        assert!(Frame::read(&[9, b'1']).is_err());
        assert!(Frame::read(&[WANTED, b'"', b'x', b'"']).is_err());
    }
}
