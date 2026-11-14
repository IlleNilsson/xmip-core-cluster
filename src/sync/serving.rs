//! The answering side: a node's sync listener (ADR-0067), where every
//! other member of the cluster that has subscribers opens one link and
//! hears this node's own Events that its subscriptions' filters match.
//!
//! Every connection is mutual TLS through Xmip's own TLS (ADR-0063): the
//! peer presents a certificate reaching the cluster's anchors, so it is a
//! node of the cluster, and it is authorized as one; the Parties behind it
//! were authorized where they subscribed. The protocol is agreed in the
//! handshake as [`ALPN`], so a client that speaks something else is refused
//! before it says anything. A connection is two threads, no async runtime:
//! one reads what the peer wants and hands it to the hub's tap, one writes
//! what the tap says to write as it is raised (`sync::link::Tap`).

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use xcore::lock::Locked;

use super::link::{self, ALPN, Tap};
use crate::delivery::audit_trail::noted;
use crate::delivery::hub::Hub;
use audit::program_audit::ProgramAudit;
use net::watched::Watched;
use xcore::Severity;

use crate::Timing;

/// How many Events one write carries at most.
const BATCH: usize = 256;

/// How long a stop waits on its own wake-up connection.
const WAKE: Duration = Duration::from_secs(1);

/// The threads serving connections, so a stop can wait for each.
type Served = Arc<Mutex<Vec<JoinHandle<()>>>>;

/// What a sync listener serves: the hub whose own Events it answers with,
/// what the node presents and trusts, its bounds and its audit.
pub(super) type Serves<'a> = (&'a Hub, &'a tls::Identity, &'a Timing, &'a ProgramAudit);

/// What every connection shares.
struct Serve {
    hub: Hub,
    config: Arc<tls::ServerConfig>,
    timing: Timing,
    stopping: AtomicBool,
    audit: ProgramAudit,
}

/// The sync listener, serving until it is stopped.
pub(super) struct Serving {
    address: SocketAddr,
    serve: Arc<Serve>,
    accepting: Option<JoinHandle<()>>,
    served: Served,
}

impl Serving {
    /// Serve the hub's own Events to every node connecting to `listener`
    /// with a certificate reaching the identity's anchors.
    pub(super) fn start(given: Serves<'_>, listener: TcpListener) -> Result<Self, String> {
        let (hub, identity, timing, audit) = given;
        let config = tls::alpn::selecting(identity.server().map_err(|e| e.message)?, &[ALPN]);
        let address = listener.local_addr().map_err(|e| e.to_string())?;
        let serve = Arc::new(Serve {
            hub: hub.clone(),
            config: Arc::new(config),
            timing: *timing,
            stopping: AtomicBool::new(false),
            audit: audit.clone(),
        });
        let served: Served = Arc::default();
        let accepting = {
            let (serve, served) = (Arc::clone(&serve), Arc::clone(&served));
            std::thread::Builder::new()
                .name("xmip-event-accept".to_string())
                .spawn(move || accept(&listener, &serve, &served))
                .map_err(|e| e.to_string())?
        };
        Ok(Self {
            address,
            serve,
            accepting: Some(accepting),
            served,
        })
    }

    pub(super) const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Stop: nothing accepted after, every link ended, every thread
    /// finished.
    pub(super) fn stop(&mut self) {
        if self.serve.stopping.swap(true, Ordering::SeqCst) {
            return;
        }
        // Knocked until it has heard (`net::accepting`).
        if let Some(accepting) = self.accepting.take() {
            let address = self.address;
            let _ = net::accepting::woken(accepting, WAKE, || net::accepting::knock(address, WAKE));
        }
        let served = std::mem::take(&mut *self.served.locked());
        for thread in served {
            let _ = thread.join();
        }
    }
}

impl Drop for Serving {
    fn drop(&mut self) {
        self.stop();
    }
}

fn accept(listener: &TcpListener, serve: &Arc<Serve>, served: &Served) {
    loop {
        // bounded: a sync listener waits as long as its node runs; a stop wakes it
        let accepted = listener.accept();
        if serve.stopping.load(Ordering::SeqCst) {
            return;
        }
        let Ok((connection, peer)) = accepted else {
            continue;
        };
        let serve = Arc::clone(serve);
        let Ok(thread) = std::thread::Builder::new()
            .name("xmip-event-link".to_string())
            .spawn(move || answer(connection, peer, &serve))
        else {
            continue;
        };
        let mut served = served.locked();
        served.retain(|thread| !thread.is_finished());
        served.push(thread);
    }
}

/// One link: the handshake, then what the peer wants read on this thread
/// and what it wants written on another, until either side ends.
fn answer(connection: TcpStream, peer: SocketAddr, serve: &Arc<Serve>) {
    let timing = serve.timing;
    let settled = connection
        .set_nodelay(true)
        .and_then(|()| connection.set_read_timeout(Some(timing.handshake)))
        .and_then(|()| connection.set_write_timeout(Some(timing.write)));
    let split = settled
        .map_err(|error| error.to_string())
        .and_then(|()| tls::server(connection, Arc::clone(&serve.config)).map_err(|e| e.message))
        .and_then(|guarded| tls::duplex::split(guarded).map_err(|e| e.message))
        .and_then(|split| {
            let watched = split.reading.socket().set_read_timeout(Some(timing.watch));
            watched.map(|()| split).map_err(|error| error.to_string())
        });
    let split = match split {
        Ok(split) if split.agreed.as_deref() == Some(ALPN) => split,
        Ok(_) => {
            return said(
                serve,
                peer,
                Severity::Warning,
                "refused: not the Event link",
            );
        }
        Err(reason) => {
            return said(
                serve,
                peer,
                Severity::Warning,
                &format!("refused: {reason}"),
            );
        }
    };
    said(serve, peer, Severity::Information, "a node listens");

    let tap = serve.hub.tap();
    let out = Arc::new(Mutex::new(split.writing));
    let ending = Arc::new(AtomicBool::new(false));
    let writing = {
        let (tap, out, ending) = (Arc::clone(&tap), Arc::clone(&out), Arc::clone(&ending));
        std::thread::Builder::new()
            .name("xmip-event-link-write".to_string())
            .spawn(move || {
                let reason = carry(&tap, &out, timing.beat);
                ending.store(true, Ordering::SeqCst);
                out.locked().close();
                reason
            })
    };
    let reason = match writing {
        Ok(writing) => {
            let heard = listen(serve, &tap, &out, split.reading, &ending);
            ending.store(true, Ordering::SeqCst);
            serve.hub.untap(&tap);
            let wrote = writing.join().unwrap_or_default();
            if wrote.is_empty() { heard } else { wrote }
        }
        Err(error) => error.to_string(),
    };
    serve.hub.untap(&tap);
    said(
        serve,
        peer,
        Severity::Information,
        &format!("ended: {reason}"),
    );
}

/// Read what the peer asks until it, or this side, ends, and answer it;
/// why it ended.
fn listen<W: Write>(
    serve: &Serve,
    tap: &Tap,
    out: &Mutex<W>,
    reading: impl Read,
    ending: &AtomicBool,
) -> String {
    let mut watched = Watched {
        reading,
        ending: || serve.stopping.load(Ordering::SeqCst) || ending.load(Ordering::SeqCst),
        silence: None,
        heard: Instant::now(),
    };
    loop {
        match link::receive(&mut watched) {
            Ok(Some(frame)) => {
                let answered = tap
                    .asked(frame)
                    .and_then(|answer| link::send(&mut *out.locked(), &[answer]));
                if let Err(reason) = answered {
                    return reason;
                }
            }
            Ok(None) => return "the node closed the link".to_string(),
            Err(reason) => return reason,
        }
    }
}

/// Write what the tap says to write, as it is queued; a beat at least every
/// `beat`. Empty when the tap was closed, else why the write failed.
fn carry<W: Write>(tap: &Tap, out: &Mutex<W>, beat: Duration) -> String {
    while let Some(frames) = tap.next(beat, BATCH) {
        if let Err(reason) = link::send(&mut *out.locked(), &frames) {
            return reason;
        }
    }
    String::new()
}

/// One record of the link in the node's audit.
fn said(serve: &Serve, peer: SocketAddr, severity: Severity, message: &str) {
    let properties = BTreeMap::from([("peer".to_string(), peer.to_string())]);
    noted(&serve.audit, "event.link", severity, message, properties);
}
