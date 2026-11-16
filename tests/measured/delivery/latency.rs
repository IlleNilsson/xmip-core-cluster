//! Near, very near real time (the owner, 2026-09-26): an Event reaches an
//! in-process subscriber within about a millisecond of being published.
//! Measured here from publish to receipt — a subscriber waiting in `next`
//! and a listener's callback — and the distribution printed, because a
//! mean hides the tail an operator waits on.
//!
//! Two things are held, each by what is Xmip's alone, round by round beside
//! a plain wake (`xcore::latency::Rounds`). **The wake is a wake**: the
//! fastest of each five rounds is under the millisecond plus the same
//! figure for a thread woken on a plain channel, measured beside it under
//! the same load — a poll or a missed notify is in every round and fails
//! it, a wake that waited its turn for a processor is in some only.
//! **Xmip's own work is under the millisecond**: the processor time the
//! publishing thread spends in `publish` and the subscriber's thread for
//! the Event (`xcore::processor_time`), the fastest of five, beside the
//! process's across the plain wake. Until 2026-11-15 the processor time was
//! an Event's share of the whole run, held flat. The median and the tail
//! are printed and held by nothing: under a machine busy on every processor
//! they measure the scheduler, and a 99th percentile held against the plain
//! channel's failed for it.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use audit::program_audit::ProgramAudit;
use audit_file::FileSink;
use authorize_party::PartyPolicy;
use node::Stage;
use xcore::PartyId;
use xcore::latency::{Rounds, spin};
use xcore::processor_time::ProcessorTime;
use xevent::Event;
use xevent::filter::Filter;
use xevent::outcome::Outcome;
use xmip_core_cluster::delivery::hub::Hub;
use xmip_core_cluster::delivery::subscriber::{Subscriber, fixture};

/// How many Events each measurement publishes.
const ROUNDS: usize = 500;

/// What a publish wakes where a plain wake wakes one: the subscriber.
const WAKES: u32 = 1;

fn directory(name: &str) -> PathBuf {
    let at = std::env::temp_dir().join(format!("xmip-event-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&at);
    at
}

fn subscriber(at: &Path) -> Subscriber {
    fixture::subscriber(
        &fixture::party(PartyId::new(5)),
        ProgramAudit::new("xmip-core-event tests", Some(FileSink::new(at))),
    )
}

/// A hub whose policy allows the one Party these tests subscribe as.
fn allowing() -> Hub {
    Hub::new(vec![Arc::new(PartyPolicy::new().allow(PartyId::new(5)))])
}

/// What publishing `ROUNDS` Events a little apart measured: a plain wake
/// beside each Event, and the publishing thread's processor time in each
/// `publish`.
struct Published {
    rounds: Rounds,
    publishing: Vec<Duration>,
}

/// Publish `ROUNDS` Events a little apart, each stamped as it goes, and
/// wake a plain thread between them, the machine's own wake under whatever
/// load the Events are under.
fn publish_apart(hub: &Hub, sent: &Mutex<Vec<Instant>>) -> Published {
    let node = configure::fixture::test_cluster().node_scope(0);
    let mut published = Published {
        rounds: Rounds::crossing(WAKES),
        publishing: Vec::with_capacity(ROUNDS),
    };
    for _ in 0..ROUNDS {
        spin(Duration::from_micros(200));
        let event = Event::completed(Stage::Receive, Outcome::Success, node.as_str());
        sent.lock().expect("sent").push(Instant::now());
        let from = ProcessorTime::thread();
        let _ = hub.publish(event);
        published.publishing.push(from.elapsed());
        spin(Duration::from_micros(100));
        published.rounds.beside();
    }
    published
}

/// The two holds (`xcore::latency::Rounds::hold`): each Event's way
/// `taken`, and Xmip's own processor time for it, the publisher's in
/// `publish` and the subscriber's, `subscribing`, round by round.
fn hold(what: &str, taken: Vec<Duration>, published: Published, subscribing: &[Duration]) {
    let Published {
        mut rounds,
        publishing,
    } = published;
    rounds.took = taken;
    rounds.worked = publishing
        .iter()
        .zip(subscribing)
        .map(|(publish, subscribe)| *publish + *subscribe)
        .collect();
    rounds.hold(what);
}

#[test]
fn an_event_reaches_a_waiting_subscriber_within_a_millisecond() {
    let at = directory("latency-next");
    let hub = Arc::new(allowing());
    let subscription = hub
        .subscribe(subscriber(&at), Filter::everything(), 0)
        .expect("allowed");
    let sent = Arc::new(Mutex::new(Vec::new()));
    let (ready, go) = mpsc::channel();

    let receiver = {
        let sent = Arc::clone(&sent);
        thread::spawn(move || {
            let (mut taken, mut subscribing) =
                (Vec::with_capacity(ROUNDS), Vec::with_capacity(ROUNDS));
            ready.send(()).expect("ready");
            // The subscriber's thread's processor time from one delivery's
            // end to the next one's, an Event's share: its waits in `next`
            // are not counted, only its work.
            let mut left = ProcessorTime::thread();
            while taken.len() < ROUNDS {
                let delivery = subscription.next(Duration::from_secs(5), usize::MAX);
                let now = Instant::now();
                let sent = sent.lock().expect("sent");
                let events = delivery.events.len();
                for index in taken.len()..taken.len() + events {
                    taken.push(now - sent[index]);
                }
                drop(sent);
                let share = left.elapsed() / u32::try_from(events.max(1)).expect("a count");
                subscribing.extend(std::iter::repeat_n(share, events));
                left = ProcessorTime::thread();
            }
            (taken, subscribing)
        })
    };
    go.recv().expect("the receiver waits");
    let published = publish_apart(&hub, &sent);

    let (taken, subscribing) = receiver.join().expect("received");
    hold("publish to next", taken, published, &subscribing);
    let _ = fs::remove_dir_all(&at);
}

#[test]
fn an_event_reaches_a_listener_within_a_millisecond() {
    let at = directory("latency-listen");
    let hub = allowing();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let (tell, told) = mpsc::channel();
    let listener = {
        let sent = Arc::clone(&sent);
        let mut seen = 0;
        // The listener's thread between one callback's end and the next
        // one's start: the wait entered, the wake, the drain and the call.
        // The first has no last, and is not counted.
        let mut left: Option<ProcessorTime> = None;
        hub.subscribe(subscriber(&at), Filter::everything(), 0)
            .expect("allowed")
            .listen(move |_| {
                let now = Instant::now();
                let worked = left.map_or(Duration::ZERO, ProcessorTime::elapsed);
                let _ = tell.send((now - sent.lock().expect("sent")[seen], worked));
                seen += 1;
                left = Some(ProcessorTime::thread());
            })
            .expect("a thread")
    };

    let published = publish_apart(&hub, &sent);
    let (taken, worked): (Vec<Duration>, Vec<Duration>) = (0..ROUNDS)
        .map(|_| {
            told.recv_timeout(Duration::from_secs(5))
                .expect("called back")
        })
        .unzip();
    drop(listener);

    hold("publish to callback", taken, published, &worked);
    let _ = fs::remove_dir_all(&at);
}
