//! The in-process hub: a subscription receives what its filter matches and
//! nothing else, a refused subscriber receives nothing, a full queue never
//! holds the publisher, every delivery and refusal is audited, and a
//! listener dropped in its own callback is closed at the drop. That a
//! publish costs microseconds is measured in the module's timing program
//! (`tests/measured/hub.rs`), because anything over a millisecond apart
//! from load is a defect (CONTRIBUTING.md).

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use audit::keeper::settle;
use audit::program_audit::ProgramAudit;
use audit_file::FileSink;
use audit_file::temporary::Folder;
use authorize_party::PartyPolicy;
use node::Stage;
use xcore::{ExecutionPhase, PartyId, Severity};
use xevent::Event;
use xevent::filter::Filter;
use xevent::outcome::Outcome;
use xmip_core_cluster::delivery::hub::Hub;
use xmip_core_cluster::delivery::listener::Listener;
use xmip_core_cluster::delivery::subscriber::{Subscriber, fixture};

/// A directory of this test's own for the audit it writes.
fn directory(name: &str) -> Folder {
    Folder::new(&format!("cluster-hub-{name}"))
}

/// The program the tests subscribe as, auditing in `at`.
fn program(at: &Path) -> ProgramAudit {
    ProgramAudit::new("xmip-core-event tests", Some(FileSink::new(at)))
}

fn subscriber(audit: ProgramAudit) -> Subscriber {
    fixture::subscriber(&fixture::party(PartyId::new(42)), audit)
}

fn audited(at: &Path) -> String {
    settle();
    fs::read_to_string(at.join(audit::audit_store::FILE_NAME)).unwrap_or_default()
}

fn hub() -> Hub {
    Hub::new(vec![Arc::new(PartyPolicy::new().allow(PartyId::new(42)))])
}

fn received(outcome: Outcome) -> Event {
    let node = configure::fixture::test_cluster().node_scope(0);
    Event::completed(Stage::Receive, outcome, format!("{node}/receive/orders"))
}

/// The test cluster's scope.
fn cluster() -> String {
    configure::fixture::test_cluster().scope()
}

#[test]
fn a_subscription_receives_what_it_matches_and_the_delivery_is_audited() {
    let at = directory("match");
    let hub = hub();
    let failures = Filter::everything()
        .ending(Outcome::Failure)
        .beneath(cluster());
    let subscription = hub
        .subscribe(subscriber(program(&at)), failures, 0)
        .expect("allowed");

    assert_eq!(hub.publish(received(Outcome::Success)), 0, "not a failure");
    let failed = received(Outcome::Failure);
    assert_eq!(hub.publish(failed.clone()), 1);

    let delivery = subscription.next(Duration::from_secs(1), 16);
    assert_eq!(delivery.events.len(), 1);
    assert_eq!(*delivery.events[0], failed);
    let audit = audited(&at);
    assert!(audit.contains("action = \"event.subscribe\""), "{audit}");
    assert!(audit.contains("action = \"event.deliver\""), "{audit}");
    assert!(audit.contains(&failed.id.to_string()), "{audit}");

    drop(subscription);
    assert_eq!(hub.subscriptions(), 0, "dropping unsubscribes");
    assert!(audited(&at).contains("action = \"event.unsubscribe\""));
}

#[test]
fn a_subscriber_the_gate_refuses_gets_no_subscription_and_the_refusal_is_audited() {
    let at = directory("refused");
    let closed = Hub::new(Vec::new());

    let refused = closed.subscribe(subscriber(program(&at)), Filter::everything(), 0);

    let said = refused.err().map_or_default(|error| error.to_string());
    assert!(said.contains("no policy is configured"), "{said}");
    assert_eq!(closed.subscriptions(), 0);
    let audit = audited(&at);
    let warning = format!("severity = \"{}\"", Severity::Warning.word());
    assert!(audit.contains(&warning), "{audit}");
    assert!(audit.contains("no policy is configured"), "{audit}");
}

#[test]
fn a_full_queue_refuses_counts_and_never_holds_the_publisher() {
    let at = directory("full");
    let hub = hub();
    let subscription = hub
        .subscribe(subscriber(program(&at)), Filter::everything(), 2)
        .expect("allowed");

    let taken: usize = (0..5)
        .map(|_| hub.publish(received(Outcome::Success)))
        .sum();

    assert_eq!(taken, 2, "the queue holds two");
    let delivery = subscription.next(Duration::ZERO, 16);
    assert_eq!(delivery.events.len(), 2);
    assert_eq!(delivery.refused, 3);
    assert!(audited(&at).contains("3 matching Events refused"));
}

#[test]
fn nothing_published_is_an_empty_delivery_after_the_timeout() {
    let at = directory("empty");
    let hub = hub();
    let subscription = hub
        .subscribe(subscriber(program(&at)), Filter::everything(), 0)
        .expect("allowed");

    let started = Instant::now();
    let delivery = subscription.next(Duration::from_millis(20), 16);

    assert_eq!(delivery.events, [] as [Arc<Event>; 0]);
    assert!(started.elapsed() >= Duration::from_millis(20));
}

#[test]
fn a_listener_is_called_back_on_its_own_thread_until_dropped() {
    let at = directory("listen");
    let hub = hub();
    let subscription = hub
        .subscribe(subscriber(program(&at)), Filter::everything(), 0)
        .expect("allowed");
    let (tell, told) = mpsc::channel();
    let publisher = std::thread::current().id();

    let listener = subscription
        .listen(move |event| {
            let _ = tell.send((event.id, std::thread::current().id()));
        })
        .expect("a thread");
    let event = received(Outcome::Success);
    let _ = hub.publish(event.clone());

    let (id, thread) = told
        .recv_timeout(Duration::from_secs(2))
        .expect("called back");
    assert_eq!(id, event.id);
    assert_ne!(thread, publisher, "never on the publisher's thread");
    drop(listener);
    assert_eq!(hub.subscriptions(), 0, "dropping the listener unsubscribes");
}

/// A listener the test holds until its own callback takes it and drops it.
type Held = Arc<Mutex<Option<Listener>>>;

/// The actions `kept` holds, in the order they were kept.
fn actions(kept: &str) -> Vec<&str> {
    kept.lines()
        .filter_map(|line| line.strip_prefix("action = "))
        .map(|action| action.trim_matches('"'))
        .collect()
}

#[test]
fn a_listener_dropped_in_its_own_callback_is_closed_before_the_program_stops() {
    // A thread cannot join itself, so the drop hands the close record over
    // there and then: a program recording its stop before the callback
    // returns keeps the close record first, and an exit after the stop
    // loses nothing (ADR-0065, amendment 2026-11-20).
    let at = directory("listen-own");
    let hub = hub();
    let audit = program(&at);
    let subscription = hub
        .subscribe(subscriber(audit.clone()), Filter::everything(), 0)
        .expect("allowed");
    let held = Held::default();
    let (dropped, was_dropped) = mpsc::channel();
    let (stopped, was_stopped) = mpsc::channel();

    let taken = Arc::clone(&held);
    let listener = subscription
        .listen(move |_| {
            drop(taken.lock().expect("held").take());
            let _ = dropped.send(());
            // The program records its stop before this callback returns.
            let _ = was_stopped.recv_timeout(Duration::from_secs(5));
        })
        .expect("a thread");
    *held.lock().expect("held") = Some(listener);
    let _ = hub.publish(received(Outcome::Success));

    was_dropped
        .recv_timeout(Duration::from_secs(5))
        .expect("dropped in its callback");
    audit
        .record(
            "stop",
            ExecutionPhase::Finished,
            Severity::Information,
            Some("stopped"),
            BTreeMap::new(),
        )
        .expect("recorded");
    stopped.send(()).expect("the callback waits for the stop");
    // The callback's sender goes with the listener's thread.
    assert_eq!(
        was_dropped.recv_timeout(Duration::from_secs(5)),
        Err(RecvTimeoutError::Disconnected),
        "the listener's thread ended"
    );

    let kept = audited(&at);
    let order = actions(&kept);
    let closed = order
        .iter()
        .position(|action| *action == "event.unsubscribe");
    let stop = order.iter().position(|action| *action == "stop");
    assert!(
        closed.zip(stop).is_some_and(|(closed, stop)| closed < stop),
        "the close record is kept before the stop record; kept: {order:?}"
    );
}

#[test]
fn a_listener_dropped_in_its_own_callback_is_called_back_no_more() {
    // Both are queued before the listener's first drain, which hands them
    // over together; the callback dropping its listener on the first is
    // not called for the second (`xmip_operate.h` section 11: the thread
    // ends after the callback does).
    let at = directory("listen-no-more");
    let hub = hub();
    let subscription = hub
        .subscribe(subscriber(program(&at)), Filter::everything(), 0)
        .expect("allowed");
    let first = received(Outcome::Success);
    let _ = hub.publish(first.clone());
    let _ = hub.publish(received(Outcome::Success));
    let held = Held::default();
    let (tell, told) = mpsc::channel();

    let taken = Arc::clone(&held);
    // Held while the listener is made, so its first callback finds it.
    let mut holding = held.lock().expect("held");
    let listener = subscription
        .listen(move |event| {
            drop(taken.lock().expect("held").take());
            let _ = tell.send(event.id);
        })
        .expect("a thread");
    *holding = Some(listener);
    drop(holding);

    let called = told
        .recv_timeout(Duration::from_secs(5))
        .expect("called back");
    assert_eq!(called, first.id);
    // The callback's sender goes with the listener's thread.
    assert_eq!(
        told.recv_timeout(Duration::from_secs(5)),
        Err(RecvTimeoutError::Disconnected),
        "called back once, and the listener's thread ended"
    );
}
