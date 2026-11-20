//! The in-process hub, measured: a publish costs microseconds, to a
//! hundred matching subscriptions and to one.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use audit::program_audit::ProgramAudit;
use audit_file::FileSink;
use authorize_party::PartyPolicy;
use file::temporary::Folder;
use node::Stage;
use xcore::PartyId;
use xcore::latency::spread;
use xevent::Event;
use xevent::filter::Filter;
use xevent::outcome::Outcome;
use xmip_core_cluster::delivery::hub::Hub;
use xmip_core_cluster::delivery::subscriber::{Subscriber, fixture};

fn subscriber(at: &Path) -> Subscriber {
    fixture::subscriber(
        &fixture::party(PartyId::new(42)),
        ProgramAudit::new("xmip-core-event tests", Some(FileSink::new(at))),
    )
}

fn received(outcome: Outcome) -> Event {
    let node = configure::fixture::test_cluster().node_scope(0);
    Event::completed(Stage::Receive, outcome, format!("{node}/receive/orders"))
}

#[test]
fn a_publish_to_a_hundred_subscriptions_takes_microseconds() {
    let at = Folder::new("cluster-hub-cost");
    let hub = Hub::new(vec![Arc::new(PartyPolicy::new().allow(PartyId::new(42)))]);
    let cluster = configure::fixture::test_cluster().scope();
    let subscriptions: Vec<_> = (0..100)
        .map(|_| {
            hub.subscribe(
                subscriber(&at),
                Filter::everything().beneath(cluster.clone()),
                4096,
            )
            .expect("allowed")
        })
        .collect();
    let events: Vec<Event> = (0..1000).map(|_| received(Outcome::Success)).collect();
    let each = published(&hub, events, "a publish to 100 matching subscriptions");
    assert!(each < Duration::from_millis(1), "a publish took {each:?}");
    drop(subscriptions);

    let one = hub
        .subscribe(subscriber(&at), Filter::everything(), 4096)
        .expect("allowed");
    let events: Vec<Event> = (0..1000).map(|_| received(Outcome::Success)).collect();
    let each = published(&hub, events, "a publish to 1 matching subscription");
    assert!(each < Duration::from_millis(1), "a publish took {each:?}");
    drop(one);
}

/// `events` published one by one on `hub`, each timed, printed under
/// `what`: the fastest of five (`xcore::latency::spread`). A publish waits
/// on no wake — it offers the Event to each matching subscription's queue —
/// so no plain wake is measured beside it. Until 2026-11-15 the thousand
/// were timed together, an average.
fn published(hub: &Hub, events: Vec<Event>, what: &str) -> Duration {
    let taken = events
        .into_iter()
        .map(|event| {
            let started = Instant::now();
            let _ = hub.publish(event);
            started.elapsed()
        })
        .collect();
    spread(what, taken).fastest_of_five
}
