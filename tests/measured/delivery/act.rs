//! An operator's acts on a subscription (ADR-0065, amendment 2026-09-29),
//! measured: a resume wakes a drain waiting on the subscription at once,
//! the fastest of five over its rounds (`xcore::latency::spread`). Until
//! 2026-11-14 one resume was timed once.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use audit::program_audit::ProgramAudit;
use authorize_party::PartyPolicy;
use node::Stage;
use observe::Act;
use xcore::PartyId;
use xcore::latency::spread;
use xevent::Event;
use xevent::filter::Filter;
use xevent::outcome::Outcome;
use xmip_core_cluster::delivery::hub::Hub;
use xmip_core_cluster::delivery::subscriber::{Subscriber, fixture};

fn directory(name: &str) -> PathBuf {
    let at = std::env::temp_dir().join(format!("xmip-event-act-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&at);
    at
}

fn subscriber(at: &Path) -> Subscriber {
    fixture::subscriber(
        &fixture::party(PartyId::new(42)),
        ProgramAudit::new("xmip-core-event tests", Some(at)),
    )
}

#[test]
fn a_resume_wakes_a_waiting_drain_within_a_millisecond() {
    let at = directory("resume-measured");
    let hub = Arc::new(Hub::new(vec![Arc::new(
        PartyPolicy::new().allow(PartyId::new(42)),
    )]));
    let subscription = hub
        .subscribe(subscriber(&at), Filter::everything(), 0)
        .expect("allowed");
    let id = subscription.id();
    let node = configure::fixture::test_cluster().node_scope(0);

    // One drain, kept across the rounds: each round it is asked to wait on
    // the subscription, and says when it woke and how many Events it took.
    let (ask, asked) = mpsc::channel::<()>();
    let (tell, told) = mpsc::channel();
    let waiting = std::thread::spawn(move || {
        for () in asked {
            let delivery = subscription.next(Duration::from_secs(5), 16);
            tell.send((Instant::now(), delivery.events.len()))
                .expect("told");
        }
        subscription
    });
    let woken = (0..ROUNDS)
        .map(|_| {
            hub.act(id, Act::Pause, "ilian").expect("paused");
            let _ = hub.publish(Event::completed(
                Stage::Receive,
                Outcome::Success,
                format!("{node}/receive/a"),
            ));
            ask.send(()).expect("the drain waits");
            std::thread::sleep(Duration::from_millis(20));
            let resumed_at = Instant::now();
            hub.act(id, Act::Resume, "ilian").expect("resumed");
            let (woke, taken) = told.recv_timeout(Duration::from_secs(5)).expect("woke");
            assert_eq!(taken, 1);
            woke.duration_since(resumed_at)
        })
        .collect();
    drop(ask);
    drop(waiting.join().expect("joined"));
    let woken = spread("a resume to the drain waiting on it woken", woken);
    assert!(
        woken.fastest_of_five < Duration::from_millis(1),
        "resume wakes the drain at once: {woken:?}"
    );
    let _ = std::fs::remove_dir_all(&at);
}

/// How many resumes are timed.
const ROUNDS: usize = 50;
