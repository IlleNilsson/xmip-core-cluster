//! An operator's acts on a subscription (ADR-0065, amendment 2026-09-29),
//! measured: a resume wakes a drain waiting on the subscription at once,
//! round by round beside a plain wake, the fastest of five
//! (`xcore::latency::Rounds`; a resume wakes one thread, the drain, as a
//! plain wake does). Until 2026-11-14 one resume was timed once, and until
//! 2026-11-15 the fastest of five was held flat.

use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use audit::program_audit::ProgramAudit;
use audit_file::FileSink;
use audit_file::temporary::Folder;
use authorize_party::PartyPolicy;
use node::Stage;
use observe::Act;
use xcore::PartyId;
use xcore::latency::Rounds;
use xcore::processor_time::ProcessorTime;
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

#[test]
fn a_resume_wakes_a_waiting_drain_within_a_millisecond() {
    let at = Folder::new("cluster-act-resume-measured");
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
    let mut rounds = Rounds::crossing(1);
    for _ in 0..ROUNDS {
        hub.act(id, Act::Pause, "ilian").expect("paused");
        let _ = hub.publish(Event::completed(
            Stage::Receive,
            Outcome::Success,
            format!("{node}/receive/a"),
        ));
        ask.send(()).expect("the drain waits");
        std::thread::sleep(Duration::from_millis(20));
        // The round: the resume to the drain woken, and this process's
        // processor time to its word back, as a plain wake's round counts.
        let from = ProcessorTime::process();
        let resumed_at = Instant::now();
        hub.act(id, Act::Resume, "ilian").expect("resumed");
        let (woke, taken) = told.recv_timeout(Duration::from_secs(5)).expect("woke");
        rounds.took.push(woke.duration_since(resumed_at));
        rounds.worked.push(from.elapsed());
        assert_eq!(taken, 1);
        rounds.beside();
    }
    drop(ask);
    drop(waiting.join().expect("joined"));
    rounds.hold("a resume to the drain waiting on it woken");
}

/// How many resumes are timed.
const ROUNDS: usize = 50;
