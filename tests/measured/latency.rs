//! What crossing to another node adds to an Event (the millisecond rule):
//! raised on the second node, received by a subscriber waiting on the
//! first — the link's queue on the second, its writing thread, Xmip's TLS
//! on loopback, the reading thread on the first, the subscriber's queue —
//! measured from publish to receipt, and the
//! same Event published to a subscriber on its own node beside it, so the
//! difference is what the hop adds. The distribution is printed, because a
//! mean hides the tail.
//!
//! Two things are held, each as the fastest round in every five
//! (`xcore::latency::Rounds`): a poll or a busy loop is in every round, a wake that
//! waited its turn for a processor in some only. **The wake is a wake**: the
//! crossing is under the millisecond plus what the machine takes to wake a
//! thread on a plain channel, three times — the hop wakes three threads
//! where the node's own delivery wakes one — measured round by round beside
//! it under the same load. **Xmip's own work is under the millisecond**:
//! this process's processor time across a crossing — both nodes, the link's
//! threads and Xmip's TLS (`xcore::processor_time`) — is under the
//! millisecond plus the process's processor time across the plain wake,
//! which counts what the nodes' own threads run beside it. The median and
//! the tail are printed and held by nothing: on a machine busy on every
//! processor they measure the scheduler.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::test_cluster::{Authority, Roster, TestNode, failures, name};
use xcore::latency::{Rounds, spread};
use xcore::processor_time::ProcessorTime;
use xevent::outcome::Outcome;

/// How many Events are measured.
const ROUNDS: usize = 500;

/// How many threads the hop wakes where the node's own delivery, and a
/// plain wake, wake one (`xcore::latency::Rounds`).
const WAKES: u32 = 3;

#[test]
fn an_event_crossing_to_another_node_adds_well_under_a_millisecond() {
    let (authority, roster) = (Authority::new(), Arc::new(Roster::default()));
    let one = TestNode::join(&name(0), &authority, &roster);
    let two = TestNode::join(&name(1), &authority, &roster);
    TestNode::follow(&[&one, &two]);
    let across = one.subscribe(1, failures());
    let here = two.subscribe(2, failures());

    let (mut rounds, mut own) = (Rounds::crossing(WAKES), Vec::new());
    for _ in 0..ROUNDS {
        thread::sleep(Duration::from_micros(200));
        let from = ProcessorTime::process();
        let sent = Instant::now();
        two.raise(Outcome::Failure);
        let delivered = here.next(Duration::from_secs(5), 1);
        own.push(sent.elapsed());
        let heard = across.next(Duration::from_secs(5), 1);
        rounds.took.push(sent.elapsed());
        rounds.worked.push(from.elapsed());
        assert_eq!((delivered.events.len(), heard.events.len()), (1, 1));
        rounds.beside();
    }

    let own = spread("raised on the second to a subscriber on itself", own);
    let crossed = rounds.hold("raised on the second to a subscriber on the first");
    println!(
        "the hop adds about {:?}, the fastest of five",
        crossed.fastest_of_five.saturating_sub(own.fastest_of_five)
    );
}
