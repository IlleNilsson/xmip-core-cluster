//! A node's part in the sync plane: its sync listener serving the other
//! members (`serving.rs`), its link to each of them (`upstream.rs`), and
//! the following of who they are.

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use xcore::lock::Locked;

use audit::program_audit::ProgramAudit;
use xcore::Severity;
use xcore::wake::Revision;

use super::link::ALPN;
use super::serving::Serving;
use super::upstream::Upstream;
use crate::delivery::audit_trail::noted;
use crate::delivery::hub::Hub;
use crate::{ClusterError, Member, Membership, Timing};

/// How a node takes part in the sync plane.
pub struct Joining {
    /// This node: `xmip:///<cluster>/node/<name>`.
    pub node: String,
    /// Its sync listener, bound.
    pub listener: TcpListener,
    /// What it presents to the other nodes, and the anchors their
    /// certificates must reach.
    pub identity: tls::Identity,
    pub membership: Arc<dyn Membership>,
    /// Every bound the plane runs by: the node's `[tuning]`.
    pub timing: Timing,
    /// Where the links are audited: the node's own audit.
    pub audit: ProgramAudit,
}

/// A node's part in the sync plane: its sync listener answering the other
/// members, and its links to them.
pub struct Plane {
    address: SocketAddr,
    serving: Serving,
    following: Arc<Follow>,
    thread: Option<JoinHandle<()>>,
}

/// What the following thread holds.
struct Follow {
    hub: Hub,
    node: String,
    membership: Arc<dyn Membership>,
    config: Arc<tls::ClientConfig>,
    timing: Timing,
    audit: ProgramAudit,
    signal: Revision,
    stopping: AtomicBool,
    upstreams: Mutex<BTreeMap<String, (Member, Upstream)>>,
}

impl Plane {
    /// `hub`'s node joins the sync plane: its own Events are served on the
    /// sync listener, and it follows the members.
    ///
    /// # Errors
    /// The identity makes no TLS configuration, or a thread did not start.
    pub fn join(hub: &Hub, joining: Joining) -> Result<Self, ClusterError> {
        let refused = |reason: String| ClusterError::new(format!("joining the cluster: {reason}"));
        let client = joining.identity.client().map_err(|e| refused(e.message))?;
        let config = Arc::new(tls::alpn::offering(client, &[ALPN]));
        let served = (hub, &joining.identity, &joining.timing, &joining.audit);
        let serving = Serving::start(served, joining.listener).map_err(refused)?;
        let following = Arc::new(Follow {
            hub: hub.clone(),
            node: joining.node,
            membership: joining.membership,
            config,
            timing: joining.timing,
            audit: joining.audit,
            signal: Revision::default(),
            stopping: AtomicBool::new(false),
            upstreams: Mutex::new(BTreeMap::new()),
        });
        let thread = {
            let following = Arc::clone(&following);
            std::thread::Builder::new()
                .name("xmip-event-cluster".to_string())
                .spawn(move || following.run())
                .map_err(|error| refused(error.to_string()))?
        };
        Ok(Self {
            address: serving.address(),
            serving,
            following,
            thread: Some(thread),
        })
    }

    /// Where its sync listener answers.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Read the members again now, on this thread: what a node calls when
    /// the cluster's record says they changed. When it returns, every
    /// member is linked or said to be unheard, and a subscribe made after
    /// waits one round trip.
    pub fn follow(&self) {
        self.following.read();
    }

    /// The members followed now, this node apart.
    #[must_use]
    pub fn members(&self) -> Vec<Member> {
        self.following
            .upstreams
            .locked()
            .values()
            .map(|(member, _)| member.clone())
            .collect()
    }

    /// Leave: every link ended, the sync listener closed, every thread
    /// finished but one still in a connect, which ends with it.
    pub fn stop(mut self) {
        self.halt();
    }

    fn halt(&mut self) {
        self.following.stopping.store(true, Ordering::SeqCst);
        self.following.signal.raise();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.serving.stop();
    }
}

impl Drop for Plane {
    fn drop(&mut self) {
        self.halt();
    }
}

impl Follow {
    fn run(&self) {
        loop {
            let seen = self.signal.now();
            if self.stopping.load(Ordering::SeqCst) {
                break;
            }
            self.read();
            self.signal.wait(seen, self.timing.follow);
        }
        let upstreams = std::mem::take(&mut *self.upstreams.locked());
        drop(upstreams);
    }

    /// Read the members and follow them.
    fn read(&self) {
        match self.membership.members() {
            Ok(members) => self.reconcile(members),
            Err(error) => self.said(
                Severity::Warning,
                &format!("the members could not be read, those known are kept: {error}"),
            ),
        }
    }

    /// A link to every other member, and none to a node that left or moved
    /// its sync listener.
    fn reconcile(&self, members: Vec<Member>) {
        let wanted: BTreeMap<String, Member> = members
            .into_iter()
            .filter(|member| member.node != self.node)
            .map(|member| (member.node.clone(), member))
            .collect();
        let mut upstreams = self.upstreams.locked();
        let gone: Vec<String> = upstreams
            .iter()
            .filter(|(node, (member, _))| {
                wanted.get(*node).map(|wanted| &wanted.address) != Some(&member.address)
            })
            .map(|(node, _)| node.clone())
            .collect();
        for node in gone {
            upstreams.remove(&node);
            self.said(Severity::Information, &format!("{node} left, or moved"));
        }
        for (node, member) in wanted {
            if upstreams.contains_key(&node) {
                continue;
            }
            let following = (&self.hub, self.node.as_str(), &self.config, &self.audit);
            match Upstream::start(following, member.clone(), self.timing) {
                Ok(upstream) => {
                    upstreams.insert(node.clone(), (member, upstream));
                    self.said(Severity::Information, &format!("{node} followed"));
                }
                Err(reason) => {
                    let _ = self.hub.not_heard(&self.node, &node, &reason);
                    self.said(
                        Severity::Warning,
                        &format!("{node} cannot be followed: {reason}"),
                    );
                }
            }
        }
        // Every member followed is linked, or said unheard, before this
        // ends: the links this reading started, and those another reading
        // started that are still being made — the following thread and
        // `Plane::follow` read at once, and either may start a link.
        let held: Vec<u64> = upstreams
            .values()
            .map(|(_, upstream)| upstream.link)
            .collect();
        drop(upstreams);
        self.hub
            .links_made(&held, self.timing.connect + self.timing.handshake);
    }

    fn said(&self, severity: Severity, message: &str) {
        let properties = BTreeMap::from([("node".to_string(), self.node.clone())]);
        noted(&self.audit, "event.cluster", severity, message, properties);
    }
}
