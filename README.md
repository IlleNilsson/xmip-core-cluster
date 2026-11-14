# xmip-core-cluster

The Cluster: the nodes of one Xmip installation, who its members are now,
which of them takes a piece of work, and the one rotation every round robin
of the cluster takes. Platform since 2026-11-13, mounted at
`module/platform/cluster` (ADR-0058, amendment 2026-11-13).

- **`Member` and `Membership`** — a node as the other nodes reach it: its
  scope, its sync and operate addresses (`None` where it serves none), what
  it declares (`node::Capability`), its place among the Storage nodes and
  when it stopped in order; and the cluster's record of them, kept in Xmip
  Storage's administration database, which a node implements and every
  follower of the members takes (`deployment-model.md` section 7; ADR-0065,
  amendment 2026-10-02).
- **`place`** — placement is a match (ADR-0056 clause 4): among the
  candidates whose capability satisfies a `node::Requirement`, the next in
  the rotation's turn takes the work; where none does it is refused,
  `Unplaced`, naming the requirement and every node considered. A
  `Candidate` is a member, or a node of a cluster's file with what it
  declares. Nothing reads a name. **`every`** is the same match for work
  each able node runs, a Work Process on every node serving its stage, its
  turns shared through Xmip Storage; it refuses as `place` does.
- **`Rotation`** — where the next turn starts: one atomic step a turn, each
  turn asking every place once, from one further than the turn before.
  Xmip Storage's round robin and placement take their turns from it
  (ADR-0056 and `deployment-model.md` section 3, amendments 2026-11-13).

- **`Timing`** — every bound the sync plane keeps, each a `[tuning]` key
  per cluster and per node (`sync_beat`, `sync_silence`, `sync_handshake`,
  `sync_watch`, `sync_connect`, `sync_write`, `sync_retry`,
  `sync_retry_most`, `sync_follow`), its defaults the constants in
  `timing.rs`.

All of that is the crate's pure part: it opens no socket, starts no thread
and reads no clock, so `xmip-core-configure` and `xmip-core-persist` take
it with `default-features = false`.

## Delivery (ADR-0065)

The Event, its one subscription rule and its wire form are
`xmip-core-event`'s. Delivering it to the subscribers that asked is this
crate's, behind the feature `delivery`, on by default and included by
`sync`; it opens no socket, so the runtime and the Essentials take it
without the plane (ADR-0065, amendment 2026-11-13).

- **Who may see it.** A subscriber is a Party
  (`delivery::subscriber::Subscriber`), and the authorization gate decides,
  once, when it subscribes: `authorize::authorize` for a Send at the scope
  it reaches, each Event type as the Contract. Nothing configured is a
  refusal. A subscriber is the caller a gate proved, never a Party it
  names: `Subscriber::proven` takes the identity the authentication gate
  answered and the declared Party it resolved to, and refuses one not
  proven or resolved to no Party or another (ADR-0019, amendment
  2026-11-11). A program in a node's own process is the account the
  operating system names, proven by the runtime's gate and resolved
  through that node's Parties. Its Party is then authorized by the hub's
  gate (`delivery::gate`), the one policy list the hub is handed by
  `Hub::authorize_by`, which the program hosting the hub takes from the
  authorize capability (for "these Parties", `xmip-core-authorize-party`'s
  `PartyPolicy`, which this crate's tests use); `Hub::process` admits
  nobody until it is handed one (ADR-0065, amendment 2026-09-26). No node
  hands the process's hub its configured policy as it starts yet, so in a
  program's process it is that program's policy that decides (open problem
  67). A subscribe refused before it has a subscriber — the caller proved
  to be no Party — is audited by `subscriber::refused` under the names the
  hub records its own refusals with.
- **In process.** `delivery::hub::Hub` fans an Event out to every matching
  subscription through a bounded queue each and never waits for a
  subscriber: a full queue refuses the Event for that subscriber and
  counts it. `EventSubscription::next` waits for the first Event and wakes
  on arrival; `EventSubscription::listen` calls back on a thread of its own
  instead (`delivery::listener`). `Hub::process` is the process's hub, and
  `xmip_operate.h` section 11 is its C boundary, forwarded by the runtime's
  library and bound by C, C++, .NET, Java and Python (`xmip-core-abi`).
- **Over the wire.** `delivery::forward::Forwarder` carries a
  subscription's Events through an `xevent::wire::Wire` — Xmip's own http,
  kafka and amqp transports each implement one — at least once, in order,
  each attempt judged by the resilience guards.
- **Operated.** `Hub::standing` lists each open Event subscription as the
  `observe::EventSubscription` a node publishes — node and number,
  subscriber, what it asks for (`Filter::said`), state and its queue's
  counts — and `Hub::act` pauses, resumes or removes one (`observe::Act`,
  whose words are written once for every noun an operator acts on;
  `delivery::act`): paused, it keeps queuing up to its capacity and hands
  nothing over; resumed, it hands over what queued; removed, it is closed
  and its holder finds it so. Each act is audited in the subscriber's
  audit with who took it. A surface that reads a node only through its
  publication never acts on it (ADR-0014, amendment 2026-11-13). The
  handle a subscriber holds is `hub::EventSubscription`, so no identifier
  here reads as a Subscription, which picks a published Message up
  (ADR-0013, amendment 2026-09-30).
- **Any node is the cluster's door** (ADR-0065, amendments 2026-09-26 and
  2026-10-02). A subscriber on whichever node it reaches hears the
  matching Events of every node in the cluster. Connecting to a node where
  its Events are raised saves the one hop they would cross; it is a
  choice, never required (amendment 2026-10-12). The Event conversation is
  `sync::link`, compiled with `delivery` because the hub keeps its taps and
  carriers by it: the frames of the Event link, agreed as `xmip-event/1`
  (`Frame`, `receive`, `send`); the answering side, a `Tap` per node
  listening here, which holds what that node's subscriptions want and
  queues this node's own Events that match, so an Event crosses at most
  one hop, once, and never comes back; and the asking side on the hub,
  which asks every other member for what its subscriptions' filters match
  (`Hub::asked`), takes what each answers (`Hub::answered`) and knows how
  far each link carries them, so a subscribe returns once every member
  that can be reached carries its filter. A member that cannot be heard is
  unheard, and nothing missing is silent: every `Delivery` carries who is
  unheard now — by which node, the member, since when and why, as
  `observe::Unheard` — and a change wakes a waiting drain with no Event
  (`unheard_changed`); `Hub::unheard` answers the same for the node's
  publication, where every operator surface reads it as *not hearing
  `<node>` since `<time>`: `<why>`* (`observe::Unheard::said`); and the
  node's audit records it as `event.link` (`audit_trail::noted`). What a
  link's queue on the far node refused crosses as a count and is missed
  on the subscriptions it matched. A link is the cluster's and not a
  Party's: it is not an Event subscription, `Hub::standing` does not list
  it and `Hub::act` does not reach it.
- **Audited.** Every subscription, delivery, refusal and forward is
  recorded in the subscriber's program audit (ADR-0062), handed to the
  audit capability's keeper (`audit::keeper`) so no Event waits for a disk
  and unsubscribing never does either; the program's next direct record —
  its stop — is kept after them.
- **Tests build theirs through `delivery::subscriber::fixture`**, behind
  the `test-support` feature as `configure::fixture` is: a subscriber
  whose caller is built proven, as a gate would answer. Only a
  dev-dependency turns it on — this crate's own tests, through a
  dev-dependency on itself, and the AMQP, HTTP and Kafka transports'
  event-wire tests — which keeps a test's helper out of a node's build. It
  guards nothing: `Subscriber::proven` and a proven
  `context::AuthenticatedIdentity` are public, so any code linked into a
  process can build one, and what proves a program's subscriber is the
  runtime's gate (ADR-0019, amendment 2026-11-11).

Held by `tests/hub.rs`, `tests/act.rs` and `tests/forward.rs`.

## The sync plane

The sync plane is the running part, behind the default feature `sync`
(`sync::Plane`; ADR-0067): a node's sync listener answering the other
members over Xmip's mutual TLS (`sync/serving.rs`), a link to each of them
(`sync/upstream.rs`), and the following of who they are from a
`Membership`. It carries the cluster's Events, so any node is the
cluster's door (ADR-0065); it hands every frame to the Event conversation
(`sync::link`) and decides nothing about one. A member naming no sync
listener is unheard, and said so. Held by `tests/sync.rs` (nodes in this
process) and `tests/sync_process.rs` (nodes in processes of their own).

## Near real time

The owner, 2026-09-26: an Event reaches an in-process subscriber within
about a millisecond of being published. The module's timing program,
`tests/measured`, run after the other tests alone and one test at a time
(`doc/governance/rust-style.md` section 5c), holds it:
`delivery/latency.rs` measures publish to receipt, drained and called
back, and asserts it; `delivery/hub.rs` what a publish costs the
publisher; `delivery/act.rs` how soon a resume wakes a waiting drain; and
`latency.rs` what crossing to another node adds. Measured on the owner's
Windows machine, release build: a publish to one matching subscription
about 0.7 µs, to a hundred about 15 µs; publish to receipt a median of
about 40 µs and a 99th percentile under 100 µs.

A Cluster is not a broker. Its work store, the Ledger (`doc/terminology.md`,
*Ledger*), is the Cluster's, reached through Xmip Storage, the nodes
declaring the Storage role. The Cluster is where Process State belongs, so
execution ownership may move between nodes without the state moving. The
same Message is kept from being recovered by two nodes at once by a
time-limited claim through Xmip Storage (`deployment-model.md` section 9).

`architecture.toml` carries the maturity.
