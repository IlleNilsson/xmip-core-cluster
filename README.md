# xmip-core-cluster

The Cluster: the Nodes that make up one Xmip installation, its membership,
and which of them can take a piece of work that needs a capability.

A Cluster is not a broker and not a shared database: every Node's ToDo is its
own (`doc/terminology.md`, *ToDo*). The Cluster is where Process State
belongs, so execution ownership may move between nodes without the state
moving. How the same Journey is kept from being recovered by two nodes at once
is open, and this crate does not pretend otherwise.

`doc/architecture/runtime-model.md` section 22 and `deployment-model.md`
section 9 govern it; `architecture.toml` carries the maturity.
