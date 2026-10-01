# xmip-core-cluster

The Cluster: the Nodes that make up one Xmip installation, its membership,
and which of them can take a piece of work that needs a capability.

A Cluster is not a broker. Its work store, the Ledger (`doc/terminology.md`,
*Ledger*), is the Cluster's, reached through Xmip Storage, the Nodes
declaring the Storage role. The Cluster is where Process State belongs, so
execution ownership may move between nodes without the state moving. The
same Journey is kept from being recovered by two nodes at once by a
time-limited claim through Xmip Storage — decided 2026-10-01 and not built
here yet.

`doc/architecture/runtime-model.md` section 22 and `deployment-model.md`
section 9 govern it; `architecture.toml` carries the maturity.
