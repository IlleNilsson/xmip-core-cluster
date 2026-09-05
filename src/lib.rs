#![forbid(unsafe_code)]

use node::Node;
use xcore::{ClusterId, NodeId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Cluster {
    pub cluster_id: ClusterId,
    pub name: String,
    pub nodes: Vec<Node>,
}

impl Cluster {
    pub fn select_capable_node(&self, capability: &str) -> Option<NodeId> {
        self.nodes
            .iter()
            .find(|node| node.supports(capability))
            .map(|node| node.node_id)
    }
}
