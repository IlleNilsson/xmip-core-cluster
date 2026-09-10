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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn node(id: u128, capability: &str) -> Node {
        Node {
            node_id: NodeId::new(id),
            name: format!("node-{id}"),
            roles: BTreeSet::new(),
            capabilities: [capability.to_string()].into_iter().collect(),
            trusted: true,
        }
    }

    #[test]
    fn the_first_capable_node_is_selected_and_none_when_none_is() {
        let cluster = Cluster {
            cluster_id: ClusterId::new(1),
            name: "c".to_string(),
            nodes: vec![
                node(1, "transport:file"),
                node(2, "transport:http"),
                node(3, "transport:http"),
            ],
        };
        assert_eq!(
            cluster.select_capable_node("transport:http"),
            Some(NodeId::new(2))
        );
        assert_eq!(cluster.select_capable_node("transport:mqtt"), None);
    }
}
