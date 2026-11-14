//! The members of the cluster, and where the cluster keeps them.
//!
//! A member is a node of the cluster as the other nodes reach it: the node,
//! where its sync listener answers (ADR-0067), and what it declares
//! (ADR-0056), which is what work is placed on it by ([`crate::place`]).
//! Who the members are is the cluster's record, never a node's own list:
//! a [`Membership`] answers it, kept in Xmip Storage's administration
//! database (`deployment-model.md` section 7; ADR-0065, amendment
//! 2026-10-02).

use node::Capability;

use crate::ClusterError;

/// One member of the cluster, as another node reaches it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Member {
    /// The node: `xmip:///<cluster>/node/<name>`.
    pub node: String,
    /// Where its sync listener answers: `host:port`, the host the name its
    /// certificate is issued for — or `None` where it serves none, never an
    /// empty string (ADR-0067: no node starts its sync listener yet).
    pub address: Option<String>,
    /// What it declares: its roles, and so the stages it serves, and its
    /// online capability (ADR-0056).
    pub capability: Capability,
    /// Where its operate listener answers on the network, `host:port`, or
    /// `None` where it serves none (ADR-0067, `[operate] listen`).
    pub operate: Option<String>,
    /// A Storage node's place in `[storage] nodes`, the first listed the
    /// first primary — found by the process it answers as, never by its
    /// name — or `None` for a node that is no Storage node listed there.
    pub storage_place: Option<usize>,
    /// When it stopped in order, in nanoseconds since the Unix epoch on Xmip
    /// Storage's clock; `None` while it runs.
    pub stopped: Option<u64>,
}

/// Where the cluster keeps its members: Xmip Storage's administration
/// database, in a node (`deployment-model.md` section 7).
pub trait Membership: Send + Sync {
    /// Every member now; this node among them or not.
    ///
    /// # Errors
    /// The record could not be read; the members known before are kept.
    fn members(&self) -> Result<Vec<Member>, ClusterError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use node::NodeRole;

    struct Listed(Vec<Member>);

    impl Membership for Listed {
        fn members(&self) -> Result<Vec<Member>, ClusterError> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn a_membership_answers_its_members_with_what_each_declares() {
        let cluster = configure::fixture::test_cluster();
        let member = Member {
            node: cluster.node_scope(0),
            address: Some("127.0.0.1:7400".to_string()),
            capability: Capability::of(&[NodeRole::Receiving]).with_online(true),
            operate: None,
            storage_place: None,
            stopped: None,
        };
        let membership: Box<dyn Membership> = Box::new(Listed(vec![member.clone()]));
        let members = membership.members().expect("listed");
        assert_eq!(members, [member]);
        assert!(members[0].capability.is_online());
    }
}
