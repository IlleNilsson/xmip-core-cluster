//! Which node takes a piece of work (ADR-0056 clauses 3 and 4, amendment
//! 2026-11-13).
//!
//! Placement is a match: among the nodes whose capability satisfies what
//! the work requires ([`node::Capability::satisfies`]), the next in the
//! rotation's turn takes it ([`place`]), or, for work every able node runs,
//! each of them does ([`every`]). Where none does, the work is refused, and the
//! refusal names both sides — the requirement that went unmet and every
//! node considered, with what each declares — never placed somewhere that
//! cannot serve it. Nothing is read from a node's name: a node is chosen by
//! what it declares, and named only to say which one it was.

use std::fmt;

use node::{Capability, Requirement};

use crate::{Member, Rotation};

/// A node placement may choose: the name a refusal says it by, and what it
/// declares. A member is one; so is a node of a cluster's file, named and
/// declared there.
pub trait Candidate {
    /// The node, as a refusal names it.
    fn node(&self) -> &str;
    /// What the node declares.
    fn capability(&self) -> &Capability;
}

impl Candidate for Member {
    fn node(&self) -> &str {
        &self.node
    }

    fn capability(&self) -> &Capability {
        &self.capability
    }
}

impl Candidate for (String, Capability) {
    fn node(&self) -> &str {
        &self.0
    }

    fn capability(&self) -> &Capability {
        &self.1
    }
}

/// Work no node could take: what it required, and every node considered
/// with what it declared.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Unplaced {
    pub requirement: Requirement,
    pub considered: Vec<(String, Capability)>,
}

impl fmt::Display for Unplaced {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "REFUSED: no node satisfies work requiring {}; considered ",
            self.requirement.said()
        )?;
        if self.considered.is_empty() {
            return f.write_str("none.");
        }
        let considered: Vec<String> = self
            .considered
            .iter()
            .map(|(node, capability)| {
                let roles = capability.words();
                let declared = if roles.is_empty() {
                    "no role".to_string()
                } else {
                    roles
                };
                format!("{node} ({declared}; {})", capability.word())
            })
            .collect();
        write!(f, "{}.", considered.join(", "))
    }
}

impl std::error::Error for Unplaced {}

/// The node of `candidates` that takes work requiring `requirement`: of
/// those satisfying it, in the order given, the one `rotation`'s next turn
/// starts at.
///
/// # Errors
///
/// Where no candidate satisfies it: [`Unplaced`], naming the requirement
/// and every candidate considered (ADR-0056 clause 4).
pub fn place<'a, C: Candidate>(
    requirement: Requirement,
    candidates: &'a [C],
    rotation: &Rotation,
) -> Result<&'a C, Unplaced> {
    let able = every(requirement, candidates)?;
    let place = rotation.next(able.len()).unwrap_or_default();
    Ok(able[place])
}

/// Every node of `candidates` that takes work requiring `requirement`, in
/// the order given: work each node able to runs, as a Work Process runs on
/// every node serving its stage, its turns shared through Xmip Storage.
///
/// # Errors
///
/// Where no candidate satisfies it: [`Unplaced`], as [`place`] refuses.
pub fn every<C: Candidate>(
    requirement: Requirement,
    candidates: &[C],
) -> Result<Vec<&C>, Unplaced> {
    let able: Vec<&C> = candidates
        .iter()
        .filter(|candidate| candidate.capability().satisfies(requirement))
        .collect();
    if able.is_empty() {
        return Err(Unplaced {
            requirement,
            considered: candidates
                .iter()
                .map(|candidate| (candidate.node().to_string(), candidate.capability().clone()))
                .collect(),
        });
    }
    Ok(able)
}

#[cfg(test)]
mod tests {
    use super::*;
    use node::{NodeRole, Stage};

    /// The test cluster's first `roles.len()` nodes, each declaring its
    /// roles: named by the fixture, never written here.
    fn nodes(roles: &[&[NodeRole]]) -> Vec<(String, Capability)> {
        let cluster = configure::fixture::test_cluster();
        roles
            .iter()
            .enumerate()
            .map(|(place, roles)| (cluster.node(place).name.clone(), Capability::of(roles)))
            .collect()
    }

    #[test]
    fn work_goes_round_the_nodes_that_satisfy_it_and_never_elsewhere() {
        let nodes = nodes(&[
            &[NodeRole::Receiving],
            &[NodeRole::Sending],
            &[NodeRole::Executing],
            &[NodeRole::Storage],
        ]);
        let rotation = Rotation::new();
        let receive = Requirement::serving(Stage::Receive);
        let placed: Vec<&str> = (0..4)
            .map(|_| {
                place(receive, &nodes, &rotation)
                    .expect("placed")
                    .0
                    .as_str()
            })
            .collect();
        assert_eq!(
            placed,
            [&nodes[0].0, &nodes[2].0, &nodes[0].0, &nodes[2].0].map(String::as_str)
        );
    }

    #[test]
    fn a_node_is_chosen_by_what_it_declares_and_renaming_changes_nothing() {
        let mut nodes = nodes(&[&[NodeRole::Sending], &[NodeRole::Receiving]]);
        let receive = Requirement::serving(Stage::Receive);
        let chosen = place(receive, &nodes, &Rotation::new()).expect("placed");
        assert_eq!(chosen.1, Capability::of(&[NodeRole::Receiving]));
        nodes.swap(0, 1);
        let (first, second) = (nodes[0].0.clone(), nodes[1].0.clone());
        nodes[0].0 = second;
        nodes[1].0 = first;
        let chosen = place(receive, &nodes, &Rotation::new()).expect("placed");
        assert_eq!(chosen.1, Capability::of(&[NodeRole::Receiving]));
    }

    #[test]
    fn work_no_node_satisfies_is_refused_naming_both_sides() {
        let nodes = nodes(&[&[NodeRole::Receiving], &[]]);
        let send = Requirement::serving(Stage::Send).with_online(true);
        let refused = place(send, &nodes, &Rotation::new()).expect_err("no node sends");
        assert_eq!(refused.requirement, send);
        assert_eq!(refused.considered, nodes);
        assert_eq!(
            refused.to_string(),
            format!(
                "REFUSED: no node satisfies work requiring serving Send, online; considered \
                 {} (receiving; offline), {} (no role; offline).",
                nodes[0].0, nodes[1].0
            )
        );
        let none: [(String, Capability); 0] = [];
        let alone = place(send, &none, &Rotation::new()).expect_err("no node at all");
        assert!(alone.to_string().ends_with("considered none."), "{alone}");
    }

    #[test]
    fn work_every_able_node_runs_is_on_each_of_them_and_refused_where_none_is() {
        let nodes = nodes(&[
            &[NodeRole::Receiving],
            &[NodeRole::Processing],
            &[NodeRole::Storage],
            &[NodeRole::Executing],
        ]);
        let process = Requirement::serving(Stage::Process);
        let on: Vec<&str> = every(process, &nodes)
            .expect("placed")
            .into_iter()
            .map(|node| node.0.as_str())
            .collect();
        assert_eq!(on, [nodes[1].0.as_str(), nodes[3].0.as_str()]);
        let refused = every(process, &nodes[..1]).expect_err("no node processes");
        assert_eq!(
            refused,
            place(process, &nodes[..1], &Rotation::new()).expect_err("the same refusal")
        );
        assert!(
            refused
                .to_string()
                .starts_with("REFUSED: no node satisfies work requiring serving Work Process;"),
            "{refused}"
        );
    }

    #[test]
    fn online_work_goes_only_to_an_online_node() {
        let mut nodes = nodes(&[&[NodeRole::Sending], &[NodeRole::Sending]]);
        nodes[1].1 = nodes[1].1.clone().with_online(true);
        let rotation = Rotation::new();
        let send = Requirement::serving(Stage::Send).with_online(true);
        for _ in 0..3 {
            assert_eq!(
                place(send, &nodes, &rotation).expect("placed").0,
                nodes[1].0
            );
        }
    }

    #[test]
    fn a_member_is_a_candidate() {
        let cluster = configure::fixture::test_cluster();
        let members = [Member {
            node: cluster.node_scope(0),
            address: None,
            capability: Capability::of(&[NodeRole::Executing]),
            operate: None,
            storage_place: None,
            stopped: None,
        }];
        let chosen = place(
            Requirement::serving(Stage::Send),
            &members,
            &Rotation::new(),
        );
        assert_eq!(chosen.expect("placed").node, cluster.node_scope(0));
    }
}
