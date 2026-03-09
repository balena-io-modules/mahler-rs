use std::collections::{BTreeMap, BTreeSet};

use crate::dag::Dag;
use crate::json::{OperationMatcher, PatchOperation, Path};

use super::path_utils::{
    domains_are_conflicting, longest_common_prefix, select_non_conflicting_prefer_prefixes,
};
use super::WorkUnit;

#[derive(Clone, PartialEq, Eq)]
pub(super) struct Candidate {
    pub(super) partial_plan: Dag<WorkUnit>,
    pub(super) changes: Vec<PatchOperation>,
    pub(super) path: Path,
    pub(super) domain: BTreeSet<Path>,
    pub(super) operation: OperationMatcher,
    pub(super) is_method: bool,
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Sort by reverse path ordering first, giving shorter paths
        // higher priority
        other
            .path
            .cmp(&self.path)
            // User defined methods vs actions and automatically generated
            // workflows
            .then(self.is_method.cmp(&other.is_method))
            // Sort by operation (`Any` is after all other)
            .then(self.operation.cmp(&other.operation))
    }
}

/// Merge concurrent non-conflicting candidates into a single combined candidate when possible.
///
/// When more than one candidate can run without conflicts, they are combined into a single
/// concurrent candidate backed by a parallel DAG branch. If only one (or zero) candidates
/// are non-conflicting, the list is returned unchanged.
pub(super) fn merge_concurrent_candidates(candidates: Vec<Candidate>) -> Vec<Candidate> {
    // Find the longest list of non-conflicting tasks based on paths (for prioritization)
    let non_conflicting_paths = select_non_conflicting_prefer_prefixes(
        candidates.iter().map(|Candidate { path, .. }| path),
    );

    // Track the best candidate index per path, avoiding full Candidate clones
    let mut concurrent_indices: BTreeMap<Path, usize> = BTreeMap::new();
    let mut cumulative_domain = BTreeSet::new();

    for (i, candidate) in candidates.iter().enumerate() {
        if let Some(&prev_idx) = concurrent_indices.get(&candidate.path) {
            if candidates[prev_idx] >= *candidate {
                // skip the candidate if there is a previous candidate for the path
                // with higher priority
                continue;
            }
        }

        // If the domain of the candidate doesn't conflict with the cumulative domain
        if non_conflicting_paths.iter().any(|p| p == &candidate.path)
            && !domains_are_conflicting(&cumulative_domain, candidate.domain.iter())
        {
            cumulative_domain.extend(candidate.domain.iter().cloned());
            concurrent_indices.insert(candidate.path.clone(), i);
        }
    }

    if concurrent_indices.len() > 1 {
        // The path for the candidate is the longest common prefix between child paths
        let path = longest_common_prefix(concurrent_indices.keys());

        // Partition in one O(n) pass: move concurrent candidates out, keep the rest.
        let selected: BTreeSet<usize> = concurrent_indices.into_values().collect();
        let mut concurrent = Vec::with_capacity(selected.len());
        let mut remaining = Vec::with_capacity(candidates.len() - selected.len());
        for (i, candidate) in candidates.into_iter().enumerate() {
            if selected.contains(&i) {
                concurrent.push(candidate);
            } else {
                remaining.push(candidate);
            }
        }

        let mut plan_branches = Vec::with_capacity(concurrent.len());
        let mut changes = Vec::new();
        let mut domain = BTreeSet::new();
        let mut is_method = true;

        for Candidate {
            partial_plan,
            changes: candidate_changes,
            domain: candidate_domain,
            is_method: candidate_is_method,
            ..
        } in concurrent
        {
            plan_branches.push(partial_plan);
            changes.extend(candidate_changes);
            domain.extend(candidate_domain);
            // Treat the candidate as a method when sorting if all children
            // are methods
            is_method = is_method && candidate_is_method;
        }

        // Construct a new candidate using the concurrent branches
        remaining.push(Candidate {
            partial_plan: Dag::new(plan_branches),
            changes,
            domain,
            path,
            is_method,
            operation: OperationMatcher::Update,
        });
        remaining
    } else {
        candidates
    }
}
