use std::collections::BTreeSet;
use std::hash::{DefaultHasher, Hash, Hasher};

use jsonptr::PointerBuf;

use crate::json::Path;
use crate::result::Result;
use crate::runtime::System;

/// Returns the longest subset of non-conflicting paths, preferring prefixes over specific paths.
///
/// When conflicts occur, prioritizes prefixes over more specific paths.
/// For example, if /config appears after /config/some_var, we prefer /config and remove /config/some_var.
pub(super) fn select_non_conflicting_prefer_prefixes<'a, I>(paths: I) -> Vec<Path>
where
    I: IntoIterator<Item = &'a Path>,
{
    let mut result: Vec<Path> = Vec::new();

    for p in paths.into_iter() {
        // If no existing paths are prefixes of the current path
        if !result.iter().any(|selected| selected.is_prefix_of(p)) {
            // Remove all the paths the current path is a prefix of
            result.retain(|selected| !p.is_prefix_of(selected));

            // And add the new path
            result.push(p.clone());
        }
    }
    result
}

/// Returns true if a new task domain conflicts with existing cumulative changes.
pub(super) fn domains_are_conflicting<'a, I>(cumulative_domain: &BTreeSet<Path>, domain: I) -> bool
where
    I: Iterator<Item = &'a Path>,
{
    // Check if any path in the domain sets conflict with each other
    for path1 in domain {
        for path2 in cumulative_domain.iter() {
            if path2.is_prefix_of(path1) || path1.is_prefix_of(path2) {
                return true;
            }
        }
    }
    false
}

/// Computes the longest common prefix over a list of `Path` objects
///
/// # Example
///
/// ```rust,ignore
/// let paths = vec![
///     Path::from_static("/config/server/host"),
///     Path::from_static("/config/server/port"),
///     Path::from_static("/config/server/ssl"),
/// ];
/// let result = longest_common_prefix(&paths);
/// assert_eq!(result.as_str(), "/config/server");
/// ```
pub(super) fn longest_common_prefix<'a, I>(paths: I) -> Path
where
    I: IntoIterator<Item = &'a Path>,
{
    let mut iter = paths.into_iter();

    // Get the first path to use as the base for comparison
    let first = match iter.next() {
        Some(path) => path.as_ref().tokens().collect::<Vec<_>>(),
        None => return Path::default(),
    };

    let mut prefix = first;

    for path in iter {
        let tokens = path.as_ref().tokens().collect::<Vec<_>>();
        let mut new_prefix = vec![];

        for (a, b) in prefix.iter().zip(tokens.iter()) {
            if a == b {
                new_prefix.push(a.clone());
            } else {
                break;
            }
        }

        prefix = new_prefix;
        if prefix.is_empty() {
            break;
        }
    }

    let buf = PointerBuf::from_tokens(&prefix);

    Path::new(&buf)
}

/// Get a hash for the system state
pub(super) fn hash_state(state: &System) -> u64 {
    let value = state.inner_state();

    // Create a DefaultHasher
    let mut hasher = DefaultHasher::new();

    // Hash the data
    value.hash(&mut hasher);

    // Retrieve the hash value
    hasher.finish()
}

/// Find the first element that matches the predicate or return
/// the first error
///
/// Alternative to [`std::iter::Iterator::try_find`] while that is
/// experimental
pub(super) fn try_find<T, I: Iterator<Item = T>>(
    iter: I,
    f: impl Fn(&T) -> Result<bool>,
) -> Result<Option<T>> {
    for t in iter {
        if f(&t)? {
            return Ok(Some(t));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_non_conflicting_prefer_prefixes_basic() {
        let paths = vec![Path::from_static("/a"), Path::from_static("/b")];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        assert_eq!(result, paths);
    }

    #[test]
    fn test_select_non_conflicting_prefer_prefixes_with_conflicts() {
        let paths = vec![
            Path::from_static("/config/other_var"),
            Path::from_static("/config/some_var"),
            Path::from_static("/counters/one"),
            Path::from_static("/config"),
        ];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        let expected = vec![
            Path::from_static("/counters/one"),
            Path::from_static("/config"),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_select_non_conflicting_prefer_prefixes_your_example() {
        let paths = vec![
            Path::from_static("/a"),
            Path::from_static("/b"),
            Path::from_static("/b/c"),
            Path::from_static("/b/d"),
        ];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        let expected = vec![Path::from_static("/a"), Path::from_static("/b")];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_select_non_conflicting_prefer_prefixes_no_later_prefix() {
        let paths = vec![
            Path::from_static("/config/server/host"),
            Path::from_static("/config/server/port"),
            Path::from_static("/database/host"),
        ];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        assert_eq!(result, paths);
    }

    #[test]
    fn test_select_non_conflicting_prefer_prefixes_prefix_first() {
        // Counter example: when prefix comes first, it should be kept
        // and more specific paths should be ignored
        let paths = vec![
            Path::from_static("/config"),
            Path::from_static("/config/server"),
            Path::from_static("/config/client"),
        ];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        let expected = vec![Path::from_static("/config")];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_select_non_conflicting_prefer_prefixes_root_path() {
        // Edge case: root path should dominate all other paths
        let paths = vec![
            Path::from_static(""),
            Path::from_static("/config"),
            Path::from_static("/counters"),
        ];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        let expected = vec![Path::from_static("")];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_select_non_conflicting_proper_path_prefix_vs_string_prefix() {
        // This test demonstrates the fix: /a should NOT conflict with /aa
        // because /a is not a path prefix of /aa (only a string prefix)
        let paths = vec![
            Path::from_static("/a"),
            Path::from_static("/aa"),
            Path::from_static("/a/b"),
        ];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        // /a should conflict with /a/b but NOT with /aa
        let expected = vec![Path::from_static("/a"), Path::from_static("/aa")];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_longest_common_prefix_empty() {
        let paths: Vec<Path> = vec![];
        let result = longest_common_prefix(&paths);
        assert_eq!(result.as_str(), "");
    }

    #[test]
    fn test_longest_common_prefix_single_path() {
        let paths = vec![Path::from_static("/config/server")];
        let result = longest_common_prefix(&paths);
        assert_eq!(result.as_str(), "/config/server");
    }

    #[test]
    fn test_longest_common_prefix_common_prefix() {
        let paths = vec![
            Path::from_static("/config/server/host"),
            Path::from_static("/config/server/port"),
            Path::from_static("/config/server/ssl"),
        ];
        let result = longest_common_prefix(&paths);
        assert_eq!(result.as_str(), "/config/server");
    }

    #[test]
    fn test_longest_common_prefix_no_common_prefix() {
        let paths = vec![
            Path::from_static("/config"),
            Path::from_static("/counters"),
            Path::from_static("/settings"),
        ];
        let result = longest_common_prefix(&paths);
        assert_eq!(result.as_str(), "");
    }

    #[test]
    fn test_longest_common_prefix_root_paths() {
        let paths = vec![
            Path::from_static("/a/b"),
            Path::from_static("/a/c"),
            Path::from_static("/a/d"),
        ];
        let result = longest_common_prefix(&paths);
        assert_eq!(result.as_str(), "/a");
    }
}
