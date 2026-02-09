//! Directed Acyclic Graph implementation and methods

use async_trait::async_trait;
use std::fmt;
use std::ops::Add;
use std::sync::{Arc, RwLock};

use crate::error::AggregateError;
use crate::sync::{Interrupt, Reader, Sender};

type Link<T> = Option<Arc<RwLock<Node<T>>>>;

/// DAG node type
///
/// A node in a DAG is a recursive data structure that
/// can represent either a value, a fork in the graph,
/// or a joining of paths.
///
/// For instance, the DAG below (reading from left to right)
///
/// ```text
///         + - c - d - +
/// a - b - +           + - g
///         + - e - f - +
/// ```
///
/// will contain 7 value nodes (a-g), one fork node (after b) and one join node
/// (before g)
enum Node<T> {
    Item { value: T, next: Link<T> },
    Fork { next: Vec<Link<T>> },
    Join { next: Link<T> },
}

impl<T> Node<T> {
    pub fn item(value: T, next: Link<T>) -> Self {
        Node::Item { value, next }
    }

    pub fn join(next: Link<T>) -> Self {
        Node::Join { next }
    }

    pub fn fork(next: Vec<Link<T>>) -> Self {
        Node::Fork { next }
    }

    pub fn into_link(self) -> Link<T> {
        Some(Arc::new(RwLock::new(self)))
    }
}

struct Iter<T> {
    /// Holds the current node
    /// and a stack to keep  track of the branching
    /// so the iteration knows when to continue after finding
    /// a join node
    stack: Vec<(Link<T>, Vec<usize>)>,
}

impl<T> Iterator for Iter<T> {
    type Item = Arc<RwLock<Node<T>>>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((link, branching)) = self.stack.pop() {
            if let Some(node_rc) = link {
                let node_ref = node_rc.read().unwrap();
                match &*node_ref {
                    Node::Item { next, .. } => {
                        // Push the next node onto the stack for continuation
                        self.stack.push((next.clone(), branching));
                        // Yield the current node
                        return Some(node_rc.clone());
                    }
                    Node::Fork { next } => {
                        // Push all branches onto the stack
                        for (i, branch_head) in next.iter().rev().enumerate() {
                            // Keep track of the branch within the DAG
                            let mut branching = branching.clone();
                            branching.push(i);
                            self.stack.push((branch_head.clone(), branching));
                        }
                        // Yield the fork node itself
                        return Some(node_rc.clone());
                    }
                    Node::Join { next } => {
                        let mut branching = branching;

                        // Get  the current branch at the top of the stack
                        if let Some(branch) = branching.pop() {
                            // Only continue the iteration when reaching the last
                            // branch of the node
                            if branch == 0 {
                                self.stack.push((next.clone(), branching));

                                // Yield the join node itself
                                return Some(node_rc.clone());
                            }
                        }
                    }
                }
            }
        }
        None // No more nodes to traverse
    }
}

/// Utility type to operate with Directed Acyclic Graphs (DAG)
///
/// This type is exported as a testing utility, to allow review of generated workflows using
/// automated tests.
///
///    ```rust
/// use mahler::extract::{View, Target};
/// use mahler::task::{IO, with_io};
/// use mahler::job::update;
/// use mahler::worker::Worker;
/// use mahler::dag::{Dag, seq};
///
/// fn plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> IO<i32> {
///     if *counter < tgt {
///         // Modify the counter if we are below target
///         *counter += 1;
///     }
///
///     // Return the updated counter
///     with_io(counter, |counter| async {
///         Ok(counter)
///     })
/// }
///
/// // Setup the worker domain and resources
/// let worker = Worker::new()
///                 .job("", update(plus_one).with_description(|| "+1"))
///                 .initial_state(0)
///                 .unwrap();
/// let workflow = worker.find_workflow(2).unwrap().unwrap();
///
/// // We expect a linear DAG with two tasks
/// let expected: Dag<&str> = seq!("+1", "+1");
/// assert_eq!(workflow.to_string(), expected.to_string());
/// ```
///
/// # Operating with DAGs
///
/// This module provides the [dag](`crate::dag!`), [seq](`crate::seq`) and [par](`crate::par`) macros for easy DAG construction, `Dag`
/// also implements the [`Add`] trait for simple concatenation, and [`Default`] can be used to
/// create an empty DAG.
///
/// ```rust
/// use mahler::dag::{Dag, dag, seq, par};
///
/// // Some linear DAGs
/// let ll0: Dag<i32> = seq!(1, 2, 3);
/// let ll1: Dag<i32> = seq!(4, 5, 6);
///
/// // A DAG with two branches
/// let fork: Dag<i32> = dag!(ll0, ll1);
///
/// // Continuing the DAG
/// let dag = fork + seq!(7);
///
/// // A DAG with two branches
/// let pr: Dag<i32> = par!(8,9);
///
/// // All DAGs can be concatenated
/// let dag = dag + pr;
/// ```
///
/// # String representation of a DAG
///
/// `Dag` implements `Display` for visual inspection of DAGs. `mahler` provides its own
/// text representation of a DAG, optimizing readability of the graph when displaying in logs.
///
/// Each node is represented in a separate line, with the following symbols to indicate where the
/// node is located on the graph branching.
///
/// - Each node is always preceeded by `-`
/// - The start of a new fork in the DAG is represented by a `+`
/// - The start of a new branch is represented by a `~`
/// - The relative position of the fork/branch/node is indicated by the indentation level of the node
///
/// A linear DAG
///
/// ```text
/// a - b - c
/// ```
///
/// Is represented as
///
/// ```text
/// - a
/// - b
/// - c
/// ```
///
/// Use of [pretty_assertions](https://docs.rs/pretty_assertions/latest/pretty_assertions/index.html) is a good way to visually compare results.
///
/// ```rust
/// use mahler::dag::{Dag, seq};
/// use dedent::{dedent};
/// use pretty_assertions::assert_str_eq;
///
/// let dag: Dag<char> = seq!('a', 'b', 'c');
///     assert_str_eq!(
///         dag.to_string(),
///         dedent!(
///             r#"
///             - a
///             - b
///             - c
///             "#
///         )
///     );
///
/// let dag: Dag<&str> = seq!("a", "b", "c");
/// ```
///
/// A DAG with two forks
///
/// ```text
///     + - c - d - +
/// a - +           + - g
///     + - e - f - +
/// ```
///
/// Is represented as
/// ```text
/// - a
/// + ~ - b
///     - c
///   ~ - d
///     - e
/// - g
/// ```
///
/// In code
///
/// ```rust
/// use mahler::dag::{Dag, dag, seq};
/// use dedent::{dedent};
/// use pretty_assertions::assert_str_eq;
///
/// let dag: Dag<char> = seq!('a') + dag!(seq!('b', 'c'), seq!('d', 'e')) + seq!('g');
///     assert_str_eq!(
///         dag.to_string(),
///         dedent!(
///             r#"
///             - a
///             + ~ - b
///                 - c
///               ~ - d
///                 - e
///             - g
///             "#
///         )
///     );
/// ```
///
/// The recursive nature of this representation allows for complex DAGs to be represented. For
/// instance, this represents a DAG that contains a fork within one of the branches of another
/// fork.
///
/// ```text
/// - a
/// + ~ - b
///     - c
///     + ~ - d
///         - e
///       ~ - f
///   ~ - g
///     - h
///     - i
/// - j
/// - k
/// ```
///
/// In code
///
/// ```rust
/// use mahler::dag::{Dag, dag, seq};
/// use dedent::{dedent};
/// use pretty_assertions::assert_str_eq;
///
/// let dag: Dag<char> = seq!('a')
///         + dag!(
///             seq!('b', 'c') + dag!(seq!('d', 'e'), seq!('f')),
///             seq!('g', 'h', 'i')
///         )
///         + seq!('j', 'k');
///     assert_str_eq!(
///         dag.to_string(),
///         dedent!(
///             r#"
///             - a
///             + ~ - b
///                 - c
///                 + ~ - d
///                     - e
///                   ~ - f
///               ~ - g
///                 - h
///                 - i
///             - j
///             - k
///             "#
///         )
///     );
/// ```
pub struct Dag<T> {
    head: Link<T>,
    tail: Link<T>,
}

impl<T: Clone> Clone for Dag<T> {
    /// Implements a deep clone of the DAG
    ///
    /// It creates full copy of the DAG structure and nodes, where each node is a clone of
    /// the corresponding copy of the original DAG
    fn clone(&self) -> Self {
        fn deep_clone<T: Clone>(head: Link<T>) -> (Link<T>, Link<T>) {
            if let Some(node_rc) = head {
                let node_ref = node_rc.read().unwrap();
                match &*node_ref {
                    Node::Item { value, next } => {
                        let (next, tail) = deep_clone(next.clone());
                        let node = Node::Item {
                            value: (*value).clone(),
                            next,
                        }
                        .into_link();

                        // use this node as the tail if there is no tail
                        let tail = tail.or(node.clone());

                        (node, tail)
                    }
                    Node::Fork { next } => {
                        let mut heads: Vec<Link<T>> = Vec::new();
                        let mut tails: Vec<Link<T>> = Vec::new();
                        for branch in next {
                            let (h, t) = deep_clone(branch.clone());
                            heads.push(h);
                            tails.push(t);
                        }

                        // use one of the tails to proceed with the recursion
                        if let Some(tail) = tails.last() {
                            // If the tail exists use its `next` property
                            let (join, tail) = if let Some(tail_rc) = tail {
                                let next = match &*tail_rc.read().unwrap() {
                                    Node::Item { next, .. } => next.clone(),
                                    Node::Join { next, .. } => next.clone(),
                                    _ => unreachable!("tail cannot be a fork"),
                                };

                                // follow the next node of the tail to create the join node
                                let (next, tail) = deep_clone(next);

                                // create a join node and the tail of the dag
                                let join = Node::Join { next }.into_link();
                                let tail = tail.or(join.clone());
                                (join, tail)
                            } else {
                                // if the tail is none, the join node was the last element
                                // of the dag so we need to re-create it
                                let join = Node::Join { next: None }.into_link();
                                (join.clone(), join)
                            };

                            // modify all tails to point to the new join node
                            for t_rc in tails.into_iter().flatten() {
                                match &mut *t_rc.write().unwrap() {
                                    Node::Item { ref mut next, .. } => *next = join.clone(),
                                    Node::Join { ref mut next, .. } => *next = join.clone(),
                                    _ => unreachable!("tail cannot be a fork"),
                                }
                            }

                            // return the fork node
                            (Node::Fork { next: heads }.into_link(), tail)
                        } else {
                            // the fork is empty
                            (None, None)
                        }
                    }
                    Node::Join { next } => {
                        // break the recursion here, the next node will be used when cloning
                        // the fork node
                        (next.clone(), None)
                    }
                }
            } else {
                (None, None)
            }
        }

        let (head, tail) = deep_clone(self.head.clone());
        Self { head, tail }
    }
}

impl<T> Default for Dag<T> {
    /// Create an empty DAG
    fn default() -> Self {
        Dag {
            head: None,
            tail: None,
        }
    }
}

impl<T: PartialEq> PartialEq for Dag<T> {
    fn eq(&self, other: &Self) -> bool {
        for (left, rght) in self.iter().zip(other.iter()) {
            if let (
                Node::Item {
                    value: left_value, ..
                },
                Node::Item {
                    value: rght_value, ..
                },
            ) = (&*left.read().unwrap(), &*rght.read().unwrap())
            {
                if left_value != rght_value {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }
}

impl<T: Eq> Eq for Dag<T> {}

impl<T> From<T> for Dag<T> {
    /// Create a single element `Dag<T>` for any value of type `T`
    fn from(value: T) -> Self {
        Dag::seq([value])
    }
}

impl<T> Dag<T> {
    /// Create a forking DAG from a list of branches
    ///
    /// # Arguments
    /// - `branches`: an iterable of Dag instances to use as branches
    ///
    /// # Returns
    /// A new forking `Dag` where each branch corresponds to one of the DAGs
    /// given as input
    ///
    /// # Example
    /// ```rust
    /// use mahler::dag::Dag;
    ///
    /// let br1: Dag<i32> = Dag::seq([1, 2, 3]);
    /// let br2: Dag<i32> = Dag::seq([4, 5, 6]);
    /// let dag: Dag<i32> = Dag::new([br1, br2]);
    /// assert_eq!(dag.to_string(), "+ ~ - 1\n    - 2\n    - 3\n  ~ - 4\n    - 5\n    - 6");
    /// ```
    pub fn new(branches: impl IntoIterator<Item = Dag<T>>) -> Dag<T> {
        // Filter out any branches with an empty head node
        let mut branches: Vec<Dag<T>> = branches
            .into_iter()
            .filter(|branch| branch.head.is_some())
            .collect();

        // Return the single branch if only one remains
        if branches.len() == 1 {
            return branches.pop().unwrap();
        }

        let mut next: Vec<Link<T>> = Vec::new();
        let tail = Node::<T>::join(None).into_link();
        for branch in branches {
            // Add the head link to the fork list
            next.push(branch.head);

            debug_assert!(branch.tail.is_some());
            // Link each branch tail to the join node
            if let Some(tail_rc) = branch.tail {
                match *tail_rc.write().unwrap() {
                    Node::Item { ref mut next, .. } => {
                        *next = tail.clone();
                    }
                    Node::Join { ref mut next } => {
                        *next = tail.clone();
                    }
                    // The tail should never point to a fork
                    Node::Fork { .. } => unreachable!(),
                }
            }
        }

        // Return an empty DAG if no branches remain
        if next.is_empty() {
            return Dag::default();
        }

        Dag {
            head: Node::fork(next).into_link(),
            tail,
        }
    }

    /// Create a linear DAG (a linked list) from a sequence of elements
    ///
    /// # Arguments
    /// - `elems`: an iterable of elements to include in the DAG.
    ///
    /// # Returns
    /// A `Dag` where each element is a node in sequence.
    ///
    /// # Example
    /// ```rust
    /// use mahler::dag::Dag;
    ///
    /// let dag: Dag<i32> = Dag::seq(vec![1, 2, 3]);
    /// assert_eq!(dag.to_string(), "- 1\n- 2\n- 3");
    /// ```
    pub fn seq(elems: impl IntoIterator<Item = impl Into<T>>) -> Dag<T> {
        let mut iter = elems.into_iter();
        let mut head: Link<T> = None;
        let mut tail: Link<T> = None;

        if let Some(value) = iter.next() {
            head = Node::item(value.into(), None).into_link();
            tail = head.clone();

            for value in iter {
                let new_node = Node::item(value.into(), None).into_link();
                if let Some(tail_node) = tail {
                    if let Node::Item { ref mut next, .. } = *tail_node.write().unwrap() {
                        *next = new_node.clone();
                    }
                }
                tail = new_node;
            }
        }

        Dag { head, tail }
    }

    /// Return `true` if the DAG is empty
    ///
    /// # Example
    /// ```rust
    /// use mahler::dag::Dag;
    ///
    /// let dag: Dag<i32> = Dag::default();
    /// assert!(dag.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.tail.is_none()
    }

    /// Join two DAGs
    pub fn concat(self, other: impl Into<Dag<T>>) -> Self {
        let other = other.into();
        if let Some(tail_node) = &self.tail {
            match *tail_node.write().unwrap() {
                Node::Item { ref mut next, .. } => {
                    *next = other.head;
                }
                Node::Join { ref mut next } => {
                    *next = other.head;
                }
                _ => unreachable!("tail cannot be a fork"),
            }
        } else {
            // this dag is empty
            return other;
        }

        Dag {
            head: self.head,
            tail: other.tail.or(self.tail),
        }
    }

    /// Concatenate a DAG at the head
    /// rather than the tail.
    ///
    /// This allows to build the DAG backwards and reverse
    /// it later using [`Self::reverse`]
    pub fn prepend(self, other: impl Into<Dag<T>>) -> Dag<T> {
        other.into().concat(self)
    }

    /// Return an iterator over the DAG
    ///
    /// This function is not public as not to expose the Dag internal implementation
    /// details
    fn iter(&self) -> Iter<T> {
        Iter {
            stack: vec![(self.head.clone(), Vec::new())],
        }
    }

    /// Return `true` if there is any node in the DAG that meets the given condition
    pub fn any(&self, condition: impl Fn(&T) -> bool) -> bool {
        for node in self.iter() {
            if let Node::Item { value, .. } = &*node.read().unwrap() {
                if condition(value) {
                    return true;
                }
            }
        }
        false
    }

    /// Return `true` if the given condition is met for every node in the DAG
    pub fn all(&self, condition: impl Fn(&T) -> bool) -> bool {
        for node in self.iter() {
            if let Node::Item { value, .. } = &*node.read().unwrap() {
                if !condition(value) {
                    return false;
                }
            }
        }
        true
    }

    /// Creates a shallow clone of the DAG by cloning only the head and tail references.
    ///
    /// # Warning
    ///
    /// <div class="warning">
    /// This creates shared ownership of the internal DAG structure. Modifying nodes
    /// through one DAG instance will affect all shallow clones since they share the
    /// same underlying references.
    /// </div>
    ///
    /// This method is primarily used internally for efficient DAG manipulation during
    /// planning where we need temporary DAG handles without full deep cloning.
    ///
    /// # Example
    ///
    /// ```rust
    /// use mahler::dag::{Dag, seq};
    ///
    /// let original: Dag<i32> = seq!(1, 2, 3);
    /// let new_dag = original.shallow_clone().concat(3);
    ///
    /// // Both `original` and `shallow` reference the same internal nodes so the
    /// // original will be modified too
    /// assert_eq!(original.to_string(), new_dag.to_string());
    ///
    /// ```
    pub fn shallow_clone(&self) -> Self {
        Self {
            head: self.head.clone(),
            tail: self.tail.clone(),
        }
    }

    /// Reverses the execution order of the DAG.
    ///
    /// This method consumes the original DAG and returns a new DAG where:
    /// - Sequential items are reversed in order
    /// - Fork/join structures are preserved but their contents are reversed
    /// - The original DAG's tail becomes the new head, and vice versa
    ///
    /// # Example
    ///
    /// Reversing this DAG:
    /// ```text
    ///         + - c - d - +
    /// a - b - +           + - g
    ///         + - e - f - +
    /// ```
    ///
    /// Returns:
    /// ```text
    ///     + - d - c - +
    /// g - +           + - b - a
    ///     + - f - e - +
    /// ```
    pub fn reverse(self) -> Dag<T> {
        let Dag { head, .. } = self;

        // The tail will be determined during reversal: it's the node that
        // receives prev=None (the outermost head becomes the tail)
        let mut tail: Link<T> = None;

        // Stack format: (current_node, previous_node, branch_path)
        // - current_node: The node we're currently processing
        // - previous_node: What this node should point to after reversal
        // - branch_path: Track nested fork positions for join reconstruction
        let mut stack = vec![(head, None as Link<T>, Vec::<usize>::new())];

        // Accumulates branch results until we can construct a fork node
        let mut results: Vec<Link<T>> = Vec::new();

        // Depth-first traversal that rewires nodes to point backward
        while let Some((head, prev, branching)) = stack.pop() {
            if let Some(node_rc) = head.clone() {
                match *node_rc.write().unwrap() {
                    Node::Item { ref mut next, .. } => {
                        // Store the current next node before rewiring
                        let newhead = next.clone();

                        // If prev is None, this is the original head becoming the new tail
                        if prev.is_none() {
                            tail = head.clone();
                        }

                        // Rewire this node to point backward (to previous node)
                        *next = prev;

                        // Continue processing the original next node
                        // This node becomes the "previous" for the next iteration
                        stack.push((newhead, head, branching));
                    }

                    Node::Fork { ref next } => {
                        // In reversal, a fork becomes a join point that branches converge to
                        // If prev is None, this fork is the original head, and the join
                        // replacing it becomes the new tail
                        let is_outermost = prev.is_none();
                        let prev = Node::join(prev).into_link();

                        if is_outermost {
                            tail = prev.clone();
                        }

                        // Process branches in reverse order to preserve their relative positioning
                        // after reversal (branch 0 stays branch 0, etc.)
                        for (i, br_head) in next.iter().rev().enumerate() {
                            // Track this branch's position in the fork for later join reconstruction
                            let mut branching = branching.clone();
                            branching.push(i);

                            // Each branch will be processed independently and converge at prev
                            stack.push((br_head.clone(), prev.clone(), branching));
                        }
                    }

                    Node::Join { ref next } => {
                        // In reversal, a join becomes a fork point that branches diverge from
                        // Accumulate the branch result for later fork construction
                        results.push(prev);

                        let mut branching = branching;
                        if let Some(branch) = branching.pop() {
                            // Only construct the fork when we've processed all branches
                            // Branch 0 is processed last (due to reverse iteration)
                            if branch == 0 {
                                // Create a fork node from all accumulated branch results
                                let head = Node::fork(results).into_link();
                                stack.push((next.clone(), head, branching));

                                // Reset for next potential fork construction
                                results = Vec::new();
                            }
                        }
                    }
                }
            } else {
                // Handle None nodes (end of a branch) by storing the previous node
                results.push(prev);
            }
        }

        // Construct the final reversed DAG from accumulated results
        if results.is_empty() {
            // Empty DAG case
            Dag::default()
        } else {
            // Should have exactly one result representing the new head
            debug_assert!(
                results.len() == 1,
                "Expected exactly one result after reversal"
            );

            let head = results.pop().unwrap();

            Dag { head, tail }
        }
    }
}

impl<T, R> Add<R> for Dag<T>
where
    R: Into<Dag<T>>,
{
    type Output = Self;

    fn add(self, other: R) -> Self {
        self.concat(other)
    }
}

/// Convert the DAG into a formatted string representation.
///
/// # Example
/// ```rust
/// use mahler::dag::{Dag, dag, seq};
///
/// let dag: Dag<char> = dag!(seq!('A', 'B'), seq!('C', 'D')) + seq!('E');
/// assert_eq!(
///     dag.to_string(),
///     "+ ~ - A\n    - B\n  ~ - C\n    - D\n- E"
/// );
/// ```
impl<T: fmt::Display> fmt::Display for Dag<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn fmt_node<T: fmt::Display>(
            f: &mut fmt::Formatter<'_>,
            // the current node
            node: &Node<T>,
            // the indentation level
            indent: usize,
            // the current index within the branch
            index: usize,
            // a stack storing the index of the fork and whether the
            // branch is the last one
            branching: Vec<(usize, bool)>,
        ) -> fmt::Result {
            let fmt_newline =
                |f: &mut fmt::Formatter, level: usize, condition: bool| -> fmt::Result {
                    if condition {
                        writeln!(f)?;
                        write!(f, "{}", "  ".repeat(level))?;
                    }
                    Ok(())
                };

            match node {
                Node::Item { value, next } => {
                    fmt_newline(f, indent, index > 0)?;
                    write!(f, "- {value}")?;

                    if let Some(next_rc) = next {
                        fmt_node(f, &*next_rc.read().unwrap(), indent, index + 1, branching)?;
                    }
                }
                Node::Fork { next } => {
                    fmt_newline(f, indent, index > 0)?;
                    write!(f, "+ ")?;

                    for (br_idx, branch) in next.iter().enumerate() {
                        if let Some(branch_head) = branch {
                            fmt_newline(f, indent + 1, br_idx > 0)?;
                            write!(f, "~ ")?;

                            let mut updated_branching = branching.clone();
                            updated_branching.push((index, br_idx == next.len() - 1));

                            fmt_node(
                                f,
                                &*branch_head.read().unwrap(),
                                indent + 2,
                                0,
                                updated_branching,
                            )?;
                        }
                    }
                }
                Node::Join { next } => {
                    let mut branching = branching;
                    if let Some((index, is_last)) = branching.pop() {
                        if is_last {
                            if let Some(next_rc) = next {
                                fmt_node(
                                    f,
                                    &*next_rc.read().unwrap(),
                                    indent - 2,
                                    index + 1,
                                    branching,
                                )?;
                            }
                        }
                    }
                }
            }
            Ok(())
        }

        if let Some(root) = &self.head {
            fmt_node(
                f,
                &*root.read().unwrap(),
                0,          // Initial indent level
                0,          // Initial index
                Vec::new(), // Initial branching
            )?
        }
        Ok(())
    }
}

/// Construct a linear DAG
///
/// ```rust
/// use mahler::dag::{Dag, seq};
///
/// // Construct a DAG of i32
/// let lli: Dag<i32> = seq!(1, 2, 3);
///
/// // Construct a DAG of str
/// let lls: Dag<&str> = seq!("a", "b", "c");
/// ```
#[macro_export]
macro_rules! seq {
    ($($value:expr),* $(,)?) => {
        Dag::seq([$($value),*])
    };
}

/// Construct a branching DAG
///
/// ```rust
/// use mahler::dag::{Dag, seq, dag};
///
/// // Construct a DAG of i32 with two branches
/// let dag: Dag<i32> = dag!(
///     seq!(1, 2, 3),
///     seq!(4, 5, 6)
/// );
/// ```
#[macro_export]
macro_rules! dag {
    ($($branch:expr),* $(,)?) => {
        Dag::new([$($branch),*])
    };
}

/// Construct a branching DAG with single item branches
///
/// ```rust
/// use mahler::dag::{Dag, par};
///
/// // Construct a DAG of i32 with three branches of one element each
/// let dag: Dag<i32> = par!(1, 2, 3);
/// ```
#[macro_export]
macro_rules! par {
    // If the input is a list of values (strings, etc.), convert each to a single-element Dag
    ($($value:expr),* $(,)?) => {
        Dag::new([
            $(Dag::seq([$value])),*
        ])
    }
}

/// DAG execution status
pub enum ExecutionStatus {
    /// All tasks in the DAG were executed
    Completed,

    /// The execution was interrupted
    Interrupted,
}

/// Utility trait for executable DAGs
///
/// Workflow items implementing this trait can be executed as part of a DAG (workflow) execution.
#[async_trait]
pub trait Task {
    /// The input type for the Task
    type Input;

    /// The resulting changes introduced by the task
    type Changes;
    type Error;

    async fn run(
        &self,
        input: &Self::Input,
        channel: &Sender<Self::Changes>,
    ) -> Result<Self::Changes, Self::Error>;
}

impl<T> Dag<T>
where
    T: Task + Clone,
    T::Input: Clone,
{
    /// Run the DAG
    ///
    /// This is only available for DAG items that implement Task
    ///
    /// NOTE: this is not public to avoid exposing external crate types
    pub async fn execute(
        self,
        input: &Reader<T::Input>,
        channel: Sender<T::Changes>,
        interrupt: Interrupt,
    ) -> Result<ExecutionStatus, AggregateError<T::Error>> {
        enum InnerNode<T> {
            Item { task: T, next: Link<T> },
            Fork { branches: Vec<Link<T>> },
            Join { next: Link<T> },
        }

        enum InnerError<E> {
            Failure(Vec<E>),
            Interrupted,
        }

        async fn run_task<T: Task>(
            task: T,
            value: &T::Input,
            channel: &Sender<T::Changes>,
            interrupt: &Interrupt,
        ) -> Result<T::Changes, InnerError<T::Error>> {
            let future = task.run(value, channel);

            // XXX: this assumes tasks are cancel-safe which might be a source
            // of problems in the future
            // See: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
            // alternatively we might want to let tasks check the interrupt directly?
            tokio::select! {
                _ = interrupt.wait() => {
                    Err(InnerError::Interrupted)
                }
                result = future => {
                    result.map_err(|e|  InnerError::Failure(vec![e]))
                }
            }
        }

        async fn exec_node<T>(
            node: Link<T>,
            input: &Reader<T::Input>,
            channel: &Sender<T::Changes>,
            interrupt: &Interrupt,
        ) -> Result<Link<T>, InnerError<T::Error>>
        where
            T: Task + Clone,
            T::Input: Clone,
        {
            let mut current = node;
            let mut errors = Vec::new();

            // Try to run nodes as a sequence
            while let Some(node_rc) = current {
                if interrupt.is_set() {
                    return Err(InnerError::Interrupted);
                }

                // We get the node data in advance to avoid holding
                // the node across the await
                let node = match &*node_rc.read().unwrap() {
                    Node::Item { value, next } => InnerNode::Item {
                        task: value.clone(),
                        next: next.clone(),
                    },
                    Node::Fork { next } => InnerNode::Fork {
                        branches: next.clone(),
                    },
                    Node::Join { next } => InnerNode::Join { next: next.clone() },
                };

                match node {
                    // If a Item node is found, just run the task and continue with tne next node
                    InnerNode::Item { task, next } => {
                        let value = {
                            // Read the up-to-date shared state
                            let guard = input.read().await;
                            guard.clone()
                        };

                        match run_task(task, &value, channel, interrupt).await {
                            Ok(changes) => {
                                // Send task changes back to the channel, it is the
                                // receiver responsibility to merge changes back on the shared
                                // state
                                if channel.send(changes).await.is_err() {
                                    return Err(InnerError::Interrupted);
                                }
                            }
                            Err(InnerError::Interrupted) => return Err(InnerError::Interrupted),
                            Err(InnerError::Failure(mut err)) => {
                                errors.append(&mut err);
                                break;
                            }
                        };

                        current = next;
                    }
                    // If a fork node is found, run each branch until encoutering the exit `Join`
                    // node and continue there
                    InnerNode::Fork { branches } => {
                        let mut futures = Vec::new();

                        for branch in branches.into_iter().filter(|b| b.is_some()) {
                            futures.push(exec_node(branch, input, channel, interrupt));
                        }

                        // Join the futures from the individual branches
                        // NOTE: at some point we might want to spawn new tokio tasks
                        // for each future
                        let results = futures::future::join_all(futures).await;

                        let mut join_next: Link<T> = None;

                        for res in results {
                            match res {
                                Ok(next) => {
                                    join_next = next;
                                }
                                Err(e) => match e {
                                    InnerError::Interrupted => return Err(InnerError::Interrupted),
                                    InnerError::Failure(mut err) => errors.append(&mut err),
                                },
                            }
                        }

                        // Stop running if there are failures on any branch
                        if !errors.is_empty() {
                            return Err(InnerError::Failure(errors));
                        }

                        // After all branches, continue after the Join
                        current = join_next;
                    }
                    // If a join node is found, just return its continuation
                    InnerNode::Join { next } => {
                        return Ok(next);
                    }
                }
            }

            if errors.is_empty() {
                Ok(None)
            } else {
                Err(InnerError::Failure(errors))
            }
        }

        let mut next = self.head;
        while next.is_some() {
            next = match exec_node(next, input, &channel, &interrupt).await {
                Ok(next) => next,
                Err(InnerError::Interrupted) => return Ok(ExecutionStatus::Interrupted),
                Err(InnerError::Failure(err)) => return Err(AggregateError(err)),
            }
        }

        Ok(ExecutionStatus::Completed)
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use dedent::dedent;
    use pretty_assertions::{assert_eq, assert_str_eq};
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Instant,
    };

    use super::*;
    use crate::sync::{channel, rw_lock};

    fn is_item<T>(node: &Arc<RwLock<Node<T>>>) -> bool {
        if let Node::Item { .. } = &*node.read().unwrap() {
            return true;
        }
        false
    }

    #[test]
    fn test_empty_dag() {
        let dag: Dag<i32> = Dag::default();
        assert!(dag.head.is_none());
    }

    #[test]
    fn test_dag_from_list() {
        let elements = vec![1, 2, 3, 4];
        let dag = Dag::<i32>::seq(elements.clone());
        let mut head = dag.head;

        for &value in &elements {
            assert!(head.is_some());
            if let Some(head_rc) = head {
                if let Node::Item {
                    value: node_value,
                    next,
                } = &*head_rc.read().unwrap()
                {
                    assert_eq!(*node_value, value);
                    head = next.clone();
                } else {
                    panic!("expected an item node");
                }
            }
        }
        assert!(head.is_none());
    }

    #[test]
    fn test_dag_from_empty_list() {
        let dag: Dag<i32> = Dag::seq(Vec::<i32>::new());
        assert!(dag.is_empty());

        // empty branches
        let dag: Dag<i32> = Dag::new(vec![Dag::seq(Vec::<i32>::new())]);
        assert!(dag.is_empty());
    }

    #[test]
    fn test_dag_from_single_branch() {
        let dag: Dag<i32> = dag!(seq!(1, 2, 3));
        assert!(dag.head.is_some());
        // a dag from single branch is just a list
        if let Some(head_rc) = dag.head {
            let node = &*head_rc.read().unwrap();
            assert!(matches!(node, Node::Item { value: 1, .. }));
        }
    }

    #[test]
    fn test_dag_construction() {
        let dag: Dag<i32> = seq!(1, 2, 3, 4);

        assert!(dag.head.is_some());
        if let Some(head_rc) = dag.head {
            let node = &*head_rc.read().unwrap();
            assert!(matches!(node, Node::Item { value: 1, .. }));
        }

        assert!(dag.tail.is_some());
        if let Some(tail_rc) = dag.tail {
            let node = &*tail_rc.read().unwrap();
            assert!(matches!(node, Node::Item { value: 4, .. }));
        }
    }

    #[test]
    fn test_clone_sequence() {
        let dag: Dag<i32> = seq!(1, 2, 3);
        let clone = dag.clone();
        assert_eq!(clone.to_string(), "- 1\n- 2\n- 3");
    }

    #[test]
    fn test_clone_fork() {
        let dag: Dag<i32> = seq!(1) + par!(2, 3, 4) + seq!(5);
        let clone = dag.clone();
        assert_eq!(clone.to_string(), "- 1\n+ ~ - 2\n  ~ - 3\n  ~ - 4\n- 5");
    }

    #[test]
    fn test_clone_deep_nested_dag() {
        let dag: Dag<char> = seq!('A')
            + dag!(
                seq!('B', 'C') + dag!(seq!('D', 'E'), seq!('F')),
                seq!('G', 'H', 'I')
            )
            + seq!('J', 'K');

        let dag = dag.clone();
        assert_str_eq!(
            dag.to_string(),
            dedent!(
                r#"
                - A
                + ~ - B
                    - C
                    + ~ - D
                        - E
                      ~ - F
                  ~ - G
                    - H
                    - I
                - J
                - K
                "#
            )
        );
    }

    #[test]
    fn test_iterate_linear_graph() {
        let elements = vec![1, 2, 3];
        let dag = Dag::<i32>::seq(elements.clone());

        // Collect the values in the order they are returned by the iterator
        let mut result = Vec::new();

        for node in dag.iter() {
            let node_ref = node.read().unwrap();
            match &*node_ref {
                Node::Item { value, .. } => result.push(*value), // Collect the value
                Node::Fork { .. } => panic!("unexpected fork node in a linear graph"),
                Node::Join { .. } => panic!("unexpected join node in a linear graph"),
            }
        }

        // Ensure the order is correct
        assert_eq!(result, elements);
    }

    #[test]
    fn test_iterate_forked_graph() {
        let dag: Dag<i32> = seq!(1, 2)
            + dag!(
                seq!(3) + dag!(seq!(4, 5), dag!(seq!(6), seq!(7)) + seq!(8)) + seq!(9),
                seq!(10) + dag!(seq!(11), seq!(12)),
            )
            + seq!(13);
        let elems: Vec<i32> = dag
            .iter()
            .filter(is_item)
            .map(|node| match &*node.read().unwrap() {
                Node::Item { value, .. } => *value,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(elems, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13])
    }

    #[test]
    fn test_empty_dag_string_representation() {
        let dag: Dag<char> = Dag::default();
        assert_eq!(dag.to_string(), "");
    }

    #[test]
    fn converts_linked_list_to_string() {
        let dag: Dag<char> = seq!('A', 'B', 'C', 'D');
        assert_str_eq!(
            dag.to_string(),
            dedent!(
                r#"
                - A
                - B
                - C
                - D
                "#
            )
        );
    }

    #[test]
    fn modifying_a_clone_should_not_affect_the_original() {
        let dag: Dag<char> = seq!('A') + par!('B', 'C', 'D');

        let new_dag = dag.clone() + seq!('E');

        assert_str_eq!(
            new_dag.to_string(),
            dedent!(
                r#"
                - A
                + ~ - B
                  ~ - C
                  ~ - D
                - E
                "#
            ),
            "new dag should contain the new element"
        );
        assert_str_eq!(
            dag.to_string(),
            dedent!(
                r#"
                - A
                + ~ - B
                  ~ - C
                  ~ - D
                "#
            ),
            "old dag should remain the same"
        );
    }

    #[test]
    fn test_concatenation_with_empty_dag() {
        // Test 1: Non-empty + Empty
        let non_empty: Dag<i32> = seq!(1, 2, 3);
        let empty: Dag<i32> = Dag::default();

        assert!(!non_empty.is_empty());
        assert!(empty.is_empty());

        let result = non_empty.clone() + empty.clone();
        assert!(!result.is_empty());
        assert_eq!(result.to_string(), "- 1\n- 2\n- 3");

        // Test 2: Empty + Non-empty
        let result2 = empty + non_empty;
        assert!(!result2.is_empty());
        assert_eq!(result2.to_string(), "- 1\n- 2\n- 3");
    }

    #[test]
    fn test_concatenation_with_forked_empty_dag() {
        // Test concatenating with a DAG that has empty branches
        let non_empty: Dag<i32> = seq!(1, 2);
        let forked_with_empty: Dag<i32> = dag!(seq!(3), Dag::default());

        let result = non_empty + forked_with_empty;
        assert!(!result.is_empty());
        // The empty branch should be filtered out during construction
        assert_eq!(result.to_string(), "- 1\n- 2\n- 3");
    }

    #[test]
    fn test_empty_dag_concatenation_preserves_tail() {
        // This test checks if tail is properly preserved when concatenating with empty DAGs
        let first: Dag<i32> = seq!(1);
        let second: Dag<i32> = Dag::default(); // Empty
        let third: Dag<i32> = seq!(2);

        // Chain: first + empty + third
        let result = first + second + third;
        assert!(!result.is_empty());
        assert_eq!(result.to_string(), "- 1\n- 2");
    }

    #[test]
    fn test_basic_concatenation_of_sequences() {
        // This test checks if tail is properly preserved when concatenating with empty DAGs
        let first: Dag<i32> = seq!(1, 2);
        let second: Dag<i32> = Dag::default(); // Empty
        let third: Dag<i32> = seq!(3);

        // Chain: first + empty + third
        let result = first + second + third;
        assert!(!result.is_empty());
        assert_eq!(result.to_string(), "- 1\n- 2\n- 3");
    }

    #[test]
    fn test_basic_prepend() {
        // This test checks if tail is properly preserved when concatenating with empty DAGs
        let first: Dag<i32> = seq!(1, 2);
        let second: Dag<i32> = Dag::default(); // Empty
        let third: Dag<i32> = seq!(3);

        let result = third.prepend(second).prepend(first);
        assert!(!result.is_empty());
        assert_eq!(result.to_string(), "- 1\n- 2\n- 3");
    }

    #[test]
    fn test_prepend_with_forked_empty_dag() {
        // Test concatenating with a DAG that has empty branches
        let non_empty: Dag<i32> = seq!(1, 2);
        let forked_with_empty: Dag<i32> = dag!(seq!(3), Dag::default());

        let result = forked_with_empty.prepend(non_empty);
        assert!(!result.is_empty());
        // The empty branch should be filtered out during construction
        assert_eq!(result.to_string(), "- 1\n- 2\n- 3");
    }

    #[test]
    fn test_dag_new_with_shared_nodes() {
        // This test checks if DAG::new() properly handles cases where branches might share nodes
        let single_element: Dag<i32> = seq!(42);

        // Create a fork where one branch is the single element DAG
        let branch1 = single_element.clone();
        let branch2 = seq!(1, 2);

        let forked_dag = dag!(branch1, branch2);

        // Check that the original single_element DAG wasn't corrupted
        assert_eq!(single_element.to_string(), "- 42");
        assert_eq!(forked_dag.to_string(), "+ ~ - 42\n  ~ - 1\n    - 2");
    }

    #[test]
    fn test_dag_new_with_multiple_single_elements() {
        // Test forking multiple single-element DAGs
        let elem1: Dag<i32> = seq!(1);
        let elem2: Dag<i32> = seq!(2);
        let elem3: Dag<i32> = seq!(3);

        let forked = dag!(elem1.clone(), elem2.clone(), elem3.clone());

        // Original elements should be unchanged
        assert_eq!(elem1.to_string(), "- 1");
        assert_eq!(elem2.to_string(), "- 2");
        assert_eq!(elem3.to_string(), "- 3");

        // Forked DAG should be correct
        assert_eq!(forked.to_string(), "+ ~ - 1\n  ~ - 2\n  ~ - 3");
    }

    #[test]
    fn test_dag_new_edge_cases() {
        // Test empty branches
        let empty1: Dag<i32> = Dag::default();
        let empty2: Dag<i32> = Dag::default();
        let non_empty: Dag<i32> = seq!(42);

        // DAG with only empty branches should return empty
        let all_empty = dag!(empty1.clone(), empty2.clone());
        assert!(all_empty.is_empty());

        // DAG with mix of empty and non-empty should work
        let mixed = dag!(empty1, non_empty.clone(), empty2);
        assert_eq!(mixed.to_string(), "- 42");

        // Single non-empty branch should return the branch directly
        let single_branch = dag!(non_empty);
        assert_eq!(single_branch.to_string(), "- 42");
    }

    #[test]
    fn test_dag_seq_edge_cases() {
        // Empty sequence should create empty DAG
        let empty_seq: Dag<i32> = Dag::seq(Vec::<i32>::new());
        assert!(empty_seq.is_empty());
        assert_eq!(empty_seq.to_string(), "");

        // Single element sequence
        let single: Dag<i32> = seq!(42);
        assert!(!single.is_empty());
        assert_eq!(single.to_string(), "- 42");
    }

    #[test]
    fn converts_branching_dag_to_string() {
        let dag: Dag<char> = dag!(seq!('A', 'B'), seq!('C', 'D', 'E')) + seq!('F');
        assert_str_eq!(
            dag.to_string(),
            dedent!(
                r#"
                + ~ - A
                    - B
                  ~ - C
                    - D
                    - E
                - F
                "#
            )
        );
    }

    #[test]
    fn converts_complex_dag_to_string() {
        let dag: Dag<char> = seq!('A')
            + dag!(
                seq!('B', 'C') + dag!(seq!('D', 'E'), seq!('F')),
                seq!('G', 'H', 'I')
            )
            + seq!('J', 'K');
        assert_str_eq!(
            dag.to_string(),
            dedent!(
                r#"
                - A
                + ~ - B
                    - C
                    + ~ - D
                        - E
                      ~ - F
                  ~ - G
                    - H
                    - I
                - J
                - K
                "#
            )
        );
    }

    #[test]
    fn converts_numeric_dag_to_string() {
        let dag: Dag<i32> = seq!(1, 2)
            + dag!(
                seq!(3) + dag!(seq!(4, 5), dag!(seq!(6), seq!(7)) + seq!(8)) + seq!(9),
                seq!(10) + par!(11, 12),
            )
            + seq!(13);

        assert_str_eq!(
            dag.to_string(),
            dedent!(
                r#"
                - 1
                - 2
                + ~ - 3
                    + ~ - 4
                        - 5
                      ~ + ~ - 6
                          ~ - 7
                        - 8
                    - 9
                  ~ - 10
                    + ~ - 11
                      ~ - 12
                - 13
            "#
            )
        )
    }

    #[tokio::test]
    async fn it_executes_simple_dag() {
        #[derive(Clone)]
        struct DummyTask;

        #[async_trait]
        impl Task for DummyTask {
            type Input = ();
            type Changes = ();
            type Error = ();

            async fn run(
                &self,
                _: &Self::Input,
                _: &Sender<Self::Changes>,
            ) -> Result<Self::Changes, Self::Error> {
                Ok(())
            }
        }

        let dag: Dag<DummyTask> = seq!(DummyTask, DummyTask, DummyTask);
        let (reader, _writer) = rw_lock(());
        let (tx, mut rx) = channel(10);
        let sigint = Interrupt::new();

        let count_atomic = Arc::new(AtomicUsize::new(0));
        let counter = count_atomic.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let c = counter.load(Ordering::Relaxed);
                counter.store(c + 1, Ordering::Relaxed);
                msg.ack();
            }
        });

        let result = dag.execute(&reader, tx, sigint).await;
        assert!(matches!(result, Ok(ExecutionStatus::Completed)));
        assert_eq!(count_atomic.load(Ordering::Relaxed), 3);
    }

    #[derive(Clone)]
    struct SleepyTask {
        pub name: &'static str,
        pub delay_ms: u64,
    }

    #[async_trait]
    impl Task for SleepyTask {
        type Input = ();
        type Changes = &'static str;
        type Error = ();

        async fn run(
            &self,
            _: &Self::Input,
            _: &Sender<Self::Changes>,
        ) -> Result<Self::Changes, Self::Error> {
            tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
            Ok(self.name)
        }
    }

    #[tokio::test]
    async fn test_concurrent_execution() {
        let task_a = SleepyTask {
            name: "A",
            delay_ms: 100,
        };
        let task_b = SleepyTask {
            name: "B",
            delay_ms: 100,
        };
        let task_c = SleepyTask {
            name: "C",
            delay_ms: 0,
        };

        let dag: Dag<SleepyTask> = dag!(seq!(task_a), seq!(task_b)) + seq!(task_c);

        let (input, _writer) = rw_lock(());
        let (tx, mut rx) = channel::<&'static str>(10);
        let sigint = Interrupt::new();

        let start = Instant::now();

        // Collect all results
        let results = Arc::new(tokio::sync::RwLock::new(Vec::new()));
        {
            let results = Arc::clone(&results);
            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    let mut res = results.write().await;
                    res.push(msg.data);
                    msg.ack();
                }
            });
        }

        let exec_result = dag.execute(&input, tx, sigint).await;
        let elapsed = start.elapsed();
        assert!(matches!(exec_result, Ok(ExecutionStatus::Completed)));

        let results = results.read().await;
        assert_eq!(*results, vec!["A", "B", "C"]);

        // Because a and b run concurrently, total time should be just a bit over 100ms, not 200ms
        assert!(
            elapsed.as_millis() < 200,
            "Execution took too long, not concurrent!"
        );
    }

    #[tokio::test]
    async fn test_interrupt_during_execution() {
        let dag: Dag<SleepyTask> = seq!(
            SleepyTask {
                name: "A",
                delay_ms: 100
            },
            SleepyTask {
                name: "B",
                delay_ms: 100
            },
            SleepyTask {
                name: "C",
                delay_ms: 100
            }
        );

        let (input, _writer) = rw_lock(());
        let (tx, mut rx) = channel::<&'static str>(10);
        let interrupt = Interrupt::new();

        let interrupt_clone = interrupt.clone();

        // Collect all results
        let results = Arc::new(tokio::sync::RwLock::new(Vec::new()));
        {
            let results = Arc::clone(&results);
            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    let mut res = results.write().await;
                    res.push(msg.data);
                    msg.ack();
                }
            });
        }

        // Set interrupt after 50ms
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            interrupt_clone.trigger();
        });

        let exec_result = dag.execute(&input, tx, interrupt).await;

        assert!(matches!(exec_result, Ok(ExecutionStatus::Interrupted)));

        // Could be 0, 1, maybe 2 (depending on timing) but not all 3
        let results = results.read().await;
        assert!(
            results.len() < 3,
            "Expected partial execution but got all results"
        );
    }

    #[derive(Clone)]
    struct MaybeFailTask {
        name: &'static str,
        fail: bool,
    }

    #[async_trait]
    impl Task for MaybeFailTask {
        type Input = ();
        type Changes = &'static str;
        type Error = &'static str; // Simple error
        async fn run(
            &self,
            _: &Self::Input,
            _: &Sender<Self::Changes>,
        ) -> Result<Self::Changes, Self::Error> {
            if self.fail {
                Err("task failed")
            } else {
                Ok(self.name)
            }
        }
    }

    #[tokio::test]
    async fn test_error_interrupts_execution() {
        let dag: Dag<MaybeFailTask> = dag!(
            dag!(
                seq!(
                    MaybeFailTask {
                        name: "A",
                        fail: false
                    },
                    MaybeFailTask {
                        name: "B",
                        fail: false
                    }
                ),
                seq!(
                    MaybeFailTask {
                        name: "C",
                        fail: true
                    },
                    MaybeFailTask {
                        name: "D",
                        fail: false
                    }
                )
            ),
            seq!(MaybeFailTask {
                name: "E",
                fail: false
            })
        ) + seq!(MaybeFailTask {
            name: "F",
            fail: false
        });

        let (input, _writer) = rw_lock(());
        let (tx, mut rx) = channel::<&'static str>(10);
        let interrupt = Interrupt::new();

        // Collect all results
        let results = Arc::new(tokio::sync::RwLock::new(Vec::new()));
        {
            let results = Arc::clone(&results);
            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    let mut res = results.write().await;
                    res.push(msg.data);
                    msg.ack();
                }
            });
        }

        let exec_result = dag.execute(&input, tx, interrupt).await;

        assert!(exec_result.is_err(), "Expected execution to fail on error");

        // Only successful tasks should have sent their changes
        let results = results.read().await;
        assert_eq!(*results, vec!["A", "E", "B"]);
    }

    #[test]
    fn test_contructing_linear_inverted_dag() {
        let dag: Dag<i32> = Dag::default().prepend(1).prepend(2).prepend(3);

        let elems: Vec<i32> = dag
            .iter()
            .filter(is_item)
            .map(|node| match &*node.read().unwrap() {
                Node::Item { value, .. } => *value,
                _ => unreachable!(),
            })
            .collect();

        assert_eq!(elems, vec![3, 2, 1])
    }

    #[test]
    fn test_contructing_forking_inverted_dag() {
        let dag: Dag<i32> = Dag::default()
            .prepend(1)
            .prepend(2)
            .prepend(3)
            .prepend(par!(5, 4))
            .prepend(6);

        let elems: Vec<i32> = dag
            .iter()
            .filter(is_item)
            .map(|node| match &*node.read().unwrap() {
                Node::Item { value, .. } => *value,
                _ => unreachable!(),
            })
            .collect();

        assert_eq!(elems, vec![6, 5, 4, 3, 2, 1])
    }

    #[test]
    fn test_reverse_dag() {
        let dag: Dag<i32> = Dag::default()
            .prepend(1)
            .prepend(2)
            .prepend(3)
            .prepend(par!(4, 5))
            .prepend(6)
            .reverse();

        let elems: Vec<i32> = dag
            .iter()
            .filter(is_item)
            .map(|node| match &*node.read().unwrap() {
                Node::Item { value, .. } => *value,

                _ => unreachable!(),
            })
            .collect();

        assert_eq!(elems, vec![1, 2, 3, 4, 5, 6])
    }

    #[test]
    fn test_reverse_empty_dag() {
        let dag: Dag<i32> = Dag::default().reverse();
        assert!(dag.head.is_none());
        assert!(dag.tail.is_none());
    }

    #[test]
    fn test_reverse_dag_with_forks() {
        let dag: Dag<char> = seq!('A') + dag!(seq!('B', 'C'), seq!('D')) + seq!('E');
        let reversed = dag.reverse();
        assert_str_eq!(
            reversed.to_string(),
            dedent!(
                r#"
                - E
                + ~ - C
                    - B
                  ~ - D
                - A
                "#
            )
        );
    }

    #[test]
    fn test_reverse_dag_with_just_a_fork() {
        let dag: Dag<char> = dag!(seq!('A', 'B'), seq!('C'));

        // we test that the dag is well-formed by concatenating a new value
        let reversed = dag.reverse() + seq!('D');
        assert_str_eq!(
            reversed.to_string(),
            dedent!(
                r#"
                + ~ - B
                    - A
                  ~ - C
                - D
                "#
            )
        );
    }

    #[test]
    fn test_reverse_single_item() {
        let dag: Dag<i32> = seq!(42);
        let reversed = dag.reverse();

        let elems: Vec<i32> = reversed
            .iter()
            .filter(is_item)
            .map(|node| match &*node.read().unwrap() {
                Node::Item { value, .. } => *value,
                _ => unreachable!(),
            })
            .collect();

        assert_eq!(elems, vec![42]);
    }

    #[test]
    fn test_reverse_nested_forks() {
        // Create a DAG with nested parallel sections:
        // A -> (B -> (C, D), E) -> F
        let inner_fork = dag!(seq!('C'), seq!('D'));
        let branch1 = seq!('B') + inner_fork;
        let branch2 = seq!('E');
        let dag: Dag<char> = seq!('A') + dag!(branch1, branch2) + seq!('F');

        let reversed = dag.reverse();

        // Should become: F -> ((C, D) -> B, E) -> A
        assert_str_eq!(
            reversed.to_string(),
            dedent!(
                r#"
                - F
                + ~ + ~ - C
                      ~ - D
                    - B
                  ~ - E
                - A
                "#
            )
        );
    }

    #[test]
    fn test_reverse_multiple_sequential_sections() {
        let dag: Dag<i32> = seq!(1, 2) + dag!(seq!(3, 4), seq!(5, 6)) + seq!(7, 8);
        let reversed = dag.reverse();

        let elems: Vec<i32> = reversed
            .iter()
            .filter(is_item)
            .map(|node| match &*node.read().unwrap() {
                Node::Item { value, .. } => *value,
                _ => unreachable!(),
            })
            .collect();

        // Original order: 1, 2, [3, 4 || 5, 6], 7, 8
        // Reversed order: 8, 7, [4, 3 || 6, 5], 2, 1
        assert_eq!(elems, vec![8, 7, 4, 3, 6, 5, 2, 1]);
    }

    #[test]
    fn test_reverse_three_way_fork() {
        let dag: Dag<char> = seq!('A') + dag!(seq!('B'), seq!('C'), seq!('D')) + seq!('E');
        let reversed = dag.reverse();

        assert_str_eq!(
            reversed.to_string(),
            dedent!(
                r#"
                - E
                + ~ - B
                  ~ - C
                  ~ - D
                - A
                "#
            )
        );
    }

    #[test]
    fn test_reverse_preserves_execution_semantics() {
        // Test that reversing twice returns to original execution order
        let original: Dag<i32> = seq!(1, 2) + dag!(seq!(3, 4), seq!(5)) + seq!(6);
        let double_reversed = original.shallow_clone().reverse().reverse();

        let original_elems: Vec<i32> = original
            .iter()
            .filter(is_item)
            .map(|node| match &*node.read().unwrap() {
                Node::Item { value, .. } => *value,
                _ => unreachable!(),
            })
            .collect();

        let double_reversed_elems: Vec<i32> = double_reversed
            .iter()
            .filter(is_item)
            .map(|node| match &*node.read().unwrap() {
                Node::Item { value, .. } => *value,
                _ => unreachable!(),
            })
            .collect();

        assert_eq!(original_elems, double_reversed_elems);
    }

    #[test]
    fn test_reverse_empty_branches() {
        // Test DAG with some empty branches (should be filtered out)
        let dag: Dag<i32> = dag!(seq!(1, 2), Dag::default(), seq!(3));
        let reversed = dag.reverse();

        let elems: Vec<i32> = reversed
            .iter()
            .filter(is_item)
            .map(|node| match &*node.read().unwrap() {
                Node::Item { value, .. } => *value,
                _ => unreachable!(),
            })
            .collect();

        // Empty branch should be filtered out
        assert_eq!(elems, vec![2, 1, 3]);
    }
}
