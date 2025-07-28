use async_trait::async_trait;
use std::fmt;
use std::ops::Add;
use std::sync::{Arc, RwLock};

use super::channel::Sender;
use super::{AggregateError, Interrupt};

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

#[derive(Clone)]
/// Utility type to operate with Directed Acyclic Graphs (DAG)
///
/// This type is exported as a testing utility, to allow review of generated workflows using
/// automated tests.
///
///    ```rust
/// use mahler::task::{self, prelude::*};
/// use mahler::extract::{View, Target};
/// use mahler::worker::Worker;
/// use mahler::{Dag, seq};
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
/// let worker: Worker<i32> = Worker::new()
///                 .job("", update(plus_one).with_description(|| "+1"));
/// let workflow = worker.find_workflow(0, 2).unwrap();
///
/// // We expect a linear DAG with two tasks
/// let expected: Dag<&str> = seq!("+1", "+1");
/// assert_eq!(workflow.to_string(), expected.to_string());
/// ```
///
/// # Operating with DAGs
///
/// This module provides the [dag](`crate::dag`), [seq](`crate::seq`) and [par](`crate::par`) macros for easy DAG construction, `Dag`
/// also implements the [`Add`] trait for simple concatenation, and [`Default`] can be used to
/// create an empty DAG.
///
/// ```rust
/// use mahler::{Dag, dag, seq, par};
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
/// use mahler::{Dag, seq};
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
/// use mahler::{Dag, dag, seq};
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
/// use mahler::{Dag, dag, seq};
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
    /// use mahler::Dag;
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
    /// use mahler::Dag;
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
    /// use mahler::Dag;
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
        if let Some(tail_node) = self.tail {
            match *tail_node.write().unwrap() {
                Node::Item { ref mut next, .. } => {
                    *next = other.head;
                }
                Node::Join { ref mut next } => {
                    *next = other.head;
                }
                // The tail should never point to a fork
                Node::Fork { .. } => unreachable!(),
            }
        } else {
            // this dag is empty
            return other;
        }

        Dag {
            head: self.head,
            tail: other.tail,
        }
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
/// use mahler::{Dag, dag, seq};
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
/// use mahler::{Dag, seq};
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
/// use mahler::{Dag, seq, dag};
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
/// use mahler::{Dag, par};
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

pub(crate) enum ExecutionStatus {
    Completed,
    Interrupted,
}

#[async_trait]
/// Utility trait for executable DAGs
///
/// This is different from [`crate::task::Task`]. Workflow items implementing this trait can be
/// executed as part of a DAG (workflow) execution.
pub trait Task {
    /// The input type for the Task
    type Input;

    /// The resulting changes introduced by the task
    type Changes;
    type Error;

    async fn run(&self, input: &Self::Input) -> Result<Self::Changes, Self::Error>;
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
    pub(crate) async fn execute(
        self,
        input: &Arc<tokio::sync::RwLock<T::Input>>,
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
            interrupt: &Interrupt,
        ) -> Result<T::Changes, InnerError<T::Error>> {
            let future = task.run(value);

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
            input: &Arc<tokio::sync::RwLock<T::Input>>,
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

                        match run_task(task, &value, interrupt).await {
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
    use tracing::debug;

    use super::*;
    use crate::workflow::channel;

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
        assert!(dag.tail.is_none());
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
    fn test_dag_construction() {
        let dag: Dag<i32> = seq!(1, 2, 3, 4);

        assert!(dag.head.is_some());
        if let Some(head_rc) = dag.head {
            if let Node::Item { value, .. } = &*head_rc.read().unwrap() {
                assert_eq!(value, &1)
            }
        }

        assert!(dag.tail.is_some());
        if let Some(tail_rc) = dag.tail {
            if let Node::Item { value, .. } = &*tail_rc.read().unwrap() {
                assert_eq!(value, &4)
            }
        }
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

            async fn run(&self, _input: &Self::Input) -> Result<Self::Changes, Self::Error> {
                Ok(())
            }
        }

        let dag: Dag<DummyTask> = seq!(DummyTask, DummyTask, DummyTask);
        let reader = Arc::new(tokio::sync::RwLock::new(()));
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

        async fn run(&self, _input: &Self::Input) -> Result<Self::Changes, Self::Error> {
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

        let input = Arc::new(tokio::sync::RwLock::new(()));
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
        debug!("Execution time: {:?}", elapsed);
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

        let input = Arc::new(tokio::sync::RwLock::new(()));
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
        async fn run(&self, _input: &Self::Input) -> Result<Self::Changes, Self::Error> {
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

        let input = Arc::new(tokio::sync::RwLock::new(()));
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
}
