//! Directed Acyclic Graph implementation and methods

use async_trait::async_trait;
use std::fmt;
use std::ops::Add;

use crate::error::AggregateError;
use crate::sync::{Interrupt, Reader, Sender};

/// A node in the graph
#[derive(Clone, PartialEq, Eq)]
enum Node<T> {
    /// A single element
    Item(T),

    /// Branches that run concurrently and reconverge at this node
    ///
    /// Branches are never empty and there are always at least two of them.
    /// [`Dag::new`] is the only constructor that establishes this.
    Fork(Vec<Dag<T>>),
}
/// Utility type to operate with Directed Acyclic Graphs (DAG)
///
/// This type is exported as a testing utility, to allow review of generated workflows using
/// automated tests. It is also the type the planner builds and the runtime executes.
///
/// A `Dag` is built out of sequences and parallel branches, so it can represent any graph
/// where every set of parallel branches reconverges at a single point. Graphs where branches
/// reconverge at different points, e.g. `A -> C`, `A -> D` and `B -> D`, cannot be expressed.
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
/// # Equality
///
/// Two DAGs are equal when they have the same shape and the same values in the same
/// positions. Parallel branches are compared in order, so `par!(1, 2)` and `par!(2, 1)`
/// are not equal even though both describe the same set of concurrent work.
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
#[derive(Clone, PartialEq, Eq)]
pub struct Dag<T>(Vec<Node<T>>);

impl<T> Default for Dag<T> {
    /// Create an empty DAG
    fn default() -> Self {
        Dag(Vec::new())
    }
}

impl<T> From<T> for Dag<T> {
    /// Create a single element `Dag<T>` for any value of type `T`
    fn from(value: T) -> Self {
        Dag(vec![Node::Item(value)])
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
        let mut branches: Vec<Dag<T>> = branches
            .into_iter()
            .filter(|branch| !branch.is_empty())
            .collect();

        // A single branch is just a sequence, there is nothing to run in parallel
        if branches.len() <= 1 {
            return branches.pop().unwrap_or_default();
        }

        Dag(vec![Node::Fork(branches)])
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
        Dag(elems
            .into_iter()
            .map(|elem| Node::Item(elem.into()))
            .collect())
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
        self.0.is_empty()
    }

    /// Join two DAGs
    ///
    /// # Example
    /// ```rust
    /// use mahler::dag::Dag;
    ///
    /// let dag: Dag<i32> = Dag::seq([1, 2]).concat(Dag::seq([3]));
    /// assert_eq!(dag.to_string(), "- 1\n- 2\n- 3");
    /// ```
    pub fn concat(mut self, other: impl Into<Dag<T>>) -> Self {
        self.0.extend(other.into().0);
        self
    }

    /// Return `true` if there is any node in the DAG that meets the given condition
    pub fn any(&self, condition: impl Fn(&T) -> bool) -> bool {
        // Walk each sequence in turn, queueing branches to visit later
        let mut pending: Vec<&Dag<T>> = Vec::new();
        let mut current = self;
        loop {
            for node in &current.0 {
                match node {
                    Node::Item(value) => {
                        if condition(value) {
                            return true;
                        }
                    }
                    Node::Fork(branches) => pending.extend(branches.iter()),
                }
            }

            match pending.pop() {
                Some(next) => current = next,
                None => return false,
            }
        }
    }

    /// Return `true` if the given condition is met for every node in the DAG
    pub fn all(&self, condition: impl Fn(&T) -> bool) -> bool {
        !self.any(|value| !condition(value))
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
        fmt_seq(f, self, 0)
    }
}

/// Render one sequence of the DAG
///
/// `indent` is the nesting level, worth two spaces each.
fn fmt_seq<T: fmt::Display>(
    f: &mut fmt::Formatter<'_>,
    dag: &Dag<T>,
    indent: usize,
) -> fmt::Result {
    for (index, node) in dag.0.iter().enumerate() {
        if index > 0 {
            writeln!(f)?;
            write!(f, "{}", "  ".repeat(indent))?;
        }

        match node {
            Node::Item(value) => write!(f, "- {value}")?,
            Node::Fork(branches) => {
                write!(f, "+ ")?;
                for (position, branch) in branches.iter().enumerate() {
                    // The first branch continues on the line opened by `+`
                    if position > 0 {
                        writeln!(f)?;
                        write!(f, "{}", "  ".repeat(indent + 1))?;
                    }
                    write!(f, "~ ")?;
                    fmt_seq(f, branch, indent + 2)?;
                }
            }
        }
    }

    Ok(())
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

enum ExecError<E> {
    Failure(Vec<E>),
    Interrupted,
}

/// Run one sequence to completion
///
/// Branches run concurrently and all of them must terminate before the sequence
/// continues past the fork.
async fn exec_seq<T>(
    dag: &Dag<T>,
    input: &Reader<T::Input>,
    channel: &Sender<T::Changes>,
    interrupt: &Interrupt,
) -> Result<(), ExecError<T::Error>>
where
    T: Task,
    T::Input: Clone,
{
    for node in &dag.0 {
        if interrupt.is_set() {
            return Err(ExecError::Interrupted);
        }

        match node {
            Node::Item(value) => {
                // Copy the shared state so the read guard is not held across the await
                let state = {
                    let guard = input.read().await;
                    guard.clone()
                };

                // XXX: this assumes tasks are cancel-safe which might be a source
                // of problems in the future
                // See: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
                let result = tokio::select! {
                    _ = interrupt.wait() => return Err(ExecError::Interrupted),
                    result = value.run(&state, channel) => result,
                };

                match result {
                    // The receiver is responsible for merging the changes back into
                    // the shared state
                    Ok(changes) => {
                        if channel.send(changes).await.is_err() {
                            return Err(ExecError::Interrupted);
                        }
                    }
                    Err(e) => return Err(ExecError::Failure(vec![e])),
                }
            }
            Node::Fork(branches) => {
                // NOTE: at some point we might want to spawn new tokio tasks
                // for each future
                let results = futures::future::join_all(
                    branches
                        .iter()
                        .map(|branch| exec_seq(branch, input, channel, interrupt)),
                )
                .await;

                let mut errors = Vec::new();
                for result in results {
                    match result {
                        Ok(()) => {}
                        Err(ExecError::Interrupted) => return Err(ExecError::Interrupted),
                        Err(ExecError::Failure(err)) => errors.extend(err),
                    }
                }

                // Stop running if there are failures on any branch
                if !errors.is_empty() {
                    return Err(ExecError::Failure(errors));
                }
            }
        }
    }

    Ok(())
}

impl<T> Dag<T>
where
    T: Task,
    T::Input: Clone,
{
    /// Run the DAG
    ///
    /// This is only available for DAG items that implement Task
    pub async fn execute(
        self,
        input: &Reader<T::Input>,
        channel: Sender<T::Changes>,
        interrupt: Interrupt,
    ) -> Result<ExecutionStatus, AggregateError<T::Error>> {
        match exec_seq(&self, input, &channel, &interrupt).await {
            Ok(()) => Ok(ExecutionStatus::Completed),
            Err(ExecError::Interrupted) => Ok(ExecutionStatus::Interrupted),
            Err(ExecError::Failure(errors)) => Err(AggregateError(errors)),
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use dedent::dedent;
    use pretty_assertions::{assert_eq, assert_str_eq};
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        sync::Arc,
        time::Instant,
    };

    use super::*;
    use crate::sync::{channel, rw_lock};

    #[test]
    fn test_empty_dag() {
        let dag: Dag<i32> = Dag::default();
        assert!(dag.is_empty());
    }

    #[test]
    fn test_dag_from_list() {
        let dag = Dag::<i32>::seq(vec![1, 2, 3, 4]);
        assert_eq!(dag.to_string(), "- 1\n- 2\n- 3\n- 4");
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
        // a dag from a single branch is just a list
        let dag: Dag<i32> = dag!(seq!(1, 2, 3));
        assert_eq!(dag.to_string(), "- 1\n- 2\n- 3");
    }

    #[test]
    fn test_dag_construction() {
        let dag: Dag<i32> = seq!(1) + par!(2, 3) + seq!(4);
        assert_eq!(dag.to_string(), "- 1\n+ ~ - 2\n  ~ - 3\n- 4");
    }

    #[test]
    fn test_dags_are_immutable() {
        // Extending a DAG must not affect other DAGs built from it
        let shared: Dag<i32> = seq!(9);
        let a: Dag<i32> = seq!(1).concat(shared.clone());
        let b: Dag<i32> = seq!(2).concat(shared.clone());

        let extended = shared.clone().concat(seq!(99));

        assert_eq!(a.to_string(), "- 1\n- 9");
        assert_eq!(b.to_string(), "- 2\n- 9");
        assert_eq!(shared.to_string(), "- 9");
        assert_eq!(extended.to_string(), "- 9\n- 99");
    }

    #[test]
    fn test_equality_is_structural() {
        let two: Dag<i32> = seq!(1, 2);
        assert!(two == Dag::<i32>::seq([1, 2]));

        // a prefix is not equal to the whole
        assert!(
            two != Dag::<i32>::seq([1, 2, 3]),
            "a prefix is not the whole"
        );

        // parallel branches are not a sequence, and their order matters
        let fork: Dag<i32> = par!(1, 2);
        assert!(fork != Dag::<i32>::seq([1, 2]));
        assert!(fork != par!(2, 1), "branch order is significant");

        // what follows a parallel section is part of the comparison
        let left: Dag<i32> = par!(1, 2) + seq!(3);
        let right: Dag<i32> = par!(1, 2) + seq!(4);
        assert!(left != right, "the continuation of a fork must be compared");
    }

    #[test]
    fn test_visit_linear_graph() {
        let dag = Dag::<i32>::seq(vec![1, 2, 3]);

        for value in 1..=3 {
            assert!(dag.any(|v| *v == value), "{value} was not visited");
        }
        assert!(dag.all(|v| (1..=3).contains(v)));
        assert!(!dag.any(|v| *v == 4));
    }

    #[test]
    fn test_visit_forked_graph() {
        let dag: Dag<i32> = seq!(1, 2)
            + dag!(
                seq!(3) + dag!(seq!(4, 5), dag!(seq!(6), seq!(7)) + seq!(8)) + seq!(9),
                seq!(10) + dag!(seq!(11), seq!(12)),
            )
            + seq!(13);
        // every value is reachable, including inside nested branches
        for value in 1..=13 {
            assert!(dag.any(|v| *v == value), "{value} was not visited");
        }
        assert!(dag.all(|v| (1..=13).contains(v)));
        assert!(!dag.any(|v| *v == 14));
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
    fn test_concatenating_through_an_empty_dag_is_the_identity() {
        // Concatenating through an empty DAG should behave as if it were not there
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
        let first: Dag<i32> = seq!(1, 2);
        let second: Dag<i32> = Dag::default(); // Empty
        let third: Dag<i32> = seq!(3);

        // Chain: first + empty + third
        let result = first + second + third;
        assert!(!result.is_empty());
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
}
