use async_trait::async_trait;
use std::fmt;
use std::ops::Add;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use crate::ack_channel::Sender;

type Link<T> = Option<Arc<RwLock<Node<T>>>>;

/**
* A node in a DAG is a recursive data structure that
* can represent either a value, a fork in the graph,
* or a joining of paths.
*
* For instance, the DAG below (reading from left to right)
*
* ```text
*         + - c - d - +
* a - b - +           + - g
*         + - e - f - +
* ```
*
* will contain 7 value nodes (a-g), one fork node (after b) and one join node
* (before g)
*/
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
pub struct Dag<T> {
    head: Link<T>,
    tail: Link<T>,
}

impl<T> Default for Dag<T> {
    fn default() -> Self {
        Dag {
            head: None,
            tail: None,
        }
    }
}

impl<T> Dag<T> {
    pub fn new() -> Self {
        Dag::default()
    }

    /// Check if the DAG is empty
    ///
    /// # Example
    /// ```rust
    /// use gustav::Dag;
    ///
    /// let dag: Dag<i32> = Dag::default();
    /// assert!(dag.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.tail.is_none()
    }

    pub fn with_head(self, value: T) -> Dag<T> {
        let head = Node::item(value, self.head).into_link();
        let mut tail = self.tail;

        if tail.is_none() {
            tail = head.clone();
        }

        Dag { head, tail }
    }

    /// Concatenate a DAG at the head
    /// rather than the tail.
    ///
    /// This allows to build the DAG backwards and reverse
    /// it later using `reverse`
    pub fn prepend(self, dag: Dag<T>) -> Dag<T> {
        let Dag { head, tail } = dag;

        if let Some(tail_rc) = tail {
            match *tail_rc.write().unwrap() {
                Node::Item { ref mut next, .. } => *next = self.head,
                Node::Join { ref mut next } => *next = self.head,
                _ => unreachable!(),
            }
        } else {
            return self;
        }

        Dag {
            head,
            tail: self.tail,
        }
    }

    /// Link the current Dag to the Dag
    /// passed as argument
    pub fn concat(self, other: Dag<T>) -> Self {
        debug_assert!(self.tail.is_some());
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
        }

        Dag {
            head: self.head,
            tail: other.tail,
        }
    }

    /// Return an iterator over the Dag
    ///
    /// This function is not public as not to expose the Dag internal implementation
    /// details
    fn iter(&self) -> Iter<T> {
        Iter {
            stack: vec![(self.head.clone(), Vec::new())],
        }
    }

    /// Test if there is some node in the Dag that meets the
    /// condition given as argument
    pub fn some(&self, condition: impl Fn(&T) -> bool) -> bool {
        for node in self.iter() {
            if let Node::Item { value, .. } = &*node.read().unwrap() {
                if condition(value) {
                    return true;
                }
            }
        }
        false
    }

    /// Test if every node in the DAG meets the condition given
    /// as argument
    pub fn every(&self, condition: impl Fn(&T) -> bool) -> bool {
        for node in self.iter() {
            if let Node::Item { value, .. } = &*node.read().unwrap() {
                if !condition(value) {
                    return false;
                }
            }
        }
        true
    }

    /// Traverse the DAG sequentially, applying a fold function at each node.
    ///
    /// # Arguments
    /// - `initial`: Initial accumulator value.
    /// - `fold_fn`: Function to process each node and update the accumulator.
    ///
    /// # Example
    /// ```rust
    /// use gustav::Dag;
    /// use gustav::{dag,seq};
    ///
    /// let dag: Dag<char> = dag!(seq!('h', 'e', 'l', 'l', 'o'), seq!(' '), seq!('m', 'y'))
    ///        + seq!(' ')
    ///        + dag!(
    ///            seq!('o', 'l', 'd'),
    ///            seq!(' '),
    ///            seq!('f', 'r', 'i', 'e', 'n', 'd')
    ///        );
    ///        
    ///    let msg = dag.fold(
    ///        String::new(),
    ///        |acc, c| acc + c.to_string().as_ref(),
    ///    );
    ///
    ///    assert_eq!(msg, "hello my old friend")
    /// ```
    pub fn fold<U>(&self, initial: U, fold_fn: impl Fn(U, &T) -> U) -> U {
        let mut acc = initial;
        for node in self.iter() {
            if let Node::Item { value, .. } = &*node.read().unwrap() {
                acc = fold_fn(acc, value);
            }
        }
        acc
    }

    /// Create a linear DAG from a list of elements.
    ///
    /// # Arguments
    /// - `elems`: A vector of elements to include in the DAG.
    ///
    /// # Returns
    /// A `Dag` where each element is a node in sequence.
    ///
    /// # Example
    /// ```rust
    /// use gustav::Dag;
    ///
    /// let dag: Dag<i32> = Dag::from_sequence(vec![1, 2, 3]);
    /// assert_eq!(dag.to_string(), "- 1\n- 2\n- 3");
    /// ```
    pub fn from_sequence(elems: impl IntoIterator<Item = impl Into<T>>) -> Dag<T> {
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

    /// Create a forking DAG from a list of branches
    ///
    /// # Arguments
    /// - `branches`: a vector of Dag instaances to use as branches
    ///
    /// # Returns
    /// A new forking `Dag` where each branch corresponds to one of the DAGs
    /// given as input
    ///
    /// # Example
    /// ```rust
    /// use gustav::Dag;
    ///
    /// let br1: Dag<i32> = Dag::from_sequence([1, 2, 3]);
    /// let br2: Dag<i32> = Dag::from_sequence([4, 5, 6]);
    /// let dag: Dag<i32> = Dag::from_branches([br1, br2]);
    /// assert_eq!(dag.to_string(), "+ ~ - 1\n    - 2\n    - 3\n  ~ - 4\n    - 5\n    - 6");
    /// ```
    pub fn from_branches(branches: impl IntoIterator<Item = Dag<T>>) -> Dag<T> {
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
            return Dag::new();
        }

        Dag {
            head: Node::fork(next).into_link(),
            tail,
        }
    }

    /**
     * Return a new DAG that is the reverse of the
     * current DAG
     *
     * e.g. reversing
     *
     * ```text
     *         + - c - d - +
     * a - b - +           + - g
     *         + - e - f - +
     * ```
     *
     * should return
     *
     * ```text
     *     + - d - c - +
     * g - +           + - b - a
     *     + - f - e - +
     * ```
     *
     */
    pub fn reverse(self) -> Dag<T> {
        let Dag { head, .. } = self;
        let tail = head.clone();
        let mut stack = vec![(head, None as Link<T>, Vec::<usize>::new())];
        let mut results: Vec<Link<T>> = Vec::new();

        while let Some((head, prev, branching)) = stack.pop() {
            if let Some(node_rc) = head.clone() {
                match *node_rc.write().unwrap() {
                    Node::Item { ref mut next, .. } => {
                        // copy the next node to continue the operation
                        let newhead = next.clone();
                        // Update the pointer to point to the previous node
                        *next = prev;

                        // Add the next node to the stack
                        stack.push((newhead, head, branching));
                    }
                    Node::Fork { ref next } => {
                        // Create a join node for branches to point to
                        let prev = Node::join(prev).into_link();

                        for (i, br_head) in next.iter().rev().enumerate() {
                            // Keep track of the branch within the DAG
                            let mut branching = branching.clone();
                            branching.push(i);

                            // Add a new stack item for each branch
                            stack.push((br_head.clone(), prev.clone(), branching));
                        }
                    }
                    Node::Join { ref next } => {
                        // Store the accumulated result when reaching a join node
                        results.push(prev);

                        let mut branching = branching;
                        if let Some(branch) = branching.pop() {
                            // But only join the branches on the last branch
                            if branch == 0 {
                                // Reverse the branches so iterating returns
                                // the right order
                                results.reverse();
                                let head = Node::fork(results).into_link();
                                stack.push((next.clone(), head, branching));

                                // Clear the branch list
                                results = Vec::new();
                            }
                        }
                    }
                }
            } else {
                results.push(prev);
            }
        }

        if results.is_empty() {
            Dag::new()
        } else {
            debug_assert!(results.len() == 1);
            let head = results.pop().unwrap();
            Dag { head, tail }
        }
    }
}

impl<T> Add for Dag<T> {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        self.concat(other)
    }
}

/// Convert the DAG into a formatted string representation.
///
/// # Example
/// ```rust
/// use gustav::{Dag, dag, seq};
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
                    write!(f, "- {}", value)?;

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

#[macro_export]
macro_rules! seq {
    ($($value:expr),* $(,)?) => {
        Dag::from_sequence([$($value),*])
    };
}

#[macro_export]
macro_rules! dag {
    ($($branch:expr),* $(,)?) => {
        Dag::from_branches([$($branch),*])
    };
}

pub enum ExecutionStatus {
    Completed,
    Interrupted,
}

#[async_trait]
pub trait Task {
    type Input;
    type Changes;
    type Error;

    async fn run(&self, input: &Self::Input) -> Result<Self::Changes, Self::Error>;
}

impl<T> Dag<T> {
    pub(crate) async fn execute(
        self,
        input: &Arc<tokio::sync::RwLock<T::Input>>,
        channel: Sender<T::Changes>,
        sigint: &Arc<AtomicBool>,
    ) -> Result<ExecutionStatus, T::Error>
    where
        T: Task + Clone,
        T::Input: Clone,
    {
        // TODO: implement parallel execution of the DAG
        for node in self.iter() {
            let task = {
                if let Node::Item { value, .. } = &*node.read().unwrap() {
                    // This clone is necessary for now because of the iterator, but a future version
                    // of execute will just consume the DAG, avoiding the need
                    // for cloning
                    value.clone()
                } else {
                    continue;
                }
            };

            // Read the updated value
            let value = {
                let value_guard = input.read().await;
                // Unfortunately this clone is needed to avoid
                // holding on to the read lock for long running
                // operations
                value_guard.clone()
            };

            // Run the task
            let changes = task.run(&value).await?;

            // Communicate the changes to the channel
            if channel.send(changes).await.is_err()
                || sigint.load(std::sync::atomic::Ordering::Relaxed)
            {
                // Terminate the execution if the receiver closes
                // or the workflow is interrupted
                return Ok(ExecutionStatus::Interrupted);
            }
        }
        Ok(ExecutionStatus::Completed)
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use dedent::dedent;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::ack_channel::ack_channel;

    fn is_item<T>(node: &Arc<RwLock<Node<T>>>) -> bool {
        if let Node::Item { .. } = &*node.read().unwrap() {
            return true;
        }
        false
    }

    #[test]
    fn test_empty_dag() {
        let dag: Dag<i32> = Dag::new();
        assert!(dag.head.is_none());
        assert!(dag.tail.is_none());
    }

    #[test]
    fn test_dag_from_list() {
        let elements = vec![1, 2, 3, 4];
        let dag = Dag::<i32>::from_sequence(elements.clone());
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
        let dag = Dag::<i32>::from_sequence(elements.clone());

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
    fn test_contructing_linear_inverted_dag() {
        let dag: Dag<i32> = Dag::new().with_head(1).with_head(2).with_head(3);
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
        let dag: Dag<i32> = Dag::new()
            .with_head(1)
            .with_head(2)
            .with_head(3)
            .prepend(dag!(seq!(5), seq!(4)))
            .with_head(6);
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
        let dag: Dag<i32> = Dag::new()
            .with_head(1)
            .with_head(2)
            .with_head(3)
            .prepend(dag!(seq!(5), seq!(4)))
            .with_head(6)
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
    fn test_iterate_reversed_dag() {
        let dag: Dag<i32> = Dag::new()
            .with_head(1)
            .with_head(2)
            .with_head(3)
            .prepend(dag!(seq!(5), seq!(4)))
            .with_head(6)
            .reverse();
        let mut reverse: Vec<i32> = dag
            .iter()
            .filter(is_item)
            .map(|node| match &*node.read().unwrap() {
                Node::Item { value, .. } => *value,
                _ => unreachable!(),
            })
            .collect();
        reverse.reverse();

        let forward: Vec<i32> = dag
            .reverse()
            .iter()
            .filter(is_item)
            .map(|node| match &*node.read().unwrap() {
                Node::Item { value, .. } => *value,
                _ => unreachable!(),
            })
            .collect();

        assert_eq!(reverse, forward);
    }

    #[test]
    fn test_reverse_empty_dag() {
        let dag: Dag<i32> = Dag::new().reverse();
        assert!(dag.head.is_none());
        assert!(dag.tail.is_none());
    }

    #[test]
    fn test_reverse_dag_with_forks() {
        let dag: Dag<char> = seq!('A') + dag!(seq!('B', 'C'), seq!('D')) + seq!('E');
        let reversed = dag.reverse();
        assert_eq!(
            reversed.to_string(),
            dedent!(
                r#"
                - E
                + ~ - D
                  ~ - C
                    - B
                - A
                "#
            )
        );
    }

    #[test]
    fn test_empty_dag_string_representation() {
        let dag: Dag<char> = Dag::new();
        assert_eq!(dag.to_string(), "");
    }

    #[test]
    fn converts_linked_list_to_string() {
        let dag: Dag<char> = seq!('A', 'B', 'C', 'D');
        assert_eq!(
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
        assert_eq!(
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
        assert_eq!(
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
                seq!(10) + dag!(seq!(11), seq!(12)),
            )
            + seq!(13);

        assert_eq!(
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
        let (tx, mut rx) = ack_channel(10);
        let sigint = Arc::new(AtomicBool::new(false));

        let count_atomic = Arc::new(AtomicUsize::new(0));
        let counter = count_atomic.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let c = counter.load(Ordering::Relaxed);
                counter.store(c + 1, Ordering::Relaxed);
                msg.ack();
            }
        });

        let result = dag.execute(&reader, tx, &sigint).await;
        assert!(matches!(result, Ok(ExecutionStatus::Completed)));
        assert_eq!(count_atomic.load(Ordering::Relaxed), 3);
    }
}
