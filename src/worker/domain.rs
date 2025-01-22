use matchit::Router;
use std::collections::{BTreeSet, HashMap};

use super::intent::{Intent, Operation};
use crate::path::PathArgs;

#[derive(Default)]
pub struct Domain {
    // The router stores a list of intents matching a route
    router: Router<BTreeSet<Intent>>,
    // The index stores the reverse relation of job id to a route
    index: HashMap<Box<str>, String>,
}

// Placeholder string to replace escaped parameters
// in a route
const PLACEHOLDER: &str = "__gustav_placeholder__";

impl Domain {
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            index: HashMap::new(),
        }
    }

    // TODO: it would be great to figure out a way to validate
    // that the pointer is valid for the parent state at compile time
    pub fn job(self, route: &'static str, intent: Intent) -> Self {
        let Self {
            mut router,
            mut index,
        } = self;

        let job_id = String::from(intent.job.id());
        let operation = intent.operation.clone();

        // Remove the route from the router if it exists or create
        // a new set if it doesn't
        let mut queue = router.remove(route).unwrap_or_default();

        // Do not allow the same job to be assigned to
        // multiple operations. This could cause problems at
        // runtime
        if queue.iter().any(|i| i.job.id() == job_id) {
            panic!(
                "cannot assign job '{}' to operation '{:?}', a previous assignment exists",
                job_id, operation
            )
        }

        // Insert the route to the queue
        queue.insert(intent);

        // (re)insert the queue to the router, we should not have
        // conflicts here
        router.insert(route, queue).expect("route should be valid");

        // Only allow one assignment of a job to a route
        if let Some(oldroute) = index.insert(job_id.clone().into_boxed_str(), String::from(route)) {
            panic!(
                "cannot assign job '{}' to route '{}', a previous assignment exists to '{}'",
                job_id, route, oldroute
            )
        }

        Self { router, index }
    }

    // This allows to find the path that a task relates to from the
    // job it belongs to and the arguments given by the user as part
    // of the context.
    //
    // This implementation is still missing a ton of edge cases but should
    // work as a proof of concept
    //
    // This will no longer be dead code when the planner
    // is implemented
    pub(crate) fn get_path(&self, job_id: &str, args: PathArgs) -> Option<String> {
        if let Some(route) = self.index.get(job_id) {
            let mut route = route.clone();
            let placeholder = PLACEHOLDER.to_string();

            // for each key in path args look for a parameter
            // in the route and replace it by the value
            for (k, v) in args.0.iter() {
                // look for double bracket versions first and replace
                // by a placeholder
                let escaped = format!("{{{{{}}}}}", k);
                route = route.replace(&escaped, &placeholder);

                let param = format!("{{{}}}", k);
                route = route.replace(&param, v);

                // TODO: we should also replace wildcards (`{*param}`)
                // with their corresponding argument

                // Replace placeholder for its unescaped version
                route = route.replace(&placeholder, &escaped);
            }

            // TODO: for each escaped value `{{param}}` we should replace it
            // with `{param}`

            // QUESTION: Should be fail if there are still parameters?
            return Some(route);
        }

        None
    }

    /// Find matches for the given path in the domain
    /// the matches are sorted in order that they should be
    /// tested
    pub(crate) fn at(&self, path: &str) -> Option<(PathArgs, impl Iterator<Item = &Intent>)> {
        self.router
            .at(path)
            .map(|matched| {
                (
                    PathArgs::from(matched.params),
                    matched
                        .value
                        .iter()
                        .filter(|i| i.operation != Operation::None),
                )
            })
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::extract::{Target, Update};
    use crate::path::PathArgs;
    use crate::task::*;
    use crate::worker::*;

    fn plus_one(mut counter: Update<i32>, tgt: Target<i32>) -> Update<i32> {
        if *counter < *tgt {
            *counter += 1;
        }

        // Update implements IntoResult
        counter
    }

    fn plus_two(counter: Update<i32>, tgt: Target<i32>) -> Vec<Task> {
        if *tgt - *counter < 2 {
            // Returning an empty result tells the planner
            // the task is not applicable to reach the target
            return vec![];
        }

        vec![
            plus_one.into_task(Context::new().with_target(*tgt)),
            plus_one.into_task(Context::new().with_target(*tgt)),
        ]
    }

    #[test]
    fn it_finds_jobs_ordered_by_degree() {
        let domain = Domain::new()
            .job("/counters/{counter}", update(plus_one))
            .job("/counters/{counter}", update(plus_two));

        let jobs: Vec<&str> = domain
            .at("/counters/{counter}")
            .map(|(_, iter)| iter.map(|i| i.job.id()).collect())
            .unwrap();

        // It should return compound jobs first
        assert_eq!(
            jobs,
            vec![plus_two.into_job().id(), plus_one.into_job().id()]
        );
    }

    #[test]
    fn it_ignores_none_jobs() {
        let domain = Domain::new()
            .job("/counters/{counter}", none(plus_one))
            .job("/counters/{counter}", update(plus_two));

        let jobs: Vec<&str> = domain
            .at("/counters/{counter}")
            .map(|(_, iter)| iter.map(|i| i.job.id()).collect())
            .unwrap();

        // It should not return jobs for None operations
        assert_eq!(jobs, vec![plus_two.into_job().id()]);
    }

    #[test]
    fn it_constructs_a_path_given_arguments() {
        let domain = Domain::new()
            .job("/counters/{counter}", none(plus_one))
            .job("/counters/{counter}", update(plus_two));

        let args = PathArgs(vec![(Arc::from("counter"), String::from("one"))]);
        let path = domain.get_path(plus_one.into_job().id(), args).unwrap();
        assert_eq!(path, String::from("/counters/one"))
    }

    #[test]
    #[should_panic]
    fn it_fails_if_assigning_the_same_job_to_multiple_ops() {
        Domain::new()
            .job("/counters/{counter}", update(plus_one))
            .job("/counters/{counter}", update(plus_one));
    }

    #[test]
    #[should_panic]
    fn it_fails_if_assigning_the_same_job_to_multiple_routes() {
        Domain::new()
            .job("/counters/{counter}", update(plus_one))
            .job("/numbers/{counter}", create(plus_one));
    }
}
