use anyhow::anyhow;
use matchit::Router;
use std::collections::btree_set::Iter;
use std::collections::{BTreeSet, HashMap};
use thiserror::Error;

use crate::path::PathArgs;
use crate::task::Job;

#[derive(Debug, Error)]
#[error(transparent)]
pub struct PathSearchError(#[from] anyhow::Error);

#[derive(Default, Debug, Clone)]
pub struct Domain {
    // The router stores a list of jobs matching a route
    router: Router<BTreeSet<Job>>,
    // The index stores the reverse relation of job id to a route
    index: HashMap<Box<str>, String>,
}

impl Domain {
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            index: HashMap::new(),
        }
    }

    /// Add a job to a domain
    ///
    /// # Panics
    ///
    /// This function will panic if the route is not a valid path
    /// or if a job is assigned to multiple routes
    pub fn job(self, route: &'static str, job: Job) -> Self {
        // TODO: it would be great to figure out a way to validate
        // that the pointer is valid for the parent state at compile time
        let Self {
            mut router,
            mut index,
        } = self;

        let job_id = String::from(job.id());
        let operation = job.operation();

        // Remove the route from the router if it exists or create
        // a new set if it doesn't
        let mut queue = router.remove(route).unwrap_or_default();

        // Do not allow the same job to be assigned to
        // multiple operations. This could cause problems at
        // runtime
        if queue.iter().any(|j| j.id() == job_id) {
            panic!(
                "cannot assign job '{job_id}' to operation '{operation:?}', a previous assignment exists"
            )
        }

        // Insert the route to the queue
        let updated = queue.insert(job);

        // (re)insert the queue to the router, we should not have
        // conflicts here
        router.insert(route, queue).expect("route should be valid");

        // Only allow one assignment of a job to a route
        if updated {
            if let Some(oldroute) =
                index.insert(job_id.clone().into_boxed_str(), String::from(route))
            {
                panic!(
                    "cannot assign job '{job_id}' to route '{route}', a previous assignment exists to '{oldroute}'"
                )
            }
        }

        Self { router, index }
    }

    pub fn jobs<const N: usize>(self, route: &'static str, list: [Job; N]) -> Self {
        list.into_iter()
            .fold(self, |domain, job| domain.job(route, job))
    }

    // This allows to find the path that a task relates to from the
    // job it belongs to and the arguments given by the user as part
    // of the context. It will also remove any unused args from the
    // PathArgs passed as argument. This is not a great interface, but
    // it allows to parse only once
    pub(crate) fn find_path_for_job(
        &self,
        job_id: &str,
        args: &mut PathArgs,
    ) -> Result<String, PathSearchError> {
        if let Some(route) = self.index.get(job_id) {
            let mut route = route.clone();
            let mut replacements = Vec::new();
            let mut used_keys = Vec::new();

            // Step 1: Replace `{param}` and `{*param}` placeholders
            for (k, v) in args.iter() {
                let param = format!("{{{k}}}");
                let wildcard_param = format!("{{*{k}}}");
                let escaped_param = format!("{{{{{k}}}}}");

                // Replace escaped parameters with a temp placeholder, but do not mark as used
                let placeholder = format!("__ESCAPED_{k}__");
                route = route.replace(&escaped_param, &placeholder);

                // Only mark as used if actual replacement occurs
                if route.contains(&param) || route.contains(&wildcard_param) {
                    used_keys.push(k.clone());
                    replacements.push((param, v.clone()));
                    replacements.push((wildcard_param, v.clone()));
                }
            }

            // Apply replacements
            for (param, value) in replacements {
                route = route.replace(&param, &value);
            }

            // Convert `{{param}}` → `{param}`
            let mut final_route = String::new();
            let mut chars = route.chars().peekable();
            let mut missing_args = Vec::new();

            while let Some(c) = chars.next() {
                if c == '{' && chars.peek() == Some(&'{') {
                    chars.next(); // Skip second '{'
                    final_route.push('{');
                } else if c == '}' && chars.peek() == Some(&'}') {
                    chars.next(); // Skip second '}'
                    final_route.push('}');
                } else if c == '{' {
                    let mut placeholder = String::from("{");
                    while let Some(&next) = chars.peek() {
                        placeholder.push(next);
                        chars.next();
                        if next == '}' {
                            break;
                        }
                    }

                    if placeholder.ends_with('}') {
                        missing_args.push(placeholder.clone());
                    }

                    final_route.push_str(&placeholder);
                } else {
                    final_route.push(c);
                }
            }

            if !missing_args.is_empty() {
                return Err(anyhow!(
                    "missing arguments for task {job_id}: {:?}",
                    missing_args
                ))?;
            }

            // Restore escaped parameters
            for (k, _) in args.iter() {
                let placeholder = format!("__ESCAPED_{k}__");
                let param = format!("{{{k}}}");
                final_route = final_route.replace(&placeholder, &param);
            }

            // Retain only the used keys (excluding those used only as escaped)
            args.retain(|(k, _)| used_keys.contains(k));

            Ok(final_route)
        } else {
            Err(anyhow!(
                "could not find a job with id {job_id} in the search domain"
            ))?
        }
    }

    // Find a job given the path and the id
    pub(crate) fn find_job(&self, path: &str, job_id: &str) -> Option<&Job> {
        self.router
            .at(path)
            .ok()
            .and_then(|matched| matched.value.iter().find(|job| job.id() == job_id))
    }

    /// Find matches for the given path in the domain
    /// the matches are sorted in order that they should be
    /// tested
    pub(crate) fn find_matching_jobs(&self, path: &str) -> Option<(PathArgs, Iter<Job>)> {
        self.router
            .at(path)
            .map(|matched| (PathArgs::from(matched.params), matched.value.iter()))
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::extract::{Target, View};
    use crate::path::PathArgs;
    use crate::task::*;

    fn plus_one(mut counter: View<i32>, tgt: Target<i32>) -> View<i32> {
        if *counter < *tgt {
            *counter += 1;
        }

        // Update implements IntoResult
        counter
    }

    fn plus_two(counter: View<i32>, tgt: Target<i32>) -> Vec<Task> {
        if *tgt - *counter < 2 {
            // Returning an empty result tells the planner
            // the task is not applicable to reach the target
            return vec![];
        }

        vec![plus_one.with_target(*tgt), plus_one.with_target(*tgt)]
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

    #[test]
    fn it_constructs_a_path_given_arguments() {
        let domain = Domain::new()
            .job("/counters/{counter}", none(plus_one))
            .job("/counters/{counter}", update(plus_two));

        let mut args = PathArgs(vec![(Arc::from("counter"), String::from("one"))]);
        let path = domain.find_path_for_job(plus_one.id(), &mut args).unwrap();
        assert_eq!(path, String::from("/counters/one"))
    }

    #[test]
    fn test_wildcard_parameter_replacement() {
        let func = |file: View<()>| file;
        let domain = Domain::new().job("/files/{*path}", update(func));

        let mut args = PathArgs(vec![(
            Arc::from("path"),
            "documents/report.pdf".to_string(),
        )]);
        let result = domain.find_path_for_job(func.id(), &mut args).unwrap();

        assert_eq!(result, "/files/documents/report.pdf".to_string());
    }

    #[test]
    fn test_escaped_parameters_remain() {
        let func = |file: View<()>| file;
        let domain = Domain::new().job("/data/{{counter}}/edit", update(func));

        let mut args = PathArgs(vec![(Arc::from("counter"), "456".to_string())]);
        let result = domain.find_path_for_job(func.id(), &mut args).unwrap();

        assert_eq!(result, "/data/{counter}/edit".to_string()); // Escaped `{counter}` remains unchanged
        assert_eq!(args, PathArgs(vec![])); // counter was never used so it should have been removed
    }

    #[test]
    fn test_mixed_placeholders() {
        let func = |file: View<()>| file;
        let domain = Domain::new().job("/users/{id}/files/{{file}}/{*path}", update(func));

        let mut args = PathArgs(vec![
            (Arc::from("id"), "42".to_string()),
            (Arc::from("path"), "reports/january.csv".to_string()),
            (Arc::from("unused"), "some-value".to_string()),
        ]);
        let result = domain.find_path_for_job(func.id(), &mut args).unwrap();

        assert_eq!(
            result,
            "/users/42/files/{file}/reports/january.csv".to_string()
        );

        assert_eq!(
            args,
            PathArgs(vec![
                (Arc::from("id"), "42".to_string()),
                (Arc::from("path"), "reports/january.csv".to_string()),
            ])
        );
    }

    #[test]
    fn test_no_replacement_if_job_not_found() {
        let func = |file: View<()>| file;
        let domain = Domain::new();

        let mut args = PathArgs(vec![(Arc::from("counter"), "999".to_string())]);

        let result = domain.find_path_for_job(func.id(), &mut args);
        assert!(result.is_err());
    }

    #[test]
    fn test_error_if_unmatched_placeholders_remain() {
        let func = |file: View<()>| file;
        let domain = Domain::new().job("/tasks/{task_id}/check", update(func));

        let mut args = PathArgs(vec![]); // No arguments provided
        let result = domain.find_path_for_job(func.id(), &mut args);
        assert!(result.is_err());
    }
}
