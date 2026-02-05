use matchit::Router;
use std::collections::btree_set::Iter;
use std::collections::{BTreeSet, HashMap};

use crate::error::{Error, ErrorKind};
use crate::exception::Exception;
use crate::job::Job;
use crate::json::PathArgs;
use crate::result::Result;
use crate::task::Id as TaskId;

#[derive(Default, Debug, Clone)]
pub struct Domain {
    // The router stores a list of jobs matching a route
    jobs: Router<BTreeSet<Job>>,
    // List of exceptions matching a route
    exceptions: Router<BTreeSet<Exception>>,
    // The index stores the reverse relation of task id to a route
    index: HashMap<TaskId, String>,
}

impl Domain {
    pub fn new() -> Self {
        Self {
            jobs: Router::new(),
            exceptions: Router::new(),
            index: HashMap::new(),
        }
    }

    /// Add a job to a domain
    ///
    /// # Panics
    ///
    /// This function will panic if the route is not a valid path
    /// or if a job is assigned to multiple routes
    pub fn job(mut self, route: &'static str, job: Job) -> Self {
        self.insert_job(route, job);
        self
    }

    pub fn insert_job(&mut self, route: &'static str, job: Job) {
        // TODO: it would be great to figure out a way to validate
        // that the pointer is valid for the parent state at compile time
        let Self {
            ref mut jobs,
            ref mut index,
            ..
        } = self;

        let task_id = job.id();

        // Remove the route from the router if it exists or create
        // a new set if it doesn't
        let mut queue = jobs.remove(route).unwrap_or_default();
        queue.insert(job);

        // (re)insert the queue to the router, we should not have
        // conflicts here
        jobs.insert(route, queue).expect("route should be valid");

        // Only allow one assignment of a job to a route
        if let Some(oldroute) = index.insert(task_id, String::from(route)) {
            if oldroute != route {
                panic!(
                    "cannot assign '{task_id}' to route '{route}', a previous assignment exists to '{oldroute}'"
                )
            }
        }
    }

    /// Remove all jobs with the given id
    pub fn remove_job(&mut self, id: TaskId) -> bool {
        let Self {
            ref mut jobs,
            ref mut index,
            ..
        } = self;

        // find the route from the index
        if let Some(route) = index.remove(&id) {
            // Remove the route from the router if it exists
            if let Some(mut queue) = jobs.remove(&route) {
                queue.retain(|job| job.id() != id);
                // (re)insert the queue to the router
                jobs.insert(route, queue).expect("route should be valid");

                return true;
            }
        }

        false
    }

    /// Add an exception to the domain
    pub fn exception(self, route: &'static str, exception: Exception) -> Self {
        let Self { mut exceptions, .. } = self;

        // Remove the route from the router if it exists or create
        // a new set if it doesn't
        let mut queue = exceptions.remove(route).unwrap_or_default();
        queue.insert(exception);

        // (re)insert the queue to the router, we should not have
        // conflicts here
        exceptions
            .insert(route, queue)
            .expect("route should be valid");

        Self { exceptions, ..self }
    }

    pub fn jobs<const N: usize>(self, route: &'static str, list: [Job; N]) -> Self {
        list.into_iter()
            .fold(self, |domain, job| domain.job(route, job))
    }

    /// Find the path that matches the given task id, replacing the path arguments
    /// from the provided list
    ///
    /// It will also remove any unused args from the
    /// PathArgs passed as argument. This is not a great interface, but
    /// it allows to parse only once
    pub(crate) fn find_path(&self, task_id: TaskId, args: &mut PathArgs) -> Result<String> {
        if let Some(route) = self.index.get(&task_id) {
            let mut route = route.clone();
            let mut replacements = Vec::new();
            let mut used_keys = Vec::new();

            // Step 1: Look for any `{param}` or `{*param}` references
            // for every key in the argument list. If there any escaped params {{param}}, replace them
            // with a tem placeholder.
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

            // Convert any other `{{param}}` → `{param}`
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

            // If there are non escaped {param} in the list, then these
            // arguments are missing a value and we need to fail
            if !missing_args.is_empty() {
                return Err(Error::new(
                    ErrorKind::MissingArgs,
                    format!(
                        "missing required arguments for job '{task_id}': {}",
                        missing_args.join(", ")
                    ),
                ));
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
            Err(Error::new(
                ErrorKind::UnregisteredJob,
                format!("{task_id} not in domain"),
            ))
        }
    }

    /// Find a job given the path and the id
    pub fn find_job(&self, path: &str, task_id: TaskId) -> Option<&Job> {
        self.jobs
            .at(path)
            .ok()
            .and_then(|matched| matched.value.iter().find(|job| job.id() == task_id))
    }

    /// Find [`Job`] matches for the given path in the domain
    pub fn find_matching_jobs(&self, path: &str) -> Option<(PathArgs, Iter<'_, Job>)> {
        self.jobs
            .at(path)
            .map(|matched| (PathArgs::from(matched.params), matched.value.iter()))
            .ok()
    }

    /// Find [`Exception`] matches for the given path in the domain
    pub fn find_matching_exceptions(&self, path: &str) -> Option<(PathArgs, Iter<'_, Exception>)> {
        self.exceptions
            .at(path)
            .map(|matched| (PathArgs::from(matched.params), matched.value.iter()))
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::extract::{Target, View};
    use crate::job::*;
    use crate::json::PathArgs;
    use crate::task::{Handler, Task};

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
    fn it_fails_if_assigning_the_same_job_to_multiple_routes() {
        Domain::new()
            .job("/counters/{counter}", update(plus_one))
            .job("/numbers/{counter}", update(plus_one));
    }

    #[test]
    fn it_constructs_a_path_given_arguments() {
        let domain = Domain::new()
            .job("/counters/{counter}", none(plus_one))
            .job("/counters/{counter}", update(plus_two));

        let mut args = PathArgs::from(vec![("counter", "one")]);
        let path = domain.find_path(plus_one.id(), &mut args).unwrap();
        assert_eq!(path, String::from("/counters/one"))
    }

    #[test]
    fn test_wildcard_parameter_replacement() {
        let func = |file: View<()>| file;
        let domain = Domain::new().job("/files/{*path}", update(func));

        let mut args = PathArgs::from(vec![("path", "documents/report.pdf")]);
        let result = domain.find_path(func.id(), &mut args).unwrap();

        assert_eq!(result, "/files/documents/report.pdf".to_string());
    }

    #[test]
    fn test_escaped_parameters_remain() {
        let func = |file: View<()>| file;
        let domain = Domain::new().job("/data/{{counter}}/edit", update(func));

        let mut args = PathArgs::from(vec![("counter", "456")]);
        let result = domain.find_path(func.id(), &mut args).unwrap();

        assert_eq!(result, "/data/{counter}/edit".to_string()); // Escaped `{counter}` remains unchanged
        assert_eq!(args, PathArgs::default()); // counter was never used so it should have been removed
    }

    #[test]
    fn test_mixed_placeholders() {
        let func = |file: View<()>| file;
        let domain = Domain::new().job("/users/{id}/files/{{file}}/{*path}", update(func));

        let mut args = PathArgs::from(vec![
            ("id", "42"),
            ("path", "reports/january.csv"),
            ("unused", "some-value"),
        ]);
        let result = domain.find_path(func.id(), &mut args).unwrap();

        assert_eq!(
            result,
            "/users/42/files/{file}/reports/january.csv".to_string()
        );

        assert_eq!(
            args,
            PathArgs::from(vec![("id", "42"), ("path", "reports/january.csv"),])
        );
    }

    #[test]
    fn test_no_replacement_if_job_not_found() {
        let func = |file: View<()>| file;
        let domain = Domain::new();

        let mut args = PathArgs::from(vec![("counter", "999")]);

        let result = domain.find_path(func.id(), &mut args);
        assert!(result.is_err());
    }

    #[test]
    fn test_error_if_unmatched_placeholders_remain() {
        let func = |file: View<()>| file;
        let domain = Domain::new().job("/tasks/{task_id}/check", update(func));

        let mut args = PathArgs::default(); // No arguments provided
        let result = domain.find_path(func.id(), &mut args);
        assert!(result.is_err());
    }

    // See: https://github.com/ibraheemdev/matchit/issues/75
    // we ignore it for now until matchit releases a new version
    #[test]
    #[ignore]
    fn test_finds_jobs_for_empty_paths() {
        let func = |view: View<()>| view;
        let domain = Domain::new()
            .job("", update(func))
            .job("/other", update(plus_two));

        if let Some((_, mut jobs)) = domain.find_matching_jobs("") {
            assert!(jobs.any(|j| j.id() == func.id()));
        } else {
            panic!("should find a job")
        }
    }
}
