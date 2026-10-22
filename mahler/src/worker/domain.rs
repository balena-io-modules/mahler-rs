use matchit::Router;
use std::slice::Iter;

use crate::error::{Error, ErrorKind};
use crate::exception::Exception;
use crate::job::Job;
use crate::json::PathArgs;
use crate::result::Result;
use crate::task::Id as TaskId;

#[derive(Default, Debug, Clone)]
pub struct Domain {
    // The router stores the position on `jobs` of the sorted list of jobs matching a route.
    // Using a position allows to add jobs to an existing route without re-inserting it
    router: Router<usize>,
    // Routes are few, so they are looked up linearly when inserting jobs
    jobs: Vec<(&'static str, Vec<Job>)>,
    // Sorted list of exceptions matching a route
    exceptions: Router<Vec<Exception>>,
    // The index stores the reverse relation of task id to a route. Domains have few jobs,
    // for which a linear search is faster than hashing the id
    index: Vec<(TaskId, &'static str)>,
}

/// Insert an item into a sorted list, keeping the existing item if an equal one exists
///
/// Lists are kept sorted (like a `BTreeSet`) but sized to their contents, as most routes
/// only have a handful of jobs.
fn insert_sorted<T: Ord>(list: &mut Vec<T>, item: T) {
    if let Err(pos) = list.binary_search(&item) {
        list.reserve_exact(1);
        list.insert(pos, item);
    }
}

impl Domain {
    pub fn new() -> Self {
        Self::default()
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
        let task_id = job.id();

        let position = match self.jobs.iter().position(|(r, _)| *r == route) {
            Some(position) => position,
            None => {
                let position = self.jobs.len();
                self.router
                    .insert(route, position)
                    .expect("route should be valid");
                self.jobs.push((route, Vec::new()));
                position
            }
        };
        insert_sorted(&mut self.jobs[position].1, job);

        // Only allow one assignment of a job to a route
        match self.index.iter().find(|(id, _)| *id == task_id) {
            Some((_, oldroute)) if *oldroute != route => panic!(
                "cannot assign '{task_id}' to route '{route}', a previous assignment exists to '{oldroute}'"
            ),
            Some(_) => {}
            None => self.index.push((task_id, route)),
        }
    }

    /// Add an exception to the domain
    pub fn exception(self, route: &'static str, exception: Exception) -> Self {
        let Self { mut exceptions, .. } = self;

        // Remove the route from the router if it exists or create
        // a new list if it doesn't
        let mut queue = exceptions.remove(route).unwrap_or_default();
        insert_sorted(&mut queue, exception);

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
        let Some(&(_, route)) = self.index.iter().find(|(id, _)| *id == task_id) else {
            return Err(Error::new(
                ErrorKind::UnregisteredJob,
                format!("{task_id} not in domain"),
            ));
        };

        let mut path = String::with_capacity(route.len());
        // keys of the arguments used by the route, in the order they first appear
        let mut used_keys: Vec<&str> = Vec::new();
        let mut missing_args: Vec<&str> = Vec::new();

        let bytes = route.as_bytes();
        // start of the route text not yet copied to the path
        let mut start = 0;
        let mut pos = 0;
        while pos < bytes.len() {
            match (bytes[pos], bytes.get(pos + 1)) {
                // `{{` and `}}` are escaped braces, keep only the first one
                (b'{', Some(b'{')) | (b'}', Some(b'}')) => {
                    path.push_str(&route[start..=pos]);
                    pos += 2;
                    start = pos;
                }
                // a `{name}` or `{*name}` parameter
                (b'{', _) => {
                    // an unterminated parameter is kept as literal text
                    let Some(len) = route[pos..].find('}') else {
                        break;
                    };
                    path.push_str(&route[start..pos]);
                    let param = &route[pos..=pos + len];
                    let inner = &param[1..param.len() - 1];
                    let name = inner.strip_prefix('*').unwrap_or(inner);
                    match args.iter().find(|(k, _)| k.as_ref() == name) {
                        Some((_, value)) => {
                            path.push_str(value);
                            if !used_keys.contains(&name) {
                                used_keys.push(name);
                            }
                        }
                        None => missing_args.push(param),
                    }
                    pos += len + 1;
                    start = pos;
                }
                _ => pos += 1,
            }
        }
        path.push_str(&route[start..]);

        // If there are non escaped {param} in the route without a value, the
        // arguments are missing and we need to fail
        if !missing_args.is_empty() {
            return Err(Error::new(
                ErrorKind::MissingArgs,
                format!(
                    "missing required arguments for job '{task_id}': {}",
                    missing_args.join(", ")
                ),
            ));
        }

        // Retain only the used keys, sorted by the order they appear in the route
        args.retain(|(k, _)| used_keys.contains(&k.as_ref()));
        args.sort_by_key(|(k, _)| used_keys.iter().position(|uk| *uk == k.as_ref()));

        Ok(path)
    }

    /// Find a job given the path and the id
    pub fn find_job(&self, path: &str, task_id: TaskId) -> Option<&Job> {
        self.router.at(path).ok().and_then(|matched| {
            self.jobs[*matched.value]
                .1
                .iter()
                .find(|job| job.id() == task_id)
        })
    }

    /// Find [`Job`] matches for the given path in the domain
    pub fn find_matching_jobs(&self, path: &str) -> Option<(PathArgs, Iter<'_, Job>)> {
        self.router
            .at(path)
            .map(|matched| {
                (
                    PathArgs::from(matched.params),
                    self.jobs[*matched.value].1.iter(),
                )
            })
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
    fn test_args_sorted_by_route_order() {
        let func = |file: View<()>| file;
        let domain = Domain::new().job("/users/{id}/files/{*path}", update(func));

        // Args provided in reverse order relative to the route
        let mut args = PathArgs::from(vec![("path", "reports/january.csv"), ("id", "42")]);
        let result = domain.find_path(func.id(), &mut args).unwrap();

        assert_eq!(result, "/users/42/files/reports/january.csv".to_string());

        // Args should be reordered to match route order: id before path
        assert_eq!(
            args,
            PathArgs::from(vec![("id", "42"), ("path", "reports/january.csv")])
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
    #[test]
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
