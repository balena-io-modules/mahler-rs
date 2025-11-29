use serde::{Deserialize, Serialize};
use serde_json::json;

use mahler::state::{list, AsInternal, List, State};

#[derive(State, Debug, Clone, PartialEq)]
struct Child {
    value: i32,
    name: String,
}

#[derive(State, Debug, Clone, PartialEq)]
struct Parent {
    id: u64,
    child: Child,
    tags: List<String>,
}

#[test]
fn test_metadata_serialization_structure() {
    let parent = Parent {
        id: 42,
        child: Child {
            value: 100,
            name: "test".to_string(),
        },
        tags: list!["tag1".to_string(), "tag2".to_string()],
    };

    // Serialize with metadata
    let with_metadata = serde_json::to_value(AsInternal(&parent)).unwrap();

    // Serialize normally
    let normal = serde_json::to_value(&parent).unwrap();

    // The metadata version should have halted at the root
    assert!(with_metadata.get("__mahler(halted)").is_some());
    assert_eq!(with_metadata.get("__mahler(halted)"), Some(&json!(false)));

    // Normal version should NOT have halted
    assert!(normal.get("__mahler(halted)").is_none());

    // Both should have the same fields (except halted)
    assert_eq!(with_metadata.get("id"), normal.get("id"));
    assert_eq!(with_metadata.get("tags"), normal.get("tags"));

    // The nested child should also have halted in metadata version
    let child_with_meta = with_metadata.get("child").unwrap();
    assert!(child_with_meta.get("__mahler(halted)").is_some());
    assert_eq!(child_with_meta.get("__mahler(halted)"), Some(&json!(false)));

    // Normal child should NOT have halted
    let child_normal = normal.get("child").unwrap();
    assert!(child_normal.get("__mahler(halted)").is_none());

    // Child fields should match (except halted)
    assert_eq!(child_with_meta.get("value"), child_normal.get("value"));
    assert_eq!(child_with_meta.get("name"), child_normal.get("name"));
}

#[test]
fn test_metadata_identical_except_halted() {
    let parent = Parent {
        id: 123,
        child: Child {
            value: 456,
            name: "example".to_string(),
        },
        tags: list!["a".to_string(), "b".to_string(), "c".to_string()],
    };

    // Serialize with metadata
    let with_metadata = serde_json::to_value(AsInternal(&parent)).unwrap();

    // Serialize normally
    let mut normal = serde_json::to_value(&parent).unwrap();

    // Add halted to normal to make it equivalent
    fn add_halted(val: &mut serde_json::Value) {
        if let serde_json::Value::Object(map) = val {
            map.insert("__mahler(halted)".to_string(), json!(false));
            for (_, v) in map.iter_mut() {
                add_halted(v);
            }
        } else if let serde_json::Value::Array(arr) = val {
            for item in arr {
                add_halted(item);
            }
        }
    }

    add_halted(&mut normal);

    // Now they should be identical
    assert_eq!(with_metadata, normal);
}

#[test]
fn test_custom_halted() {
    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct CustomState {
        active: bool,
        count: i32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct CustomStateTarget {
        active: bool,
        count: i32,
    }

    impl State for CustomState {
        type Target = CustomStateTarget;

        fn is_halted(&self) -> bool {
            !self.active
        }

        fn as_internal<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            use serde::ser::SerializeStruct;
            let mut state = serializer.serialize_struct("CustomState", 3)?;
            state.serialize_field("__mahler(halted)", &self.is_halted())?;
            state.serialize_field("active", &self.active)?;
            state.serialize_field("count", &self.count)?;
            state.end()
        }
    }

    let active_state = CustomState {
        active: true,
        count: 5,
    };

    let inactive_state = CustomState {
        active: false,
        count: 10,
    };

    // Active state should have halted: false
    let active_json = serde_json::to_value(AsInternal(&active_state)).unwrap();
    assert_eq!(active_json.get("__mahler(halted)"), Some(&json!(false)));

    // Inactive state should have halted: true (state is halted when not active)
    let inactive_json = serde_json::to_value(AsInternal(&inactive_state)).unwrap();
    assert_eq!(inactive_json.get("__mahler(halted)"), Some(&json!(true)));
}

#[test]
fn test_nested_custom_halted() {
    use AsInternal;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct Task {
        name: String,
        completed: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct TaskTarget {
        name: String,
        completed: bool,
    }

    impl State for Task {
        type Target = TaskTarget;

        fn is_halted(&self) -> bool {
            self.completed
        }

        fn as_internal<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            use serde::ser::SerializeStruct;
            let mut state = serializer.serialize_struct("Task", 3)?;
            state.serialize_field("__mahler(halted)", &self.is_halted())?;
            state.serialize_field("name", &self.name)?;
            state.serialize_field("completed", &self.completed)?;
            state.end()
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct Project {
        title: String,
        task: Task,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct ProjectTarget {
        title: String,
        task: TaskTarget,
    }

    impl State for Project {
        type Target = ProjectTarget;

        fn as_internal<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            use serde::ser::SerializeStruct;
            let mut state = serializer.serialize_struct("Project", 3)?;
            state.serialize_field("__mahler(halted)", &self.is_halted())?;
            state.serialize_field("title", &self.title)?;
            state.serialize_field("task", &AsInternal(&self.task))?;
            state.end()
        }
    }

    let project = Project {
        title: "My Project".to_string(),
        task: Task {
            name: "Task 1".to_string(),
            completed: true,
        },
    };

    let json = serde_json::to_value(AsInternal(&project)).unwrap();

    // Project should have halted: false (default)
    assert_eq!(json.get("__mahler(halted)"), Some(&json!(false)));

    // Task should have halted: true (because completed = true, state is halted)
    let task_json = json.get("task").unwrap();
    assert_eq!(task_json.get("__mahler(halted)"), Some(&json!(true)));
}

#[test]
fn test_vec_metadata_serialization() {
    // Using derive macro. `halted()` returns false by default

    #[derive(State, Debug, Clone)]
    struct Item {
        id: i32,
        active: bool,
    }

    #[derive(State, Debug, Clone)]
    struct Container {
        items: List<Item>,
    }

    let container = Container {
        items: list![
            Item {
                id: 1,
                active: true,
            },
            Item {
                id: 2,
                active: false,
            },
            Item {
                id: 3,
                active: true,
            },
        ],
    };

    let json = serde_json::to_value(AsInternal(&container)).unwrap();

    let items = json.get("items").unwrap().as_array().unwrap();
    assert_eq!(items.len(), 3);

    // All items use default halted() which returns false
    assert_eq!(items[0].get("__mahler(halted)"), Some(&json!(false)));
    assert_eq!(items[1].get("__mahler(halted)"), Some(&json!(false)));
    assert_eq!(items[2].get("__mahler(halted)"), Some(&json!(false)));
}

#[test]
fn test_option_metadata_serialization() {
    #[derive(State, Debug, Clone)]
    struct Config {
        enabled: bool,
    }

    #[derive(State, Debug, Clone)]
    struct Settings {
        config: Option<Config>,
    }

    let with_config = Settings {
        config: Some(Config { enabled: false }),
    };

    let json = serde_json::to_value(AsInternal(&with_config)).unwrap();

    let config = json.get("config").unwrap();
    // Default halted() returns false
    assert_eq!(config.get("__mahler(halted)"), Some(&json!(false)));

    let without_config = Settings { config: None };

    let json2 = serde_json::to_value(AsInternal(&without_config)).unwrap();
    // None values are now skipped in serialization
    assert_eq!(json2.get("config"), None);
}

#[test]
fn test_should_halt_if_attribute() {
    fn is_inactive(service: &Service) -> bool {
        !service.active
    }

    #[derive(State, Debug, Clone)]
    #[mahler(should_halt_if = "is_inactive")]
    struct Service {
        name: String,
        active: bool,
    }

    let active_service = Service {
        name: "web".to_string(),
        active: true,
    };

    let inactive_service = Service {
        name: "worker".to_string(),
        active: false,
    };

    // Active service should have halted: false
    let active_json = serde_json::to_value(AsInternal(&active_service)).unwrap();
    assert_eq!(active_json.get("__mahler(halted)"), Some(&json!(false)));
    assert_eq!(active_json.get("name"), Some(&json!("web")));

    // Inactive service should have halted: true
    let inactive_json = serde_json::to_value(AsInternal(&inactive_service)).unwrap();
    assert_eq!(inactive_json.get("__mahler(halted)"), Some(&json!(true)));
    assert_eq!(inactive_json.get("name"), Some(&json!("worker")));
}

#[test]
fn test_should_halt_if_with_nested_types() {
    fn is_completed(task: &Task) -> bool {
        task.completed
    }

    #[derive(State, Debug, Clone)]
    #[mahler(should_halt_if = "is_completed")]
    struct Task {
        name: String,
        completed: bool,
    }

    #[derive(State, Debug, Clone)]
    struct Project {
        title: String,
        tasks: List<Task>,
    }

    let project = Project {
        title: "My Project".to_string(),
        tasks: list![
            Task {
                name: "Task 1".to_string(),
                completed: true,
            },
            Task {
                name: "Task 2".to_string(),
                completed: false,
            },
        ],
    };

    let json = serde_json::to_value(AsInternal(&project)).unwrap();

    // Project should have halted: false (default)
    assert_eq!(json.get("__mahler(halted)"), Some(&json!(false)));

    // Check tasks
    let tasks = json.get("tasks").unwrap().as_array().unwrap();
    assert_eq!(tasks.len(), 2);

    // First task is completed, so halted: true
    assert_eq!(tasks[0].get("__mahler(halted)"), Some(&json!(true)));
    assert_eq!(tasks[0].get("name"), Some(&json!("Task 1")));

    // Second task is not completed, so halted: false
    assert_eq!(tasks[1].get("__mahler(halted)"), Some(&json!(false)));
    assert_eq!(tasks[1].get("name"), Some(&json!("Task 2")));
}

#[test]
fn test_should_halt_if_with_impl_method() {
    #[derive(State, Debug, Clone)]
    #[mahler(should_halt_if = "Self::is_inactive")]
    struct Item {
        id: i32,
        active: bool,
    }

    impl Item {
        fn is_inactive(&self) -> bool {
            !self.active
        }
    }

    let active_item = Item {
        id: 1,
        active: true,
    };

    let inactive_item = Item {
        id: 2,
        active: false,
    };

    // Active item should have halted: false
    let active_json = serde_json::to_value(AsInternal(&active_item)).unwrap();
    assert_eq!(active_json.get("__mahler(halted)"), Some(&json!(false)));

    // Inactive item should have halted: true
    let inactive_json = serde_json::to_value(AsInternal(&inactive_item)).unwrap();
    assert_eq!(inactive_json.get("__mahler(halted)"), Some(&json!(true)));
}
