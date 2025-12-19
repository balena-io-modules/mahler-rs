use mahler::state::{list, List, Map, State};

// Test the derive macro works for named fields
#[derive(State, Debug, PartialEq)]
struct Service {
    pub name: String,

    pub image: Option<String>,

    pub port: u16,

    // This field should not appear in ServiceStateTarget
    #[mahler(internal)]
    pub container_id: Option<String>,
}

// Test the derive macro works for newtypes - single field tuple structs
#[derive(State, Debug, PartialEq)]
struct NewtypeId(pub u64);

// Test the derive macro works for unit structs
#[derive(State, Debug, PartialEq)]
struct UnitState;

fn to_target<T: State>(t: &T) -> Result<T::Target, serde_json::Error> {
    let value = serde_json::to_value(t)?;
    let target = serde_json::from_value(value)?;
    Ok(target)
}

#[test]
fn test_named_fields() {
    let service = Service {
        name: "web-server".to_string(),
        image: Some("nginx:latest".to_string()),
        port: 80,
        container_id: Some("abc123".to_string()),
    };

    // Test into_target conversion
    let target: ServiceTarget = to_target(&service).unwrap();
    assert_eq!(target.name, "web-server");
    assert_eq!(target.image, Some("nginx:latest".to_string()));
    assert_eq!(target.port, 80);
}

#[test]
fn test_newtype_struct() {
    let id = NewtypeId(42);

    // Test into_target conversion
    let target = to_target(&id).unwrap();
    assert_eq!(target.0, 42);
}

#[test]
fn test_unit_struct() {
    let unit = UnitState;

    // Test into_target conversion
    let _target = to_target(&unit).unwrap();

    // Unit structs work as expected
}

#[test]
fn test_serialization_compatibility() {
    let service = Service {
        name: "api".to_string(),
        image: None, // Test skip_serializing_if behavior
        port: 3000,
        container_id: Some("def456".to_string()),
    };

    let target = to_target(&service).unwrap();

    // Both should serialize None the same way for optional fields
    let service_json = serde_json::to_value(&service).unwrap();
    let target_json = serde_json::to_value(&target).unwrap();

    // Remove internal field from service JSON for comparison
    let mut service_json_obj = service_json.as_object().unwrap().clone();
    service_json_obj.remove("container_id");

    assert_eq!(serde_json::Value::Object(service_json_obj), target_json);
}

// Test nested State types
#[derive(State, Debug, Clone, PartialEq)]
struct DatabaseConfig {
    host: String,
    port: u16,
    #[mahler(internal)]
    connection_pool_size: Option<u32>,
}

#[derive(State, Debug, PartialEq)]
struct ApplicationConfig {
    name: String,
    database: DatabaseConfig,
    services: List<Service>,
    backup_database: Option<DatabaseConfig>,
}

#[test]
fn test_nested_model_types() {
    let app_config = ApplicationConfig {
        name: "my-app".to_string(),
        database: DatabaseConfig {
            host: "localhost".to_string(),
            port: 5432,
            connection_pool_size: Some(10),
        },
        services: list![
            Service {
                name: "web".to_string(),
                image: Some("nginx".to_string()),
                port: 80,
                container_id: Some("web123".to_string()),
            },
            Service {
                name: "api".to_string(),
                image: Some("node".to_string()),
                port: 3000,
                container_id: Some("api456".to_string()),
            },
        ],
        backup_database: Some(DatabaseConfig {
            host: "backup.example.com".to_string(),
            port: 5432,
            connection_pool_size: Some(5),
        }),
    };

    // Test into_target conversion
    let target = to_target(&app_config).unwrap();

    // Verify basic fields
    assert_eq!(target.name, "my-app");

    // Verify nested State type uses Target type
    assert_eq!(target.database.host, "localhost");
    assert_eq!(target.database.port, 5432);
    // connection_pool_size should not exist in DatabaseConfigTarget

    // Verify Vec<State> uses Vec<Target>
    assert_eq!(target.services.len(), 2);
    assert_eq!(target.services[0].name, "web");
    assert_eq!(target.services[0].port, 80);

    // Verify Option<State> uses Option<Target>
    let backup = target.backup_database.as_ref().unwrap();
    assert_eq!(backup.host, "backup.example.com");
    assert_eq!(backup.port, 5432);
}

#[test]
fn test_primitive_types_remain_unchanged() {
    let service = Service {
        name: "test".to_string(),
        image: None,
        port: 8080,
        container_id: Some("test123".to_string()),
    };

    let target = to_target(&service).unwrap();

    // Primitive types should be unchanged
    assert_eq!(target.name, service.name); // String -> String
    assert_eq!(target.port, service.port); // u16 -> u16
    assert_eq!(target.image, service.image); // Option<String> -> Option<String>
}

// Test enum support - only unit variants allowed
#[derive(State, Debug, PartialEq, Clone)]
enum Priority {
    Low,
    Medium,
    High,
}

#[derive(State, Debug, PartialEq)]
struct TaskConfig {
    name: String,
    priority: Priority,
    #[mahler(internal)]
    internal_id: Option<u64>,
}

#[test]
fn test_enum_model_support() {
    // Test simple enum with unit variants
    let priority = Priority::High;
    let target_priority = to_target(&priority).unwrap();
    assert_eq!(target_priority, priority);
}

#[test]
fn test_struct_with_enum_fields() {
    let task = TaskConfig {
        name: "test-task".to_string(),
        priority: Priority::High,
        internal_id: Some(999),
    };

    let target = to_target(&task).unwrap();

    // Basic fields
    assert_eq!(target.name, "test-task");

    // Enum fields should be unchanged (Target = Self for enums)
    assert_eq!(target.priority, Priority::High);

    // Internal field excluded
    // target.internal_id doesn't exist in TaskConfigTarget
}

#[test]
fn test_enum_serialization_compatibility() {
    let priority = Priority::Medium;
    let target = to_target(&priority).unwrap();

    // Both should serialize identically since Target = Self for enums
    let priority_json = serde_json::to_value(&priority).unwrap();
    let target_json = serde_json::to_value(&target).unwrap();

    assert_eq!(priority_json, target_json);
}

#[test]
fn test_map_model_support() {
    let mut map: Map<String, i32> = Map::new();
    map.insert("key1".to_string(), 100);
    map.insert("key2".to_string(), 200);

    let target = to_target(&map).unwrap();

    assert_eq!(target.get("key1"), Some(&100));
    assert_eq!(target.get("key2"), Some(&200));
    assert_eq!(target.len(), 2);
}

#[test]
fn test_nested_collections_model_support() {
    // Test HashMap with State values
    let mut services: Map<String, Service> = Map::new();
    services.insert(
        "web".to_string(),
        Service {
            name: "web-service".to_string(),
            image: Some("nginx".to_string()),
            port: 80,
            container_id: Some("web123".to_string()),
        },
    );
    services.insert(
        "api".to_string(),
        Service {
            name: "api-service".to_string(),
            image: Some("node".to_string()),
            port: 3000,
            container_id: Some("api456".to_string()),
        },
    );

    let target = to_target(&services).unwrap();

    assert_eq!(target.len(), 2);
    let web_service = target.get("web").unwrap();
    assert_eq!(web_service.name, "web-service");
    assert_eq!(web_service.port, 80);

    let api_service = target.get("api").unwrap();
    assert_eq!(api_service.name, "api-service");
    assert_eq!(api_service.port, 3000);
}

#[test]
fn test_basic_serialization() {
    #[derive(State, Debug, Clone)]
    struct Config {
        database_host: String,
        api_endpoint: String,
        optional_field: Option<String>,
        #[mahler(internal)]
        internal_connection: String,
    }

    let original = Config {
        database_host: "localhost".to_string(),
        api_endpoint: "https://api.example.com".to_string(),
        optional_field: None,
        internal_connection: "connection_pool".to_string(),
    };

    let target = ConfigTarget {
        database_host: "localhost".to_string(),
        api_endpoint: "https://api.example.com".to_string(),
        optional_field: None,
    };

    // Test serialization
    let original_json = serde_json::to_string(&original).unwrap();
    let target_json = serde_json::to_string(&target).unwrap();

    assert!(original_json.contains("database_host"));
    assert!(original_json.contains("api_endpoint"));
    assert!(target_json.contains("database_host"));
    assert!(target_json.contains("api_endpoint"));
}

#[test]
fn test_mahler_derive_attribute() {
    #[derive(State)]
    #[mahler(derive(PartialEq, Eq))]
    struct Database {
        host: String,
        port: u16,
        #[mahler(internal)]
        connection_pool: Option<String>,
    }

    let db = Database {
        host: "localhost".to_string(),
        port: 5432,
        connection_pool: Some("pool".to_string()),
    };

    let target = to_target(&db).unwrap();

    // Test that the derives work
    assert_eq!(target.host, "localhost");
    assert_eq!(target.port, 5432);

    // Test Debug derive (default)
    let debug_str = format!("{target:?}");
    assert!(debug_str.contains("localhost"));

    // Test Clone derive (default)
    let cloned = target.clone();
    assert_eq!(cloned.host, "localhost");

    // Test PartialEq and Eq derives (from #[mahler(derive(...))])
    let target2 = DatabaseTarget {
        host: "localhost".to_string(),
        port: 5432,
    };
    assert_eq!(target, target2);
}

#[test]
fn test_option_default_deserialization() {
    #[derive(State, Debug, PartialEq)]
    struct Config {
        name: String,
        port: Option<u16>,
        host: Option<String>,
        enabled: Option<bool>,
    }

    // Test deserialization with missing Option fields
    let json_str = r#"{"name": "test", "port": 8080}"#;
    let config: Config = serde_json::from_str(json_str).unwrap();

    assert_eq!(config.name, "test");
    assert_eq!(config.port, Some(8080));
    assert_eq!(config.host, None); // Should default to None
    assert_eq!(config.enabled, None); // Should default to None

    // Test with all fields present
    let json_str2 = r#"{"name": "prod", "port": 3000, "host": "localhost", "enabled": true}"#;
    let config2: Config = serde_json::from_str(json_str2).unwrap();

    assert_eq!(config2.name, "prod");
    assert_eq!(config2.port, Some(3000));
    assert_eq!(config2.host, Some("localhost".to_string()));
    assert_eq!(config2.enabled, Some(true));
}

#[test]
fn test_option_skip_none_serialization() {
    #[derive(State, Debug)]
    struct Settings {
        title: String,
        description: Option<String>,
        count: Option<i32>,
    }

    // Test that None values are skipped in serialization
    let settings = Settings {
        title: "Test".to_string(),
        description: Some("A test".to_string()),
        count: None,
    };

    let json = serde_json::to_value(&settings).unwrap();

    assert_eq!(json.get("title"), Some(&serde_json::json!("Test")));
    assert_eq!(json.get("description"), Some(&serde_json::json!("A test")));
    assert_eq!(json.get("count"), None); // Should be skipped, not null

    // Test with all None
    let settings2 = Settings {
        title: "Empty".to_string(),
        description: None,
        count: None,
    };

    let json2 = serde_json::to_value(&settings2).unwrap();

    assert_eq!(json2.get("title"), Some(&serde_json::json!("Empty")));
    assert_eq!(json2.get("description"), None); // Skipped
    assert_eq!(json2.get("count"), None); // Skipped
}

#[test]
fn test_collection_default_deserialization() {
    use mahler::state::{List, Map, Set};

    #[derive(State, Debug, PartialEq)]
    struct AppConfig {
        name: String,
        services: List<String>,
        tags: Set<String>,
        env: Map<String, String>,
    }

    // Test deserialization with missing collection fields
    let json_str = r#"{"name": "my-app"}"#;
    let config: AppConfig = serde_json::from_str(json_str).unwrap();

    assert_eq!(config.name, "my-app");
    assert_eq!(config.services, List::new()); // Should default to empty
    assert_eq!(config.tags, Set::new()); // Should default to empty
    assert_eq!(config.env, Map::new()); // Should default to empty
    assert!(config.services.is_empty());
    assert!(config.tags.is_empty());
    assert!(config.env.is_empty());

    // Test with all fields present
    let json_str2 = r#"{
        "name": "prod-app",
        "services": ["web", "api"],
        "tags": ["production", "v1"],
        "env": {"PORT": "8080", "HOST": "localhost"}
    }"#;
    let config2: AppConfig = serde_json::from_str(json_str2).unwrap();

    assert_eq!(config2.name, "prod-app");
    assert_eq!(config2.services.len(), 2);
    assert!(config2.services.contains(&"web".to_string()));
    assert!(config2.services.contains(&"api".to_string()));
    assert_eq!(config2.tags.len(), 2);
    assert!(config2.tags.contains(&"production".to_string()));
    assert_eq!(config2.env.len(), 2);
    assert_eq!(config2.env.get("PORT"), Some(&"8080".to_string()));
}
