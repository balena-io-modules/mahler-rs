use mahler::State;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

// Test the derive macro works for named fields
#[derive(State, Serialize, Deserialize, Debug, PartialEq)]
struct Service {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    pub port: u16,

    // This field should not appear in ServiceStateTarget
    #[mahler(internal)]
    pub container_id: Option<String>,
}

// Test the derive macro works for tuple
#[derive(State, Serialize, Deserialize, Debug, PartialEq)]
struct Point(pub f64, pub f64);

// Test the derive macro works for unit structs
#[derive(State, Serialize, Deserialize, Debug, PartialEq)]
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
fn test_tuple_struct() {
    let point = Point(1.0, 2.0);

    // Test into_target conversion
    let target = to_target(&point).unwrap();
    assert_eq!(target.0, 1.0);
    assert_eq!(target.1, 2.0);
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
#[derive(State, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct DatabaseConfig {
    host: String,
    port: u16,
    #[mahler(internal)]
    connection_pool_size: Option<u32>,
}

#[derive(State, Serialize, Deserialize, Debug, PartialEq)]
struct ApplicationConfig {
    name: String,
    database: DatabaseConfig,
    services: Vec<Service>,
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
        services: vec![
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

// Test enum support
#[derive(State, Serialize, Deserialize, Debug, PartialEq, Clone)]
enum Status {
    Pending,
    Running { pid: u32 },
    Completed { exit_code: i32 },
    Failed { error: String },
}

#[derive(State, Serialize, Deserialize, Debug, PartialEq, Clone)]
enum Priority {
    Low,
    Medium,
    High,
}

#[derive(State, Serialize, Deserialize, Debug, PartialEq)]
struct TaskConfig {
    name: String,
    status: Status,
    priority: Priority,
    #[mahler(internal)]
    internal_id: Option<u64>,
}

#[test]
fn test_enum_model_support() {
    let status1 = Status::Pending;
    let status2 = Status::Running { pid: 1234 };
    let status3 = Status::Completed { exit_code: 0 };
    let status4 = Status::Failed {
        error: "timeout".to_string(),
    };

    // Test that enum Target = Self (no transformation)
    let target1 = to_target(&status1).unwrap();
    let target2 = to_target(&status2).unwrap();
    let target3 = to_target(&status3).unwrap();
    let target4 = to_target(&status4).unwrap();

    assert_eq!(target1, status1);
    assert_eq!(target2, status2);
    assert_eq!(target3, status3);
    assert_eq!(target4, status4);

    // Test simple enum
    let priority = Priority::High;
    let target_priority = to_target(&priority).unwrap();
    assert_eq!(target_priority, priority);
}

#[test]
fn test_struct_with_enum_fields() {
    let task = TaskConfig {
        name: "test-task".to_string(),
        status: Status::Running { pid: 5678 },
        priority: Priority::High,
        internal_id: Some(999),
    };

    let target = to_target(&task).unwrap();

    // Basic fields
    assert_eq!(target.name, "test-task");

    // Enum fields should be unchanged (Target = Self for enums)
    assert_eq!(target.status, Status::Running { pid: 5678 });
    assert_eq!(target.priority, Priority::High);

    // Internal field excluded
    // target.internal_id doesn't exist in TaskConfigTarget
}

#[test]
fn test_enum_serialization_compatibility() {
    let status = Status::Failed {
        error: "network error".to_string(),
    };
    let target = to_target(&status).unwrap();

    // Both should serialize identically since Target = Self for enums
    let status_json = serde_json::to_value(&status).unwrap();
    let target_json = serde_json::to_value(&target).unwrap();

    assert_eq!(status_json, target_json);
}

#[test]
fn test_hashmap_model_support() {
    let mut map: HashMap<String, i32> = HashMap::new();
    map.insert("key1".to_string(), 100);
    map.insert("key2".to_string(), 200);

    let target = to_target(&map).unwrap();

    assert_eq!(target.get("key1"), Some(&100));
    assert_eq!(target.get("key2"), Some(&200));
    assert_eq!(target.len(), 2);
}

#[test]
fn test_btreemap_model_support() {
    let mut map: BTreeMap<String, i32> = BTreeMap::new();
    map.insert("alpha".to_string(), 1);
    map.insert("beta".to_string(), 2);

    let target = to_target(&map).unwrap();

    assert_eq!(target.get("alpha"), Some(&1));
    assert_eq!(target.get("beta"), Some(&2));
    assert_eq!(target.len(), 2);
}

#[test]
fn test_tuple_model_support() {
    // Test unit tuple
    let unit: () = ();
    to_target(&unit).unwrap();

    // Test single element tuple
    let single: (i32,) = (42,);
    let target_single = to_target(&single).unwrap();
    assert_eq!(target_single, (42,));

    // Test pair
    let pair: (String, i32) = ("hello".to_string(), 42);
    let target_pair = to_target(&pair).unwrap();
    assert_eq!(target_pair, ("hello".to_string(), 42));

    // Test triple
    let triple: (String, i32, bool) = ("world".to_string(), 123, true);
    let target_triple = to_target(&triple).unwrap();
    assert_eq!(target_triple, ("world".to_string(), 123, true));

    // Test large tuple (8 elements)
    let large: (i32, String, bool, f64, u16, char, i8, u32) = (
        1,
        "test".to_string(),
        false,
        std::f64::consts::PI,
        500,
        'x',
        -5,
        999,
    );
    let target_large = to_target(&large).unwrap();
    assert_eq!(
        target_large,
        (
            1,
            "test".to_string(),
            false,
            std::f64::consts::PI,
            500,
            'x',
            -5,
            999
        )
    );
}

#[test]
fn test_nested_collections_model_support() {
    // Test HashMap with State values
    let mut services: HashMap<String, Service> = HashMap::new();
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
fn test_tuple_with_model_types() {
    let config = (
        "app-config".to_string(),
        Service {
            name: "main-service".to_string(),
            image: Some("alpine".to_string()),
            port: 8080,
            container_id: Some("main789".to_string()),
        },
        42u16,
    );

    let target = to_target(&config).unwrap();

    assert_eq!(target.0, "app-config");
    assert_eq!(target.1.name, "main-service");
    assert_eq!(target.1.port, 8080);
    assert_eq!(target.2, 42);
}

#[test]
fn test_derive_extraction() {
    // Test that target structs get Debug and Clone by default
    #[derive(State, Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct TestDeriveExtraction {
        name: String,
        #[mahler(internal)]
        internal_field: Option<String>,
    }

    let test = TestDeriveExtraction {
        name: "test".to_string(),
        internal_field: Some("internal".to_string()),
    };

    let target = to_target(&test).unwrap();

    // Basic functionality test
    assert_eq!(target.name, "test");

    // Test that Clone and Debug are available on the target type
    let cloned_target = target.clone();
    assert_eq!(cloned_target.name, "test");

    // Test Debug formatting works
    let debug_str = format!("{target:?}");
    assert!(debug_str.contains("test"));

    // Test that the basic derives work as expected
    // Note: Only Debug and Clone are automatically derived
    // Additional derives like Hash would need to be manually added
}

#[test]
fn test_serde_attribute_propagation() {
    use serde::{Deserialize, Serialize};

    #[derive(State, Serialize, Deserialize, Debug, Clone)]
    #[serde(rename_all = "camelCase")]
    struct ConfigWithSerde {
        database_host: String,
        api_endpoint: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        optional_field: Option<String>,
        #[mahler(internal)]
        internal_connection: String,
    }

    let original = ConfigWithSerde {
        database_host: "localhost".to_string(),
        api_endpoint: "https://api.example.com".to_string(),
        optional_field: None,
        internal_connection: "connection_pool".to_string(),
    };

    let target = ConfigWithSerdeTarget {
        database_host: "localhost".to_string(),
        api_endpoint: "https://api.example.com".to_string(),
        optional_field: None,
    };

    // Test serialization with camelCase attribute
    let original_json = serde_json::to_string(&original).unwrap();
    let target_json = serde_json::to_string(&target).unwrap();

    // Both should serialize with camelCase due to the serde attribute
    assert!(original_json.contains("databaseHost"));
    assert!(original_json.contains("apiEndpoint"));
    assert!(target_json.contains("databaseHost"));
    assert!(target_json.contains("apiEndpoint"));

    // Test that field-level serde attributes work
    assert!(!target_json.contains("optionalField")); // Should be skipped when None
}
