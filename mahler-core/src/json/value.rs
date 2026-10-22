use serde_json::value::Serializer as JsonSerializer;
use serde_json::{Error, Map};

use crate::serde::ser::{self, Error as _, Impossible, Serialize};

/// Re-export of [`serde_json::Value`] for usability
pub use serde_json::Value;

/// Convert a `T` into a [`Value`]
///
/// Equivalent to [`serde_json::to_value`], but objects are built from all their entries at
/// once rather than inserting the entries one by one, which is considerably faster when
/// converting large states.
///
/// # Errors
///
/// Fails under the same conditions as [`serde_json::to_value`], for instance if `T` is a
/// map with non-string keys.
pub fn to_value<T: Serialize + ?Sized>(value: &T) -> Result<Value, Error> {
    value.serialize(Serializer)
}

/// Whether a struct is one `serde_json` uses internally to serialize a raw value or an
/// arbitrary precision number, when the `raw_value` or `arbitrary_precision` features are
/// enabled. These are only serializable by `serde_json` itself.
pub(super) fn is_serde_json_private(name: &str) -> bool {
    name == "$serde_json::private::RawValue" || name == "$serde_json::private::Number"
}

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
    type Ok = Value;
    type Error = Error;

    type SerializeSeq = SerializeVec;
    type SerializeTuple = SerializeVec;
    type SerializeTupleStruct = SerializeVec;
    type SerializeTupleVariant = SerializeTupleVariant;
    type SerializeMap = SerializeMap;
    type SerializeStruct = SerializeStruct;
    type SerializeStructVariant = SerializeStructVariant;

    fn serialize_bool(self, v: bool) -> Result<Value, Error> {
        JsonSerializer.serialize_bool(v)
    }

    fn serialize_i8(self, v: i8) -> Result<Value, Error> {
        JsonSerializer.serialize_i8(v)
    }

    fn serialize_i16(self, v: i16) -> Result<Value, Error> {
        JsonSerializer.serialize_i16(v)
    }

    fn serialize_i32(self, v: i32) -> Result<Value, Error> {
        JsonSerializer.serialize_i32(v)
    }

    fn serialize_i64(self, v: i64) -> Result<Value, Error> {
        JsonSerializer.serialize_i64(v)
    }

    fn serialize_i128(self, v: i128) -> Result<Value, Error> {
        JsonSerializer.serialize_i128(v)
    }

    fn serialize_u8(self, v: u8) -> Result<Value, Error> {
        JsonSerializer.serialize_u8(v)
    }

    fn serialize_u16(self, v: u16) -> Result<Value, Error> {
        JsonSerializer.serialize_u16(v)
    }

    fn serialize_u32(self, v: u32) -> Result<Value, Error> {
        JsonSerializer.serialize_u32(v)
    }

    fn serialize_u64(self, v: u64) -> Result<Value, Error> {
        JsonSerializer.serialize_u64(v)
    }

    fn serialize_u128(self, v: u128) -> Result<Value, Error> {
        JsonSerializer.serialize_u128(v)
    }

    fn serialize_f32(self, v: f32) -> Result<Value, Error> {
        JsonSerializer.serialize_f32(v)
    }

    fn serialize_f64(self, v: f64) -> Result<Value, Error> {
        JsonSerializer.serialize_f64(v)
    }

    fn serialize_char(self, v: char) -> Result<Value, Error> {
        JsonSerializer.serialize_char(v)
    }

    fn serialize_str(self, v: &str) -> Result<Value, Error> {
        JsonSerializer.serialize_str(v)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Value, Error> {
        JsonSerializer.serialize_bytes(v)
    }

    fn serialize_none(self) -> Result<Value, Error> {
        JsonSerializer.serialize_none()
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Value, Error> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Value, Error> {
        JsonSerializer.serialize_unit()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Value, Error> {
        JsonSerializer.serialize_unit_struct(name)
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Value, Error> {
        JsonSerializer.serialize_unit_variant(name, variant_index, variant)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Value, Error> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Value, Error> {
        let mut object = Map::new();
        object.insert(String::from(variant), to_value(value)?);
        Ok(Value::Object(object))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<SerializeVec, Error> {
        Ok(SerializeVec(Vec::with_capacity(len.unwrap_or(0))))
    }

    fn serialize_tuple(self, len: usize) -> Result<SerializeVec, Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<SerializeVec, Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<SerializeTupleVariant, Error> {
        Ok(SerializeTupleVariant {
            variant,
            elements: Vec::with_capacity(len),
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<SerializeMap, Error> {
        let entries = match len {
            Some(len) if len >= BULK_BUILD_MIN_LEN => Entries::Bulk(Vec::with_capacity(len)),
            _ => Entries::Map(Map::new()),
        };
        Ok(SerializeMap {
            entries,
            next_key: None,
        })
    }

    fn serialize_struct(self, name: &'static str, len: usize) -> Result<SerializeStruct, Error> {
        if is_serde_json_private(name) {
            return JsonSerializer
                .serialize_struct(name, len)
                .map(SerializeStruct::Json);
        }
        Ok(SerializeStruct::Fields(Map::new()))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<SerializeStructVariant, Error> {
        Ok(SerializeStructVariant {
            variant,
            fields: Map::new(),
        })
    }
}

pub(super) struct SerializeVec(Vec<Value>);

impl ser::SerializeSeq for SerializeVec {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.0.push(to_value(value)?);
        Ok(())
    }

    fn end(self) -> Result<Value, Error> {
        Ok(Value::Array(self.0))
    }
}

impl ser::SerializeTuple for SerializeVec {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Value, Error> {
        ser::SerializeSeq::end(self)
    }
}

impl ser::SerializeTupleStruct for SerializeVec {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Value, Error> {
        ser::SerializeSeq::end(self)
    }
}

pub(super) struct SerializeTupleVariant {
    variant: &'static str,
    elements: Vec<Value>,
}

impl ser::SerializeTupleVariant for SerializeTupleVariant {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.elements.push(to_value(value)?);
        Ok(())
    }

    fn end(self) -> Result<Value, Error> {
        let mut object = Map::new();
        object.insert(String::from(self.variant), Value::Array(self.elements));
        Ok(Value::Object(object))
    }
}

/// Minimum number of entries for which building a map at once from all its entries is faster
/// than inserting them one by one
///
/// Smaller maps fit in a single B-tree node, where inserting is cheap and collecting the
/// entries first would only add an allocation.
pub(crate) const BULK_BUILD_MIN_LEN: usize = 12;

enum Entries {
    Map(Map<String, Value>),
    Bulk(Vec<(String, Value)>),
}

pub(super) struct SerializeMap {
    entries: Entries,
    next_key: Option<String>,
}

fn key_must_be_a_string() -> Error {
    Error::custom("key must be a string")
}

/// Converts map keys into strings, accepting the same keys as `serde_json` and producing
/// the same strings
pub(super) struct KeySerializer;

impl ser::Serializer for KeySerializer {
    type Ok = String;
    type Error = Error;

    type SerializeSeq = Impossible<String, Error>;
    type SerializeTuple = Impossible<String, Error>;
    type SerializeTupleStruct = Impossible<String, Error>;
    type SerializeTupleVariant = Impossible<String, Error>;
    type SerializeMap = Impossible<String, Error>;
    type SerializeStruct = Impossible<String, Error>;
    type SerializeStructVariant = Impossible<String, Error>;

    fn serialize_bool(self, v: bool) -> Result<String, Error> {
        Ok(v.to_string())
    }

    fn serialize_i8(self, v: i8) -> Result<String, Error> {
        Ok(v.to_string())
    }

    fn serialize_i16(self, v: i16) -> Result<String, Error> {
        Ok(v.to_string())
    }

    fn serialize_i32(self, v: i32) -> Result<String, Error> {
        Ok(v.to_string())
    }

    fn serialize_i64(self, v: i64) -> Result<String, Error> {
        Ok(v.to_string())
    }

    fn serialize_i128(self, v: i128) -> Result<String, Error> {
        Ok(v.to_string())
    }

    fn serialize_u8(self, v: u8) -> Result<String, Error> {
        Ok(v.to_string())
    }

    fn serialize_u16(self, v: u16) -> Result<String, Error> {
        Ok(v.to_string())
    }

    fn serialize_u32(self, v: u32) -> Result<String, Error> {
        Ok(v.to_string())
    }

    fn serialize_u64(self, v: u64) -> Result<String, Error> {
        Ok(v.to_string())
    }

    fn serialize_u128(self, v: u128) -> Result<String, Error> {
        Ok(v.to_string())
    }

    // floats are formatted by serde_json, which uses its own shortest representation
    fn serialize_f32(self, v: f32) -> Result<String, Error> {
        if !v.is_finite() {
            return Err(Error::custom(
                "float key must be finite (got NaN or +/-inf)",
            ));
        }
        serde_json::to_string(&v)
    }

    fn serialize_f64(self, v: f64) -> Result<String, Error> {
        if !v.is_finite() {
            return Err(Error::custom(
                "float key must be finite (got NaN or +/-inf)",
            ));
        }
        serde_json::to_string(&v)
    }

    fn serialize_char(self, v: char) -> Result<String, Error> {
        Ok(String::from(v))
    }

    fn serialize_str(self, v: &str) -> Result<String, Error> {
        Ok(String::from(v))
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<String, Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_none(self) -> Result<String, Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_some<T: Serialize + ?Sized>(self, _value: &T) -> Result<String, Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_unit(self) -> Result<String, Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<String, Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<String, Error> {
        Ok(String::from(variant))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<String, Error> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<String, Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Error> {
        Err(key_must_be_a_string())
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(key_must_be_a_string())
    }
}

impl ser::SerializeMap for SerializeMap {
    type Ok = Value;
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Error> {
        self.next_key = Some(key.serialize(KeySerializer)?);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        let key = self
            .next_key
            .take()
            .expect("serialize_value should be called after serialize_key");
        let value = to_value(value)?;
        match &mut self.entries {
            Entries::Map(map) => {
                map.insert(key, value);
            }
            Entries::Bulk(entries) => entries.push((key, value)),
        }
        Ok(())
    }

    fn end(self) -> Result<Value, Error> {
        match self.entries {
            Entries::Map(map) => Ok(Value::Object(map)),
            // a map built from all its entries keeps the last value for repeated keys, like
            // inserting the entries one by one would
            Entries::Bulk(entries) => Ok(Value::Object(Map::from_iter(entries))),
        }
    }
}

/// Structs usually have few fields, so these are inserted directly rather than collected
/// first, which would cost an extra allocation per struct
pub(super) enum SerializeStruct {
    Fields(Map<String, Value>),
    /// A raw value or arbitrary precision number, only serializable by `serde_json`
    Json(<JsonSerializer as ser::Serializer>::SerializeStruct),
}

impl ser::SerializeStruct for SerializeStruct {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        match self {
            SerializeStruct::Fields(fields) => {
                fields.insert(String::from(key), to_value(value)?);
                Ok(())
            }
            SerializeStruct::Json(json) => json.serialize_field(key, value),
        }
    }

    fn end(self) -> Result<Value, Error> {
        match self {
            SerializeStruct::Fields(fields) => Ok(Value::Object(fields)),
            SerializeStruct::Json(json) => ser::SerializeStruct::end(json),
        }
    }
}

pub(super) struct SerializeStructVariant {
    variant: &'static str,
    fields: Map<String, Value>,
}

impl ser::SerializeStructVariant for SerializeStructVariant {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        self.fields.insert(String::from(key), to_value(value)?);
        Ok(())
    }

    fn end(self) -> Result<Value, Error> {
        let mut object = Map::new();
        object.insert(String::from(self.variant), Value::Object(self.fields));
        Ok(Value::Object(object))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashMap};

    use pretty_assertions::assert_eq;
    use serde::Serialize;

    #[derive(Serialize)]
    enum Shape {
        Unit,
        Newtype(u8),
        Tuple(i32, String),
        Struct { z: bool, a: Option<f64> },
    }

    #[derive(Serialize)]
    struct Unit;

    #[derive(Serialize)]
    struct Wrapper(Vec<char>);

    #[derive(Serialize)]
    struct Pair(u8, u8);

    #[derive(Serialize)]
    struct Everything {
        zeta: Vec<Shape>,
        alpha: BTreeMap<String, Unit>,
        ints: HashMap<i64, Wrapper>,
        bools: BTreeMap<bool, Pair>,
        nothing: Option<u8>,
        text: &'static str,
        #[serde(flatten)]
        extra: BTreeMap<String, u32>,
    }

    #[test]
    fn it_converts_like_serde_json() {
        let value = Everything {
            zeta: vec![
                Shape::Unit,
                Shape::Newtype(3),
                Shape::Tuple(-1, String::from("x")),
                Shape::Struct { z: true, a: None },
                Shape::Struct {
                    z: false,
                    a: Some(1.5),
                },
            ],
            alpha: BTreeMap::from([(String::from("b"), Unit), (String::from("a"), Unit)]),
            ints: HashMap::from([(10, Wrapper(vec!['a'])), (-2, Wrapper(vec![]))]),
            bools: BTreeMap::from([(true, Pair(1, 2))]),
            nothing: None,
            text: "hello",
            extra: BTreeMap::from([(String::from("text"), 7), (String::from("y"), 8)]),
        };

        assert_eq!(
            to_value(&value).unwrap(),
            serde_json::to_value(&value).unwrap()
        );
    }

    #[test]
    fn it_converts_keys_like_serde_json() {
        fn same<K: Serialize + Ord>(key: K) {
            let value = BTreeMap::from([(key, 1)]);
            match (to_value(&value), serde_json::to_value(&value)) {
                (Ok(ours), Ok(theirs)) => assert_eq!(ours, theirs),
                (Err(ours), Err(theirs)) => assert_eq!(ours.to_string(), theirs.to_string()),
                (ours, theirs) => panic!("{ours:?} != {theirs:?}"),
            }
        }

        #[derive(Serialize, PartialEq, Eq, PartialOrd, Ord)]
        enum Kind {
            Unit,
            Newtype(u8),
        }

        #[derive(Serialize, PartialEq, Eq, PartialOrd, Ord)]
        struct Name(String);

        #[derive(Serialize, PartialEq)]
        struct Float(f32);
        impl Eq for Float {}
        impl PartialOrd for Float {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for Float {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.total_cmp(&other.0)
            }
        }

        same(Float(0.1));
        same(Float(1e20));
        same(i128::MAX);
        same(u128::MAX);
        same(-7i8);
        same('c');
        same(true);
        same(Some(String::from("x")));
        same(None::<String>);
        same(());
        same(Kind::Unit);
        same(Kind::Newtype(1));
        same(Name(String::from("n")));
        same(vec![1]);
    }

    #[test]
    fn it_rejects_non_string_keys() {
        let value = BTreeMap::from([(vec![1], 1)]);
        assert!(to_value(&value).is_err());
        assert!(serde_json::to_value(&value).is_err());
    }
}
