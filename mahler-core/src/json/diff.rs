//! JSON diffing
//!
//! Produces the same patch as [`json_patch::diff`], which the planner calls for every
//! simulated task and every search iteration. Paths are built in a single reusable
//! string, while `json_patch` encodes and allocates a token for every key it visits.
//!
//! [`diff_serialized`] and [`diff_to_serialized`] diff a serializable value without
//! converting it to a [`Value`] first, which saves building, and then dropping, a copy of
//! the whole state.

use std::borrow::Cow;
use std::fmt::Write;
use std::sync::OnceLock;

use jsonptr::{Pointer, PointerBuf, Token};
use serde_json::map::Iter as MapIter;
use serde_json::value::Serializer as JsonSerializer;
use serde_json::{Error, Map};

use super::value::{
    self, is_serde_json_private, to_value, KeySerializer, SerializeStructVariant,
    SerializeTupleVariant,
};
use super::{AddOperation, PatchOperation, RemoveOperation, ReplaceOperation, Value};
use crate::serde::ser::{self, Serialize};

/// Compute the operations that turn `left` into `right`
///
/// The paths of the operations are relative to `prefix`, which allows to diff a value
/// nested somewhere in a larger document.
pub fn diff(left: &Value, right: &Value, prefix: &Pointer) -> Vec<PatchOperation> {
    let mut path = String::from(prefix.as_str());
    let mut ops = Vec::new();
    diff_value(left, right, &mut path, &mut ops);
    ops
}

/// Compute the operations that turn the JSON representation of `left` into `right`
///
/// The result is the same as `diff(&to_value(left)?, right, Pointer::root())`.
///
/// # Errors
///
/// Fails if `left` cannot be converted into JSON, see [`to_value`]
pub fn diff_serialized<T: Serialize + ?Sized>(
    left: &T,
    right: &Value,
) -> Result<Vec<PatchOperation>, Error> {
    let mut path = String::new();
    let mut ops = Vec::new();
    left.serialize(Differ {
        right,
        path: &mut path,
        ops: &mut ops,
    })?;
    Ok(ops)
}

/// Compute the operations that turn `left` into the JSON representation of `right`
///
/// The result is the same as `diff(left, &to_value(right)?, prefix)`, but only the values
/// added or replaced in `left` are converted to JSON.
///
/// # Errors
///
/// Fails if `right` cannot be converted into JSON, see [`to_value`]
pub fn diff_to_serialized<T: Serialize + ?Sized>(
    left: &Value,
    right: &T,
    prefix: &Pointer,
) -> Result<Vec<PatchOperation>, Error> {
    let mut path = String::from(prefix.as_str());
    let mut ops = Vec::new();
    right.serialize(DifferTo {
        left,
        path: &mut path,
        ops: &mut ops,
    })?;
    Ok(ops)
}

fn pointer(path: &str) -> PointerBuf {
    PointerBuf::parse(path).expect("diff paths should be built from encoded tokens")
}

/// Append a key to the path as an encoded token
fn push_key(path: &mut String, key: &str) {
    path.push('/');
    path.push_str(Token::new(key).encoded());
}

fn push_index(path: &mut String, index: usize) {
    write!(path, "/{index}").expect("writing to a string should not fail");
}

/// Check that a value can be converted into JSON, without converting it
///
/// Values that are not part of the diff are still checked, so the diff fails whenever
/// converting the value would.
fn validate<T: Serialize + ?Sized>(value: &T) -> Result<(), Error> {
    serde_json::to_writer(std::io::sink(), value)
}

/// Whether objects iterate their keys in sorted order
///
/// They do unless the `preserve_order` feature of `serde_json` is enabled, in which case
/// they iterate in insertion order. Diffing a serialized object has to replicate the
/// order the object would have.
fn sorted_objects() -> bool {
    static SORTED: OnceLock<bool> = OnceLock::new();
    *SORTED.get_or_init(|| {
        let mut object = Map::new();
        object.insert(String::from("b"), Value::Null);
        object.insert(String::from("a"), Value::Null);
        object.keys().next().is_some_and(|key| key == "a")
    })
}

fn diff_value(left: &Value, right: &Value, path: &mut String, ops: &mut Vec<PatchOperation>) {
    match (left, right) {
        (Value::Object(left), Value::Object(right)) => {
            let len = path.len();
            let mut right_entries = right.iter().peekable();

            // Objects being diffed usually have the same keys, in which case both are
            // walked in lockstep, without looking up every key on the other object
            let mut left_keys = left.iter();
            let mut matched = 0;
            while let Some((key, right_value)) = right_entries.peek() {
                let Some((left_key, left_value)) = left_keys.next() else {
                    break;
                };
                if left_key != *key {
                    break;
                }
                push_key(path, key);
                diff_value(left_value, right_value, path, ops);
                path.truncate(len);
                right_entries.next();
                matched += 1;
            }

            // From the first different key on, look up keys on the other object
            for (key, right_value) in right_entries {
                push_key(path, key);
                match left.get(key) {
                    Some(left_value) => diff_value(left_value, right_value, path, ops),
                    None => ops.push(PatchOperation::Add(AddOperation {
                        path: pointer(path),
                        value: right_value.clone(),
                    })),
                }
                path.truncate(len);
            }

            if matched == left.len() && matched == right.len() {
                // same keys, nothing to remove
                return;
            }
            for key in left.keys().filter(|key| !right.contains_key(*key)) {
                push_key(path, key);
                ops.push(PatchOperation::Remove(RemoveOperation {
                    path: pointer(path),
                }));
                path.truncate(len);
            }
        }
        (Value::Array(left), Value::Array(right)) => {
            let len = path.len();
            // removing an element shifts the index of the elements after it
            let mut shift = 0usize;
            for idx in 0..left.len().max(right.len()) {
                push_index(path, idx - shift);
                match (left.get(idx), right.get(idx)) {
                    (Some(left), Some(right)) => diff_value(left, right, path, ops),
                    (Some(_), None) => {
                        shift += 1;
                        ops.push(PatchOperation::Remove(RemoveOperation {
                            path: pointer(path),
                        }));
                    }
                    (None, Some(right)) => ops.push(PatchOperation::Add(AddOperation {
                        path: pointer(path),
                        value: right.clone(),
                    })),
                    (None, None) => unreachable!("index is within the longest array"),
                }
                path.truncate(len);
            }
        }
        (left, right) if left == right => {}
        (_, right) => ops.push(PatchOperation::Replace(ReplaceOperation {
            path: pointer(path),
            value: right.clone(),
        })),
    }
}

/// A serializer that diffs the value it serializes, see [`Differ`] and [`DifferTo`]
trait DiffSerializer {
    /// Diff a value that has been converted to JSON
    fn value(self, value: Value) -> Result<(), Error>;
}

/// Serializer methods shared by [`Differ`] and [`DifferTo`]
///
/// Values with no children are converted to JSON and diffed as such, the less common
/// shapes are converted as a whole with [`Materialize`].
macro_rules! diff_serializer_methods {
    () => {
        fn serialize_bool(self, v: bool) -> Result<(), Error> {
            self.value(Value::Bool(v))
        }

        fn serialize_i8(self, v: i8) -> Result<(), Error> {
            self.value(JsonSerializer.serialize_i8(v)?)
        }

        fn serialize_i16(self, v: i16) -> Result<(), Error> {
            self.value(JsonSerializer.serialize_i16(v)?)
        }

        fn serialize_i32(self, v: i32) -> Result<(), Error> {
            self.value(JsonSerializer.serialize_i32(v)?)
        }

        fn serialize_i64(self, v: i64) -> Result<(), Error> {
            self.value(JsonSerializer.serialize_i64(v)?)
        }

        fn serialize_i128(self, v: i128) -> Result<(), Error> {
            self.value(JsonSerializer.serialize_i128(v)?)
        }

        fn serialize_u8(self, v: u8) -> Result<(), Error> {
            self.value(JsonSerializer.serialize_u8(v)?)
        }

        fn serialize_u16(self, v: u16) -> Result<(), Error> {
            self.value(JsonSerializer.serialize_u16(v)?)
        }

        fn serialize_u32(self, v: u32) -> Result<(), Error> {
            self.value(JsonSerializer.serialize_u32(v)?)
        }

        fn serialize_u64(self, v: u64) -> Result<(), Error> {
            self.value(JsonSerializer.serialize_u64(v)?)
        }

        fn serialize_u128(self, v: u128) -> Result<(), Error> {
            self.value(JsonSerializer.serialize_u128(v)?)
        }

        fn serialize_f32(self, v: f32) -> Result<(), Error> {
            self.value(JsonSerializer.serialize_f32(v)?)
        }

        fn serialize_f64(self, v: f64) -> Result<(), Error> {
            self.value(JsonSerializer.serialize_f64(v)?)
        }

        fn serialize_char(self, v: char) -> Result<(), Error> {
            self.serialize_str(v.encode_utf8(&mut [0; 4]))
        }

        fn serialize_bytes(self, v: &[u8]) -> Result<(), Error> {
            self.value(JsonSerializer.serialize_bytes(v)?)
        }

        fn serialize_none(self) -> Result<(), Error> {
            self.value(Value::Null)
        }

        fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), Error> {
            value.serialize(self)
        }

        fn serialize_unit(self) -> Result<(), Error> {
            self.value(Value::Null)
        }

        fn serialize_unit_struct(self, _name: &'static str) -> Result<(), Error> {
            self.value(Value::Null)
        }

        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            variant: &'static str,
        ) -> Result<(), Error> {
            self.serialize_str(variant)
        }

        fn serialize_newtype_struct<T: Serialize + ?Sized>(
            self,
            _name: &'static str,
            value: &T,
        ) -> Result<(), Error> {
            value.serialize(self)
        }

        fn serialize_newtype_variant<T: Serialize + ?Sized>(
            self,
            _name: &'static str,
            _variant_index: u32,
            variant: &'static str,
            value: &T,
        ) -> Result<(), Error> {
            // the variant is an object with a single key
            let mut object = self.serialize_map(Some(1))?;
            ser::SerializeMap::serialize_entry(&mut object, variant, value)?;
            ser::SerializeMap::end(object)
        }

        fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Error> {
            self.serialize_seq(Some(len))
        }

        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            len: usize,
        ) -> Result<Self::SerializeTupleStruct, Error> {
            self.serialize_seq(Some(len))
        }

        fn serialize_tuple_variant(
            self,
            name: &'static str,
            variant_index: u32,
            variant: &'static str,
            len: usize,
        ) -> Result<Self::SerializeTupleVariant, Error> {
            Ok(Materialize {
                inner: value::Serializer.serialize_tuple_variant(
                    name,
                    variant_index,
                    variant,
                    len,
                )?,
                differ: self,
            })
        }

        fn serialize_struct(
            self,
            name: &'static str,
            len: usize,
        ) -> Result<Self::SerializeStruct, Error> {
            if is_serde_json_private(name) {
                return Ok(DiffStruct::Materialize(Materialize {
                    inner: value::Serializer.serialize_struct(name, len)?,
                    differ: self,
                }));
            }
            self.serialize_map(Some(len)).map(DiffStruct::Object)
        }

        fn serialize_struct_variant(
            self,
            name: &'static str,
            variant_index: u32,
            variant: &'static str,
            len: usize,
        ) -> Result<Self::SerializeStructVariant, Error> {
            Ok(Materialize {
                inner: value::Serializer.serialize_struct_variant(
                    name,
                    variant_index,
                    variant,
                    len,
                )?,
                differ: self,
            })
        }
    };
}

/// Implement the tuple traits of a sequence serializer by forwarding to `SerializeSeq`
macro_rules! forward_tuples_to_seq {
    ($seq:ident) => {
        impl ser::SerializeTuple for $seq<'_> {
            type Ok = ();
            type Error = Error;

            fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
                ser::SerializeSeq::serialize_element(self, value)
            }

            fn end(self) -> Result<(), Error> {
                ser::SerializeSeq::end(self)
            }
        }

        impl ser::SerializeTupleStruct for $seq<'_> {
            type Ok = ();
            type Error = Error;

            fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
                ser::SerializeSeq::serialize_element(self, value)
            }

            fn end(self) -> Result<(), Error> {
                ser::SerializeSeq::end(self)
            }
        }
    };
}

/// Serializer that diffs the serialized value against `right` instead of building it
///
/// Containers are matched against the container at the same location in `right`,
/// leaves are compared once serialized. Whenever `left` cannot have the same shape as
/// `right` at some location, the value is replaced, as [`diff`] would do.
struct Differ<'a> {
    right: &'a Value,
    path: &'a mut String,
    ops: &'a mut Vec<PatchOperation>,
}

impl Differ<'_> {
    fn replace(&mut self) {
        self.ops.push(PatchOperation::Replace(ReplaceOperation {
            path: pointer(self.path),
            value: self.right.clone(),
        }));
    }
}

impl DiffSerializer for Differ<'_> {
    fn value(self, left: Value) -> Result<(), Error> {
        diff_value(&left, self.right, self.path, self.ops);
        Ok(())
    }
}

impl<'a> ser::Serializer for Differ<'a> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = DiffSeq<'a>;
    type SerializeTuple = DiffSeq<'a>;
    type SerializeTupleStruct = DiffSeq<'a>;
    type SerializeTupleVariant = Materialize<Self, SerializeTupleVariant>;
    type SerializeMap = DiffObject<'a>;
    type SerializeStruct = DiffStruct<DiffObject<'a>, Self>;
    type SerializeStructVariant = Materialize<Self, SerializeStructVariant>;

    diff_serializer_methods!();

    fn serialize_str(mut self, v: &str) -> Result<(), Error> {
        // compared in place to avoid copying the string
        if self.right.as_str() != Some(v) {
            self.replace();
        }
        Ok(())
    }

    fn serialize_seq(mut self, _len: Option<usize>) -> Result<DiffSeq<'a>, Error> {
        let right = match self.right {
            Value::Array(right) => Some(right),
            // an array is always different from anything that is not an array
            _ => {
                self.replace();
                None
            }
        };
        Ok(DiffSeq {
            right,
            path: self.path,
            ops: self.ops,
            index: 0,
        })
    }

    fn serialize_map(mut self, _len: Option<usize>) -> Result<DiffObject<'a>, Error> {
        let right = match self.right {
            Value::Object(right) => Some(right),
            // an object is always different from anything that is not an object
            _ => {
                self.replace();
                None
            }
        };
        let lockstep = right.map(|right| Lockstep::new(right, self.ops));
        Ok(DiffObject {
            right,
            path: self.path,
            ops: self.ops,
            entries: Vec::new(),
            next_key: None,
            lockstep,
        })
    }
}

/// Diffs an array against the array at the same location on the right side
struct DiffSeq<'a> {
    /// `None` if the right side is not an array
    right: Option<&'a Vec<Value>>,
    path: &'a mut String,
    ops: &'a mut Vec<PatchOperation>,
    index: usize,
}

impl ser::SerializeSeq for DiffSeq<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        let index = self.index;
        self.index += 1;

        let Some(right) = self.right else {
            // the array was replaced
            return validate(value);
        };

        let len = self.path.len();
        match right.get(index) {
            Some(right) => {
                push_index(self.path, index);
                value.serialize(Differ {
                    right,
                    path: self.path,
                    ops: self.ops,
                })?;
            }
            None => {
                validate(value)?;
                // every element past the end of the right array is removed at the same
                // index, as the ones before it are removed first
                push_index(self.path, right.len());
                self.ops.push(PatchOperation::Remove(RemoveOperation {
                    path: pointer(self.path),
                }));
            }
        }
        self.path.truncate(len);
        Ok(())
    }

    fn end(self) -> Result<(), Error> {
        let Some(right) = self.right else {
            return Ok(());
        };

        let len = self.path.len();
        for (index, value) in right.iter().enumerate().skip(self.index) {
            push_index(self.path, index);
            self.ops.push(PatchOperation::Add(AddOperation {
                path: pointer(self.path),
                value: value.clone(),
            }));
            self.path.truncate(len);
        }
        Ok(())
    }
}

forward_tuples_to_seq!(DiffSeq);

/// An entry of a serialized object with the operations on its value
struct Entry<'a> {
    key: Cow<'a, str>,
    ops: Vec<PatchOperation>,
}

/// Tracks the entries of a serialized object that come in the order of the object it is
/// diffed against, in which case their operations are already in order and are written
/// as they come
struct Lockstep<'a> {
    /// The object the entries are matched against
    object: &'a Map<String, Value>,
    /// Entries of the object not matched yet
    remaining: MapIter<'a>,
    /// Position of the first operation of the matched entries
    start: usize,
}

impl<'a> Lockstep<'a> {
    fn new(object: &'a Map<String, Value>, ops: &[PatchOperation]) -> Self {
        Self {
            object,
            remaining: object.iter(),
            start: ops.len(),
        }
    }

    /// Match the next entry of the object if it has the given key
    fn next(&mut self, key: &str) -> Option<&'a Value> {
        let mut remaining = self.remaining.clone();
        match remaining.next() {
            Some((next_key, value)) if next_key == key => {
                self.remaining = remaining;
                Some(value)
            }
            _ => None,
        }
    }

    /// Move the operations of the entries matched so far to their own entries
    ///
    /// The operations of every entry are contiguous and under the entry path.
    fn into_entries(
        self,
        path: &mut String,
        ops: &mut Vec<PatchOperation>,
        entries: &mut Vec<Entry<'a>>,
    ) {
        let matched = self.object.len() - self.remaining.len();
        let mut written = ops.split_off(self.start).into_iter().peekable();
        let len = path.len();
        for key in self.object.keys().take(matched) {
            push_key(path, key);
            let prefix = Pointer::parse(path.as_str()).expect("path should be encoded");
            let mut entry_ops = Vec::new();
            while let Some(op) = written.next_if(|op| op.path().starts_with(prefix)) {
                entry_ops.push(op);
            }
            path.truncate(len);
            entries.push(Entry {
                key: Cow::Borrowed(key),
                ops: entry_ops,
            });
        }
    }
}

/// Order the entries of a serialized object as the object would iterate them
///
/// A repeated key keeps the last value, at the position of the first one when objects
/// keep the insertion order.
fn order_entries(entries: &mut Vec<Entry<'_>>, sorted: bool) {
    if sorted {
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        entries.dedup_by(|later, earlier| {
            let repeated = later.key == earlier.key;
            if repeated {
                std::mem::swap(&mut later.ops, &mut earlier.ops);
            }
            repeated
        });
        return;
    }

    let mut unique: Vec<Entry<'_>> = Vec::with_capacity(entries.len());
    for entry in entries.drain(..) {
        match unique.iter_mut().find(|e| e.key == entry.key) {
            Some(first) => first.ops = entry.ops,
            None => unique.push(entry),
        }
    }
    *entries = unique;
}

/// Find the entry with the given key on entries ordered by [`order_entries`]
fn find_entry(entries: &[Entry<'_>], key: &str, sorted: bool) -> Option<usize> {
    if sorted {
        entries.binary_search_by(|e| e.key.as_ref().cmp(key)).ok()
    } else {
        entries.iter().position(|e| e.key == key)
    }
}

/// The object diffs, which receive the entries of a serialized object
trait ObjectDiff<'a> {
    fn entry<T: Serialize + ?Sized>(&mut self, key: Cow<'a, str>, value: &T) -> Result<(), Error>;

    fn finish(self) -> Result<(), Error>;
}

/// Diffs an object against the object at the same location on the right side
///
/// [`diff`] orders operations by the keys on the right side, followed by the removed keys,
/// while the entries are serialized in whatever order the type defines. Unless the
/// entries come in the order of the right side, the operations of every entry are kept
/// apart and sorted once all of them have been serialized.
struct DiffObject<'a> {
    /// `None` if the right side is not an object
    right: Option<&'a Map<String, Value>>,
    path: &'a mut String,
    ops: &'a mut Vec<PatchOperation>,
    /// Entries that came out of order with the right side
    entries: Vec<Entry<'a>>,
    next_key: Option<String>,
    /// Set while the entries come in the order of the right side
    lockstep: Option<Lockstep<'a>>,
}

impl<'a> ObjectDiff<'a> for DiffObject<'a> {
    fn entry<T: Serialize + ?Sized>(&mut self, key: Cow<'a, str>, value: &T) -> Result<(), Error> {
        let Some(right) = self.right else {
            // the object was replaced
            return validate(value);
        };

        let len = self.path.len();
        if let Some(lockstep) = self.lockstep.as_mut() {
            if let Some(right_value) = lockstep.next(&key) {
                push_key(self.path, &key);
                value.serialize(Differ {
                    right: right_value,
                    path: self.path,
                    ops: self.ops,
                })?;
                self.path.truncate(len);
                return Ok(());
            }
            if let Some(lockstep) = self.lockstep.take() {
                lockstep.into_entries(self.path, self.ops, &mut self.entries);
            }
        }

        let mut ops = Vec::new();
        match right.get(key.as_ref()) {
            Some(right_value) => {
                push_key(self.path, &key);
                value.serialize(Differ {
                    right: right_value,
                    path: self.path,
                    ops: &mut ops,
                })?;
                self.path.truncate(len);
            }
            // removed once the object is complete
            None => validate(value)?,
        }
        self.entries.push(Entry { key, ops });
        Ok(())
    }

    fn finish(mut self) -> Result<(), Error> {
        let Some(right) = self.right else {
            return Ok(());
        };

        let len = self.path.len();
        if let Some(Lockstep { remaining, .. }) = self.lockstep {
            // every entry is on the right side, the remaining keys are new
            for (key, right_value) in remaining {
                push_key(self.path, key);
                self.ops.push(PatchOperation::Add(AddOperation {
                    path: pointer(self.path),
                    value: right_value.clone(),
                }));
                self.path.truncate(len);
            }
            return Ok(());
        }

        let sorted = sorted_objects();
        order_entries(&mut self.entries, sorted);

        for (key, right_value) in right {
            match find_entry(&self.entries, key, sorted) {
                Some(position) => self.ops.append(&mut self.entries[position].ops),
                None => {
                    push_key(self.path, key);
                    self.ops.push(PatchOperation::Add(AddOperation {
                        path: pointer(self.path),
                        value: right_value.clone(),
                    }));
                    self.path.truncate(len);
                }
            }
        }

        for entry in self
            .entries
            .iter()
            .filter(|e| !right.contains_key(e.key.as_ref()))
        {
            push_key(self.path, &entry.key);
            self.ops.push(PatchOperation::Remove(RemoveOperation {
                path: pointer(self.path),
            }));
            self.path.truncate(len);
        }
        Ok(())
    }
}

impl ser::SerializeMap for DiffObject<'_> {
    type Ok = ();
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
        self.entry(Cow::Owned(key), value)
    }

    fn end(self) -> Result<(), Error> {
        self.finish()
    }
}

/// Diffs a struct, which is an object unless it is private to `serde_json`
enum DiffStruct<O, D> {
    Object(O),
    /// A raw value or arbitrary precision number, only serializable by `serde_json`
    Materialize(Materialize<D, value::SerializeStruct>),
}

impl<'a, O, D> ser::SerializeStruct for DiffStruct<O, D>
where
    O: ObjectDiff<'a>,
    D: DiffSerializer,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        match self {
            DiffStruct::Object(object) => object.entry(Cow::Borrowed(key), value),
            DiffStruct::Materialize(materialize) => {
                ser::SerializeStruct::serialize_field(&mut materialize.inner, key, value)
            }
        }
    }

    fn end(self) -> Result<(), Error> {
        match self {
            DiffStruct::Object(object) => object.finish(),
            DiffStruct::Materialize(Materialize { inner, differ }) => {
                differ.value(ser::SerializeStruct::end(inner)?)
            }
        }
    }
}

/// Converts a value to JSON and diffs it once complete, for the less common shapes
struct Materialize<D, S> {
    inner: S,
    differ: D,
}

impl<D: DiffSerializer> ser::SerializeTupleVariant for Materialize<D, SerializeTupleVariant> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.inner.serialize_field(value)
    }

    fn end(self) -> Result<(), Error> {
        let Materialize { inner, differ } = self;
        differ.value(ser::SerializeTupleVariant::end(inner)?)
    }
}

impl<D: DiffSerializer> ser::SerializeStructVariant for Materialize<D, SerializeStructVariant> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        self.inner.serialize_field(key, value)
    }

    fn end(self) -> Result<(), Error> {
        let Materialize { inner, differ } = self;
        differ.value(ser::SerializeStructVariant::end(inner)?)
    }
}

/// Serializer that diffs `left` against the serialized value instead of building it
///
/// The mirror of [`Differ`]: the serialized value is the target of the operations, so
/// only the parts of it that are added or replaced are converted to JSON.
struct DifferTo<'a> {
    left: &'a Value,
    path: &'a mut String,
    ops: &'a mut Vec<PatchOperation>,
}

impl DifferTo<'_> {
    /// Replace the value on the left, known to be different
    fn replace(self, right: Value) {
        match (self.left, right) {
            // containers of the same kind are diffed rather than replaced
            (left @ Value::Object(_), right @ Value::Object(_))
            | (left @ Value::Array(_), right @ Value::Array(_)) => {
                diff_value(left, &right, self.path, self.ops)
            }
            (_, right) => self.ops.push(PatchOperation::Replace(ReplaceOperation {
                path: pointer(self.path),
                value: right,
            })),
        }
    }
}

impl DiffSerializer for DifferTo<'_> {
    fn value(self, right: Value) -> Result<(), Error> {
        if *self.left != right {
            self.replace(right);
        }
        Ok(())
    }
}

impl<'a> ser::Serializer for DifferTo<'a> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = DiffToSeq<'a>;
    type SerializeTuple = DiffToSeq<'a>;
    type SerializeTupleStruct = DiffToSeq<'a>;
    type SerializeTupleVariant = Materialize<Self, SerializeTupleVariant>;
    type SerializeMap = DiffToObject<'a>;
    type SerializeStruct = DiffStruct<DiffToObject<'a>, Self>;
    type SerializeStructVariant = Materialize<Self, SerializeStructVariant>;

    diff_serializer_methods!();

    fn serialize_str(self, v: &str) -> Result<(), Error> {
        // compared in place, the string is only copied if it changed
        if self.left.as_str() != Some(v) {
            self.replace(Value::String(String::from(v)));
        }
        Ok(())
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<DiffToSeq<'a>, Error> {
        let state = match self.left {
            Value::Array(left) => SeqState::Diff { left, index: 0 },
            // an array always replaces anything that is not an array
            _ => SeqState::Replace(Vec::with_capacity(len.unwrap_or(0))),
        };
        Ok(DiffToSeq {
            state,
            path: self.path,
            ops: self.ops,
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<DiffToObject<'a>, Error> {
        let state = match self.left {
            Value::Object(left) => ObjectState::Diff {
                left,
                entries: Vec::new(),
                lockstep: Some(Lockstep::new(left, self.ops)),
            },
            // an object always replaces anything that is not an object
            _ => ObjectState::Replace(value::Serializer.serialize_map(len)?),
        };
        Ok(DiffToObject {
            state,
            path: self.path,
            ops: self.ops,
            next_key: None,
        })
    }
}

enum SeqState<'a> {
    /// Diff against the array on the left
    Diff { left: &'a Vec<Value>, index: usize },
    /// The left side is not an array, the elements are collected to replace it
    Replace(Vec<Value>),
}

/// Diffs the array on the left against a serialized array
struct DiffToSeq<'a> {
    state: SeqState<'a>,
    path: &'a mut String,
    ops: &'a mut Vec<PatchOperation>,
}

impl ser::SerializeSeq for DiffToSeq<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        let (left, index) = match &mut self.state {
            SeqState::Diff { left, index } => (*left, index),
            SeqState::Replace(elements) => {
                elements.push(to_value(value)?);
                return Ok(());
            }
        };

        let len = self.path.len();
        push_index(self.path, *index);
        match left.get(*index) {
            Some(left) => value.serialize(DifferTo {
                left,
                path: self.path,
                ops: self.ops,
            })?,
            None => self.ops.push(PatchOperation::Add(AddOperation {
                path: pointer(self.path),
                value: to_value(value)?,
            })),
        }
        self.path.truncate(len);
        *index += 1;
        Ok(())
    }

    fn end(self) -> Result<(), Error> {
        match self.state {
            SeqState::Diff { left, index } => {
                // every element past the end of the new array is removed at the same index,
                // as the ones before it are removed first
                let len = self.path.len();
                push_index(self.path, index);
                for _ in index..left.len() {
                    self.ops.push(PatchOperation::Remove(RemoveOperation {
                        path: pointer(self.path),
                    }));
                }
                self.path.truncate(len);
            }
            SeqState::Replace(elements) => {
                self.ops.push(PatchOperation::Replace(ReplaceOperation {
                    path: pointer(self.path),
                    value: Value::Array(elements),
                }));
            }
        }
        Ok(())
    }
}

forward_tuples_to_seq!(DiffToSeq);

enum ObjectState<'a> {
    /// Diff against the object on the left
    Diff {
        left: &'a Map<String, Value>,
        /// Entries that came out of order with the left side
        entries: Vec<Entry<'a>>,
        /// Set while the entries come in the order of the left side
        lockstep: Option<Lockstep<'a>>,
    },
    /// The left side is not an object, the entries are collected to replace it
    Replace(value::SerializeMap),
}

/// Diffs the object on the left against a serialized object
///
/// [`diff`] orders operations by the keys of the new object, in the order the object
/// iterates them, followed by the keys removed from the left side.
struct DiffToObject<'a> {
    state: ObjectState<'a>,
    path: &'a mut String,
    ops: &'a mut Vec<PatchOperation>,
    next_key: Option<String>,
}

impl<'a> ObjectDiff<'a> for DiffToObject<'a> {
    fn entry<T: Serialize + ?Sized>(&mut self, key: Cow<'a, str>, value: &T) -> Result<(), Error> {
        let (left, entries, lockstep) = match &mut self.state {
            ObjectState::Diff {
                left,
                entries,
                lockstep,
            } => (*left, entries, lockstep),
            ObjectState::Replace(object) => {
                return ser::SerializeMap::serialize_entry(object, key.as_ref(), value);
            }
        };

        let len = self.path.len();
        if let Some(step) = lockstep.as_mut() {
            if let Some(left_value) = step.next(&key) {
                push_key(self.path, &key);
                value.serialize(DifferTo {
                    left: left_value,
                    path: self.path,
                    ops: self.ops,
                })?;
                self.path.truncate(len);
                return Ok(());
            }
            if let Some(step) = lockstep.take() {
                step.into_entries(self.path, self.ops, entries);
            }
        }

        let mut ops = Vec::new();
        push_key(self.path, &key);
        match left.get(key.as_ref()) {
            Some(left) => value.serialize(DifferTo {
                left,
                path: self.path,
                ops: &mut ops,
            })?,
            None => ops.push(PatchOperation::Add(AddOperation {
                path: pointer(self.path),
                value: to_value(value)?,
            })),
        }
        self.path.truncate(len);
        entries.push(Entry { key, ops });
        Ok(())
    }

    fn finish(self) -> Result<(), Error> {
        let (left, mut entries, lockstep) = match self.state {
            ObjectState::Diff {
                left,
                entries,
                lockstep,
            } => (left, entries, lockstep),
            ObjectState::Replace(object) => {
                let right = ser::SerializeMap::end(object)?;
                self.ops.push(PatchOperation::Replace(ReplaceOperation {
                    path: pointer(self.path),
                    value: right,
                }));
                return Ok(());
            }
        };

        let len = self.path.len();
        if let Some(Lockstep { remaining, .. }) = lockstep {
            // every entry is on the left side, the remaining keys were removed
            for (key, _) in remaining {
                push_key(self.path, key);
                self.ops.push(PatchOperation::Remove(RemoveOperation {
                    path: pointer(self.path),
                }));
                self.path.truncate(len);
            }
            return Ok(());
        }

        let sorted = sorted_objects();
        order_entries(&mut entries, sorted);
        for entry in entries.iter_mut() {
            self.ops.append(&mut entry.ops);
        }

        for key in left.keys() {
            if find_entry(&entries, key, sorted).is_none() {
                push_key(self.path, key);
                self.ops.push(PatchOperation::Remove(RemoveOperation {
                    path: pointer(self.path),
                }));
                self.path.truncate(len);
            }
        }
        Ok(())
    }
}

impl ser::SerializeMap for DiffToObject<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Error> {
        if let ObjectState::Replace(object) = &mut self.state {
            return ser::SerializeMap::serialize_key(object, key);
        }
        self.next_key = Some(key.serialize(KeySerializer)?);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        if let ObjectState::Replace(object) = &mut self.state {
            return ser::SerializeMap::serialize_value(object, value);
        }
        let key = self
            .next_key
            .take()
            .expect("serialize_value should be called after serialize_key");
        self.entry(Cow::Owned(key), value)
    }

    fn end(self) -> Result<(), Error> {
        self.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use pretty_assertions::assert_eq;
    use serde::Serialize;
    use serde_json::json;

    fn assert_same_as_json_patch(left: Value, right: Value) {
        let json_patch::Patch(expected) = json_patch::diff(&left, &right);
        assert_eq!(diff(&left, &right, Pointer::root()), expected);
        assert_eq!(diff_serialized(&left, &right).unwrap(), expected);
        assert_eq!(
            diff_to_serialized(&left, &right, Pointer::root()).unwrap(),
            expected
        );
    }

    #[test]
    fn it_diffs_like_json_patch() {
        assert_same_as_json_patch(
            json!({"title": "Goodbye!", "author": {"givenName": "John", "familyName": "Doe"},
                   "tags": ["example", "sample"], "content": "unchanged"}),
            json!({"title": "Hello!", "author": {"givenName": "John"},
                   "tags": ["example"], "content": "unchanged", "phone": "+01"}),
        );
        assert_same_as_json_patch(json!([1, 2, 3, 4]), json!([2]));
        assert_same_as_json_patch(json!([1]), json!([1, {"a": [2]}, 3]));
        assert_same_as_json_patch(json!({"a/b": {"c~d": 1}}), json!({"a/b": {"c~d": 2}}));
        assert_same_as_json_patch(json!({"a": 1}), json!([1]));
        assert_same_as_json_patch(json!([1]), json!({"a": 1}));
        assert_same_as_json_patch(json!({"a": [1, 2]}), json!({"a": "x"}));
        assert_same_as_json_patch(json!(1), json!(1));
        assert_same_as_json_patch(json!(1), json!(1.0));
        assert_same_as_json_patch(json!(-1), json!("-1"));
        assert_same_as_json_patch(
            json!({"a": 1, "b": 2, "d": {"x": 4}}),
            json!({"a": 2, "c": 3, "d": {"x": 5}}),
        );
        assert_same_as_json_patch(json!({"a": 1, "b": 2}), json!({"a": 1}));
        assert_same_as_json_patch(json!({"a": 1}), json!({"a": 2, "b": 2}));
        assert_same_as_json_patch(
            json!({"x": [{"y": 1}]}),
            json!({"x": [{"y": null}], "z": {}}),
        );
        assert_same_as_json_patch(json!({"b": 1, "a": 2}), json!({"c": 1, "a": 2}));
    }

    #[test]
    fn it_prefixes_paths() {
        let ops = diff(
            &json!({"a": 1, "b": 2}),
            &json!({"a": 2}),
            Pointer::from_static("/root/x~1y"),
        );
        let expected: json_patch::Patch = serde_json::from_value(json!([
            {"op": "replace", "path": "/root/x~1y/a", "value": 2},
            {"op": "remove", "path": "/root/x~1y/b"},
        ]))
        .unwrap();
        assert_eq!(ops, expected.0);
    }

    #[derive(Serialize)]
    enum Shape {
        Unit,
        Newtype(String),
        Tuple(i32, char),
        Struct { z: bool, a: Option<f64> },
    }

    // fields are not in alphabetical order on purpose
    #[derive(Serialize)]
    struct Item {
        zeta: Vec<Shape>,
        alpha: BTreeMap<u32, String>,
        bytes: Vec<u8>,
        nothing: Option<u8>,
        #[serde(flatten)]
        extra: BTreeMap<String, i64>,
    }

    fn item(shapes: Vec<Shape>, extra: &[(&str, i64)]) -> Item {
        Item {
            zeta: shapes,
            alpha: BTreeMap::from([(2, String::from("two")), (10, String::from("ten"))]),
            bytes: vec![1, 2],
            nothing: None,
            extra: extra.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
        }
    }

    #[test]
    fn it_diffs_serialized_values_like_json_values() {
        let lefts = [
            item(vec![], &[]),
            item(
                vec![
                    Shape::Unit,
                    Shape::Newtype(String::from("n")),
                    Shape::Tuple(1, 'c'),
                    Shape::Struct { z: true, a: None },
                ],
                &[("nothing", 1), ("beta", 2)],
            ),
            item(
                vec![Shape::Struct {
                    z: false,
                    a: Some(1.5),
                }],
                &[("zeta", 3)],
            ),
        ];
        let rights = [
            json!(null),
            json!([]),
            json!({}),
            json!({"zeta": [], "alpha": {"2": "two"}, "bytes": [1], "nothing": 3, "omega": 1}),
            json!({"zeta": ["Unit", {"Newtype": "m"}, {"Tuple": [1, "d"]},
                            {"Struct": {"z": true, "a": null}}, "Unit"],
                   "alpha": {"10": "ten", "2": "dos"}, "bytes": "x", "beta": 2}),
            json!({"zeta": [{"Struct": {"z": false}}], "alpha": [], "bytes": [1, 2, 3]}),
        ];

        for left in lefts.iter() {
            for right in rights.iter() {
                let expected = diff(&to_value(left).unwrap(), right, Pointer::root());
                assert_eq!(diff_serialized(left, right).unwrap(), expected, "{right}");

                // the same values, the other way around
                let prefix = Pointer::from_static("/p");
                let expected = diff(right, &to_value(left).unwrap(), prefix);
                assert_eq!(
                    diff_to_serialized(right, left, prefix).unwrap(),
                    expected,
                    "{right}"
                );
            }
        }
    }

    // `a` and `b` come in the order of a sorted object, `z` and `c` do not
    #[derive(Serialize)]
    struct Partial {
        a: BTreeMap<String, Vec<u8>>,
        b: Vec<u8>,
        z: u8,
        c: Option<u8>,
    }

    #[test]
    fn it_diffs_entries_that_stop_coming_in_order() {
        let left = Partial {
            a: BTreeMap::from([(String::from("x"), vec![1, 2]), (String::from("y"), vec![])]),
            b: vec![1],
            z: 1,
            c: None,
        };
        let rights = [
            json!({"a": {"x": [2], "y": [3]}, "b": [2, 3], "c": 3, "z": 4}),
            json!({"a": {"x": [1, 2], "w": []}, "b": [], "c": null, "d": 1}),
            json!({"a": {"x": [1, 2], "y": []}, "b": [1], "c": null, "z": 1}),
            json!({"a": {}, "b": [1]}),
        ];
        for right in rights.iter() {
            let expected = diff(&to_value(&left).unwrap(), right, Pointer::root());
            assert_eq!(diff_serialized(&left, right).unwrap(), expected, "{right}");

            let expected = diff(right, &to_value(&left).unwrap(), Pointer::root());
            assert_eq!(
                diff_to_serialized(right, &left, Pointer::root()).unwrap(),
                expected,
                "{right}"
            );
        }
    }

    #[test]
    fn it_fails_to_diff_non_string_keys() {
        let left = BTreeMap::from([(vec![1], 1)]);
        assert!(diff_serialized(&left, &json!({})).is_err());
        assert!(diff_serialized(&left, &json!(1)).is_err());
        assert!(diff_to_serialized(&json!({}), &left, Pointer::root()).is_err());
        assert!(diff_to_serialized(&json!(1), &left, Pointer::root()).is_err());
    }
}
