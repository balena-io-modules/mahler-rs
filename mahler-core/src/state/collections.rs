//! Collection types for State values.
//!
//! This module provides List, Set, and Map collection types that work with State
//! with standardized serialization. These should be used instead of Vec, HashSet,
//! HashMap, etc. in State types.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, Index, IndexMut};

use crate::serde;

use super::{State, StateDeserializer};
use crate::json::BULK_BUILD_MIN_LEN;

/// Capacity to pre-allocate for elements of type `T` from a deserializer size hint
///
/// The hint comes from the input, so it is capped, like serde does, to avoid allocating
/// more than 1 MiB before any element has been read.
fn cautious<T>(hint: Option<usize>) -> usize {
    const MAX_PREALLOC_BYTES: usize = 1024 * 1024;
    let max = MAX_PREALLOC_BYTES / std::mem::size_of::<T>().max(1);
    hint.unwrap_or(0).min(max)
}

/// Whether a collection of the hinted size is worth bulk building, see
/// [`BULK_BUILD_MIN_LEN`]
fn is_large(hint: Option<usize>) -> bool {
    hint.is_some_and(|len| len >= BULK_BUILD_MIN_LEN)
}

/// A list of State values (newtype over Vec).
///
/// Use this instead of Vec in State types.
///
/// # Example
/// ```
/// use mahler::state::{list, List, State};
///
/// #[derive(State)]
/// struct MyState {
///     items: List<String>,
/// }
///
/// let state = MyState {
///     items: list!["a".to_string(), "b".to_string()],
/// };
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct List<T>(Vec<T>);

impl<T> List<T> {
    /// Creates a new empty List.
    pub fn new() -> Self {
        List(Vec::new())
    }

    /// Unwraps the List into the inner Vec.
    pub fn into_inner(self) -> Vec<T> {
        self.0
    }

    /// Returns true if the list is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<T> Default for List<T> {
    fn default() -> Self {
        List(Vec::new())
    }
}

impl<T> Deref for List<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for List<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> From<Vec<T>> for List<T> {
    fn from(vec: Vec<T>) -> Self {
        List(vec)
    }
}

impl<T> From<List<T>> for Vec<T> {
    fn from(list: List<T>) -> Self {
        list.into_inner()
    }
}

impl<T> FromIterator<T> for List<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        List(Vec::from_iter(iter))
    }
}

impl<T> IntoIterator for List<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a List<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut List<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

impl<T> Index<usize> for List<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl<T> IndexMut<usize> for List<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}

impl<T: fmt::Debug> fmt::Debug for List<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: serde::Serialize> serde::Serialize for List<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for item in &self.0 {
            seq.serialize_element(&item)?;
        }
        seq.end()
    }
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for List<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(ListVisitor::<T>(PhantomData))
    }
}

impl<T: State> State for List<T> {
    type Target = List<T::Target>;
}

struct ListVisitor<T>(PhantomData<T>);

impl<'de, T: serde::Deserialize<'de>> serde::de::Visitor<'de> for ListVisitor<T> {
    type Value = List<T>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a list")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut vec = Vec::with_capacity(cautious::<T>(seq.size_hint()));
        while let Some(elem) = seq.next_element_seed(StateDeserializer::<T>(PhantomData))? {
            vec.push(elem);
        }
        Ok(List(vec))
    }
}

/// A set of State values (newtype over BTreeSet).
///
/// Use this instead of HashSet or BTreeSet in State types.
///
/// # Example
/// ```
/// use mahler::state::{set, Set, State};
///
/// #[derive(State)]
/// struct MyState {
///     tags: Set<String>,
/// }
///
/// let state = MyState {
///     tags: set!["tag1".to_string(), "tag2".to_string()],
/// };
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Set<T: Ord>(BTreeSet<T>);

impl<T: Ord> Default for Set<T> {
    fn default() -> Self {
        Set(BTreeSet::new())
    }
}

impl<T: Ord> Set<T> {
    /// Creates a new empty Set.
    pub fn new() -> Self {
        Set(BTreeSet::new())
    }

    /// Unwraps the Set into the inner BTreeSet.
    pub fn into_inner(self) -> BTreeSet<T> {
        self.0
    }

    /// Returns true if the set is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<T: Ord> Deref for Set<T> {
    type Target = BTreeSet<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Ord> DerefMut for Set<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Ord> From<BTreeSet<T>> for Set<T> {
    fn from(set: BTreeSet<T>) -> Self {
        Set(set)
    }
}

impl<T: Ord> From<Set<T>> for BTreeSet<T> {
    fn from(set: Set<T>) -> Self {
        set.into_inner()
    }
}

impl<T: Ord> FromIterator<T> for Set<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Set(BTreeSet::from_iter(iter))
    }
}

impl<T: Ord> IntoIterator for Set<T> {
    type Item = T;
    type IntoIter = std::collections::btree_set::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T: Ord> IntoIterator for &'a Set<T> {
    type Item = &'a T;
    type IntoIter = std::collections::btree_set::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T: Ord + fmt::Debug> fmt::Debug for Set<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: serde::Serialize + Ord> serde::Serialize for Set<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for item in &self.0 {
            seq.serialize_element(&item)?;
        }
        seq.end()
    }
}

impl<'de, T: serde::Deserialize<'de> + Ord> serde::Deserialize<'de> for Set<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(SetVisitor::<T>(PhantomData))
    }
}

impl<T: State + Ord> State for Set<T>
where
    T::Target: Ord,
{
    type Target = Set<T::Target>;
}

struct SetVisitor<T>(PhantomData<T>);

impl<'de, T: serde::Deserialize<'de> + Ord> serde::de::Visitor<'de> for SetVisitor<T> {
    type Value = Set<T>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a set")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let seed = || StateDeserializer::<T>(PhantomData);
        if !is_large(seq.size_hint()) {
            let mut set = BTreeSet::new();
            while let Some(elem) = seq.next_element_seed(seed())? {
                set.insert(elem);
            }
            return Ok(Set(set));
        }

        // bulk building a large set is faster than inserting one element at a time
        let mut elems = Vec::with_capacity(cautious::<T>(seq.size_hint()));
        while let Some(elem) = seq.next_element_seed(seed())? {
            elems.push(elem);
        }
        // Like inserting, keep the first of equal elements. Elements usually come sorted
        // and unique, in which case there is nothing to remove
        if !elems.windows(2).all(|pair| pair[0] < pair[1]) {
            elems.sort();
            elems.dedup();
        }
        Ok(Set(BTreeSet::from_iter(elems)))
    }
}

/// A map of State keys to State values (newtype over BTreeMap).
///
/// Use this instead of HashMap or BTreeMap in State types.
///
/// # Example
/// ```
/// use mahler::state::{map, Map, State};
///
/// #[derive(State)]
/// struct MyState {
///     config: Map<String, i32>,
/// }
///
/// let state = MyState {
///     config: map![
///         "key1".to_string() => 100,
///         "key2".to_string() => 200,
///     ],
/// };
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Map<K: Ord, V>(BTreeMap<K, V>);

impl<K: Ord, V> Default for Map<K, V> {
    fn default() -> Self {
        Map(BTreeMap::new())
    }
}

impl<K: Ord + fmt::Debug, V: fmt::Debug> fmt::Debug for Map<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<K: Ord, V> Map<K, V> {
    /// Creates a new empty Map.
    pub fn new() -> Self {
        Map(BTreeMap::new())
    }

    /// Unwraps the Map into the inner BTreeMap.
    pub fn into_inner(self) -> BTreeMap<K, V> {
        self.0
    }

    /// Returns true if the map is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<K: Ord, V> Deref for Map<K, V> {
    type Target = BTreeMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K: Ord, V> DerefMut for Map<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K: Ord, V, const N: usize> From<[(K, V); N]> for Map<K, V> {
    fn from(arr: [(K, V); N]) -> Self {
        Map(BTreeMap::from(arr))
    }
}

impl<K: Ord, V> From<BTreeMap<K, V>> for Map<K, V> {
    fn from(map: BTreeMap<K, V>) -> Self {
        Map(map)
    }
}

impl<K: Ord, V> From<Map<K, V>> for BTreeMap<K, V> {
    fn from(map: Map<K, V>) -> Self {
        map.into_inner()
    }
}

impl<K: Ord, V> FromIterator<(K, V)> for Map<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Map(BTreeMap::from_iter(iter))
    }
}

impl<K: Ord, V> IntoIterator for Map<K, V> {
    type Item = (K, V);
    type IntoIter = std::collections::btree_map::IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, K: Ord, V> IntoIterator for &'a Map<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = std::collections::btree_map::Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a, K: Ord, V> IntoIterator for &'a mut Map<K, V> {
    type Item = (&'a K, &'a mut V);
    type IntoIter = std::collections::btree_map::IterMut<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

impl<K: Ord, V> Index<&K> for Map<K, V> {
    type Output = V;

    fn index(&self, key: &K) -> &Self::Output {
        &self.0[key]
    }
}

impl<K: serde::Serialize + Ord, V: serde::Serialize> serde::Serialize for Map<K, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(&k, &v)?;
        }
        map.end()
    }
}

impl<'de, K: serde::Deserialize<'de> + Ord, V: serde::Deserialize<'de>> serde::Deserialize<'de>
    for Map<K, V>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(MapVisitor::<K, V>(PhantomData))
    }
}

impl<K: State + Ord, V: State> State for Map<K, V>
where
    K::Target: Ord,
{
    type Target = Map<K::Target, V::Target>;
}

struct MapVisitor<K, V>(PhantomData<(K, V)>);

impl<'de, K: serde::Deserialize<'de> + Ord, V: serde::Deserialize<'de>> serde::de::Visitor<'de>
    for MapVisitor<K, V>
{
    type Value = Map<K, V>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a map")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let key_seed = || StateDeserializer::<K>(PhantomData);
        let value_seed = || StateDeserializer::<V>(PhantomData);
        if !is_large(map.size_hint()) {
            let mut result = BTreeMap::new();
            while let Some((key, value)) = map.next_entry_seed(key_seed(), value_seed())? {
                result.insert(key, value);
            }
            return Ok(Map(result));
        }

        // Collecting first lets a large map be bulk built, which is much faster than
        // inserting one entry at a time when the entries already come sorted, as they do
        // from JSON
        let mut entries = Vec::with_capacity(cautious::<(K, V)>(map.size_hint()));
        while let Some(entry) = map.next_entry_seed(key_seed(), value_seed())? {
            entries.push(entry);
        }
        // Like inserting, keep the first of equal keys with the last value. Keys usually come
        // sorted and unique, as they do from JSON, in which case there is nothing to remove
        if !entries.windows(2).all(|pair| pair[0].0 < pair[1].0) {
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            entries.dedup_by(|later, earlier| {
                let repeated = later.0 == earlier.0;
                if repeated {
                    std::mem::swap(&mut later.1, &mut earlier.1);
                }
                repeated
            });
        }
        Ok(Map(BTreeMap::from_iter(entries)))
    }
}

/// Creates a `List<T>` from a list of expressions.
///
/// # Example
/// ```
/// use mahler::state::list;
///
/// let items = list![1, 2, 3];
/// ```
#[macro_export]
macro_rules! list {
    () => {
        $crate::state::List::new()
    };
    ($($item:expr),+ $(,)?) => {
        $crate::state::List::from(vec![$($item),+])
    };
}

/// Creates a `Set<T>` from a list of expressions.
///
/// # Example
/// ```
/// use mahler::state::set;
///
/// let items = set![1, 2, 3];
/// ```
#[macro_export]
macro_rules! set {
    () => {
        $crate::state::Set::new()
    };
    ($($item:expr),+ $(,)?) => {
        $crate::state::Set::from(std::collections::BTreeSet::from([$($item),+]))
    };
}

/// Creates a `Map<K, V>` from key-value pairs.
///
/// # Example
/// ```
/// use mahler::state::map;
///
/// let config = map![
///     "key1".to_string() => 100,
///     "key2".to_string() => 200,
/// ];
/// ```
#[macro_export]
macro_rules! map {
    () => {
        $crate::state::Map::new()
    };
    ($($key:expr => $value:expr),+ $(,)?) => {
        $crate::state::Map::from(std::collections::BTreeMap::from([$(($key, $value)),+]))
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::serde::de::value::MapDeserializer;
    use ::serde::Deserialize;
    use serde_json::json;

    /// Ordered by name only, so elements with different versions compare equal
    #[derive(Deserialize, Debug)]
    struct Item {
        name: String,
        version: u8,
    }

    impl PartialEq for Item {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
        }
    }
    impl Eq for Item {}
    impl PartialOrd for Item {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for Item {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.name.cmp(&other.name)
        }
    }

    /// `len` items, with the first two equal, before and after the bulk build threshold
    fn items(len: usize) -> Vec<serde_json::Value> {
        let mut items = vec![
            json!({"name": "a", "version": 1}),
            json!({"name": "a", "version": 2}),
        ];
        items.extend((2..len).map(|i| json!({"name": format!("n{i:02}"), "version": 0})));
        items
    }

    #[test]
    fn sets_keep_the_first_of_equal_elements_at_any_size() {
        for len in [3, BULK_BUILD_MIN_LEN + 2] {
            let set: Set<Item> = serde_json::from_value(json!(items(len))).unwrap();
            assert_eq!(set.len(), len - 1);
            assert_eq!(set.iter().next().map(|i| i.version), Some(1), "len {len}");
        }
    }

    #[test]
    fn maps_keep_the_first_of_equal_keys_and_the_last_value_at_any_size() {
        for len in [3, BULK_BUILD_MIN_LEN + 2] {
            // equal keys cannot come from a JSON object, so the entries are a sequence of
            // pairs deserialized through a map adapter
            let pairs: Vec<(serde_json::Value, u8)> = items(len)
                .into_iter()
                .enumerate()
                .map(|(i, item)| (item, i as u8))
                .collect();
            let deserializer: MapDeserializer<_, serde_json::Error> =
                MapDeserializer::new(pairs.into_iter());
            let map = Map::<Item, u8>::deserialize(deserializer).unwrap();
            assert_eq!(map.len(), len - 1);
            let (key, value) = map.iter().next().unwrap();
            assert_eq!((key.version, *value), (1, 1), "len {len}");
        }
    }
}
