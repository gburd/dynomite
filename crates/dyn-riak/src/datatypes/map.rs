//! Riak observed-remove map (ORSWOT-style).
//!
//! A [`Map`] is a CRDT keyed by [`FieldKey`]; each value is one
//! of the primitive Riak CRDTs or, recursively, another map.
//! Field presence follows the OR-Set "add wins" rule: each
//! field carries an add-tag set and a remove-tag set, and the
//! field is exposed iff at least one add tag is not shadowed by
//! a remove tag. The CRDT value lives alongside the tags and is
//! merged independently; merge is per-field and recursive on
//! nested maps.
//!
//! # Field types
//!
//! [`FieldType`] enumerates the five datatypes a field may
//! carry:
//!
//! * [`FieldType::Counter`]
//! * [`FieldType::OrSet`]
//! * [`FieldType::LwwRegister`]
//! * [`FieldType::EwFlag`]
//! * [`FieldType::NestedMap`]
//!
//! A field's name and type together form the [`FieldKey`]; two
//! fields with the same name but different types are distinct
//! (this is how Riak's `riak_dt_map` namespaces fields).
//!
//! # Operations
//!
//! Operations are wrapped in [`MapOp`]: an `Update` carries the
//! per-type [`NestedOp`]; a `Remove` tombstones the field's
//! currently-observed add tags. Updates increment a per-actor
//! counter and mint a fresh tag, so a concurrent
//! `Update` after `Remove` carries a tag the remove never saw
//! and survives the merge ("add wins").
//!
//! # Removal divergence from Riak
//!
//! When a field is removed, the underlying CRDT value is left
//! intact; only its add-tag set is tombstoned. A subsequent
//! re-add through the same actor mints a fresh tag and exposes
//! the field again with whatever lattice state it had before
//! the remove. Riak's reference behaves the same way at the
//! CRDT layer; the user-visible reset semantics in classic
//! Riak emerges from clients that always assign before reading
//! after a remove. The brief documents this explicitly under
//! "any behavior on Map field removal that diverges from
//! Riak's exact semantics".

use std::collections::{BTreeMap, BTreeSet};

use crate::datatypes::set::Tag;
use crate::datatypes::{ActorId, Crdt, EwFlag, LwwRegister, OrSet, PnCounter};

/// Field type tag carried by every [`FieldKey`].
///
/// The discriminants match the wire constants in
/// [`crate::proto::pb::datatypes`] so a wire decode of a
/// `MapField.field_type` `int32` round-trips cleanly through
/// [`FieldType::from_wire`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FieldType {
    /// Field holds a [`PnCounter`].
    Counter = 1,
    /// Field holds an [`OrSet`].
    OrSet = 2,
    /// Field holds an [`LwwRegister`].
    LwwRegister = 3,
    /// Field holds an [`EwFlag`].
    EwFlag = 4,
    /// Field holds a nested [`Map`].
    NestedMap = 5,
}

impl FieldType {
    /// Convert a wire `int32` field-type code to a `FieldType`.
    /// Returns `None` for codes outside the published range.
    #[must_use]
    pub fn from_wire(code: i32) -> Option<Self> {
        match code {
            1 => Some(FieldType::Counter),
            2 => Some(FieldType::OrSet),
            3 => Some(FieldType::LwwRegister),
            4 => Some(FieldType::EwFlag),
            5 => Some(FieldType::NestedMap),
            _ => None,
        }
    }

    /// Wire `int32` for use in `MapField.field_type`.
    #[must_use]
    pub fn to_wire(self) -> i32 {
        self as i32
    }
}

/// `(name, type)` pair that uniquely identifies a field in a
/// [`Map`]. Two fields with the same `name` but different
/// `field_type` are distinct.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FieldKey {
    /// Field name. Riak uses arbitrary byte strings; conformant
    /// clients send UTF-8.
    pub name: Vec<u8>,
    /// Field type tag.
    pub field_type: FieldType,
}

impl FieldKey {
    /// Build a key from name and type.
    pub fn new(name: impl Into<Vec<u8>>, field_type: FieldType) -> Self {
        Self {
            name: name.into(),
            field_type,
        }
    }
}

/// Per-type CRDT value held in a map field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FieldValue {
    /// PN-Counter cell.
    Counter(PnCounter),
    /// OR-Set cell.
    OrSet(OrSet),
    /// LWW-Register cell.
    LwwRegister(LwwRegister),
    /// Enable-wins-flag cell.
    EwFlag(EwFlag),
    /// Nested map cell. `Box`ed so the outer enum stays
    /// `Sized`.
    NestedMap(Box<Map>),
}

impl FieldValue {
    /// Construct a fresh empty value for the given field type.
    fn empty_for(field_type: FieldType) -> Self {
        match field_type {
            FieldType::Counter => FieldValue::Counter(PnCounter::new()),
            FieldType::OrSet => FieldValue::OrSet(OrSet::new()),
            FieldType::LwwRegister => FieldValue::LwwRegister(LwwRegister::new()),
            FieldType::EwFlag => FieldValue::EwFlag(EwFlag::new()),
            FieldType::NestedMap => FieldValue::NestedMap(Box::new(Map::new())),
        }
    }

    /// Merge `other` into `self` per-type. Type mismatch is a
    /// no-op: the field-type carried in [`FieldKey`] is canonical
    /// and a mismatch implies the caller built the map by hand
    /// with inconsistent keys, which the API does not allow.
    fn merge(&mut self, other: &FieldValue) {
        match (self, other) {
            (FieldValue::Counter(a), FieldValue::Counter(b)) => a.merge(b),
            (FieldValue::OrSet(a), FieldValue::OrSet(b)) => a.merge(b),
            (FieldValue::LwwRegister(a), FieldValue::LwwRegister(b)) => a.merge(b),
            (FieldValue::EwFlag(a), FieldValue::EwFlag(b)) => a.merge(b),
            (FieldValue::NestedMap(a), FieldValue::NestedMap(b)) => a.merge(b),
            _ => {}
        }
    }
}

/// Per-type operation applied to a map field by [`MapOp::Update`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NestedOp {
    /// Apply a signed delta to a counter.
    Counter(i64),
    /// Add an element to an OR-Set.
    SetAdd(Vec<u8>),
    /// Remove an element from an OR-Set.
    SetRemove(Vec<u8>),
    /// Assign a value with explicit microsecond timestamp to a
    /// register.
    RegisterAssign {
        /// New register value.
        value: Vec<u8>,
        /// Timestamp in microseconds since the Unix epoch.
        ts_micros: u64,
    },
    /// Enable (`true`) or disable (`false`) an EW flag.
    Flag(bool),
    /// Apply a [`MapOp`] to a nested map.
    Map(Box<MapOp>),
}

/// Top-level operation against a [`Map`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MapOp {
    /// Update or create a field. The field's CRDT receives the
    /// nested op; the field itself is tagged with a fresh
    /// per-actor add tag.
    Update {
        /// Field to update.
        field: FieldKey,
        /// Per-type op to apply.
        op: NestedOp,
    },
    /// Tombstone the field's currently-observed add tags. The
    /// CRDT value is left intact; concurrent updates with fresh
    /// tags survive.
    Remove {
        /// Field to remove.
        field: FieldKey,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct FieldEntry {
    adds: BTreeSet<Tag>,
    removes: BTreeSet<Tag>,
    value: FieldValue,
}

impl FieldEntry {
    fn new(field_type: FieldType) -> Self {
        Self {
            adds: BTreeSet::new(),
            removes: BTreeSet::new(),
            value: FieldValue::empty_for(field_type),
        }
    }

    fn is_present(&self) -> bool {
        self.adds.iter().any(|t| !self.removes.contains(t))
    }

    fn merge(&mut self, other: &FieldEntry) {
        for tag in &other.adds {
            self.adds.insert(tag.clone());
        }
        for tag in &other.removes {
            self.removes.insert(tag.clone());
        }
        self.value.merge(&other.value);
    }
}

impl Default for FieldValue {
    fn default() -> Self {
        FieldValue::Counter(PnCounter::new())
    }
}

/// Observed-remove map CRDT.
///
/// Holds a `BTreeMap<FieldKey, FieldEntry>` plus a per-actor
/// counter used to mint OR-Set tags on `Update`. Merge unions
/// both columns, recursively merging field values.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Map {
    fields: BTreeMap<FieldKey, FieldEntry>,
    actor_counters: BTreeMap<ActorId, u64>,
}

impl Map {
    /// Construct an empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply `op` from `actor`. `Update` mints a fresh tag and
    /// applies the nested op to the field's CRDT; `Remove`
    /// tombstones currently-observed add tags.
    pub fn apply(&mut self, actor: &ActorId, op: &MapOp) {
        match op {
            MapOp::Update { field, op } => self.apply_update(actor, field, op),
            MapOp::Remove { field } => self.apply_remove(field),
        }
    }

    fn apply_update(&mut self, actor: &ActorId, field: &FieldKey, op: &NestedOp) {
        let counter = self.actor_counters.entry(actor.clone()).or_insert(0);
        *counter = counter.checked_add(1).expect("map counter overflow");
        let tag = Tag {
            actor: actor.clone(),
            counter: *counter,
        };
        let entry = self
            .fields
            .entry(field.clone())
            .or_insert_with(|| FieldEntry::new(field.field_type));
        entry.adds.insert(tag);
        Self::apply_nested(actor, op, &mut entry.value);
    }

    fn apply_nested(actor: &ActorId, op: &NestedOp, value: &mut FieldValue) {
        match (op, value) {
            (NestedOp::Counter(delta), FieldValue::Counter(c)) => c.apply(actor, *delta),
            (NestedOp::SetAdd(elt), FieldValue::OrSet(s)) => {
                s.add(actor, elt.clone());
            }
            (NestedOp::SetRemove(elt), FieldValue::OrSet(s)) => s.remove(elt),
            (
                NestedOp::RegisterAssign {
                    value: v,
                    ts_micros,
                },
                FieldValue::LwwRegister(r),
            ) => {
                r.assign(actor, *ts_micros, v.clone());
            }
            (NestedOp::Flag(true), FieldValue::EwFlag(f)) => {
                f.enable(actor);
            }
            (NestedOp::Flag(false), FieldValue::EwFlag(f)) => f.disable(),
            (NestedOp::Map(inner), FieldValue::NestedMap(m)) => m.apply(actor, inner),
            _ => {
                // Type mismatch between op and field; not
                // reachable when the caller uses `FieldKey` to
                // pick the field, but harmless if it does occur.
            }
        }
    }

    fn apply_remove(&mut self, field: &FieldKey) {
        if let Some(entry) = self.fields.get_mut(field) {
            for tag in entry.adds.clone() {
                entry.removes.insert(tag);
            }
        }
    }

    /// Whether `field` is present in the value projection.
    #[must_use]
    pub fn contains(&self, field: &FieldKey) -> bool {
        self.fields.get(field).is_some_and(FieldEntry::is_present)
    }

    /// Borrow a field's CRDT value, if the field is present.
    #[must_use]
    pub fn get(&self, field: &FieldKey) -> Option<&FieldValue> {
        self.fields
            .get(field)
            .filter(|e| e.is_present())
            .map(|e| &e.value)
    }

    /// Iterate over present fields.
    pub fn iter(&self) -> impl Iterator<Item = (&FieldKey, &FieldValue)> {
        self.fields
            .iter()
            .filter(|(_, e)| e.is_present())
            .map(|(k, e)| (k, &e.value))
    }
}

impl Crdt for Map {
    type Value = BTreeMap<FieldKey, FieldValue>;

    fn merge(&mut self, other: &Self) {
        for (actor, &count) in &other.actor_counters {
            let entry = self.actor_counters.entry(actor.clone()).or_insert(0);
            if *entry < count {
                *entry = count;
            }
        }
        for (key, other_entry) in &other.fields {
            let entry = self
                .fields
                .entry(key.clone())
                .or_insert_with(|| FieldEntry::new(key.field_type));
            entry.merge(other_entry);
        }
    }

    fn value(&self) -> BTreeMap<FieldKey, FieldValue> {
        self.fields
            .iter()
            .filter_map(|(k, e)| {
                if e.is_present() {
                    Some((k.clone(), e.value.clone()))
                } else {
                    None
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aid(name: &str) -> ActorId {
        ActorId::new("dc1", name)
    }

    fn counter_field(name: &str) -> FieldKey {
        FieldKey::new(name.as_bytes(), FieldType::Counter)
    }

    fn set_field(name: &str) -> FieldKey {
        FieldKey::new(name.as_bytes(), FieldType::OrSet)
    }

    fn flag_field(name: &str) -> FieldKey {
        FieldKey::new(name.as_bytes(), FieldType::EwFlag)
    }

    fn register_field(name: &str) -> FieldKey {
        FieldKey::new(name.as_bytes(), FieldType::LwwRegister)
    }

    fn map_field(name: &str) -> FieldKey {
        FieldKey::new(name.as_bytes(), FieldType::NestedMap)
    }

    #[test]
    fn fresh_map_is_empty() {
        let m = Map::new();
        assert!(m.value().is_empty());
    }

    #[test]
    fn update_creates_field_with_correct_type() {
        let a = aid("a");
        let mut m = Map::new();
        let f = counter_field("hits");
        m.apply(
            &a,
            &MapOp::Update {
                field: f.clone(),
                op: NestedOp::Counter(5),
            },
        );
        assert!(m.contains(&f));
        match m.get(&f) {
            Some(FieldValue::Counter(c)) => assert_eq!(c.value(), 5),
            _ => panic!("counter field missing or wrong type"),
        }
    }

    #[test]
    fn remove_drops_field_from_value() {
        let a = aid("a");
        let mut m = Map::new();
        let f = flag_field("on");
        m.apply(
            &a,
            &MapOp::Update {
                field: f.clone(),
                op: NestedOp::Flag(true),
            },
        );
        assert!(m.contains(&f));
        m.apply(&a, &MapOp::Remove { field: f.clone() });
        assert!(!m.contains(&f));
    }

    #[test]
    fn three_typed_fields_round_trip_through_value() {
        let a = aid("a");
        let mut m = Map::new();
        let cf = counter_field("c");
        let sf = set_field("s");
        let rf = register_field("r");
        m.apply(
            &a,
            &MapOp::Update {
                field: cf.clone(),
                op: NestedOp::Counter(7),
            },
        );
        m.apply(
            &a,
            &MapOp::Update {
                field: sf.clone(),
                op: NestedOp::SetAdd(b"x".to_vec()),
            },
        );
        m.apply(
            &a,
            &MapOp::Update {
                field: rf.clone(),
                op: NestedOp::RegisterAssign {
                    value: b"hello".to_vec(),
                    ts_micros: 100,
                },
            },
        );
        let v = m.value();
        assert_eq!(v.len(), 3);
        match v.get(&cf) {
            Some(FieldValue::Counter(c)) => assert_eq!(c.value(), 7),
            _ => panic!("counter missing"),
        }
        match v.get(&sf) {
            Some(FieldValue::OrSet(s)) => assert!(s.contains(b"x")),
            _ => panic!("set missing"),
        }
        match v.get(&rf) {
            Some(FieldValue::LwwRegister(r)) => assert_eq!(r.value(), b"hello".to_vec()),
            _ => panic!("register missing"),
        }
    }

    #[test]
    fn concurrent_remove_loses_to_concurrent_update() {
        let a = aid("a");
        let b = aid("b");
        let f = counter_field("c");
        let mut shared = Map::new();
        shared.apply(
            &a,
            &MapOp::Update {
                field: f.clone(),
                op: NestedOp::Counter(1),
            },
        );

        let mut left = shared.clone();
        left.apply(&a, &MapOp::Remove { field: f.clone() });
        assert!(!left.contains(&f));

        let mut right = shared.clone();
        right.apply(
            &b,
            &MapOp::Update {
                field: f.clone(),
                op: NestedOp::Counter(2),
            },
        );

        let mut merged = left.clone();
        merged.merge(&right);
        assert!(
            merged.contains(&f),
            "add wins on tie: concurrent update beats remove"
        );
    }

    #[test]
    fn nested_map_merge_is_recursive() {
        let a = aid("a");
        let outer_key = map_field("inner");
        let inner_counter = counter_field("hits");

        let mut left = Map::new();
        left.apply(
            &a,
            &MapOp::Update {
                field: outer_key.clone(),
                op: NestedOp::Map(Box::new(MapOp::Update {
                    field: inner_counter.clone(),
                    op: NestedOp::Counter(3),
                })),
            },
        );
        let mut right = Map::new();
        right.apply(
            &aid("b"),
            &MapOp::Update {
                field: outer_key.clone(),
                op: NestedOp::Map(Box::new(MapOp::Update {
                    field: inner_counter.clone(),
                    op: NestedOp::Counter(4),
                })),
            },
        );
        left.merge(&right);
        match left.get(&outer_key) {
            Some(FieldValue::NestedMap(inner)) => match inner.get(&inner_counter) {
                Some(FieldValue::Counter(c)) => assert_eq!(c.value(), 7),
                _ => panic!("nested counter missing"),
            },
            _ => panic!("nested map missing"),
        }
    }

    #[test]
    fn merge_is_commutative() {
        let a = aid("a");
        let b = aid("b");
        let cf = counter_field("c");
        let sf = set_field("s");
        let mut x = Map::new();
        x.apply(
            &a,
            &MapOp::Update {
                field: cf.clone(),
                op: NestedOp::Counter(3),
            },
        );
        x.apply(
            &a,
            &MapOp::Update {
                field: sf.clone(),
                op: NestedOp::SetAdd(b"x".to_vec()),
            },
        );
        let mut y = Map::new();
        y.apply(
            &b,
            &MapOp::Update {
                field: cf.clone(),
                op: NestedOp::Counter(5),
            },
        );
        y.apply(
            &b,
            &MapOp::Update {
                field: sf.clone(),
                op: NestedOp::SetAdd(b"y".to_vec()),
            },
        );

        let mut left = x.clone();
        left.merge(&y);
        let mut right = y.clone();
        right.merge(&x);
        assert_eq!(left.value(), right.value());
    }

    #[test]
    fn merge_is_idempotent() {
        let a = aid("a");
        let cf = counter_field("c");
        let mut m = Map::new();
        m.apply(
            &a,
            &MapOp::Update {
                field: cf,
                op: NestedOp::Counter(7),
            },
        );
        let snap = m.clone();
        m.merge(&snap);
        assert_eq!(m.value(), snap.value());
    }

    #[test]
    fn fields_with_same_name_but_different_types_are_distinct() {
        let a = aid("a");
        let mut m = Map::new();
        let counter_x = counter_field("x");
        let flag_x = flag_field("x");
        m.apply(
            &a,
            &MapOp::Update {
                field: counter_x.clone(),
                op: NestedOp::Counter(1),
            },
        );
        m.apply(
            &a,
            &MapOp::Update {
                field: flag_x.clone(),
                op: NestedOp::Flag(true),
            },
        );
        assert_eq!(m.value().len(), 2);
        assert!(m.contains(&counter_x));
        assert!(m.contains(&flag_x));
    }

    #[test]
    fn field_type_wire_round_trips() {
        for ty in [
            FieldType::Counter,
            FieldType::OrSet,
            FieldType::LwwRegister,
            FieldType::EwFlag,
            FieldType::NestedMap,
        ] {
            assert_eq!(FieldType::from_wire(ty.to_wire()), Some(ty));
        }
        assert!(FieldType::from_wire(0).is_none());
        assert!(FieldType::from_wire(99).is_none());
    }
}
