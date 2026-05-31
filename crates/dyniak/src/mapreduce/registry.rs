//! Built-in phase function registry.
//!
//! The registry is the named-builtin alternative to Riak's
//! JavaScript / Erlang interpreter. A [`Phase::Map`] or
//! [`Phase::Reduce`] referencing a function name is resolved by the
//! [`PhaseRegistry`] at executor time. Unknown names produce
//! [`crate::mapreduce::MrError::UnknownFunction`].
//!
//! [`Phase::Map`]: crate::mapreduce::phase::Phase::Map
//! [`Phase::Reduce`]: crate::mapreduce::phase::Phase::Reduce
//!
//! # Function shape
//!
//! Map functions receive a single inbound JSON value plus an
//! optional argument and return zero or more JSON outputs. Reduce
//! functions receive every inbound JSON value as a slice plus an
//! optional argument and return zero or more JSON outputs.
//!
//! Built-in implementations live in [`crate::mapreduce::builtins`].

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use crate::mapreduce::executor::MrError;

/// Map-phase function. Pure: takes one input plus an optional
/// argument, returns zero or more outputs. Implementations must
/// not block; for I/O-bound work, push the work to a future task
/// in a follow-up slice.
pub type MapFn = Arc<dyn Fn(&Value, Option<&Value>) -> Result<Vec<Value>, MrError> + Send + Sync>;

/// Reduce-phase function. Pure: takes every accumulated input plus
/// an optional argument, returns zero or more outputs.
pub type ReduceFn =
    Arc<dyn Fn(&[Value], Option<&Value>) -> Result<Vec<Value>, MrError> + Send + Sync>;

/// Named registry of map and reduce functions.
#[derive(Clone, Default)]
pub struct PhaseRegistry {
    map_fns: HashMap<String, MapFn>,
    reduce_fns: HashMap<String, ReduceFn>,
}

impl PhaseRegistry {
    /// Build an empty registry. See [`crate::mapreduce::builtins::default_registry`]
    /// for the registry pre-populated with the built-in functions.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a map function.
    pub fn register_map(&mut self, name: impl Into<String>, f: MapFn) -> &mut Self {
        self.map_fns.insert(name.into(), f);
        self
    }

    /// Register a reduce function.
    pub fn register_reduce(&mut self, name: impl Into<String>, f: ReduceFn) -> &mut Self {
        self.reduce_fns.insert(name.into(), f);
        self
    }

    /// Look up a map function by name.
    #[must_use]
    pub fn map_fn(&self, name: &str) -> Option<&MapFn> {
        self.map_fns.get(name)
    }

    /// Look up a reduce function by name.
    #[must_use]
    pub fn reduce_fn(&self, name: &str) -> Option<&ReduceFn> {
        self.reduce_fns.get(name)
    }

    /// Number of registered map functions.
    #[must_use]
    pub fn map_count(&self) -> usize {
        self.map_fns.len()
    }

    /// Number of registered reduce functions.
    #[must_use]
    pub fn reduce_count(&self) -> usize {
        self.reduce_fns.len()
    }

    /// Names of all registered map functions, sorted.
    #[must_use]
    pub fn map_names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.map_fns.keys().cloned().collect();
        v.sort();
        v
    }

    /// Names of all registered reduce functions, sorted.
    #[must_use]
    pub fn reduce_names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.reduce_fns.keys().cloned().collect();
        v.sort();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_registry_finds_nothing() {
        let r = PhaseRegistry::new();
        assert!(r.map_fn("anything").is_none());
        assert!(r.reduce_fn("anything").is_none());
        assert_eq!(r.map_count(), 0);
        assert_eq!(r.reduce_count(), 0);
    }

    #[test]
    fn registers_and_looks_up_a_map_fn() {
        let mut r = PhaseRegistry::new();
        let f: MapFn = Arc::new(|v: &Value, _: Option<&Value>| Ok(vec![v.clone()]));
        r.register_map("identity", f);
        assert_eq!(r.map_count(), 1);
        let lookup = r.map_fn("identity").expect("present");
        let out = (lookup)(&serde_json::json!(42), None).expect("ok");
        assert_eq!(out, vec![serde_json::json!(42)]);
    }

    #[test]
    fn registers_and_looks_up_a_reduce_fn() {
        let mut r = PhaseRegistry::new();
        let f: ReduceFn = Arc::new(|vs: &[Value], _: Option<&Value>| Ok(vs.to_vec()));
        r.register_reduce("identity", f);
        assert_eq!(r.reduce_count(), 1);
        let lookup = r.reduce_fn("identity").expect("present");
        let out = (lookup)(&[serde_json::json!(1), serde_json::json!(2)], None).expect("ok");
        assert_eq!(out, vec![serde_json::json!(1), serde_json::json!(2)]);
    }
}
