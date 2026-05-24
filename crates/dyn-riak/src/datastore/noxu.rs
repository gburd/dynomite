//! Bridge from [`crate`] to the in-process Noxu DB storage engine.
//!
//! `NoxuDatastore` opens a single [`noxu_db::Environment`] per process
//! and one default [`noxu_db::Database`] for the keyspace. Every put,
//! get and delete is auto-committed against that database.
//!
//! The datastore satisfies [`dynomite::embed::Datastore`] so an
//! embedder can install it into a [`dynomite::embed::Server`]; today
//! the [`Datastore::dispatch`] surface is a counter-only trampoline
//! while the Riak K/V trait is being designed in the follow-up
//! slice. The owning [`NoxuDatastore::get`], [`NoxuDatastore::put`],
//! and [`NoxuDatastore::delete`] methods carry the real semantics
//! and are what the Riak protocol layer calls into.
//!
//! The module is gated behind the `noxu` Cargo feature so the rest
//! of the crate compiles without the lamdb checkout. Toggle the
//! feature on with `cargo build --features dyn-riak/noxu`.

use std::path::Path;
use std::sync::{Arc, Mutex};

use dynomite::embed::hooks::{BoxFuture, Datastore, DatastoreError, Protocol};
use dynomite::msg::{Msg, MsgType};
use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, NoxuError,
    OperationStatus,
};

/// Errors produced by [`NoxuDatastore`] operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NoxuDatastoreError {
    /// Failed to open the environment, the database, or perform an
    /// operation on the database.
    #[error("noxu: {0}")]
    Noxu(#[from] NoxuError),
}

/// Riak-shaped storage bridge backed by Noxu DB.
///
/// Construct with [`NoxuDatastore::open`] (production) or
/// [`NoxuDatastore::open_in`] (tests / examples). The struct is cheap
/// to clone via [`Arc`].
#[derive(Clone)]
pub struct NoxuDatastore {
    inner: Arc<Inner>,
}

struct Inner {
    /// The Noxu environment is held alive for the lifetime of the
    /// datastore so the database handle remains valid.
    _env: Mutex<Environment>,
    /// Default database used by the v0.0.1 slice. The follow-up
    /// slice replaces this with one database per (vnode, kind)
    /// pair as described in `docs/riak-compat-plan.md` Section 3.2.
    db: Mutex<Database>,
}

impl NoxuDatastore {
    /// Open or create a Noxu environment rooted at `path` and the
    /// default database used by the v0.0.1 Riak slice.
    ///
    /// The `path` directory must be writable. A fresh environment is
    /// created if `allow_create` semantics permit; otherwise the
    /// underlying [`NoxuError`] is surfaced unchanged.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    /// use dyn_riak::datastore::NoxuDatastore;
    /// let ds = NoxuDatastore::open(Path::new("/var/lib/dynomite/riak"))
    ///     .expect("open noxu environment");
    /// ds.put(b"hello", b"world").expect("put");
    /// assert_eq!(ds.get(b"hello").unwrap().as_deref(), Some(&b"world"[..]));
    /// ```
    pub fn open(path: &Path) -> Result<Self, NoxuDatastoreError> {
        Self::open_with_db_name(path, "riak.objects")
    }

    /// Variant of [`Self::open`] that lets the caller pick the
    /// database name. Used by tests that need disjoint environments
    /// in the same process.
    pub fn open_with_db_name(path: &Path, db_name: &str) -> Result<Self, NoxuDatastoreError> {
        let env_config = EnvironmentConfig::new(path.to_path_buf())
            .with_allow_create(true)
            .with_transactional(false);
        let env = Environment::open(env_config)?;

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(false);
        let db = env.open_database(None, db_name, &db_config)?;

        Ok(Self {
            inner: Arc::new(Inner {
                _env: Mutex::new(env),
                db: Mutex::new(db),
            }),
        })
    }

    /// Convenience constructor that opens the datastore inside an
    /// existing temp directory. Equivalent to [`Self::open`] but
    /// makes intent obvious in tests.
    pub fn open_in(dir: &Path) -> Result<Self, NoxuDatastoreError> {
        Self::open(dir)
    }

    /// Look up `key`. Returns `Ok(None)` for a missing key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, NoxuDatastoreError> {
        let mut value = DatabaseEntry::new();
        let key_entry = DatabaseEntry::from_bytes(key);
        let db = self.lock_db();
        match db.get(None, &key_entry, &mut value)? {
            OperationStatus::Success => Ok(Some(value.data().to_vec())),
            OperationStatus::NotFound | OperationStatus::KeyExists => Ok(None),
        }
    }

    /// Insert or overwrite the value at `key`.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), NoxuDatastoreError> {
        let key_entry = DatabaseEntry::from_bytes(key);
        let value_entry = DatabaseEntry::from_bytes(value);
        let db = self.lock_db();
        db.put(None, &key_entry, &value_entry)?;
        Ok(())
    }

    /// Remove the value at `key`. Returns `Ok(false)` if the key was
    /// absent, `Ok(true)` if it was present and is now removed.
    pub fn delete(&self, key: &[u8]) -> Result<bool, NoxuDatastoreError> {
        let key_entry = DatabaseEntry::from_bytes(key);
        let db = self.lock_db();
        match db.delete(None, &key_entry)? {
            OperationStatus::Success => Ok(true),
            OperationStatus::NotFound | OperationStatus::KeyExists => Ok(false),
        }
    }

    fn lock_db(&self) -> std::sync::MutexGuard<'_, Database> {
        // The Mutex is only contended by the datastore itself; a
        // poisoned guard means another caller paniced mid-op, in
        // which case the database state is undefined and the
        // process should not continue silently. Recovering the
        // guard preserves liveness so callers see the failure as
        // a NoxuError on their next operation rather than a
        // poison panic propagating out of an arbitrary public
        // method.
        self.inner
            .db
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Datastore for NoxuDatastore {
    fn protocol(&self) -> Protocol {
        Protocol::Custom
    }

    fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
        // Stage-shape note: the v0.0.1 dispatch is a trampoline that
        // returns a routing acknowledgement so the substrate's
        // accounting fires. Real K/V execution happens through
        // [`Self::get`], [`Self::put`], and [`Self::delete`] which
        // the Riak PBC server calls directly.
        Box::pin(async move {
            let mut rsp = Msg::new(req.id(), MsgType::Unknown, false);
            rsp.set_parent_id(req.id());
            Ok(rsp)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn put_get_delete_round_trips() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");

        ds.put(b"alpha", b"one").expect("put alpha");
        ds.put(b"beta", b"two").expect("put beta");

        assert_eq!(ds.get(b"alpha").unwrap().as_deref(), Some(&b"one"[..]));
        assert_eq!(ds.get(b"beta").unwrap().as_deref(), Some(&b"two"[..]));
        assert_eq!(ds.get(b"missing").unwrap(), None);

        assert!(ds.delete(b"alpha").unwrap());
        assert!(!ds.delete(b"alpha").unwrap());
        assert_eq!(ds.get(b"alpha").unwrap(), None);
        assert_eq!(ds.get(b"beta").unwrap().as_deref(), Some(&b"two"[..]));
    }

    #[test]
    fn put_overwrites_existing_value() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");

        ds.put(b"k", b"v1").expect("put v1");
        ds.put(b"k", b"v2").expect("put v2");
        assert_eq!(ds.get(b"k").unwrap().as_deref(), Some(&b"v2"[..]));
    }

    #[tokio::test]
    async fn datastore_dispatch_is_a_trampoline() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");
        let req = Msg::new(7, MsgType::Unknown, true);
        let rsp = <NoxuDatastore as Datastore>::dispatch(&ds, req)
            .await
            .expect("dispatch");
        assert_eq!(rsp.parent_id(), 7);
    }
}
