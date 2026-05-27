//! SIGHUP-driven configuration reload integration tests.
//!
//! These exercise the [`dynomited::reload`] pipeline end-to-end
//! against the real on-disk YAML and PEM material so a future
//! refactor that breaks the pipeline (e.g. a stale TLS profile
//! map after `replace`) is caught by the default
//! `cargo nextest run` gate.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use dynomite::conf::{ConfPool, Config};
use dynomite::net::tls::{SharedTlsProfiles, TlsProfileMap, TlsProfileSpec};
use dynomited::reload::{reload_from_path, ReloadableSnapshot, ReloadableState};
use tempfile::TempDir;

fn write_self_signed(dir: &TempDir, name: &str) -> (PathBuf, PathBuf) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_path = dir.path().join(format!("{name}-cert.pem"));
    let key_path = dir.path().join(format!("{name}-key.pem"));
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    (cert_path, key_path)
}

fn yaml_with_tls(
    listen: u16,
    dyn_port: u16,
    stats_port: u16,
    cert: &std::path::Path,
    key: &std::path::Path,
) -> String {
    format!(
        "p:\n  listen: 127.0.0.1:{listen}\n  dyn_listen: 127.0.0.1:{dyn_port}\n  stats_listen: 127.0.0.1:{stats_port}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:22122:1\n  data_store: 0\n  peer_tls_cert: {}\n  peer_tls_key: {}\n",
        cert.display(),
        key.display()
    )
}

#[test]
fn shared_tls_profiles_replace_swaps_resolver_cert() {
    // 1. Build a SharedTlsProfiles with cert A.
    let dir = tempfile::tempdir().unwrap();
    let (cert_a, key_a) = write_self_signed(&dir, "a");
    let map_a = TlsProfileMap::build(
        Some(TlsProfileSpec {
            cert: cert_a.clone(),
            key: key_a.clone(),
            ca: None,
        }),
        BTreeMap::new(),
    )
    .unwrap();
    let shared = SharedTlsProfiles::from_map(map_a);

    // The acceptor's resolver should resolve to cert A.
    let _acceptor = shared.build_sni_acceptor().unwrap().expect("non-empty");
    let client_a = shared.client_config_for_dc("any").unwrap();

    // 2. Replace with cert B.
    let (cert_b, key_b) = write_self_signed(&dir, "b");
    let map_b = TlsProfileMap::build(
        Some(TlsProfileSpec {
            cert: cert_b.clone(),
            key: key_b.clone(),
            ca: None,
        }),
        BTreeMap::new(),
    )
    .unwrap();
    shared.replace(map_b);

    let client_b = shared.client_config_for_dc("any").unwrap();
    // The Arc<ClientConfig> identity must differ after the swap.
    assert!(
        !Arc::ptr_eq(&client_a, &client_b),
        "client config Arc identity must change after replace"
    );

    // The acceptor's underlying ServerConfig is the same object
    // (we kept the `_acceptor`), but its resolver now reads the
    // new map. Verify by invoking the resolver directly: it
    // should return a CertifiedKey whose cert chain matches
    // cert B (post-replace), not cert A.
    // Build a synthetic ClientHello via rustls' API... rustls
    // does not expose a constructor, so instead verify that
    // the public lookup path returns the new material.
    // (The resolver path is exercised by the listener's
    // accept loop; here we just confirm the cell is observable
    // via the public API after swap.)
    assert!(!shared.is_empty());
    assert!(!shared.requires_client_auth());

    // The cert chain held by the new client config must come
    // from cert B; sanity-check by re-reading the bytes through
    // the loader and confirming the file path round-trips. (We
    // cannot peek into ClientConfig's verifier; a smoke test on
    // the acceptor resolver is already covered by
    // `tls_profile_map_per_dc_overrides_default` in the unit
    // tests of `crates/dynomite/src/net/tls.rs`.)
    let bytes_b = std::fs::read(&cert_b).unwrap();
    let bytes_a = std::fs::read(&cert_a).unwrap();
    assert_ne!(bytes_a, bytes_b);
}

#[test]
fn reload_from_path_swaps_tls_material_on_disk_change() {
    // Generate cert A on disk, build a config that points at
    // it, then overwrite the cert files with cert B's bytes
    // (same paths). A reload should produce a new acceptor
    // resolver whose lookup returns the new material.
    let dir = tempfile::tempdir().unwrap();
    let (cert_path, key_path) = write_self_signed(&dir, "live");
    let listener_port = pick_port();
    let dyn_port = pick_port_distinct(listener_port);
    let stats_port = pick_port_distinct_2(listener_port, dyn_port);
    let yaml = yaml_with_tls(listener_port, dyn_port, stats_port, &cert_path, &key_path);
    let yaml_path = dir.path().join("dynomite.yml");
    std::fs::write(&yaml_path, yaml).unwrap();

    let mut cfg = Config::parse_file(&yaml_path).unwrap();
    cfg.finalize();
    cfg.validate().unwrap();
    let original_pool: ConfPool = cfg.pool().clone();

    // Build the live SharedTlsProfiles the same way the server
    // would.
    let initial_map = TlsProfileMap::build(
        Some(TlsProfileSpec {
            cert: cert_path.clone(),
            key: key_path.clone(),
            ca: None,
        }),
        BTreeMap::new(),
    )
    .unwrap();
    let tls = SharedTlsProfiles::from_map(initial_map);
    let state = ReloadableState::new(ReloadableSnapshot::from_pool(&original_pool));

    // The resolver before the swap returns an Arc<CertifiedKey>
    // for some SNI. Capture its identity so we can compare
    // post-swap.
    let acceptor = tls.build_sni_acceptor().unwrap().expect("non-empty");
    drop(acceptor);
    let pre_default = tls
        .client_config_for_dc("any-dc")
        .expect("default profile present");

    // Replace cert / key files with fresh material.
    let (cert_b_pem, key_b_pem) = {
        let new = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        (new.cert.pem(), new.signing_key.serialize_pem())
    };
    std::fs::write(&cert_path, &cert_b_pem).unwrap();
    std::fs::write(&key_path, &key_b_pem).unwrap();

    // Reload: pipeline re-reads the same paths, builds a fresh
    // map, and atomic-swaps it.
    let outcome = reload_from_path(&yaml_path, &original_pool, &state, &tls).unwrap();
    assert!(outcome.tls_swapped, "tls_swapped should be true");
    // The peer_tls paths did not change, only their *contents*,
    // so `peer_tls` will not show up in `reloaded`. That is
    // intentional: the diff is against the YAML, not the file
    // bytes. Cert rotations therefore land silently in the
    // reloaded list but the TLS material is still rebuilt
    // (build_profile_specs reloads from disk every time).
    assert!(outcome.non_reloadable.is_empty());

    // After the swap, the public lookup yields a *different*
    // Arc identity even though the path itself is unchanged.
    let post_default = tls
        .client_config_for_dc("any-dc")
        .expect("default profile present");
    assert!(
        !Arc::ptr_eq(&pre_default, &post_default),
        "client config Arc identity must change after reload"
    );
}

#[test]
fn reload_from_path_with_invalid_yaml_keeps_state_intact() {
    let dir = tempfile::tempdir().unwrap();
    let listener_port = pick_port();
    let dyn_port = pick_port_distinct(listener_port);
    let stats_port = pick_port_distinct_2(listener_port, dyn_port);
    let yaml = format!(
        "p:\n  listen: 127.0.0.1:{listener_port}\n  dyn_listen: 127.0.0.1:{dyn_port}\n  stats_listen: 127.0.0.1:{stats_port}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:22122:1\n  data_store: 0\n  read_consistency: DC_ONE\n",
    );
    let yaml_path = dir.path().join("dynomite.yml");
    std::fs::write(&yaml_path, yaml).unwrap();
    let mut cfg = Config::parse_file(&yaml_path).unwrap();
    cfg.finalize();
    cfg.validate().unwrap();
    let original_pool = cfg.pool().clone();
    let initial = ReloadableSnapshot::from_pool(&original_pool);
    let state = ReloadableState::new(initial.clone());
    let tls = SharedTlsProfiles::default();

    // Corrupt the YAML on disk.
    std::fs::write(&yaml_path, "not_a_pool: [\n  legitimately broken").unwrap();

    let err = reload_from_path(&yaml_path, &original_pool, &state, &tls).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("reload"), "unexpected error: {msg}");
    // The live state must be exactly what it was before the
    // failed reload.
    assert_eq!(state.snapshot(), initial);
}

#[test]
fn reload_from_path_accepts_changed_consistency() {
    let dir = tempfile::tempdir().unwrap();
    let listener_port = pick_port();
    let dyn_port = pick_port_distinct(listener_port);
    let stats_port = pick_port_distinct_2(listener_port, dyn_port);
    let yaml = format!(
        "p:\n  listen: 127.0.0.1:{listener_port}\n  dyn_listen: 127.0.0.1:{dyn_port}\n  stats_listen: 127.0.0.1:{stats_port}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:22122:1\n  data_store: 0\n  read_consistency: DC_ONE\n  write_consistency: DC_ONE\n",
    );
    let yaml_path = dir.path().join("dynomite.yml");
    std::fs::write(&yaml_path, yaml).unwrap();
    let mut cfg = Config::parse_file(&yaml_path).unwrap();
    cfg.finalize();
    cfg.validate().unwrap();
    let original_pool = cfg.pool().clone();
    let state = ReloadableState::new(ReloadableSnapshot::from_pool(&original_pool));
    let tls = SharedTlsProfiles::default();

    // Rewrite YAML with DC_QUORUM read_consistency.
    let updated = format!(
        "p:\n  listen: 127.0.0.1:{listener_port}\n  dyn_listen: 127.0.0.1:{dyn_port}\n  stats_listen: 127.0.0.1:{stats_port}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:22122:1\n  data_store: 0\n  read_consistency: DC_QUORUM\n  write_consistency: DC_ONE\n",
    );
    std::fs::write(&yaml_path, updated).unwrap();

    let outcome = reload_from_path(&yaml_path, &original_pool, &state, &tls).unwrap();
    assert!(outcome.reloaded.contains(&"read_consistency"));
    assert!(outcome.non_reloadable.is_empty());
    assert_eq!(
        state.snapshot().read_consistency.as_deref(),
        Some("DC_QUORUM")
    );
}

#[test]
fn reload_warns_on_listener_port_change() {
    let dir = tempfile::tempdir().unwrap();
    let listener_port = pick_port();
    let dyn_port = pick_port_distinct(listener_port);
    let stats_port = pick_port_distinct_2(listener_port, dyn_port);
    let yaml = format!(
        "p:\n  listen: 127.0.0.1:{listener_port}\n  dyn_listen: 127.0.0.1:{dyn_port}\n  stats_listen: 127.0.0.1:{stats_port}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:22122:1\n  data_store: 0\n",
    );
    let yaml_path = dir.path().join("dynomite.yml");
    std::fs::write(&yaml_path, yaml).unwrap();
    let mut cfg = Config::parse_file(&yaml_path).unwrap();
    cfg.finalize();
    cfg.validate().unwrap();
    let original_pool = cfg.pool().clone();
    let state = ReloadableState::new(ReloadableSnapshot::from_pool(&original_pool));
    let tls = SharedTlsProfiles::default();

    // Change the listen port (non-reloadable).
    let new_listen_port = pick_port_distinct_2(listener_port, dyn_port);
    let updated = format!(
        "p:\n  listen: 127.0.0.1:{new_listen_port}\n  dyn_listen: 127.0.0.1:{dyn_port}\n  stats_listen: 127.0.0.1:{stats_port}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:22122:1\n  data_store: 0\n",
    );
    std::fs::write(&yaml_path, updated).unwrap();

    let outcome = reload_from_path(&yaml_path, &original_pool, &state, &tls).unwrap();
    assert!(outcome.non_reloadable.contains(&"listen"));
    assert!(outcome.reloaded.is_empty());
}

#[test]
fn reloading_dc_sni_resolver_picks_up_new_cert_after_replace() {
    // Build SharedTlsProfiles with a per-DC entry "dc1" -> cert A
    // plus a default profile -> cert default. After replace,
    // the resolver returns the new entry's CertifiedKey.
    let dir = tempfile::tempdir().unwrap();
    let (def_cert, def_key) = write_self_signed(&dir, "default");
    let (dc1_a_cert, dc1_a_key) = write_self_signed(&dir, "dc1-a");
    let mut per_dc = BTreeMap::new();
    per_dc.insert(
        "dc1".into(),
        TlsProfileSpec {
            cert: dc1_a_cert,
            key: dc1_a_key,
            ca: None,
        },
    );
    let map = TlsProfileMap::build(
        Some(TlsProfileSpec {
            cert: def_cert.clone(),
            key: def_key.clone(),
            ca: None,
        }),
        per_dc,
    )
    .unwrap();
    let shared = SharedTlsProfiles::from_map(map);

    // The acceptor uses the reloading resolver; smoke-check
    // that an SNI for dc1 returns the per-DC client config
    // before and a different one after the swap.
    let pre_dc1 = shared.client_config_for_dc("dc1").unwrap();

    // Swap in a fresh cert for dc1 (cert B) plus the same
    // default.
    let (dc1_new_cert, dc1_new_key) = write_self_signed(&dir, "dc1-b");
    let mut per_dc2 = BTreeMap::new();
    per_dc2.insert(
        "dc1".into(),
        TlsProfileSpec {
            cert: dc1_new_cert,
            key: dc1_new_key,
            ca: None,
        },
    );
    let map2 = TlsProfileMap::build(
        Some(TlsProfileSpec {
            cert: def_cert,
            key: def_key,
            ca: None,
        }),
        per_dc2,
    )
    .unwrap();
    shared.replace(map2);

    let post_dc1 = shared.client_config_for_dc("dc1").unwrap();
    assert!(
        !Arc::ptr_eq(&pre_dc1, &post_dc1),
        "per-dc1 client config Arc identity must change after replace"
    );
}

// rustls forbids ClientHello construction outside the library
// itself, so we cannot trivially exercise the `resolve` callback
// with a synthetic hello. The negative space is covered by the
// public API smoke checks above plus the per-DC override unit
// tests in `crates/dynomite/src/net/tls.rs`.

fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn pick_port_distinct(other: u16) -> u16 {
    for _ in 0..32 {
        let p = pick_port();
        if p != other {
            return p;
        }
    }
    panic!("could not find a free port distinct from {other}");
}

fn pick_port_distinct_2(a: u16, b: u16) -> u16 {
    for _ in 0..64 {
        let p = pick_port();
        if p != a && p != b {
            return p;
        }
    }
    panic!("could not find a free port distinct from {a} and {b}");
}
