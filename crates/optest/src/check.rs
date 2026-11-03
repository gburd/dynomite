//! History checkers for write-once registers.
//!
//! Dynomite does not promise linearizability, so the checker asserts what
//! each consistency level does promise, on histories where every key is
//! written at most once with a value that names its key (`<key>=<n>`):
//!
//! * **Integrity** (every level): a successful read returns null or a value
//!   some write to *that key* sent. A value written to another key means a
//!   reply reached the wrong request; a value nobody wrote is fabricated.
//! * **Read-after-acknowledged-write** (`DC_QUORUM` and stronger): a read
//!   invoked after a write to the same key completed `ok` returns that
//!   value or fails; it never returns null. Write and read quorums
//!   intersect, so a null would mean the acknowledged write was lost.
//! * **Monotonic reads** (`DC_QUORUM` and stronger): once some read of a key
//!   returned its value, no read invoked after that read completed returns
//!   null.
//!
//! Under `DC_ONE` only integrity is checked online; convergence is a
//! separate post-heal assertion over the datastores.

use std::collections::HashMap;

use crate::history::{Event, Op, Outcome};

/// What a history is checked against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Guarantee {
    /// `DC_ONE`: integrity only.
    Eventual,
    /// `DC_QUORUM`, `DC_SAFE_QUORUM`, `DC_EACH_SAFE_QUORUM`.
    Quorum,
}

/// One violation found in a history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Anomaly {
    /// Read of `key` returned a value no write to `key` sent.
    Fabricated {
        /// Key read.
        key: String,
        /// Value returned.
        value: String,
    },
    /// Read of `key` returned a value written to `other_key`.
    WrongKey {
        /// Key read.
        key: String,
        /// Key the value belongs to.
        other_key: String,
    },
    /// A write was acknowledged before the read began, yet the read saw null.
    LostAckedWrite {
        /// Key.
        key: String,
        /// Read invocation time.
        read_ns: u128,
        /// Write completion time.
        write_ok_ns: u128,
    },
    /// A read saw null after an earlier read had already seen the value.
    NonMonotonic {
        /// Key.
        key: String,
        /// The later read's invocation time.
        read_ns: u128,
    },
    /// A key was written twice (the workload broke the write-once contract).
    DoubleWrite {
        /// Key.
        key: String,
    },
}

/// The checker's verdict.
#[derive(Clone, Debug, Default)]
pub struct Report {
    /// Every violation found.
    pub anomalies: Vec<Anomaly>,
    /// Successful reads checked.
    pub reads_ok: usize,
    /// Acknowledged writes.
    pub writes_ok: usize,
    /// Operations with unknown outcome (timeouts).
    pub indeterminate: usize,
    /// Operations that failed cleanly.
    pub failed: usize,
}

impl Report {
    /// True when no anomaly was found.
    pub fn valid(&self) -> bool {
        self.anomalies.is_empty()
    }
}

fn key_of(value: &str) -> Option<&str> {
    value.rsplit_once('=').map(|(k, _)| k)
}

/// Check a write-once register history against `g`.
///
/// # Examples
///
/// ```
/// use optest::check::{check, Guarantee};
/// use optest::history::{Event, Op, Outcome};
/// let w = Event { process: 0, op: Op::Write { key: "k".into(), value: "k=1".into() },
///                 outcome: Outcome::Ok, invoke_ns: 0, complete_ns: 10 };
/// let r = Event { process: 1, op: Op::Read { key: "k".into(), value: None },
///                 outcome: Outcome::Ok, invoke_ns: 20, complete_ns: 30 };
/// assert!(check(&[w.clone(), r.clone()], Guarantee::Eventual).valid());
/// assert!(!check(&[w, r], Guarantee::Quorum).valid());
/// ```
pub fn check(events: &[Event], g: Guarantee) -> Report {
    let mut rep = Report::default();
    // key -> (value, ok completion time if acknowledged)
    let mut writes: HashMap<&str, (&str, Option<u128>)> = HashMap::new();
    for e in events {
        match e.outcome {
            Outcome::Info => rep.indeterminate += 1,
            Outcome::Fail => rep.failed += 1,
            Outcome::Ok => {}
        }
        if let Op::Write { key, value } = &e.op {
            if e.outcome == Outcome::Fail {
                continue;
            }
            if writes.contains_key(key.as_str()) {
                rep.anomalies
                    .push(Anomaly::DoubleWrite { key: key.clone() });
                continue;
            }
            let acked = (e.outcome == Outcome::Ok).then_some(e.complete_ns);
            if acked.is_some() {
                rep.writes_ok += 1;
            }
            writes.insert(key, (value, acked));
        }
    }
    // key -> earliest completion time of a read that saw the value
    let mut first_seen: HashMap<&str, u128> = HashMap::new();
    let mut reads: Vec<&Event> = events
        .iter()
        .filter(|e| e.outcome == Outcome::Ok && matches!(e.op, Op::Read { .. }))
        .collect();
    reads.sort_by_key(|e| e.invoke_ns);
    for e in &reads {
        let Op::Read { key, value } = &e.op else {
            continue;
        };
        rep.reads_ok += 1;
        if let Some(v) = value {
            match writes.get(key.as_str()) {
                Some((w, _)) if *w == v.as_str() => {
                    let t = first_seen.entry(key).or_insert(e.complete_ns);
                    *t = (*t).min(e.complete_ns);
                }
                _ => match key_of(v) {
                    Some(k) if k != key.as_str() && writes.contains_key(k) => {
                        rep.anomalies.push(Anomaly::WrongKey {
                            key: key.clone(),
                            other_key: k.to_string(),
                        });
                    }
                    _ => rep.anomalies.push(Anomaly::Fabricated {
                        key: key.clone(),
                        value: v.clone(),
                    }),
                },
            }
        }
    }
    if g == Guarantee::Quorum {
        for e in &reads {
            let Op::Read { key, value: None } = &e.op else {
                continue;
            };
            if let Some((_, Some(ok_ns))) = writes.get(key.as_str()) {
                if *ok_ns < e.invoke_ns {
                    rep.anomalies.push(Anomaly::LostAckedWrite {
                        key: key.clone(),
                        read_ns: e.invoke_ns,
                        write_ok_ns: *ok_ns,
                    });
                    continue;
                }
            }
            if let Some(seen) = first_seen.get(key.as_str()) {
                if *seen < e.invoke_ns {
                    rep.anomalies.push(Anomaly::NonMonotonic {
                        key: key.clone(),
                        read_ns: e.invoke_ns,
                    });
                }
            }
        }
    }
    rep
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(p: usize, k: &str, v: &str, o: Outcome, t0: u128, t1: u128) -> Event {
        Event {
            process: p,
            op: Op::Write {
                key: k.into(),
                value: v.into(),
            },
            outcome: o,
            invoke_ns: t0,
            complete_ns: t1,
        }
    }
    fn r(p: usize, k: &str, v: Option<&str>, t0: u128, t1: u128) -> Event {
        Event {
            process: p,
            op: Op::Read {
                key: k.into(),
                value: v.map(Into::into),
            },
            outcome: Outcome::Ok,
            invoke_ns: t0,
            complete_ns: t1,
        }
    }

    #[test]
    fn concurrent_read_may_miss_a_write_in_flight() {
        let h = [
            w(0, "a", "a=1", Outcome::Ok, 0, 100),
            r(1, "a", None, 50, 60),
        ];
        assert!(check(&h, Guarantee::Quorum).valid());
    }

    #[test]
    fn negative_control_lost_acked_write() {
        let h = [
            w(0, "a", "a=1", Outcome::Ok, 0, 10),
            r(1, "a", None, 20, 30),
        ];
        let rep = check(&h, Guarantee::Quorum);
        assert!(matches!(
            rep.anomalies[..],
            [Anomaly::LostAckedWrite { .. }]
        ));
        assert!(check(&h, Guarantee::Eventual).valid());
    }

    #[test]
    fn negative_control_wrong_key_and_fabricated() {
        let h = [
            w(0, "a", "a=1", Outcome::Ok, 0, 10),
            w(0, "b", "b=2", Outcome::Ok, 0, 10),
            r(1, "a", Some("b=2"), 20, 30),
            r(1, "b", Some("zzz"), 20, 30),
        ];
        let rep = check(&h, Guarantee::Eventual);
        assert_eq!(rep.anomalies.len(), 2, "{rep:?}");
        assert!(matches!(rep.anomalies[0], Anomaly::WrongKey { .. }));
        assert!(matches!(rep.anomalies[1], Anomaly::Fabricated { .. }));
    }

    #[test]
    fn negative_control_non_monotonic() {
        // Write outcome unknown (timed out): read-after-ack does not apply,
        // but once a read saw the value a later null is non-monotonic.
        let h = [
            w(0, "a", "a=1", Outcome::Info, 0, 1000),
            r(1, "a", Some("a=1"), 10, 20),
            r(2, "a", None, 30, 40),
        ];
        let rep = check(&h, Guarantee::Quorum);
        assert!(
            matches!(rep.anomalies[..], [Anomaly::NonMonotonic { .. }]),
            "{rep:?}"
        );
    }

    #[test]
    fn indeterminate_write_value_is_not_fabricated() {
        let h = [
            w(0, "a", "a=1", Outcome::Info, 0, 1000),
            r(1, "a", Some("a=1"), 10, 20),
        ];
        assert!(check(&h, Guarantee::Quorum).valid());
    }
}
