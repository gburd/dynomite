//! Network faults with `tc` netem on each node's egress device.
//!
//! Every node lives in its own network namespace with one interface,
//! `eth0` (see [`crate::cluster`]). A fault from node `a` to node `b` is a
//! netem band on `a`'s `eth0` plus a u32 filter on destination `b` that
//! steers the matching packets into it. The root qdisc is a `prio` qdisc
//! whose priomap sends all unmatched traffic to a pass-through band, so
//! only the pairs a scenario names are impaired.
//!
//! Each [`Impairment`] gets its own band (up to [`MAX_BANDS`] per node);
//! netem is seeded, so loss and reordering are reproducible run to run.

use std::collections::HashMap;
use std::net::Ipv4Addr;

use crate::{Error, Result};

/// Pass-through band plus impairment bands. `prio` supports 16 bands.
pub const MAX_BANDS: u8 = 15;

/// One netem impairment applied to a directed address pair.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Impairment {
    /// One-way added delay in milliseconds.
    pub delay_ms: u32,
    /// Delay jitter in milliseconds (normal distribution).
    pub jitter_ms: u32,
    /// Packet loss percentage, 0..=100. 100 is a partition.
    pub loss_pct: f32,
    /// Duplicate percentage.
    pub duplicate_pct: f32,
    /// Reorder percentage (requires a delay).
    pub reorder_pct: f32,
    /// Single-bit corruption percentage.
    pub corrupt_pct: f32,
    /// Rate limit in kbit/s; 0 means unlimited.
    pub rate_kbit: u32,
}

impl Impairment {
    /// Drop every packet: a partition in one direction.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(optest::net::Impairment::partition().loss_pct, 100.0);
    /// ```
    pub fn partition() -> Self {
        Self {
            loss_pct: 100.0,
            ..Self::default()
        }
    }

    /// Fixed one-way delay.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(optest::net::Impairment::delay(50).delay_ms, 50);
    /// ```
    pub fn delay(ms: u32) -> Self {
        Self {
            delay_ms: ms,
            ..Self::default()
        }
    }

    /// The netem argument list for this impairment, with `seed`.
    ///
    /// # Examples
    ///
    /// ```
    /// let a = optest::net::Impairment::partition().netem_args(7);
    /// assert_eq!(a, ["loss", "100%", "seed", "7"]);
    /// ```
    pub fn netem_args(&self, seed: u64) -> Vec<String> {
        let mut a: Vec<String> = Vec::new();
        if self.delay_ms > 0 {
            a.extend(["delay".into(), format!("{}ms", self.delay_ms)]);
            if self.jitter_ms > 0 {
                a.extend([
                    format!("{}ms", self.jitter_ms),
                    "distribution".into(),
                    "normal".into(),
                ]);
            }
        }
        if self.loss_pct > 0.0 {
            a.extend(["loss".into(), format!("{}%", self.loss_pct)]);
        }
        if self.duplicate_pct > 0.0 {
            a.extend(["duplicate".into(), format!("{}%", self.duplicate_pct)]);
        }
        if self.reorder_pct > 0.0 && self.delay_ms > 0 {
            a.extend(["reorder".into(), format!("{}%", self.reorder_pct)]);
        }
        if self.corrupt_pct > 0.0 {
            a.extend(["corrupt".into(), format!("{}%", self.corrupt_pct)]);
        }
        if self.rate_kbit > 0 {
            a.extend(["rate".into(), format!("{}kbit", self.rate_kbit)]);
        }
        a.extend(["seed".into(), seed.to_string()]);
        a
    }
}

/// The netem state of every node's egress device.
#[derive(Debug)]
pub struct Netem {
    tc: std::path::PathBuf,
    seed: u64,
    /// Next free band per node index; presence means the root is installed.
    bands: HashMap<usize, u8>,
}

/// Node index for an address `10.77.0.(i+1)`.
fn node_of(a: Ipv4Addr) -> Result<usize> {
    let o = a.octets();
    if o[0] == 10 && o[1] == 77 && o[2] == 0 && (1..=253).contains(&o[3]) {
        Ok(usize::from(o[3]) - 1)
    } else {
        Err(Error::Protocol(format!("{a} is not a node address")))
    }
}

impl Netem {
    /// A handle on every node's `eth0`. Any qdisc a previous handle left is
    /// removed, so the network starts whole.
    pub fn new(seed: u64) -> Result<Self> {
        let tc = crate::tools::which("tc").ok_or(Error::MissingTool("tc"))?;
        let n = Self {
            tc,
            seed,
            bands: HashMap::new(),
        };
        for i in 0..253 {
            if !std::path::Path::new("/run/netns")
                .join(crate::cluster::netns(i))
                .exists()
            {
                break;
            }
            // Deleting a root that does not exist fails; that is the clean state.
            let _ = n.tc_in(i, &["qdisc", "del", "dev", "eth0", "root"]);
        }
        Ok(n)
    }

    fn tc_in(&self, node: usize, args: &[&str]) -> Result<()> {
        let out = crate::cluster::in_netns(node, &self.tc)
            .args(args)
            .output()?;
        if out.status.success() {
            Ok(())
        } else {
            Err(Error::Command {
                cmd: format!("[n{node}] tc {}", args.join(" ")),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            })
        }
    }

    fn next_band(&mut self, node: usize) -> Result<u8> {
        if !self.bands.contains_key(&node) {
            // 16 bands; priomap sends every TOS to band 16 (index 15), the
            // pass-through band. Bands 1..=15 carry the impairments.
            let mut args = vec![
                "qdisc", "add", "dev", "eth0", "root", "handle", "1:", "prio", "bands", "16",
                "priomap",
            ];
            args.extend(["15"; 16]);
            self.tc_in(node, &args)?;
            self.bands.insert(node, 1);
        }
        let b = self.bands.get_mut(&node).expect("inserted above");
        if *b > MAX_BANDS {
            return Err(Error::Protocol(format!(
                "netem: node {node} out of prio bands"
            )));
        }
        let band = *b;
        *b += 1;
        Ok(band)
    }

    /// Impair packets from `src` to `dst` (one direction).
    pub fn impair(&mut self, src: Ipv4Addr, dst: Ipv4Addr, imp: &Impairment) -> Result<()> {
        let node = node_of(src)?;
        let band = self.next_band(node)?;
        let parent = format!("1:{band:x}");
        let handle = format!("{}:", 10 + u32::from(band));
        let seed = self
            .seed
            .wrapping_add(u64::from(band))
            .wrapping_add((node as u64) << 8);
        let netem = imp.netem_args(seed);
        let mut q = vec![
            "qdisc", "add", "dev", "eth0", "parent", &parent, "handle", &handle, "netem",
        ];
        q.extend(netem.iter().map(String::as_str));
        self.tc_in(node, &q)?;
        let d = format!("{dst}/32");
        self.tc_in(
            node,
            &[
                "filter", "add", "dev", "eth0", "parent", "1:", "protocol", "ip", "prio", "1",
                "u32", "match", "ip", "dst", &d, "flowid", &parent,
            ],
        )
    }

    /// Impair both directions between `a` and `b`.
    pub fn impair_both(&mut self, a: Ipv4Addr, b: Ipv4Addr, imp: &Impairment) -> Result<()> {
        self.impair(a, b, imp)?;
        self.impair(b, a, imp)
    }

    /// Cut `a` off from every address in `others`, both directions.
    pub fn isolate(&mut self, a: Ipv4Addr, others: &[Ipv4Addr]) -> Result<()> {
        for &o in others {
            self.impair_both(a, o, &Impairment::partition())?;
        }
        Ok(())
    }

    /// Remove every impairment: the network is whole again.
    pub fn heal(&mut self) -> Result<()> {
        let nodes: Vec<usize> = self.bands.keys().copied().collect();
        for node in nodes {
            self.tc_in(node, &["qdisc", "del", "dev", "eth0", "root"])?;
        }
        self.bands.clear();
        Ok(())
    }

    fn stat_sum(&self, what: &[&str], marker: &str) -> Result<u64> {
        let mut total = 0;
        for &node in self.bands.keys() {
            let out = crate::cluster::in_netns(node, &self.tc)
                .args(what)
                .output()?;
            let text = String::from_utf8_lossy(&out.stdout);
            total += text
                .split(marker)
                .skip(1)
                .filter_map(|s| {
                    s.split(|c: char| !c.is_ascii_digit())
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
                .sum::<u64>();
        }
        Ok(total)
    }

    /// Packets the impairment filters have steered into netem bands: proof
    /// a fault actually saw traffic.
    pub fn matched(&self) -> Result<u64> {
        self.stat_sum(
            &["-s", "filter", "show", "dev", "eth0", "parent", "1:"],
            "success ",
        )
    }

    /// Packets netem has dropped across all impaired nodes.
    pub fn dropped(&self) -> Result<u64> {
        self.stat_sum(&["-s", "qdisc", "show", "dev", "eth0"], "dropped ")
    }
}

impl Drop for Netem {
    /// Faults never outlive the handle that installed them: a following
    /// cluster in the same sandbox starts on a whole network.
    fn drop(&mut self) {
        let _ = self.heal();
    }
}
