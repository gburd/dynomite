//! Process-level fault injection: libfiu (I/O), libumem and FAINT
//! (allocation), libdirectio (page-cache bypass), libfaketime (clock).
//!
//! A [`Faults`] value describes what to preload into one node. The node is
//! launched with the matching `LD_PRELOAD` and environment; libfiu faults
//! can then be toggled while the node runs through [`FiuControl`], which
//! speaks libfiu's remote-control FIFO protocol (`<prefix>-<pid>.in` /
//! `.out`), so a scenario can, for example, start a node healthy, fail its
//! `fdatasync` calls for ten seconds, and heal it.
//!
//! libfiu failure-point names follow its POSIX preload modules, e.g.
//! `posix/io/rw/write`, `posix/io/sync/fdatasync`, `posix/io/net/connect`,
//! `posix/io/oc/open`, `libc/mm/malloc`. Random points take a probability
//! and are seeded by `FIU_PRNG_SEED`, so runs are reproducible.

use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use crate::tools::Tools;
use crate::{Error, Result};

/// A libfiu failure point to enable at launch.
#[derive(Clone, Debug, PartialEq)]
pub enum FiuPoint {
    /// Always fail `name` (with libfiu's default errno for that call).
    Always(String),
    /// Fail `name` with probability `p` in `0.0..=1.0`.
    Random(String, f64),
    /// Fail `name` once, then disable it.
    Once(String),
}

impl FiuPoint {
    /// The libfiu remote-control command that enables this point.
    ///
    /// # Examples
    ///
    /// ```
    /// use optest::fault::FiuPoint;
    /// assert_eq!(
    ///     FiuPoint::Random("posix/io/rw/write".into(), 0.25).command(),
    ///     "enable_random name=posix/io/rw/write,probability=0.25"
    /// );
    /// ```
    pub fn command(&self) -> String {
        match self {
            FiuPoint::Always(n) => format!("enable name={n}"),
            FiuPoint::Random(n, p) => format!("enable_random name={n},probability={p}"),
            FiuPoint::Once(n) => format!("enable name={n},onetime"),
        }
    }
}

/// Everything preloaded into one node process.
#[derive(Clone, Debug, Default)]
pub struct Faults {
    /// libfiu points enabled at launch. Any non-empty value (or
    /// `fiu_control`) preloads libfiu.
    pub fiu: Vec<FiuPoint>,
    /// Preload libfiu with the control FIFO open even with no points, so
    /// faults can be enabled later.
    pub fiu_control: bool,
    /// libfiu PRNG seed for random points.
    pub fiu_seed: u64,
    /// libumem mean time between allocation failures (`UMEM_DEBUG=mtbf=N`).
    pub umem_mtbf: Option<u32>,
    /// libumem guards (`UMEM_DEBUG=guards`): redzones and use-after-free
    /// detection under the libumem allocator.
    pub umem_guards: bool,
    /// Preload libdirectio so file I/O bypasses the page cache.
    pub directio: bool,
    /// libfaketime offset, e.g. `"+30s"` or `"-2m"`.
    pub faketime: Option<String>,
}

impl Faults {
    /// True when nothing is preloaded.
    pub fn is_empty(&self) -> bool {
        self.fiu.is_empty()
            && !self.fiu_control
            && self.umem_mtbf.is_none()
            && !self.umem_guards
            && !self.directio
            && self.faketime.is_none()
    }

    /// Apply this fault set to `cmd`: `LD_PRELOAD` and the libraries'
    /// environment. `fifo_prefix` is where libfiu opens its control FIFO.
    pub fn apply(&self, tools: &Tools, cmd: &mut Command, fifo_prefix: &Path) -> Result<()> {
        let mut preload: Vec<PathBuf> = Vec::new();
        if !self.fiu.is_empty() || self.fiu_control {
            let run = tools
                .fiu_run_preload
                .clone()
                .ok_or(Error::MissingTool("libfiu fiu_run_preload.so"))?;
            let posix = tools
                .fiu_posix_preload
                .clone()
                .ok_or(Error::MissingTool("libfiu fiu_posix_preload.so"))?;
            preload.push(run);
            preload.push(posix);
            let enable: Vec<String> = self.fiu.iter().map(FiuPoint::command).collect();
            cmd.env("FIU_ENABLE", enable.join("\n"));
            cmd.env("FIU_CTRL_FIFO", fifo_prefix);
            cmd.env("FIU_PRNG_SEED", self.fiu_seed.to_string());
            if let Some(dir) = tools.fiu_run_preload.as_ref().and_then(|p| p.parent()) {
                cmd.env("LD_LIBRARY_PATH", dir);
            }
        }
        if self.umem_mtbf.is_some() || self.umem_guards {
            preload.push(
                tools
                    .umem_malloc
                    .clone()
                    .ok_or(Error::MissingTool("libumem libumem_malloc.so"))?,
            );
            let mut dbg: Vec<String> = Vec::new();
            if let Some(m) = self.umem_mtbf {
                dbg.push(format!("mtbf={m}"));
            }
            if self.umem_guards {
                dbg.push("guards".into());
            }
            cmd.env("UMEM_DEBUG", dbg.join(","));
        }
        if self.directio {
            preload.push(
                tools
                    .directio
                    .clone()
                    .ok_or(Error::MissingTool("libdirectio.so"))?,
            );
        }
        if let Some(off) = &self.faketime {
            preload.push(
                tools
                    .faketime
                    .clone()
                    .ok_or(Error::MissingTool("libfaketime.so.1"))?,
            );
            cmd.env("FAKETIME", off);
            cmd.env("FAKETIME_DONT_FAKE_MONOTONIC", "1");
        }
        if !preload.is_empty() {
            let joined: Vec<String> = preload.iter().map(|p| p.display().to_string()).collect();
            cmd.env("LD_PRELOAD", joined.join(" "));
        }
        Ok(())
    }
}

/// Run-time control of libfiu in a running process.
#[derive(Debug)]
pub struct FiuControl {
    base: PathBuf,
}

impl FiuControl {
    /// Control the process `pid` launched with FIFO prefix `prefix`. Waits
    /// up to five seconds for libfiu to create the FIFOs.
    pub fn attach(prefix: &Path, pid: u32) -> Result<Self> {
        let base = PathBuf::from(format!("{}-{pid}", prefix.display()));
        let input = base.with_extension("in");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !input.exists() {
            if Instant::now() > deadline {
                return Err(Error::Timeout(format!("libfiu FIFO {}", input.display())));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(Self { base })
    }

    /// Send one remote-control command; libfiu replies `0` on success.
    pub fn send(&self, command: &str) -> Result<()> {
        let mut input = OpenOptions::new()
            .write(true)
            .open(self.base.with_extension("in"))?;
        writeln!(input, "{command}")?;
        drop(input);
        let mut reply = String::new();
        OpenOptions::new()
            .read(true)
            .open(self.base.with_extension("out"))?
            .read_to_string(&mut reply)?;
        if reply.trim() == "0" {
            Ok(())
        } else {
            Err(Error::Command {
                cmd: format!("fiu: {command}"),
                stderr: reply,
            })
        }
    }

    /// Enable `point`.
    pub fn enable(&self, point: &FiuPoint) -> Result<()> {
        self.send(&point.command())
    }

    /// Disable the failure point `name`.
    pub fn disable(&self, name: &str) -> Result<()> {
        self.send(&format!("disable name={name}"))
    }
}
