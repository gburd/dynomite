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
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use crate::tools::Tools;
use crate::{Error, Result};

/// A libfiu failure point to enable at launch or at run time.
///
/// `errno` pins the error the failing call reports. Without it libfiu
/// picks one at random from the call's documented list, which for
/// `write(2)` includes `EBADFD`, `EFAULT`, and `EINVAL`: errors a healthy
/// kernel never returns for a valid socket, and that no program is
/// expected to survive on its own internal descriptors. Scenarios name
/// the errno they mean (`EIO`, `ENOSPC`, `ECONNRESET`, ...).
#[derive(Clone, Debug, PartialEq)]
pub enum FiuPoint {
    /// Always fail `name`.
    Always {
        /// Failure point, e.g. `posix/io/net/connect`.
        name: String,
        /// errno to report, or libfiu's random pick when `None`.
        errno: Option<i32>,
    },
    /// Fail `name` with probability `p` in `0.0..=1.0`.
    Random {
        /// Failure point.
        name: String,
        /// Probability of failure per call.
        p: f64,
        /// errno to report, or libfiu's random pick when `None`.
        errno: Option<i32>,
    },
    /// Fail `name` once, then disable it.
    Once {
        /// Failure point.
        name: String,
        /// errno to report, or libfiu's random pick when `None`.
        errno: Option<i32>,
    },
}

impl FiuPoint {
    /// Always fail `name` with `errno`.
    pub fn always(name: &str, errno: i32) -> Self {
        FiuPoint::Always {
            name: name.into(),
            errno: Some(errno),
        }
    }

    /// Fail `name` with probability `p`, reporting `errno`.
    pub fn random(name: &str, p: f64, errno: i32) -> Self {
        FiuPoint::Random {
            name: name.into(),
            p,
            errno: Some(errno),
        }
    }

    /// The failure point name.
    pub fn name(&self) -> &str {
        match self {
            FiuPoint::Always { name, .. }
            | FiuPoint::Random { name, .. }
            | FiuPoint::Once { name, .. } => name,
        }
    }

    /// The libfiu remote-control command that enables this point.
    ///
    /// # Examples
    ///
    /// ```
    /// use optest::fault::FiuPoint;
    /// assert_eq!(
    ///     FiuPoint::random("posix/io/rw/write", 0.25, 5).command(),
    ///     "enable_random name=posix/io/rw/write,probability=0.25,failinfo=5"
    /// );
    /// ```
    pub fn command(&self) -> String {
        let info = |e: &Option<i32>| e.map(|e| format!(",failinfo={e}")).unwrap_or_default();
        match self {
            FiuPoint::Always { name, errno } => format!("enable name={name}{}", info(errno)),
            FiuPoint::Random { name, p, errno } => {
                format!("enable_random name={name},probability={p}{}", info(errno))
            }
            FiuPoint::Once { name, errno } => format!("enable name={name},onetime{}", info(errno)),
        }
    }
}

/// errno values scenarios inject (Linux numbering).
pub mod errno {
    /// I/O error.
    pub const EIO: i32 = 5;
    /// Resource temporarily unavailable.
    pub const EAGAIN: i32 = 11;
    /// No space left on device.
    pub const ENOSPC: i32 = 28;
    /// Broken pipe.
    pub const EPIPE: i32 = 32;
    /// Connection reset by peer.
    pub const ECONNRESET: i32 = 104;
    /// Connection refused.
    pub const ECONNREFUSED: i32 = 111;
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
            // libumem only injects mtbf failures on caches with debugging
            // enabled; `guards` is the debug mode that does so without the
            // abort-on-failure that `default` and `audit` add (verified
            // against libumem fe9ff96: `mtbf=N` alone never fails).
            let mut dbg: Vec<String> = Vec::new();
            if self.umem_guards || self.umem_mtbf.is_some() {
                dbg.push("guards".into());
            }
            if let Some(m) = self.umem_mtbf {
                dbg.push(format!("mtbf={m}"));
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

/// Upper bound on one libfiu remote-control round trip.
pub const FIU_CTRL_TIMEOUT: Duration = Duration::from_secs(5);

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
    ///
    /// The FIFO open and the reply read run on a helper thread bounded by
    /// [`FIU_CTRL_TIMEOUT`]: a target that has exited or stopped servicing
    /// its control thread would otherwise block the scenario forever on
    /// the FIFO open.
    pub fn send(&self, command: &str) -> Result<()> {
        let base = self.base.clone();
        let line = format!("{command}\n");
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let r = (|| -> std::io::Result<String> {
                let mut input = OpenOptions::new()
                    .write(true)
                    .open(base.with_extension("in"))?;
                input.write_all(line.as_bytes())?;
                drop(input);
                // libfiu keeps its end of the reply FIFO open across
                // commands, so read exactly one reply line; waiting for EOF
                // would block until the target exits.
                let out = OpenOptions::new()
                    .read(true)
                    .open(base.with_extension("out"))?;
                let mut reply = String::new();
                std::io::BufReader::new(out).read_line(&mut reply)?;
                Ok(reply)
            })();
            let _ = tx.send(r);
        });
        let reply = rx
            .recv_timeout(FIU_CTRL_TIMEOUT)
            .map_err(|_| Error::Timeout(format!("libfiu control `{command}`")))??;
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
