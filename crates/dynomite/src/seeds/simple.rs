//! `SimpleSeedsProvider` - the default, in-memory provider.
//!
//! Returns the seeds parsed at YAML load time. Mirrors the
//! reference engine's behaviour when `dyn_seed_provider:` is set
//! to `simple_provider` (or unset): the seeds list comes straight
//! from `dyn_seeds:`.
//!
//! # Examples
//!
//! ```
//! use dynomite::seeds::{simple::SimpleSeedsProvider, SeedsProvider};
//! use dynomite::conf::ConfDynSeed;
//! let s = vec![ConfDynSeed::parse("h:1:r:d:1").unwrap()];
//! let p = SimpleSeedsProvider::new(s);
//! assert_eq!(p.get_seeds().unwrap().len(), 1);
//! ```

use crate::conf::ConfDynSeed;
use crate::seeds::{SeedsError, SeedsProvider};

/// Static seeds provider.
#[derive(Clone, Debug, Default)]
pub struct SimpleSeedsProvider {
    seeds: Vec<ConfDynSeed>,
}

impl SimpleSeedsProvider {
    /// Build a provider from a fixed seeds list.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::seeds::simple::SimpleSeedsProvider;
    /// let p = SimpleSeedsProvider::new(Vec::new());
    /// assert!(p.seeds().is_empty());
    /// ```
    #[must_use]
    pub fn new(seeds: Vec<ConfDynSeed>) -> Self {
        Self { seeds }
    }

    /// Borrow the seeds list.
    #[must_use]
    pub fn seeds(&self) -> &[ConfDynSeed] {
        &self.seeds
    }
}

impl SeedsProvider for SimpleSeedsProvider {
    fn get_seeds(&self) -> Result<Vec<ConfDynSeed>, SeedsError> {
        Ok(self.seeds.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let s = vec![
            ConfDynSeed::parse("h1:1:r:d:1").unwrap(),
            ConfDynSeed::parse("h2:2:r:d:2").unwrap(),
        ];
        let p = SimpleSeedsProvider::new(s);
        let got = p.get_seeds().unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].host(), "h1");
    }
}
