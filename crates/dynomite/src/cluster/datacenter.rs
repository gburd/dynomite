//! Cluster topology: datacenters, racks, and the per-rack token
//! continuum.
//!
//! A [`Datacenter`] owns a list of [`Rack`]s; a `Rack` owns a vector
//! of [`Continuum`] points that map a [`DynToken`] to the index of
//! the [`crate::cluster::peer::Peer`] that owns the token. The shape
//! mirrors the reference engine's `struct datacenter` /
//! `struct rack` / `struct continuum` exactly.
//!
//! Token ring lookups use the same upper-bound search as the
//! reference engine's `vnode_dispatch` (the search lives in
//! [`crate::cluster::vnode`]). The data shape stays here so the
//! lookup can be tested against curated continua without spinning
//! up a full pool.
//!
//! # Examples
//!
//! ```
//! use dynomite::cluster::datacenter::{Datacenter, Rack};
//! let mut dc = Datacenter::new("dc1".into());
//! dc.upsert_rack("rack1".into());
//! assert_eq!(dc.racks().len(), 1);
//! ```

use crate::hashkit::{random_slicing::RandomSlices, DynToken};

/// Per-rack ring storage. Either the historical token continuum
/// (a sorted [`Vec<Continuum>`]) or a [`RandomSlices`] table.
/// The dispatcher consults whichever variant is present without
/// caring which one it is; the engine produces only one shape
/// per rack at a time.
#[derive(Clone, Debug, Default)]
pub enum RackRing {
    /// Historical per-peer token continuum.
    #[default]
    Continuum,
    /// Random-slicing partition table.
    RandomSlicing(RandomSlices),
}

/// One ring point: a `(token, peer_idx)` mapping.
#[derive(Clone, Debug)]
pub struct Continuum {
    /// Token at this ring position.
    pub token: DynToken,
    /// Index into the pool's peer array.
    pub peer_idx: u32,
}

impl Continuum {
    /// Construct a continuum point.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::datacenter::Continuum;
    /// use dynomite::hashkit::DynToken;
    /// let c = Continuum::new(DynToken::from_u32(7), 3);
    /// assert_eq!(c.peer_idx, 3);
    /// ```
    #[must_use]
    pub fn new(token: DynToken, peer_idx: u32) -> Self {
        Self { token, peer_idx }
    }
}

/// One rack within a datacenter.
///
/// `continuums` is sorted by token in ascending order to support
/// `vnode_dispatch`'s binary search; callers append continuum
/// points and call [`Rack::sort_continuums`] once after a batch of
/// updates.
#[derive(Clone, Debug)]
pub struct Rack {
    name: String,
    dc: String,
    nserver_continuum: u32,
    ncontinuum: u32,
    continuums: Vec<Continuum>,
    /// Optional [`RandomSlices`] table; populated when the
    /// pool's distribution is
    /// [`crate::conf::Distribution::RandomSlicing`].
    /// [`Self::continuums`] stays in sync with the peer set so
    /// the shadow distribution path can binary-search the same
    /// rack without a second build.
    ring: RackRing,
}

impl Rack {
    /// Build an empty rack.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::datacenter::Rack;
    /// let r = Rack::new("rack1".into(), "dc1".into());
    /// assert_eq!(r.name(), "rack1");
    /// assert_eq!(r.dc(), "dc1");
    /// assert!(r.continuums().is_empty());
    /// ```
    #[must_use]
    pub fn new(name: String, dc: String) -> Self {
        Self {
            name,
            dc,
            nserver_continuum: 0,
            ncontinuum: 0,
            continuums: Vec::new(),
            ring: RackRing::Continuum,
        }
    }

    /// Rack name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Owning datacenter name.
    #[must_use]
    pub fn dc(&self) -> &str {
        &self.dc
    }

    /// Borrow the continuum points (sorted by token).
    #[must_use]
    pub fn continuums(&self) -> &[Continuum] {
        &self.continuums
    }

    /// Number of distinct servers ever added (mirrors the C
    /// reference's `nserver_continuum`).
    #[must_use]
    pub fn nserver_continuum(&self) -> u32 {
        self.nserver_continuum
    }

    /// Number of continuum points (mirrors `ncontinuum`).
    #[must_use]
    pub fn ncontinuum(&self) -> u32 {
        self.ncontinuum
    }

    /// Append continuum points produced from one peer's tokens.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::datacenter::{Continuum, Rack};
    /// use dynomite::hashkit::DynToken;
    /// let mut r = Rack::new("rack1".into(), "dc1".into());
    /// r.add_peer_tokens(0, &[DynToken::from_u32(2), DynToken::from_u32(5)]);
    /// assert_eq!(r.ncontinuum(), 2);
    /// assert_eq!(r.nserver_continuum(), 2);
    /// ```
    pub fn add_peer_tokens(&mut self, peer_idx: u32, tokens: &[DynToken]) {
        for tok in tokens {
            self.continuums.push(Continuum::new(tok.clone(), peer_idx));
            self.ncontinuum = self.ncontinuum.saturating_add(1);
            self.nserver_continuum = self.nserver_continuum.saturating_add(1);
        }
    }

    /// Sort the continuum by token (ascending). Callers must invoke
    /// this once after a batch of [`Rack::add_peer_tokens`] calls so
    /// that [`crate::cluster::vnode::dispatch`] can binary-search
    /// the ring.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::datacenter::Rack;
    /// use dynomite::hashkit::DynToken;
    /// let mut r = Rack::new("r".into(), "d".into());
    /// r.add_peer_tokens(0, &[DynToken::from_u32(5)]);
    /// r.add_peer_tokens(1, &[DynToken::from_u32(2)]);
    /// r.sort_continuums();
    /// assert_eq!(r.continuums()[0].peer_idx, 1);
    /// ```
    pub fn sort_continuums(&mut self) {
        self.continuums.sort_by(|a, b| a.token.cmp(&b.token));
    }

    /// Reset all continuum state for a fresh rebuild.
    pub fn clear_continuums(&mut self) {
        self.continuums.clear();
        self.ncontinuum = 0;
        self.nserver_continuum = 0;
        self.ring = RackRing::Continuum;
    }

    /// Borrow the rack's ring representation.
    #[must_use]
    pub fn ring(&self) -> &RackRing {
        &self.ring
    }

    /// Install a [`RandomSlices`] table on this rack. The
    /// continuum stays populated so the shadow-distribution
    /// path (and any operator-side dump) can still walk the
    /// vnode view.
    pub fn set_random_slices(&mut self, slices: RandomSlices) {
        self.ring = RackRing::RandomSlicing(slices);
    }

    /// True when the rack's live distribution is random
    /// slicing.
    #[must_use]
    pub fn is_random_slicing(&self) -> bool {
        matches!(self.ring, RackRing::RandomSlicing(_))
    }

    /// Borrow the rack's [`RandomSlices`] table when one is
    /// installed.
    #[must_use]
    pub fn random_slices(&self) -> Option<&RandomSlices> {
        match &self.ring {
            RackRing::RandomSlicing(s) => Some(s),
            RackRing::Continuum => None,
        }
    }
}

/// One datacenter.
///
/// Mirrors `struct datacenter`. The
/// `preselected_rack_for_replication` field is computed by
/// [`crate::cluster::pool::ServerPool::preselect_remote_racks`]
/// and reproduces the reference engine's strategy of choosing one
/// rack per remote DC for cross-DC writes.
#[derive(Clone, Debug)]
pub struct Datacenter {
    name: String,
    racks: Vec<Rack>,
    preselected_rack_for_replication: Option<usize>,
}

impl Datacenter {
    /// Build an empty datacenter.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::datacenter::Datacenter;
    /// let dc = Datacenter::new("dc1".into());
    /// assert_eq!(dc.name(), "dc1");
    /// ```
    #[must_use]
    pub fn new(name: String) -> Self {
        Self {
            name,
            racks: Vec::new(),
            preselected_rack_for_replication: None,
        }
    }

    /// Datacenter name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Borrow the rack list.
    #[must_use]
    pub fn racks(&self) -> &[Rack] {
        &self.racks
    }

    /// Mutable rack list.
    pub fn racks_mut(&mut self) -> &mut [Rack] {
        &mut self.racks
    }

    /// Find a rack by name.
    #[must_use]
    pub fn rack(&self, name: &str) -> Option<&Rack> {
        self.racks.iter().find(|r| r.name() == name)
    }

    /// Mutably find a rack by name.
    pub fn rack_mut(&mut self, name: &str) -> Option<&mut Rack> {
        self.racks.iter_mut().find(|r| r.name() == name)
    }

    /// Find a rack and return its index.
    #[must_use]
    pub fn rack_idx(&self, name: &str) -> Option<usize> {
        self.racks.iter().position(|r| r.name() == name)
    }

    /// Insert a rack if absent; return a mutable handle to the
    /// rack regardless. Mirrors `server_get_rack`.
    pub fn upsert_rack(&mut self, name: String) -> &mut Rack {
        if let Some(idx) = self.rack_idx(&name) {
            return &mut self.racks[idx];
        }
        let dc = self.name.clone();
        self.racks.push(Rack::new(name, dc));
        let last = self.racks.len() - 1;
        &mut self.racks[last]
    }

    /// Sort racks by name (ascending). Used by
    /// [`crate::cluster::pool::ServerPool::preselect_remote_racks`]
    /// and mirrors the `array_sort(&dc->racks, rack_name_cmp)` call
    /// in `preselect_remote_rack_for_replication`.
    pub fn sort_racks(&mut self) {
        self.racks.sort_by(|a, b| a.name().cmp(b.name()));
    }

    /// Preselected rack index for replicating writes from another
    /// DC into this DC.
    #[must_use]
    pub fn preselected_rack_idx(&self) -> Option<usize> {
        self.preselected_rack_for_replication
    }

    /// Borrow the preselected rack, if any.
    #[must_use]
    pub fn preselected_rack(&self) -> Option<&Rack> {
        self.preselected_rack_for_replication
            .and_then(|i| self.racks.get(i))
    }

    /// Set the preselected rack index (used by the pool's
    /// [`preselect_remote_racks`](crate::cluster::pool::ServerPool::preselect_remote_racks)
    /// pass).
    pub fn set_preselected_rack_idx(&mut self, idx: Option<usize>) {
        self.preselected_rack_for_replication = idx;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_is_idempotent() {
        let mut dc = Datacenter::new("dc1".into());
        dc.upsert_rack("r1".into());
        dc.upsert_rack("r1".into());
        assert_eq!(dc.racks().len(), 1);
    }

    #[test]
    fn rack_continuum_sorts_by_token() {
        let mut r = Rack::new("r".into(), "d".into());
        r.add_peer_tokens(0, &[DynToken::from_u32(9)]);
        r.add_peer_tokens(1, &[DynToken::from_u32(3)]);
        r.add_peer_tokens(2, &[DynToken::from_u32(6)]);
        r.sort_continuums();
        let idxs: Vec<u32> = r.continuums().iter().map(|c| c.peer_idx).collect();
        assert_eq!(idxs, vec![1, 2, 0]);
    }

    #[test]
    fn rack_clear_resets_counters() {
        let mut r = Rack::new("r".into(), "d".into());
        r.add_peer_tokens(0, &[DynToken::from_u32(1)]);
        r.clear_continuums();
        assert_eq!(r.ncontinuum(), 0);
        assert!(r.continuums().is_empty());
    }

    #[test]
    fn sort_racks_alphabetical() {
        let mut dc = Datacenter::new("dc1".into());
        dc.upsert_rack("rb".into());
        dc.upsert_rack("ra".into());
        dc.sort_racks();
        assert_eq!(dc.racks()[0].name(), "ra");
    }
}
