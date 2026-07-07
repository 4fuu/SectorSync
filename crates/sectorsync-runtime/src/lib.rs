//! Multi-station orchestration helpers for SectorSync.

#![forbid(unsafe_code)]

use sectorsync_core::prelude::{Station, StationId};

/// Small in-process station collection for simulations and embedders.
#[derive(Clone, Debug, Default)]
pub struct StationSet {
    stations: Vec<Station>,
}

impl StationSet {
    /// Adds a station to the collection.
    pub fn push(&mut self, station: Station) {
        self.stations.push(station);
    }

    /// Gets a station by id.
    pub fn get(&self, station_id: StationId) -> Option<&Station> {
        self.stations
            .iter()
            .find(|station| station.config().station_id == station_id)
    }

    /// Gets a mutable station by id.
    pub fn get_mut(&mut self, station_id: StationId) -> Option<&mut Station> {
        self.stations
            .iter_mut()
            .find(|station| station.config().station_id == station_id)
    }

    /// Iterates over stations.
    pub fn iter(&self) -> impl Iterator<Item = &Station> {
        self.stations.iter()
    }

    /// Number of stations.
    pub fn len(&self) -> usize {
        self.stations.len()
    }

    /// Returns whether no stations are registered.
    pub fn is_empty(&self) -> bool {
        self.stations.is_empty()
    }
}
