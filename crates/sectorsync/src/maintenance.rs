//! Caller-driven load sampling and conservative split maintenance.

use sectorsync_core::{event::StationEvent, hotspot::StationLoadSample, ids::Tick};
use sectorsync_runtime::{
    CellOwnershipTable, EventRouter, EventRouterError, SplitScheduleExecutionError,
    SplitScheduleExecutionScratch, SplitScheduleExecutionView, SplitScheduleView, SplitScheduler,
    SplitSchedulerConfig, SplitSchedulerScratch, SplitSchedulerState, StationIndexSet,
    StationLoadSampler, StationLoadSamplerConfig, StationLoadSamplerScratch, StationScheduleConfig,
    StationScheduleScratch, StationScheduleView, StationScheduler, StationSet,
};

/// Retained storage owned by [`LoadSampler`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LoadSamplerCapacities {
    /// Aggregated subscriber-map entries.
    pub subscribers: usize,
    /// Cell occupancy slots.
    pub occupancy: usize,
    /// Station sample slots.
    pub samples: usize,
    /// Nested sampled-cell capacity.
    pub cells: usize,
}

/// Stateful, allocation-light product load sampler.
#[derive(Clone, Debug)]
pub struct LoadSampler {
    sampler: StationLoadSampler,
    scratch: StationLoadSamplerScratch,
}

impl LoadSampler {
    /// Creates a sampler without allocating sample storage.
    pub fn new(config: StationLoadSamplerConfig) -> Self {
        Self {
            sampler: StationLoadSampler::new(config),
            scratch: StationLoadSamplerScratch::new(),
        }
    }

    /// Samples all Stations in deterministic set order into retained storage.
    pub fn sample<'a>(
        &'a mut self,
        stations: &StationSet,
        indexes: &StationIndexSet,
        router: &EventRouter,
        subscriber_counts: &[(sectorsync_core::ids::StationId, usize)],
    ) -> &'a [StationLoadSample] {
        self.sampler.sample_all_into(
            stations,
            indexes,
            router,
            subscriber_counts,
            &mut self.scratch,
        )
    }

    /// Returns retained sampler capacities.
    pub fn retained_capacities(&self) -> LoadSamplerCapacities {
        LoadSamplerCapacities {
            subscribers: self.scratch.retained_subscriber_capacity(),
            occupancy: self.scratch.retained_occupancy_capacity(),
            samples: self.scratch.retained_sample_slots(),
            cells: self.scratch.retained_cell_capacity(),
        }
    }
}

impl Default for LoadSampler {
    fn default() -> Self {
        Self::new(StationLoadSamplerConfig::default())
    }
}

/// Retained storage owned by [`StationExecutor`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StationExecutorCapacities {
    /// Sample score entries retained for scheduling.
    pub scores: usize,
    /// Candidate slots retained for scheduling.
    pub candidates: usize,
    /// Event slots retained for ordered draining.
    pub events: usize,
}

/// Caller-driven Station scheduling and event maintenance.
#[derive(Clone, Debug)]
pub struct StationExecutor {
    scheduler: StationScheduler,
    config: StationScheduleConfig,
    schedule: StationScheduleScratch,
    events: Vec<StationEvent>,
}

impl StationExecutor {
    /// Creates an executor without allocating schedule or event storage.
    pub fn new(config: StationScheduleConfig) -> Self {
        Self {
            scheduler: StationScheduler::default(),
            config,
            schedule: StationScheduleScratch::new(),
            events: Vec::new(),
        }
    }

    /// Advances the deterministic bounded set selected from current load.
    pub fn advance_loaded<'a>(
        &'a mut self,
        stations: &mut StationSet,
        samples: &[StationLoadSample],
    ) -> StationScheduleView<'a> {
        self.scheduler
            .advance_loaded_into(stations, samples, self.config, &mut self.schedule)
    }

    /// Drains ready events in Station order into retained output.
    pub fn drain_ready_events<'a>(
        &'a mut self,
        stations: &StationSet,
        router: &mut EventRouter,
    ) -> Result<&'a [StationEvent], EventRouterError> {
        self.scheduler
            .drain_ready_events_into(stations, router, &mut self.events)?;
        Ok(&self.events)
    }

    /// Returns retained scheduling and event capacities.
    pub fn retained_capacities(&self) -> StationExecutorCapacities {
        StationExecutorCapacities {
            scores: self.schedule.score_capacity(),
            candidates: self.schedule.candidate_capacity(),
            events: self.events.capacity(),
        }
    }
}

impl Default for StationExecutor {
    fn default() -> Self {
        Self::new(StationScheduleConfig::default())
    }
}

/// Retained storage owned by [`SplitExecutor`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SplitExecutorCapacities {
    /// Retained planning decision slots.
    pub decisions: usize,
    /// Retained split action slots.
    pub actions: usize,
    /// Retained hotspot candidate slots.
    pub candidates: usize,
    /// Retained execution ownership slots.
    pub ownership_updates: usize,
    /// Retained execution migration slots.
    pub migrations: usize,
}

/// Stateful conservative split planner and executor.
#[derive(Clone, Debug)]
pub struct SplitExecutor {
    scheduler: SplitScheduler,
    state: SplitSchedulerState,
    planning: SplitSchedulerScratch,
    execution: SplitScheduleExecutionScratch,
    planned_at: Tick,
}

impl SplitExecutor {
    /// Creates an executor without allocating planning or execution output.
    pub fn new(config: SplitSchedulerConfig) -> Self {
        Self {
            scheduler: SplitScheduler::new(config),
            state: SplitSchedulerState::default(),
            planning: SplitSchedulerScratch::new(),
            execution: SplitScheduleExecutionScratch::new(),
            planned_at: Tick::new(0),
        }
    }

    /// Plans into retained storage and returns the active borrowed schedule.
    pub fn plan<'a>(
        &'a mut self,
        samples: &[StationLoadSample],
        current_tick: Tick,
    ) -> SplitScheduleView<'a> {
        self.planned_at = current_tick;
        self.scheduler
            .plan_into(samples, Some(&self.state), current_tick, &mut self.planning)
    }

    /// Returns the current planned schedule without executing it.
    pub fn planned(&self) -> SplitScheduleView<'_> {
        self.planning.view()
    }

    /// Executes the current plan and records successful actions for cooldown.
    pub fn execute_planned<'a>(
        &'a mut self,
        stations: &mut StationSet,
        indexes: &mut StationIndexSet,
        ownership: &mut CellOwnershipTable,
    ) -> Result<SplitScheduleExecutionView<'a>, SplitScheduleExecutionError> {
        let schedule = self.planning.view();
        let result = self.scheduler.execute_into(
            schedule,
            stations,
            indexes,
            ownership,
            &mut self.execution,
        )?;
        self.state.record_schedule_view(schedule, self.planned_at);
        Ok(result)
    }

    /// Returns retained planning and execution capacities.
    pub fn retained_capacities(&self) -> SplitExecutorCapacities {
        SplitExecutorCapacities {
            decisions: self.planning.retained_decision_slots(),
            actions: self.planning.retained_action_slots(),
            candidates: self.planning.retained_candidate_capacity(),
            ownership_updates: self.execution.retained_ownership_slots(),
            migrations: self.execution.retained_migration_slots(),
        }
    }
}

impl Default for SplitExecutor {
    fn default() -> Self {
        Self::new(SplitSchedulerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn product_maintenance_objects_reuse_owned_storage() {
        let stations = StationSet::default();
        let indexes = StationIndexSet::default();
        let router = EventRouter::default();
        let mut sampler = LoadSampler::default();

        assert!(sampler.sample(&stations, &indexes, &router, &[]).is_empty());

        let mut executor = SplitExecutor::default();
        assert!(executor.plan(&[], Tick::new(7)).actions.is_empty());
        assert!(executor.planned().actions.is_empty());
        assert_eq!(sampler.retained_capacities().samples, 0);
        assert_eq!(executor.retained_capacities().actions, 0);

        let mut station_executor = StationExecutor::default();
        assert!(
            station_executor
                .advance_loaded(&mut StationSet::default(), &[])
                .selected
                .is_empty()
        );
        assert!(
            station_executor
                .drain_ready_events(&stations, &mut EventRouter::default())
                .expect("empty event drain should work")
                .is_empty()
        );
    }
}
