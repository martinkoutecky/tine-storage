//! Process-local committed projection coverage; no connection, worker or queue.
//! Producers publish only after their existing ordered SQLite commit succeeds.
//! Admission and snapshot acquisition happen after this wait, in their owners.

use crate::sqlite_materialization::MaterializationError;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Condvar, Mutex, Weak,
};
use std::time::{Duration, Instant};

struct State {
    incarnation: Arc<()>,
    version: Arc<()>,
    coverage: Option<(u64, u64)>,
    failure: Option<String>,
    closed: bool,
    #[cfg(test)]
    before_wait: Option<std::sync::mpsc::Sender<()>>,
}
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

/// One projection owner's local progress. Clones share the same observation.
/// A saved target uses the backend's existing ordered position, not a new log.
#[derive(Clone)]
pub struct PhysicalProjectionQueryProgress {
    shared: Arc<Shared>,
}

/// Minimum saved position in one projection incarnation. It is not a snapshot
/// identity and cannot be serialized or transferred to another graph/session.
#[derive(Clone)]
pub struct PhysicalProjectionQueryTarget {
    incarnation: Arc<()>,
    position: u64,
}

/// Opaque version for check-then-wait subscriptions. Versions never wrap or
/// acquire meaning across owners; retaining one does not retain a database.
#[derive(Clone)]
pub struct PhysicalProjectionQueryObservation {
    owner: Weak<Shared>,
    version: Arc<()>,
}

/// Cancellation of one request's progress wait. It does not cancel another
/// consumer, close the projection or interrupt already acquired SQLite jobs.
#[derive(Clone)]
pub struct PhysicalProjectionQueryRequest {
    owner: Weak<Shared>,
    cancelled: Arc<AtomicBool>,
}

/// Pending and failed reads are not successful empty query results.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalProjectionQueryProgressOutcome {
    /// A snapshot acquired next must have at least this SQL revision, under the
    /// same owner incarnation. Read its actual revision for the result memo key.
    Ready {
        minimum_revision: u64,
    },
    Pending,
    Failed(String),
    Cancelled,
}

impl State {
    fn outcome(&self) -> PhysicalProjectionQueryProgressOutcome {
        use PhysicalProjectionQueryProgressOutcome::*;
        if self.closed {
            Cancelled
        } else if let Some(message) = &self.failure {
            Failed(message.clone())
        } else if let Some((_, revision)) = self.coverage {
            Ready {
                minimum_revision: revision,
            }
        } else {
            Pending
        }
    }
    fn changed(&mut self) {
        self.version = Arc::new(());
    }
}

impl Default for PhysicalProjectionQueryProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalProjectionQueryProgress {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    incarnation: Arc::new(()),
                    version: Arc::new(()),
                    coverage: None,
                    failure: None,
                    closed: false,
                    #[cfg(test)]
                    before_wait: None,
                }),
                changed: Condvar::new(),
            }),
        }
    }

    /// Capture at the owning save/producer boundary, not by sampling after a
    /// later edit. Merely issuing a newer target never delays an earlier one.
    pub fn target(&self, position: u64) -> PhysicalProjectionQueryTarget {
        PhysicalProjectionQueryTarget {
            incarnation: Arc::clone(&self.shared.state.lock().unwrap().incarnation),
            position,
        }
    }

    pub fn request(&self) -> PhysicalProjectionQueryRequest {
        PhysicalProjectionQueryRequest {
            owner: Arc::downgrade(&self.shared),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Publish verified committed coverage. Capture the target before the
    /// producer operation so a late old-incarnation completion is rejected.
    /// Remote-only commits may advance SQL revision at the same local position.
    pub fn publish(
        &self,
        target: &PhysicalProjectionQueryTarget,
        revision: u64,
    ) -> Result<(), MaterializationError> {
        let mut state = self.shared.state.lock().unwrap();
        Self::validate_target(&state, target)?;
        if let Some((position, previous_revision)) = state.coverage {
            if target.position < position || revision < previous_revision {
                return Err(MaterializationError::InvalidInput(
                    "projection coverage or image revision regressed".into(),
                ));
            }
            if target.position == position
                && revision == previous_revision
                && state.failure.is_none()
            {
                return Ok(());
            }
        }
        state.coverage = Some((target.position, revision));
        state.failure = None;
        state.changed();
        self.shared.changed.notify_all();
        Ok(())
    }

    pub fn fail(
        &self,
        target: &PhysicalProjectionQueryTarget,
        message: String,
    ) -> Result<(), MaterializationError> {
        let mut state = self.shared.state.lock().unwrap();
        Self::validate_target(&state, target)?;
        if state.failure.as_ref() != Some(&message) {
            state.failure = Some(message);
            state.changed();
            self.shared.changed.notify_all();
        }
        Ok(())
    }

    /// Replacement/config-rebuild invalidates old targets. The caller also
    /// cancels/drains admitted query jobs through its existing job owner.
    pub fn restart(&self) -> Result<(), MaterializationError> {
        let mut state = self.shared.state.lock().unwrap();
        if state.closed {
            return Err(MaterializationError::Incomplete(
                "projection progress is closed".into(),
            ));
        }
        state.incarnation = Arc::new(());
        state.coverage = None;
        state.failure = None;
        state.changed();
        self.shared.changed.notify_all();
        Ok(())
    }

    pub fn close(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.closed = true;
        state.changed();
        self.shared.changed.notify_all();
    }

    /// Current state and an atomically matching subscription version.
    pub fn observe(
        &self,
    ) -> (
        PhysicalProjectionQueryObservation,
        PhysicalProjectionQueryProgressOutcome,
    ) {
        let state = self.shared.state.lock().unwrap();
        (
            PhysicalProjectionQueryObservation {
                owner: Arc::downgrade(&self.shared),
                version: Arc::clone(&state.version),
            },
            state.outcome(),
        )
    }

    pub fn wait_for_target(
        &self,
        target: &PhysicalProjectionQueryTarget,
        request: &PhysicalProjectionQueryRequest,
        timeout: Duration,
    ) -> PhysicalProjectionQueryProgressOutcome {
        self.wait(request, timeout, |state| {
            if !Arc::ptr_eq(&state.incarnation, &target.incarnation) {
                return Some(PhysicalProjectionQueryProgressOutcome::Cancelled);
            }
            if state.failure.is_some() {
                return Some(state.outcome());
            }
            state
                .coverage
                .filter(|(position, _)| *position >= target.position)
                .map(|_| state.outcome())
        })
    }

    /// Wait only for a change since the observation, even if that observation
    /// was failed. This prevents a failure/retry subscriber from busy looping.
    pub fn wait_for_change(
        &self,
        observed: &PhysicalProjectionQueryObservation,
        request: &PhysicalProjectionQueryRequest,
        timeout: Duration,
    ) -> PhysicalProjectionQueryProgressOutcome {
        if !Weak::ptr_eq(&observed.owner, &Arc::downgrade(&self.shared)) {
            return PhysicalProjectionQueryProgressOutcome::Cancelled;
        }
        self.wait(request, timeout, |state| {
            (!Arc::ptr_eq(&state.version, &observed.version)).then(|| state.outcome())
        })
    }

    fn validate_target(
        state: &State,
        target: &PhysicalProjectionQueryTarget,
    ) -> Result<(), MaterializationError> {
        if state.closed || !Arc::ptr_eq(&state.incarnation, &target.incarnation) {
            return Err(MaterializationError::Incomplete(
                "projection target belongs to a closed or replaced owner".into(),
            ));
        }
        Ok(())
    }

    fn wait(
        &self,
        request: &PhysicalProjectionQueryRequest,
        timeout: Duration,
        inspect: impl Fn(&State) -> Option<PhysicalProjectionQueryProgressOutcome>,
    ) -> PhysicalProjectionQueryProgressOutcome {
        use PhysicalProjectionQueryProgressOutcome::*;
        if !Weak::ptr_eq(&request.owner, &Arc::downgrade(&self.shared)) {
            return Cancelled;
        }
        let started = Instant::now();
        let mut state = self.shared.state.lock().unwrap();
        loop {
            if request.cancelled.load(Ordering::Acquire) || state.closed {
                return Cancelled;
            }
            if let Some(outcome) = inspect(&state) {
                return outcome;
            }
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                return Pending;
            };
            if remaining.is_zero() {
                return Pending;
            }
            #[cfg(test)]
            if let Some(before_wait) = state.before_wait.take() {
                before_wait.send(()).unwrap();
            }
            state = self
                .shared
                .changed
                .wait_timeout(state, remaining)
                .unwrap()
                .0;
        }
    }
}

impl PhysicalProjectionQueryRequest {
    pub fn cancel(&self) {
        // Pair notification with the predicate mutex. Otherwise cancellation
        // could land between the waiter's flag check and its Condvar wait.
        if let Some(owner) = self.owner.upgrade() {
            let _state = owner.state.lock().unwrap();
            self.cancelled.store(true, Ordering::Release);
            owner.changed.notify_all();
        } else {
            self.cancelled.store(true, Ordering::Release);
        }
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use PhysicalProjectionQueryProgressOutcome::*;
    const WAIT: Duration = Duration::from_secs(3);

    fn about_to_wait(progress: &PhysicalProjectionQueryProgress) -> std::sync::mpsc::Receiver<()> {
        let (send, receive) = std::sync::mpsc::channel();
        progress.shared.state.lock().unwrap().before_wait = Some(send);
        receive
    }

    #[test]
    fn query_progress_fixed_target_does_not_chase_later_edits() {
        let owner = PhysicalProjectionQueryProgress::new();
        let request = owner.request();
        let target = owner.target(3);
        let _later_demand = owner.target(100);
        assert_eq!(
            owner.wait_for_target(&target, &request, Duration::ZERO),
            Pending
        );
        owner.publish(&owner.target(4), 9).unwrap();
        assert_eq!(
            owner.wait_for_target(&target, &request, Duration::ZERO),
            Ready {
                minimum_revision: 9
            }
        );
        let (observed, _) = owner.observe();
        owner.publish(&owner.target(4), 10).unwrap(); // remote commit, same local coverage
        assert_eq!(
            owner.wait_for_change(&observed, &request, Duration::ZERO),
            Ready {
                minimum_revision: 10
            }
        );
        let (observed, _) = owner.observe();
        owner.publish(&owner.target(4), 10).unwrap();
        assert_eq!(
            owner.wait_for_change(&observed, &request, Duration::ZERO),
            Pending
        );
        assert!(owner.publish(&owner.target(3), 11).is_err());
        assert!(owner.publish(&owner.target(5), 8).is_err());
        assert_eq!(
            owner.wait_for_target(&target, &request, Duration::ZERO),
            Ready {
                minimum_revision: 10
            }
        );
    }

    #[test]
    fn query_progress_commit_and_cancellation_wake_atomic_waits() {
        let owner = PhysicalProjectionQueryProgress::new();
        let target = owner.target(7);
        let request = owner.request();
        let barrier = about_to_wait(&owner);
        let worker_owner = owner.clone();
        let worker_target = target.clone();
        let worker_request = request.clone();
        let worker = std::thread::spawn(move || {
            worker_owner.wait_for_target(&worker_target, &worker_request, WAIT)
        });
        barrier.recv_timeout(WAIT).unwrap();
        owner.publish(&target, 12).unwrap();
        assert_eq!(
            worker.join().unwrap(),
            Ready {
                minimum_revision: 12
            }
        );

        let later = owner.target(8);
        let barrier = about_to_wait(&owner);
        let worker_owner = owner.clone();
        let worker_request = request.clone();
        let worker =
            std::thread::spawn(move || worker_owner.wait_for_target(&later, &worker_request, WAIT));
        barrier.recv_timeout(WAIT).unwrap();
        request.cancel();
        assert_eq!(worker.join().unwrap(), Cancelled);
        assert!(request.is_cancelled());
        assert_eq!(
            owner.wait_for_target(&target, &request, Duration::ZERO),
            Cancelled
        );
        assert_eq!(
            owner.wait_for_target(&target, &owner.request(), Duration::ZERO),
            Ready {
                minimum_revision: 12
            }
        );
    }

    #[test]
    fn query_progress_failure_recovery_and_observation_do_not_lose_changes() {
        let owner = PhysicalProjectionQueryProgress::new();
        let target = owner.target(1);
        let request = owner.request();
        let (before, _) = owner.observe();
        owner.fail(&target, "disk read failed".into()).unwrap();
        assert_eq!(
            owner.wait_for_change(&before, &request, Duration::ZERO),
            Failed("disk read failed".into())
        );
        assert_eq!(
            owner.wait_for_target(&target, &request, Duration::ZERO),
            Failed("disk read failed".into())
        );
        let (failed, _) = owner.observe();
        assert_eq!(
            owner.wait_for_change(&failed, &request, Duration::ZERO),
            Pending
        );
        let barrier = about_to_wait(&owner);
        let worker_owner = owner.clone();
        let worker =
            std::thread::spawn(move || worker_owner.wait_for_change(&failed, &request, WAIT));
        barrier.recv_timeout(WAIT).unwrap();
        owner.publish(&target, 2).unwrap();
        assert_eq!(
            worker.join().unwrap(),
            Ready {
                minimum_revision: 2
            }
        );
    }

    #[test]
    fn query_progress_replacement_close_and_wrong_owner_cancel() {
        let owner = PhysicalProjectionQueryProgress::new();
        let other = PhysicalProjectionQueryProgress::new();
        let target = owner.target(1);
        let request = owner.request();
        assert_eq!(
            other.wait_for_target(&target, &other.request(), Duration::ZERO),
            Cancelled
        );
        assert_eq!(
            owner.wait_for_target(&target, &other.request(), Duration::ZERO),
            Cancelled
        );
        assert!(other.publish(&target, 1).is_err());
        let barrier = about_to_wait(&owner);
        let worker_owner = owner.clone();
        let worker_target = target.clone();
        let worker_request = request.clone();
        let worker = std::thread::spawn(move || {
            worker_owner.wait_for_target(&worker_target, &worker_request, WAIT)
        });
        barrier.recv_timeout(WAIT).unwrap();
        owner.restart().unwrap();
        assert_eq!(worker.join().unwrap(), Cancelled);
        assert!(owner.publish(&target, 9).is_err());
        assert!(owner.fail(&target, "old worker error".into()).is_err());
        let current = owner.target(1);
        let barrier = about_to_wait(&owner);
        let worker_owner = owner.clone();
        let worker =
            std::thread::spawn(move || worker_owner.wait_for_target(&current, &request, WAIT));
        barrier.recv_timeout(WAIT).unwrap();
        owner.close();
        assert_eq!(worker.join().unwrap(), Cancelled);
        assert!(owner.restart().is_err());
        assert!(owner.publish(&owner.target(1), 10).is_err());
    }
}
