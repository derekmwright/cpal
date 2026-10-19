use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

/// Observed MMCSS registration state for one WASAPI worker, not a latency guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ThreadPriorityStatus {
    /// The optional real-time helper is disabled; native fallback priority is not observed.
    Unobserved,
    /// The worker has not yet completed its registration attempt.
    Pending,
    /// The helper returned a successful MMCSS Audio registration on this worker.
    Promoted,
    /// The helper reported registration failure. Audio processing may continue normally.
    PromotionFailed,
    /// The registering worker explicitly released its registration successfully.
    Released,
    /// The worker attempted explicit release but the helper reported failure.
    ReleaseFailed,
}

/// A read-only observation handle scoped to one stream worker lifetime.
///
/// Clones remain usable after the stream closes. A new stream always owns a new observation;
/// a previous stream's success is never inherited. No log parsing or OS query occurs on read.
#[derive(Clone)]
pub struct ThreadPriorityObservation(Arc<AtomicU8>);

impl ThreadPriorityObservation {
    pub(super) fn new(enabled: bool) -> Self {
        let initial = if enabled {
            ThreadPriorityStatus::Pending
        } else {
            ThreadPriorityStatus::Unobserved
        };
        Self(Arc::new(AtomicU8::new(initial as u8)))
    }

    /// Last state published by the worker after an actual helper result.
    pub fn status(&self) -> ThreadPriorityStatus {
        match self.0.load(Ordering::Acquire) {
            1 => ThreadPriorityStatus::Pending,
            2 => ThreadPriorityStatus::Promoted,
            3 => ThreadPriorityStatus::PromotionFailed,
            4 => ThreadPriorityStatus::Released,
            5 => ThreadPriorityStatus::ReleaseFailed,
            _ => ThreadPriorityStatus::Unobserved,
        }
    }

    #[cfg(any(feature = "audio_thread_priority", test))]
    pub(super) fn register<H, E>(&self, promote: impl FnOnce() -> Result<H, E>) -> Result<H, E> {
        let result = promote();
        let status = if result.is_ok() {
            ThreadPriorityStatus::Promoted
        } else {
            ThreadPriorityStatus::PromotionFailed
        };
        self.0.store(status as u8, Ordering::Release);
        result
    }

    #[cfg(any(feature = "audio_thread_priority", test))]
    pub(super) fn release<E>(&self, demote: impl FnOnce() -> Result<(), E>) -> Result<(), E> {
        let result = demote();
        let status = if result.is_ok() {
            ThreadPriorityStatus::Released
        } else {
            ThreadPriorityStatus::ReleaseFailed
        };
        self.0.store(status as u8, Ordering::Release);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::wasapi::priority::Guard;

    #[test]
    fn observation_tracks_results_and_remains_bound_to_its_worker() {
        let observed = ThreadPriorityObservation::new(true);
        let worker_observation = observed.clone();
        let next_generation = ThreadPriorityObservation::new(true);
        let disabled = ThreadPriorityObservation::new(false);
        let worker = std::thread::spawn(move || {
            let _guard = Guard::acquire(
                || worker_observation.register(|| Ok::<_, ()>(42)),
                |handle| {
                    assert_eq!(handle, 42);
                    worker_observation.release(|| Ok::<_, ()>(())).unwrap();
                },
            )
            .unwrap();
            assert_eq!(worker_observation.status(), ThreadPriorityStatus::Promoted);
        });
        worker.join().unwrap();
        assert_eq!(observed.status(), ThreadPriorityStatus::Released);
        assert_eq!(next_generation.status(), ThreadPriorityStatus::Pending);
        assert_eq!(disabled.status(), ThreadPriorityStatus::Unobserved);
    }

    #[test]
    fn failure_results_are_not_reported_as_success() {
        let failed = ThreadPriorityObservation::new(true);
        assert!(failed.register(|| Err::<(), _>("denied")).is_err());
        assert_eq!(failed.status(), ThreadPriorityStatus::PromotionFailed);
        let promoted = ThreadPriorityObservation::new(true);
        assert!(promoted.register(|| Ok::<_, ()>(42)).is_ok());
        assert_eq!(promoted.status(), ThreadPriorityStatus::Promoted);
        assert!(promoted.release(|| Err::<(), _>("release denied")).is_err());
        assert_eq!(promoted.status(), ThreadPriorityStatus::ReleaseFailed);
    }
}
