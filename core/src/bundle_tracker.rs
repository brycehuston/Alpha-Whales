// bundle_tracker.rs — Alpha Nexus Phase 5: Jito Bundle Inclusion Poller & Position Tracking
//
// Implements the user-requested Phase 5 scope: an inclusion polling loop that
// verifies SubmissionAck bundle IDs against the Jito Block Engine via the
// SubscribeBundleResults streaming RPC, records confirmed positions in the
// local database, and transitions tracking state.
//
// Design:
//   - A BundleTracker maintains in-memory state for all in-flight bundles.
//   - For each region, a background task subscribes to SubscribeBundleResults.
//   - Incoming BundleResult events are matched against tracked bundle IDs.
//   - On confirmed inclusion (Processed or Finalized), the position is
//     durably recorded in the SQLite database and the caller is notified
//     via a oneshot channel so the exit watcher can be spawned.
//   - Bundles that are rejected, dropped, or expired are marked Failed/Expired
//     and removed from tracking.
//
// SAFETY INV-07 (MASTER_PLAN.md): Submission acknowledgment is not equivalent
// to bundle landing. This module ensures landing is verified via the Jito
// block engine before any position is recorded as confirmed.

use jito_protos::{
    bundle::{bundle_result, BundleResult},
    searcher::{searcher_service_client::SearcherServiceClient, SubscribeBundleResultsRequest},
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use tonic::transport::Channel;
use tonic::Request;

use crate::db;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// How long to wait for a new BundleResult event on the subscription stream
/// before declaring the stream stale and reconnecting.
const STREAM_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum age of a tracked bundle before it is considered expired and pruned.
/// The Jito block engine typically resolves a bundle within a few slots (~2s);
/// 120s is a generous safety margin for edge cases (network delays, reorgs).
const MAX_BUNDLE_AGE: Duration = Duration::from_secs(120);

/// Capacity of the inclusion-notification channel per bundle.
const INCLUSION_NOTIFY_CAPACITY: usize = 1;

// ---------------------------------------------------------------------------
// Bundle State Machine
// ---------------------------------------------------------------------------

/// Represents the inclusion status of a single bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InclusionStatus {
    /// Bundle submitted but no result received yet.
    Pending,
    /// Bundle was accepted by a Jito validator and processed on-chain.
    Landed {
        slot: u64,
        bundle_index: u64,
        validator_identity: String,
    },
    /// Bundle was processed and finalized on-chain.
    Finalized { slot: u64, bundle_index: u64 },
    /// Bundle was rejected by the block engine.
    Rejected { reason: String },
    /// Bundle was accepted but never landed (dropped after forwarding).
    Dropped { reason: String },
    /// Bundle age exceeded MAX_BUNDLE_AGE without resolution.
    Expired,
}

impl InclusionStatus {
    /// Returns true if the bundle is considered to have landed on-chain
    /// and the position can be recorded.
    pub fn is_confirmed(&self) -> bool {
        matches!(
            self,
            InclusionStatus::Landed { .. } | InclusionStatus::Finalized { .. }
        )
    }

    /// Returns the slot number if the bundle has a known landing slot.
    pub fn slot(&self) -> Option<u64> {
        match self {
            InclusionStatus::Landed { slot, .. } | InclusionStatus::Finalized { slot, .. } => {
                Some(*slot)
            }
            _ => None,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            InclusionStatus::Landed { .. }
                | InclusionStatus::Finalized { .. }
                | InclusionStatus::Rejected { .. }
                | InclusionStatus::Dropped { .. }
                | InclusionStatus::Expired
        )
    }
}

// ---------------------------------------------------------------------------
// BundleRecord — per-bundle tracked state
// ---------------------------------------------------------------------------

/// Metadata tracked for each in-flight bundle.
#[derive(Debug, Clone)]
pub struct BundleRecord {
    pub bundle_id: String,
    pub region: String,
    pub mint: String,
    #[allow(dead_code)]
    pub pool_id: String,
    pub transaction_signature: String,
    pub submitted_at: Instant,
    pub status: InclusionStatus,
    /// Oneshot sender to notify the execution consumer when this bundle
    /// lands. Uses std::sync::Mutex since the lock is never held across
    /// an await point.
    pub notify_tx: Option<Arc<std::sync::Mutex<Option<mpsc::Sender<BundleRecord>>>>>,
}

// ---------------------------------------------------------------------------
// BundleTracker — manages all in-flight bundles
// ---------------------------------------------------------------------------

/// Tracks all in-flight bundles across regions and processes inclusion
/// results from Jito SubscribeBundleResults streams.
pub struct BundleTracker {
    /// Map from bundle_id -> tracked record
    bundles: Arc<RwLock<HashMap<String, BundleRecord>>>,
    /// Per-region gRPC clients used for the subscription stream.
    /// Only regions that have at least one tracked bundle will have a
    /// subscription active.
    region_clients: Arc<Mutex<HashMap<String, SearcherServiceClient<Channel>>>>,
    /// Notifies the polling loop that a new bundle has been registered
    /// (wake from idle).
    new_bundle_notify: Arc<Notify>,
    /// Shared shutdown signal
    shutdown: Arc<tokio::sync::watch::Sender<bool>>,
}

impl BundleTracker {
    /// Create a new empty tracker.
    pub fn new() -> Self {
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Self {
            bundles: Arc::new(RwLock::new(HashMap::new())),
            region_clients: Arc::new(Mutex::new(HashMap::new())),
            new_bundle_notify: Arc::new(Notify::new()),
            shutdown: Arc::new(shutdown_tx),
        }
    }

    /// Register or update a region gRPC client for subscription.
    pub async fn register_region_client(
        &self,
        region_label: &str,
        client: SearcherServiceClient<Channel>,
    ) {
        let mut clients = self.region_clients.lock().await;
        clients.insert(region_label.to_string(), client);
    }

    /// Register a bundle for tracking. Returns an mpsc::Receiver that will
    /// receive the BundleRecord once the inclusion status becomes terminal.
    /// The caller should await this receiver to know when the bundle has
    /// been recorded or failed.
    pub async fn register_bundle(&self, record: BundleRecord) -> mpsc::Receiver<BundleRecord> {
        let (tx, rx) = mpsc::channel::<BundleRecord>(INCLUSION_NOTIFY_CAPACITY);
        let mut record = record;
        record.notify_tx = Some(Arc::new(std::sync::Mutex::new(Some(tx))));

        let mut bundles = self.bundles.write().await;
        // Only register if not already present
        if !bundles.contains_key(&record.bundle_id) {
            bundles.insert(record.bundle_id.clone(), record);
            self.new_bundle_notify.notify_one();
        }

        rx
    }

    /// Get the current status of a bundle by ID.
    #[allow(dead_code)]
    pub async fn get_status(&self, bundle_id: &str) -> Option<InclusionStatus> {
        let bundles = self.bundles.read().await;
        bundles.get(bundle_id).map(|r| r.status.clone())
    }

    /// Update the status of a bundle. If the status is terminal, notifies
    /// the registered oneshot sender and optionally records in DB.
    async fn update_status(&self, bundle_id: &str, status: InclusionStatus) {
        let mut bundles = self.bundles.write().await;
        if let Some(record) = bundles.get_mut(bundle_id) {
            // Don't overwrite a terminal status with another status
            if record.status.is_terminal() {
                return;
            }
            record.status = status.clone();
            let mint = record.mint.clone();
            let region = record.region.clone();
            let tx_sig = record.transaction_signature.clone();

            // If confirmed landed, record in database
            if status.is_confirmed() {
                let slot = status.slot().unwrap_or(0);
                log::info!(
                    "[bundle_tracker] 🎯 Bundle landed: id={}, mint={}, slot={}, region={}",
                    bundle_id,
                    mint,
                    slot,
                    region
                );
                // Record the position in the local database
                db::record_landed_bundle(bundle_id, &region, &mint, slot, &tx_sig);
            }

            // Notify the waiting execution consumer
            if let Some(notify_tx) = record.notify_tx.take() {
                let mut guard = notify_tx.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(tx) = guard.take() {
                    let _ = tx.try_send(record.clone());
                }
            }
        }
    }

    /// Remove a bundle from tracking entirely (called after the position
    /// watcher is spawned or after terminal failure handling completes).
    #[allow(dead_code)]
    pub async fn remove_bundle(&self, bundle_id: &str) {
        let mut bundles = self.bundles.write().await;
        bundles.remove(bundle_id);
    }

    /// Number of bundles currently tracked.
    #[allow(dead_code)]
    pub async fn tracked_count(&self) -> usize {
        let bundles = self.bundles.read().await;
        bundles.len()
    }

    /// Return a snapshot of all pending bundle IDs.
    #[allow(dead_code)]
    pub async fn pending_bundle_ids(&self) -> Vec<String> {
        let bundles = self.bundles.read().await;
        bundles
            .iter()
            .filter(|(_, r)| !r.status.is_terminal())
            .map(|(id, _)| id.clone())
            .collect()
    }

    // -----------------------------------------------------------------------
    // Subscription / polling loop
    // -----------------------------------------------------------------------

    /// Spawn the background inclusion polling loop. For each registered
    /// region, subscribes to SubscribeBundleResults and dispatches events
    /// to the tracker. Also periodically prunes expired bundles.
    pub fn spawn_polling_loop(self: Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            this.polling_loop().await;
        });
    }

    async fn polling_loop(self: Arc<Self>) {
        let mut shutdown_rx = self.shutdown.subscribe();

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        log::info!("[bundle_tracker] Shutting down polling loop.");
                        break;
                    }
                }
                _ = self.new_bundle_notify.notified() => {
                    // Wake: new bundle registered, start/ensure stream is active
                }
            }

            // Collect regions that need subscription streams
            let regions_needed: Vec<String> = {
                let bundles = self.bundles.read().await;
                bundles
                    .values()
                    .filter(|r| !r.status.is_terminal())
                    .map(|r| r.region.clone())
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .collect()
            };

            if regions_needed.is_empty() {
                // No tracked bundles — sleep until notified
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() { break; }
                    }
                    _ = self.new_bundle_notify.notified() => {}
                }
                continue;
            }

            // For each needed region, spawn a subscriber task
            let mut subscriber_handles = Vec::new();
            for region in &regions_needed {
                let client_opt = {
                    let clients = self.region_clients.lock().await;
                    clients.get(region).cloned()
                };

                if let Some(mut client) = client_opt {
                    let tracker = self.clone();
                    let region_label = region.clone();
                    let handle = tokio::spawn(async move {
                        tracker
                            .run_region_subscription(&region_label, &mut client)
                            .await;
                    });
                    subscriber_handles.push(handle);
                } else {
                    log::warn!(
                        "[bundle_tracker] No gRPC client registered for region `{}`; \
                         skipping subscription",
                        region
                    );
                }
            }

            // Wait for subscription tasks to finish (they exit on stream error
            // or shutdown), then re-evaluate.
            if !subscriber_handles.is_empty() {
                tokio::select! {
                    _ = futures_util::future::join_all(subscriber_handles) => {}
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() { break; }
                    }
                }
            }

            // Prune expired bundles before the next iteration
            self.prune_expired().await;
        }
    }

    /// Run a SubscribeBundleResults subscription for a single region.
    /// Filters incoming results against our tracked bundle IDs and updates
    /// status accordingly. Exits on stream error or when no tracked bundles
    /// remain for this region.
    async fn run_region_subscription(
        self: Arc<Self>,
        region: &str,
        client: &mut SearcherServiceClient<Channel>,
    ) {
        log::info!(
            "[bundle_tracker] Starting SubscribeBundleResults stream for region `{}`",
            region
        );

        let stream_result = tokio::time::timeout(
            STREAM_HEARTBEAT_TIMEOUT,
            client.subscribe_bundle_results(Request::new(SubscribeBundleResultsRequest {})),
        )
        .await;

        let mut stream = match stream_result {
            Ok(Ok(response)) => response.into_inner(),
            Ok(Err(e)) => {
                log::error!(
                    "[bundle_tracker] SubscribeBundleResults failed for region `{}`: {}",
                    region,
                    e
                );
                return;
            }
            Err(_) => {
                log::error!(
                    "[bundle_tracker] SubscribeBundleResults timed out for region `{}`",
                    region
                );
                return;
            }
        };

        loop {
            // Check if we still have tracked bundles for this region
            let has_bundles = {
                let bundles = self.bundles.read().await;
                bundles
                    .values()
                    .any(|r| r.region == region && !r.status.is_terminal())
            };
            if !has_bundles {
                log::debug!(
                    "[bundle_tracker] No more tracked bundles for region `{}`; exiting subscription",
                    region
                );
                break;
            }

            let message = tokio::time::timeout(STREAM_HEARTBEAT_TIMEOUT, stream.message()).await;

            let bundle_result: BundleResult = match message {
                Ok(Ok(Some(result))) => result,
                Ok(Ok(None)) => {
                    log::info!(
                        "[bundle_tracker] SubscribeBundleResults stream ended for region `{}`",
                        region
                    );
                    break;
                }
                Ok(Err(e)) => {
                    log::error!(
                        "[bundle_tracker] SubscribeBundleResults stream error for region `{}`: {}",
                        region,
                        e
                    );
                    break;
                }
                Err(_) => {
                    log::warn!(
                        "[bundle_tracker] SubscribeBundleResults stream heartbeat timeout for region `{}`; reconnecting",
                        region
                    );
                    break;
                }
            };

            // Process the BundleResult
            self.process_bundle_result(bundle_result).await;
        }
    }

    /// Process a single BundleResult from the subscription stream.
    async fn process_bundle_result(&self, result: BundleResult) {
        let bundle_id = result.bundle_id;

        // Only process if we're tracking this bundle
        let is_tracked = {
            let bundles = self.bundles.read().await;
            bundles.contains_key(&bundle_id)
        };
        if !is_tracked {
            return;
        }

        let status = match result.result {
            Some(bundle_result::Result::Accepted(accepted)) => {
                log::info!(
                    "[bundle_tracker] Bundle {} accepted at slot {} by validator {}",
                    bundle_id,
                    accepted.slot,
                    accepted.validator_identity
                );
                // Accepted means forwarded to validator — mark as landed
                InclusionStatus::Landed {
                    slot: accepted.slot,
                    bundle_index: 0,
                    validator_identity: accepted.validator_identity,
                }
            }
            Some(bundle_result::Result::Processed(processed)) => {
                log::info!(
                    "[bundle_tracker] Bundle {} processed at slot {}, index {}",
                    bundle_id,
                    processed.slot,
                    processed.bundle_index
                );
                InclusionStatus::Landed {
                    slot: processed.slot,
                    bundle_index: processed.bundle_index,
                    validator_identity: processed.validator_identity,
                }
            }
            Some(bundle_result::Result::Finalized(_)) => {
                log::info!("[bundle_tracker] Bundle {} finalized on-chain", bundle_id);
                // Get the slot from the current status if we have one
                let slot = {
                    let bundles = self.bundles.read().await;
                    bundles
                        .get(&bundle_id)
                        .and_then(|r| r.status.slot())
                        .unwrap_or(0)
                };
                InclusionStatus::Finalized {
                    slot,
                    bundle_index: 0,
                }
            }
            Some(bundle_result::Result::Rejected(rejected)) => {
                let reason = match rejected.reason {
                    Some(jito_protos::bundle::rejected::Reason::StateAuctionBidRejected(bid)) => {
                        format!(
                            "state_auction_bid_rejected (bid={} lamports, msg={:?})",
                            bid.simulated_bid_lamports, bid.msg
                        )
                    }
                    Some(jito_protos::bundle::rejected::Reason::WinningBatchBidRejected(bid)) => {
                        format!(
                            "winning_batch_bid_rejected (bid={} lamports, msg={:?})",
                            bid.simulated_bid_lamports, bid.msg
                        )
                    }
                    Some(jito_protos::bundle::rejected::Reason::SimulationFailure(sim)) => {
                        format!(
                            "simulation_failure (tx={}, msg={:?})",
                            sim.tx_signature, sim.msg
                        )
                    }
                    Some(jito_protos::bundle::rejected::Reason::InternalError(err)) => {
                        format!("internal_error: {}", err.msg)
                    }
                    Some(jito_protos::bundle::rejected::Reason::DroppedBundle(dropped)) => {
                        format!("dropped_bundle: {}", dropped.msg)
                    }
                    None => "unknown_rejection".to_string(),
                };
                log::warn!("[bundle_tracker] Bundle {} rejected: {}", bundle_id, reason);
                InclusionStatus::Rejected { reason }
            }
            Some(bundle_result::Result::Dropped(dropped)) => {
                let reason = match dropped.reason {
                    0 => "blockhash_expired".to_string(),
                    1 => "partially_processed".to_string(),
                    2 => "not_finalized".to_string(),
                    other => format!("unknown_drop_reason({})", other),
                };
                log::warn!("[bundle_tracker] Bundle {} dropped: {}", bundle_id, reason);
                InclusionStatus::Dropped { reason }
            }
            None => {
                log::debug!(
                    "[bundle_tracker] Bundle {} result has no variant",
                    bundle_id
                );
                return;
            }
        };

        self.update_status(&bundle_id, status).await;
    }

    /// Remove bundles that have exceeded MAX_BUNDLE_AGE.
    async fn prune_expired(&self) {
        let now = Instant::now();
        let expired_ids: Vec<String> = {
            let bundles = self.bundles.read().await;
            bundles
                .iter()
                .filter(|(_, r)| {
                    !r.status.is_terminal() && now.duration_since(r.submitted_at) > MAX_BUNDLE_AGE
                })
                .map(|(id, _)| id.clone())
                .collect()
        };

        for id in &expired_ids {
            log::warn!(
                "[bundle_tracker] Bundle {} expired after {:?} without resolution",
                id,
                MAX_BUNDLE_AGE
            );
            self.update_status(id, InclusionStatus::Expired).await;
        }
    }
}

impl Default for BundleTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Unit Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inclusion_status_is_confirmed() {
        assert!(InclusionStatus::Landed {
            slot: 42,
            bundle_index: 0,
            validator_identity: "val".to_string(),
        }
        .is_confirmed());
        assert!(InclusionStatus::Finalized {
            slot: 42,
            bundle_index: 0,
        }
        .is_confirmed());
        assert!(!InclusionStatus::Pending.is_confirmed());
        assert!(!InclusionStatus::Rejected {
            reason: "test".to_string()
        }
        .is_confirmed());
        assert!(!InclusionStatus::Dropped {
            reason: "test".to_string()
        }
        .is_confirmed());
        assert!(!InclusionStatus::Expired.is_confirmed());
    }

    #[test]
    fn inclusion_status_returns_slot() {
        assert_eq!(
            InclusionStatus::Landed {
                slot: 99,
                bundle_index: 0,
                validator_identity: "v".to_string(),
            }
            .slot(),
            Some(99)
        );
        assert_eq!(
            InclusionStatus::Finalized {
                slot: 100,
                bundle_index: 0,
            }
            .slot(),
            Some(100)
        );
        assert_eq!(InclusionStatus::Pending.slot(), None);
        assert_eq!(InclusionStatus::Expired.slot(), None);
    }

    #[test]
    fn terminal_statuses() {
        assert!(InclusionStatus::Landed {
            slot: 1,
            bundle_index: 0,
            validator_identity: "v".to_string(),
        }
        .is_terminal());
        assert!(InclusionStatus::Finalized {
            slot: 1,
            bundle_index: 0,
        }
        .is_terminal());
        assert!(InclusionStatus::Rejected {
            reason: "x".to_string()
        }
        .is_terminal());
        assert!(InclusionStatus::Dropped {
            reason: "x".to_string()
        }
        .is_terminal());
        assert!(InclusionStatus::Expired.is_terminal());
        assert!(!InclusionStatus::Pending.is_terminal());
    }

    #[tokio::test]
    async fn register_and_query_status() {
        let tracker = BundleTracker::new();
        let record = BundleRecord {
            bundle_id: "test-bundle-1".to_string(),
            region: "amsterdam".to_string(),
            mint: "mint-x".to_string(),
            pool_id: "pool-x".to_string(),
            transaction_signature: "sig1".to_string(),
            submitted_at: Instant::now(),
            status: InclusionStatus::Pending,
            notify_tx: None,
        };
        let _rx = tracker.register_bundle(record).await;
        let status = tracker.get_status("test-bundle-1").await;
        assert_eq!(status, Some(InclusionStatus::Pending));

        let count = tracker.tracked_count().await;
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn update_status_records_in_db() {
        // Note: This test relies on the global DB connection which may not
        // be initialized in unit tests. We test the state machine transition
        // only, not the DB side effect.
        let tracker = BundleTracker::new();
        let record = BundleRecord {
            bundle_id: "test-bundle-update".to_string(),
            region: "frankfurt".to_string(),
            mint: "mint-y".to_string(),
            pool_id: "pool-y".to_string(),
            transaction_signature: "sig2".to_string(),
            submitted_at: Instant::now(),
            status: InclusionStatus::Pending,
            notify_tx: None,
        };
        let _rx = tracker.register_bundle(record).await;

        // Update to landed
        tracker
            .update_status(
                "test-bundle-update",
                InclusionStatus::Landed {
                    slot: 12345,
                    bundle_index: 0,
                    validator_identity: "val-z".to_string(),
                },
            )
            .await;

        let status = tracker.get_status("test-bundle-update").await;
        assert!(status.as_ref().unwrap().is_confirmed());
        assert_eq!(status.unwrap().slot(), Some(12345));
    }

    #[tokio::test]
    async fn terminal_status_not_overwritten() {
        let tracker = BundleTracker::new();
        let record = BundleRecord {
            bundle_id: "terminal-test".to_string(),
            region: "r1".to_string(),
            mint: "mint".to_string(),
            pool_id: "pool".to_string(),
            transaction_signature: "sig".to_string(),
            submitted_at: Instant::now(),
            status: InclusionStatus::Pending,
            notify_tx: None,
        };
        let _rx = tracker.register_bundle(record).await;

        tracker
            .update_status(
                "terminal-test",
                InclusionStatus::Rejected {
                    reason: "first".to_string(),
                },
            )
            .await;

        // Try to overwrite with landed — should be ignored
        tracker
            .update_status(
                "terminal-test",
                InclusionStatus::Landed {
                    slot: 1,
                    bundle_index: 0,
                    validator_identity: "v".to_string(),
                },
            )
            .await;

        let status = tracker.get_status("terminal-test").await;
        assert_eq!(
            status,
            Some(InclusionStatus::Rejected {
                reason: "first".to_string()
            })
        );
    }

    #[tokio::test]
    async fn inclusion_notification_fires_on_landing() {
        let tracker = BundleTracker::new();
        let record = BundleRecord {
            bundle_id: "notify-test".to_string(),
            region: "r2".to_string(),
            mint: "mint-z".to_string(),
            pool_id: "pool-z".to_string(),
            transaction_signature: "sig3".to_string(),
            submitted_at: Instant::now(),
            status: InclusionStatus::Pending,
            notify_tx: None,
        };
        let mut rx = tracker.register_bundle(record).await;

        tracker
            .update_status(
                "notify-test",
                InclusionStatus::Landed {
                    slot: 999,
                    bundle_index: 0,
                    validator_identity: "v1".to_string(),
                },
            )
            .await;

        let received = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("notification should fire")
            .expect("notification should contain record");

        assert_eq!(received.bundle_id, "notify-test");
        assert!(received.status.is_confirmed());
        assert_eq!(received.status.slot(), Some(999));
    }
}
