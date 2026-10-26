//! Tests for the control loop's shutdown: a stop request never cuts a tick short,
//! a claim made before it always receives its `Job`, no lane claims after it, and
//! the loop returns `Ok` at once when the request arrives between ticks or while it
//! idles.
//!
//! The stub backend hands out one queued run and one queued publish, then answers
//! empty queues, and records every call. The in-memory cluster holds each `Job`
//! creation until the test releases it, announcing that it has begun, so a test can
//! deliver the stop request while a claimed job's `Job` is being created. The stop
//! request is a future the test resolves, which announces when the loop has observed
//! it, so the order of request and release is fixed rather than raced.

use super::*;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use k8s_openapi::api::batch::v1::Job;
use tokio::net::TcpListener;
use tokio::sync::{Notify, mpsc, oneshot};

use test_cabinet_contracts::job_api::LaunchBody;
use test_cabinet_contracts::run_record::HarnessSlug;

use crate::config::DriverResources;
use crate::job::JOB_ID_LABEL;

const SERVICE_TOKEN: &str = "svc-token";

/// How long a test waits for something the loop must do promptly before failing.
/// Only a failing test ever waits this long.
const PROMPTLY: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// The stub backend
// ---------------------------------------------------------------------------

/// The stub backend's state: the queued run and publish still to hand out, and the
/// route of every call made, in order.
#[derive(Debug, Default)]
struct Backend {
    run: Option<ClaimedJob>,
    publish: Option<PublishClaim>,
    calls: Vec<&'static str>,
}

type Shared = Arc<Mutex<Backend>>;

fn calls(shared: &Shared, route: &str) -> usize {
    shared
        .lock()
        .expect("lock")
        .calls
        .iter()
        .filter(|call| **call == route)
        .count()
}

async fn claim_run(State(shared): State<Shared>) -> Response {
    let mut backend = shared.lock().expect("lock");
    backend.calls.push("POST /jobs/next");
    match backend.run.take() {
        Some(claim) => axum::Json(claim).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

async fn claim_publish(State(shared): State<Shared>) -> Response {
    let mut backend = shared.lock().expect("lock");
    backend.calls.push("POST /publish-jobs/next");
    match backend.publish.take() {
        Some(claim) => axum::Json(claim).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

/// `GET /jobs/active` and `GET /publish-jobs/active`: nothing in flight.
async fn none_active(State(shared): State<Shared>) -> Response {
    shared.lock().expect("lock").calls.push("GET active");
    axum::Json(Vec::<serde_json::Value>::new()).into_response()
}

async fn serve(shared: Shared) -> String {
    let app = Router::new()
        .route("/jobs/next", post(claim_run))
        .route("/publish-jobs/next", post(claim_publish))
        .route("/jobs/active", get(none_active))
        .route("/publish-jobs/active", get(none_active))
        .with_state(shared);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{addr}")
}

// ---------------------------------------------------------------------------
// The slow cluster
// ---------------------------------------------------------------------------

/// A cluster that owns no `Job` and holds every `Job` creation until released.
#[derive(Clone)]
struct SlowCluster {
    /// Told the job id of each creation as it begins.
    began: mpsc::UnboundedSender<String>,
    /// Ends every creation in progress, and every later one at once.
    release: Arc<Notify>,
    /// The job ids whose `Job`s were created, in order.
    created: Arc<Mutex<Vec<String>>>,
}

impl Cluster for SlowCluster {
    async fn create_job(&self, job: &Job) -> anyhow::Result<()> {
        let job_id = job
            .metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get(JOB_ID_LABEL))
            .cloned()
            .expect("every Job carries its job id");
        self.began.send(job_id.clone()).expect("test listening");
        self.release.notified().await;
        // Every later creation goes straight through.
        self.release.notify_one();
        self.created.lock().expect("lock").push(job_id);
        Ok(())
    }

    async fn list_managed(&self) -> anyhow::Result<Vec<ManagedJob>> {
        Ok(Vec::new())
    }

    async fn failure_detail(&self, job_name: &str) -> String {
        format!("Job `{job_name}` failed")
    }

    async fn delete_sandbox_pods(&self, _job_id: &str) -> anyhow::Result<usize> {
        Ok(0)
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn run_claim() -> ClaimedJob {
    ClaimedJob {
        job_id: "run-1".to_string(),
        job_token: "run-token".to_string(),
        request: LaunchBody {
            test_case: "pong".to_string(),
            version: "v1.0.0".to_string(),
            variant: "base".to_string(),
            harness: HarnessSlug::Claude,
            model: "anthropic/claude".to_string(),
            orchestrator: None,
            engine: None,
            max_runtime_seconds: Some(600),
            auth_mode: None,
            retry_count: None,
            gg_capability_set: None,
            gg_model_windows: Default::default(),
            gg_model_providers: Default::default(),
            gg_model_modalities: Default::default(),
            gg_model_prices: Default::default(),
            model_prices: None,
        },
    }
}

fn publish_claim() -> PublishClaim {
    PublishClaim {
        job_id: "pub-1".to_string(),
        job_token: "pub-token".to_string(),
        run_id: "run-0".to_string(),
    }
}

/// A config against the stub at `backend_url` with both lanes open, publishing on,
/// and a poll interval no test outlives, so a loop that idles through it rather than
/// stopping fails the test.
fn config(backend_url: &str) -> Config {
    Config {
        backend_url: backend_url.to_string(),
        service_token: SERVICE_TOKEN.to_string(),
        driver_image: "registry.example.com/tcab-driver:test".to_string(),
        namespace: "tcab".to_string(),
        sandbox_namespace: "tcab".to_string(),
        driver_service_account: None,
        max_inflight: 8,
        max_publish_inflight: 8,
        poll_interval: Duration::from_secs(3600),
        job_ttl_seconds: 300,
        driver_termination_grace_seconds: 120,
        driver_resources: DriverResources::default(),
        publisher_resources: DriverResources::default(),
        driver_secrets: Vec::new(),
        driver_subscription_secret: None,
        subscription_dir: "/var/run/tcab/subscription".to_string(),
        driver_auth_mode: None,
        passthrough_k8s_env: Vec::new(),
        publisher_image: Some("registry.example.com/tcab-publisher:test".to_string()),
        publisher_secrets: Vec::new(),
        passthrough_publisher_env: Vec::new(),
    }
}

/// A running loop and the handles a test drives it with.
struct Harness {
    backend: Shared,
    began: mpsc::UnboundedReceiver<String>,
    release: Arc<Notify>,
    created: Arc<Mutex<Vec<String>>>,
    /// Resolves the stop request.
    stop: Option<oneshot::Sender<()>>,
    /// Told once the loop has observed the stop request.
    observed: oneshot::Receiver<()>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    /// Start the loop over a backend queueing `run` and `publish`.
    async fn start(run: Option<ClaimedJob>, publish: Option<PublishClaim>) -> Self {
        let backend: Shared = Arc::new(Mutex::new(Backend {
            run,
            publish,
            calls: Vec::new(),
        }));
        let url = serve(backend.clone()).await;
        let (began_tx, began) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let created = Arc::new(Mutex::new(Vec::new()));
        let cluster = SlowCluster {
            began: began_tx,
            release: release.clone(),
            created: created.clone(),
        };
        let dispatcher = Dispatcher::new(
            config(&url),
            BackendClient::new(&url, SERVICE_TOKEN),
            cluster,
        );
        let (stop, requested) = oneshot::channel::<()>();
        let (observed_tx, observed) = oneshot::channel::<()>();
        let shutdown = async move {
            requested.await.expect("the test requests the stop");
            observed_tx.send(()).expect("test listening");
        };
        let task = tokio::spawn(dispatcher.run(shutdown));
        Self {
            backend,
            began,
            release,
            created,
            stop: Some(stop),
            observed,
            task,
        }
    }

    /// Wait for the next `Job` creation to begin, answering its job id.
    async fn creation_begins(&mut self) -> String {
        tokio::time::timeout(PROMPTLY, self.began.recv())
            .await
            .expect("a Job creation began")
            .expect("cluster alive")
    }

    /// Request the stop and wait until the loop has observed it.
    async fn stop(&mut self) {
        self.stop
            .take()
            .expect("stopped once")
            .send(())
            .expect("loop alive");
        tokio::time::timeout(PROMPTLY, &mut self.observed)
            .await
            .expect("the loop observed the stop request")
            .expect("observed once");
    }

    /// Wait for the loop to return, which must be `Ok`.
    async fn stopped(self) -> (Shared, Vec<String>) {
        tokio::time::timeout(PROMPTLY, self.task)
            .await
            .expect("the loop stopped promptly")
            .expect("the loop did not panic")
            .expect("the loop stopped cleanly");
        let created = self.created.lock().expect("lock").clone();
        (self.backend, created)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The case shutdown handling exists for: a run claimed when the stop arrives still
/// receives its driver `Job`, and the loop then stops without claiming again.
#[tokio::test]
async fn a_stop_during_a_tick_lets_the_claimed_run_receive_its_job() {
    let mut harness = Harness::start(Some(run_claim()), None).await;
    assert_eq!(harness.creation_begins().await, "run-1");
    harness.stop().await;
    // Still creating: the stop did not end the loop.
    assert!(!harness.task.is_finished());
    harness.release.notify_one();
    let (backend, created) = harness.stopped().await;
    assert_eq!(created, vec!["run-1".to_string()]);
    assert_eq!(
        calls(&backend, "POST /jobs/next"),
        1,
        "claimed again after the stop"
    );
}

/// The publish lane is tried first. A stop that arrives while its `Job` is created
/// lets that `Job` through, and the run lane, not yet reached, claims nothing.
#[tokio::test]
async fn a_stop_during_a_publish_creation_claims_no_run() {
    let mut harness = Harness::start(Some(run_claim()), Some(publish_claim())).await;
    assert_eq!(harness.creation_begins().await, "pub-1");
    harness.stop().await;
    harness.release.notify_one();
    let (backend, created) = harness.stopped().await;
    assert_eq!(created, vec!["pub-1".to_string()]);
    assert_eq!(calls(&backend, "POST /publish-jobs/next"), 1);
    assert_eq!(
        calls(&backend, "POST /jobs/next"),
        0,
        "claimed a run after the stop"
    );
}

/// With both queues empty the loop idles for the (hour-long) poll interval; a stop
/// then ends it at once.
#[tokio::test]
async fn a_stop_while_idle_ends_the_loop_at_once() {
    let mut harness = Harness::start(None, None).await;
    // The first tick has tried both lanes once their claims are answered; wait for
    // both, so the stop lands at or after the start of the idle backoff.
    tokio::time::timeout(PROMPTLY, async {
        while calls(&harness.backend, "POST /jobs/next") == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the first tick claimed");
    harness.stop().await;
    let (backend, created) = harness.stopped().await;
    assert!(created.is_empty());
    assert_eq!(calls(&backend, "POST /jobs/next"), 1);
}

/// A stop already requested when the loop starts ends it before its first tick.
#[tokio::test]
async fn a_stop_before_the_first_tick_starts_none() {
    let mut harness = Harness::start(Some(run_claim()), Some(publish_claim())).await;
    // The loop task has not run yet on this current-thread runtime, so the request is
    // in before its first poll.
    harness
        .stop
        .take()
        .expect("once")
        .send(())
        .expect("loop alive");
    let (backend, created) = harness.stopped().await;
    assert!(created.is_empty());
    assert!(backend.lock().expect("lock").calls.is_empty());
}
