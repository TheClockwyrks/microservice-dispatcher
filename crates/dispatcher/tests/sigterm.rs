//! The `tcab-dispatcher` binary's response to `SIGTERM` and an interrupt, run as a
//! process against a stub backend and a stub Kubernetes API.
//!
//! The binary is pointed at the stubs exactly as it is configured anywhere: the
//! backend by `TCAB_BACKEND_URL`, and the Kubernetes API by a kubeconfig naming the
//! stub as its server. The stub backend hands out at most one queued run; the stub
//! Kubernetes API owns no `Job`s and holds each `Job` creation until the test
//! releases it, announcing when one has begun. That fixes the order of the claim,
//! the signal and the creation's answer, so the tests wait on events, never on
//! elapsed time; the timeouts only bound a failing test.

use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use test_cabinet_contracts::job_api::{ClaimedJob, LaunchBody};
use test_cabinet_contracts::run_record::HarnessSlug;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::{Notify, mpsc};

/// How long a test waits for something the binary must do promptly. Only a failing
/// test ever waits this long.
const PROMPTLY: Duration = Duration::from_secs(20);

/// What both stubs saw.
#[derive(Default)]
struct Seen {
    /// The queued run the backend still has to hand out.
    queued: Option<ClaimedJob>,
    /// `POST /jobs/next` calls answered.
    claims: usize,
    /// `Job` creations the Kubernetes API answered `201`.
    created: usize,
}

#[derive(Clone)]
struct Stubs {
    seen: Arc<Mutex<Seen>>,
    /// Told when a `Job` creation reaches the Kubernetes API.
    began: mpsc::UnboundedSender<()>,
    /// Lets the creation in progress, and every later one, answer.
    release: Arc<Notify>,
}

fn claim() -> ClaimedJob {
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

async fn claim_next(State(stubs): State<Stubs>) -> Response {
    let mut seen = stubs.seen.lock().expect("lock");
    seen.claims += 1;
    match seen.queued.take() {
        Some(claim) => axum::Json(claim).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

async fn no_jobs_active() -> Response {
    axum::Json(Vec::<serde_json::Value>::new()).into_response()
}

async fn list_jobs() -> Response {
    axum::Json(serde_json::json!({
        "apiVersion": "batch/v1",
        "kind": "JobList",
        "metadata": {},
        "items": [],
    }))
    .into_response()
}

/// Hold the creation until released, then answer `201` with the `Job` as posted.
async fn create_job(
    State(stubs): State<Stubs>,
    axum::Json(job): axum::Json<serde_json::Value>,
) -> Response {
    stubs.began.send(()).expect("test listening");
    stubs.release.notified().await;
    stubs.release.notify_one();
    stubs.seen.lock().expect("lock").created += 1;
    (StatusCode::CREATED, axum::Json(job)).into_response()
}

async fn serve(app: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{addr}")
}

/// The running binary, its stubs, and its output, line by line.
struct Dispatcher {
    child: Child,
    stubs: Stubs,
    began: mpsc::UnboundedReceiver<()>,
    lines: mpsc::UnboundedReceiver<String>,
    output: Vec<String>,
    _kubeconfig: TempFile,
}

/// A file removed when dropped.
struct TempFile(PathBuf);

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

impl Dispatcher {
    /// Start the binary against fresh stubs whose backend queues `queued`.
    async fn start(queued: Option<ClaimedJob>) -> Self {
        let (began_tx, began) = mpsc::unbounded_channel();
        let stubs = Stubs {
            seen: Arc::new(Mutex::new(Seen {
                queued,
                ..Seen::default()
            })),
            began: began_tx,
            release: Arc::new(Notify::new()),
        };
        let backend = serve(
            Router::new()
                .route("/jobs/next", post(claim_next))
                .route("/jobs/active", get(no_jobs_active))
                .with_state(stubs.clone()),
        )
        .await;
        let kubernetes = serve(
            Router::new()
                .route(
                    "/apis/batch/v1/namespaces/tcab/jobs",
                    get(list_jobs).post(create_job),
                )
                .with_state(stubs.clone()),
        )
        .await;

        let kubeconfig = TempFile(std::env::temp_dir().join(format!(
            "tcab-dispatcher-sigterm-{}-{}.yaml",
            std::process::id(),
            kubernetes.rsplit(':').next().expect("port"),
        )));
        std::fs::write(
            &kubeconfig.0,
            format!(
                "apiVersion: v1\nkind: Config\n\
                 clusters:\n- name: stub\n  cluster:\n    server: {kubernetes}\n\
                 users:\n- name: stub\n  user:\n    token: stub-token\n\
                 contexts:\n- name: stub\n  context:\n    cluster: stub\n    user: stub\n    namespace: tcab\n\
                 current-context: stub\n"
            ),
        )
        .expect("write the kubeconfig");

        let mut child = Command::new(env!("CARGO_BIN_EXE_tcab-dispatcher"))
            .env_clear()
            .env("KUBECONFIG", &kubeconfig.0)
            .env("TCAB_BACKEND_URL", &backend)
            .env("TCAB_BACKEND_SERVICE_TOKEN", "svc-token")
            .env("TCAB_DRIVER_IMAGE", "registry.example.com/tcab-driver:test")
            .env("TCAB_DISPATCHER_NAMESPACE", "tcab")
            // An idle dispatcher would sleep this long; a test that waits it out fails.
            .env("TCAB_DISPATCHER_POLL_INTERVAL_SECONDS", "3600")
            .env("NO_COLOR", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn tcab-dispatcher");

        let (lines_tx, lines) = mpsc::unbounded_channel();
        for stream in [
            Box::new(child.stdout.take().expect("stdout"))
                as Box<dyn tokio::io::AsyncRead + Send + Unpin>,
            Box::new(child.stderr.take().expect("stderr")),
        ] {
            let lines_tx = lines_tx.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stream).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    if lines_tx.send(line).is_err() {
                        break;
                    }
                }
            });
        }

        Self {
            child,
            stubs,
            began,
            lines,
            output: Vec::new(),
            _kubeconfig: kubeconfig,
        }
    }

    fn signal(&self, name: &str) {
        let pid = self.child.id().expect("running").to_string();
        let status = std::process::Command::new("kill")
            .args([format!("-{name}"), pid])
            .status()
            .expect("run kill");
        assert!(status.success(), "kill -{name} failed");
    }

    /// Wait for a line of output containing `needle`.
    async fn logs(&mut self, needle: &str) {
        let found = tokio::time::timeout(PROMPTLY, async {
            while let Some(line) = self.lines.recv().await {
                let hit = line.contains(needle);
                self.output.push(line);
                if hit {
                    return true;
                }
            }
            false
        })
        .await;
        assert!(
            matches!(found, Ok(true)),
            "no output line contains {needle:?}; output:\n{}",
            self.output.join("\n")
        );
    }

    /// Wait for the process to exit, answering its status and all its output.
    async fn exits(mut self) -> (ExitStatus, Vec<String>, Stubs) {
        let status = tokio::time::timeout(PROMPTLY, self.child.wait())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the dispatcher did not exit; output:\n{}",
                    self.output.join("\n")
                )
            })
            .expect("wait");
        // The pipes close with the process; collect what is left of its output.
        while let Ok(Some(line)) = tokio::time::timeout(PROMPTLY, self.lines.recv()).await {
            self.output.push(line);
        }
        (status, self.output, self.stubs)
    }
}

/// The case shutdown handling exists for: `SIGTERM` lands between the claim of a
/// run and its `Job`'s creation, and the run still receives its `Job` before the
/// process exits 0.
#[tokio::test]
async fn sigterm_during_a_tick_creates_the_claimed_runs_job_and_exits_0() {
    let mut dispatcher = Dispatcher::start(Some(claim())).await;
    tokio::time::timeout(PROMPTLY, dispatcher.began.recv())
        .await
        .expect("the Job creation began")
        .expect("stub alive");

    dispatcher.signal("TERM");
    dispatcher
        .logs("shutdown requested during a tick; finishing the tick in progress")
        .await;
    assert!(
        dispatcher.child.try_wait().expect("try_wait").is_none(),
        "exited before the claimed run's Job was created"
    );
    dispatcher.stubs.release.notify_one();

    let (status, output, stubs) = dispatcher.exits().await;
    let output = output.join("\n");
    assert!(status.success(), "exited {status}; output:\n{output}");
    let seen = stubs.seen.lock().expect("lock");
    assert_eq!(seen.created, 1, "the claimed run's Job was not created");
    assert_eq!(seen.claims, 1, "claimed again after SIGTERM");
    assert!(
        output.contains("dispatched a driver Job"),
        "output:\n{output}"
    );
}

/// With an empty queue the dispatcher idles for its hour-long poll interval;
/// `SIGTERM` then ends it at once, with status 0.
#[tokio::test]
async fn sigterm_while_idle_exits_0_promptly() {
    stop_while_idle("TERM").await;
}

/// An interrupt (`Ctrl-C`) is the same stop request as `SIGTERM`.
#[tokio::test]
async fn an_interrupt_while_idle_exits_0_promptly() {
    stop_while_idle("INT").await;
}

async fn stop_while_idle(signal: &str) {
    let dispatcher = Dispatcher::start(None).await;
    // The first claim is answered once the signal handlers are in place, so the
    // signal sent after it is a stop request, never the default action.
    tokio::time::timeout(PROMPTLY, async {
        while dispatcher.stubs.seen.lock().expect("lock").claims == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the dispatcher claimed");

    dispatcher.signal(signal);
    let (status, output, stubs) = dispatcher.exits().await;
    assert!(
        status.success(),
        "exited {status} on SIG{signal}; output:\n{}",
        output.join("\n")
    );
    let seen = stubs.seen.lock().expect("lock");
    assert_eq!(seen.created, 0);
    assert_eq!(seen.claims, 1, "ticked again after SIG{signal}");
}
