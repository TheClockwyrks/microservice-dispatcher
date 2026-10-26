//! The process's stop request: `SIGTERM` or an interrupt.
//!
//! Kubernetes stops the dispatcher's pod with `SIGTERM` and waits out the pod's
//! `terminationGracePeriodSeconds` before it kills it; an operator at a terminal
//! sends an interrupt (`Ctrl-C`). Both are the same request to the control loop,
//! which finishes the tick in progress and stops (see
//! [`Dispatcher::run`](crate::controller::Dispatcher::run)). The dispatcher serves no
//! `Service` and binds no socket, so it has no readiness to fail and no drain delay
//! to hold. See the control plane's "Graceful termination" in the documentation.

use std::future::Future;

/// Install the handlers for `SIGTERM` and the interrupt, and answer a future that
/// resolves when either arrives.
///
/// The handlers are installed by this call, not when the future is first polled, so
/// a signal delivered at any point afterwards is held for the future rather than
/// ending the process by the signal's default action. Call it before anything that
/// can take time, so the stop request is never lost to a slow start.
pub fn shutdown_signal() -> std::io::Result<impl Future<Output = ()> + Send + 'static> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())?;
        let mut interrupt = signal(SignalKind::interrupt())?;
        Ok(async move {
            let name = tokio::select! {
                _ = terminate.recv() => "SIGTERM",
                _ = interrupt.recv() => "SIGINT",
            };
            tracing::info!(signal = name, "stop requested");
        })
    }
    #[cfg(not(unix))]
    {
        Ok(async move {
            // Only an interrupt reaches a process here. An error installing its
            // handler leaves no way to be asked to stop, so wait forever then.
            match tokio::signal::ctrl_c().await {
                Ok(()) => tracing::info!(signal = "interrupt", "stop requested"),
                Err(err) => {
                    tracing::warn!(error = %err, "could not listen for an interrupt");
                    std::future::pending::<()>().await;
                }
            }
        })
    }
}
