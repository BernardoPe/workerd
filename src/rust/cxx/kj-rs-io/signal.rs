//! tokio-backed signal watching: POSIX signals on Unix, the corresponding console control
//! events on Windows.
//!
//! This backs `kj_rs_io::onSignal()` (async-io.h), the tokio-loop replacement for
//! `kj::UnixEventPort::onSignal()` -- workerd uses it for SIGTERM graceful drain.
//!
//! Semantics differences vs `UnixEventPort::onSignal()` (acceptable for the drain use case):
//!
//! - No `siginfo_t` is reported; the promise just resolves.
//! - The handler is registered when the returned future is first polled (tokio registers with
//!   the process-global signal registry at `signal()` time), not at call time, and KJ does not
//!   block the signal beforehand the way `UnixEventPort::captureSignal()` does. A signal
//!   delivered before the first poll takes its default disposition.
//! - tokio's signal registration is process-wide and persists for the life of the process
//!   (dropping the future stops *watching*, but does not restore `SIG_DFL`).
//!
//! On Windows the signums workerd actually passes are mapped to their conventional console
//! control events: SIGTERM -> `ctrl_shutdown`, SIGINT -> `ctrl_c`. Anything else errors.

use crate::error::KjIoError;
use crate::error::Result;
use crate::runtime::with_runtime;

/// Resolves when the process receives signal `signum` (on Windows: the console control event
/// conventionally mapped to it). Errors immediately for unmapped signums / other platforms.
pub(crate) async fn wait_for_signal(signum: i32) -> Result<()> {
    #[cfg(unix)]
    {
        use crate::error::op;
        with_runtime(async move {
            let kind = tokio::signal::unix::SignalKind::from_raw(signum);
            let mut sig = tokio::signal::unix::signal(kind).map_err(op("signal"))?;
            sig.recv()
                .await
                .ok_or_else(|| KjIoError::other("signal", "signal stream closed unexpectedly"))?;
            Ok(())
        })
        .await
    }
    // Validated by Windows CI.
    //
    // Deliberately NOT a plain mirror of the unix arm, because tokio's signal *delivery* is not
    // a mirror: unix signals terminate in the I/O driver (i.e. on this loop thread), so the
    // unix arm may await the stream directly. On Windows, tokio's `SetConsoleCtrlHandler`
    // handler runs on an OS-spawned console-ctrl thread and broadcasts to every registered
    // watcher's stored waker FROM THAT THREAD. Awaiting the stream directly here would park a
    // clone of the bridged future's waker -- a loop-thread-only, non-atomic `kj_rs`
    // `FutureWakerCell` -- in tokio's signal registry, and one Ctrl-C/shutdown event would wake
    // it cross-thread: UB under the bridge's single-thread waker axiom. So, exactly like
    // `net.rs::resolve_host` (the model citizen for this pattern), a tokio *runtime* task owns
    // the `recv()`: the cross-thread broadcast terminates at tokio's own `Send + Sync`
    // scheduler waker (which unparks this loop), the task then runs on the loop thread and
    // hands the result back over a oneshot, waking the bridged future same-thread.
    #[cfg(windows)]
    {
        use crate::error::op;
        // `<csignal>` values as the C++ callers pass them (MSVC defines SIGINT=2, SIGTERM=15).
        // workerd's only caller passes SIGTERM (graceful drain; server/cli-io-backend.c++);
        // SIGINT is mapped for completeness.
        const SIGINT: i32 = 2;
        const SIGTERM: i32 = 15;
        with_runtime(async move {
            if signum != SIGTERM && signum != SIGINT {
                return Err(KjIoError::other(
                    "signal",
                    "kj-rs-io only watches SIGTERM/SIGINT on Windows",
                ));
            }
            let (tx, rx) = tokio::sync::oneshot::channel::<Result<()>>();
            let task = tokio::spawn(async move {
                let result = async {
                    // SIGTERM -> ctrl_shutdown, SIGINT -> ctrl_c (the conventional mappings).
                    let received = match signum {
                        SIGTERM => {
                            let mut sig =
                                tokio::signal::windows::ctrl_shutdown().map_err(op("signal"))?;
                            sig.recv().await
                        }
                        // SIGINT; anything else already errored before the spawn.
                        _ => {
                            let mut sig = tokio::signal::windows::ctrl_c().map_err(op("signal"))?;
                            sig.recv().await
                        }
                    };
                    received.ok_or_else(|| {
                        KjIoError::other("signal", "signal stream closed unexpectedly")
                    })
                }
                .await;
                let _ = tx.send(result);
            });
            // If this future is dropped (KJ promise cancelled), abort the watcher task so its
            // signal-stream registration is torn down instead of lingering for the process
            // lifetime.
            let _abort_guard = crate::runtime::AbortOnDrop(task);
            match rx.await {
                Ok(result) => result,
                Err(_) => Err(KjIoError::other("signal", "signal watcher task dropped")),
            }
        })
        .await
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = signum;
        Err(KjIoError::other(
            "signal",
            "kj-rs-io signal watching is not implemented on this platform",
        ))
    }
}
