//! NDJSON transport over the stdio of a spawned `app-server` subprocess.
//!
//! The ZCode Protocol frames messages as one JSON object per
//! `\n`-terminated line (verified empirically: there is no Content-Length
//! framing and the envelope rejects `"jsonrpc":"2.0"`). This module owns the
//! [`tokio::process::Child`] and provides framed write/read helpers plus an
//! async line reader.

use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};

// Re-export the stdio types so sibling modules can name them when splitting
// the transport into reader/writer tasks.
pub use tokio::process::{ChildStdin, ChildStdout};

use super::protocol::{IncomingMessage, OutgoingMessage, RequestId};

/// A live connection to a `zcode.cjs app-server` subprocess.
pub struct AppServerTransport {
    pub child: Child,
    pub stdin: ChildStdin,
    pub stdout: BufReader<ChildStdout>,
}

/// Spawn `node <zcode.cjs> app-server [extra args]` in the given `cwd`.
pub fn spawn(
    node: &str,
    zcode_path: &std::path::Path,
    cwd: &std::path::Path,
    extra_args: &[String],
) -> std::io::Result<AppServerTransport> {
    let mut cmd = Command::new(node);
    cmd.arg(zcode_path);
    cmd.arg("app-server");
    cmd.args(extra_args);
    cmd.current_dir(cwd);
    cmd.env("ZCODE_LOG_LEVEL", "error"); // keep stderr quiet unless we raise it
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::inherit());
    cmd.kill_on_drop(true);

    let mut child = cmd.spawn()?;
    let stdin = child.stdin.take().expect("stdin piped");
    let stdout = child.stdout.take().expect("stdout piped");

    Ok(AppServerTransport {
        child,
        stdin,
        stdout: BufReader::new(stdout),
    })
}

impl AppServerTransport {
    /// Send a message (request or notification). Adds the trailing newline.
    pub async fn send(&mut self, msg: &OutgoingMessage<'_>) -> std::io::Result<()> {
        let line = serde_json::to_string(msg)?;
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        Ok(())
    }

    /// Send a raw JSON value (for responses to server→client requests whose
    /// shape we build by hand).
    pub async fn send_raw(&mut self, value: &serde_json::Value) -> std::io::Result<()> {
        let line = serde_json::to_string(value)?;
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        Ok(())
    }

    /// Read one framed message. Returns `None` on EOF.
    ///
    /// Blank keepalive lines and unparseable lines are skipped silently
    /// (logged) so a single bad line never tears down the connection.
    pub async fn recv(&mut self) -> std::io::Result<Option<IncomingMessage>> {
        loop {
            let mut line = String::new();
            let n = self.stdout.read_line(&mut line).await?;
            if n == 0 {
                return Ok(None);
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue; // skip keepalive lines
            }
            match serde_json::from_str::<IncomingMessage>(trimmed) {
                Ok(m) => return Ok(Some(m)),
                Err(e) => {
                    tracing::warn!(target: "zcode::transport", line = %trimmed, "non-JSON line from app-server: {e}");
                    continue; // try the next line
                }
            }
        }
    }

    /// Best-effort shutdown: signal the child to exit and wait briefly.
    pub async fn shutdown(&mut self) {
        let _ = self.stdin.shutdown().await;
        let _ = self.child.start_kill();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), self.child.wait()).await;
    }
}

/// Counter for the adapter's own outgoing request ids.
#[derive(Debug, Default)]
pub struct IdSource(std::sync::atomic::AtomicI64);

impl IdSource {
    pub fn next(&self) -> RequestId {
        let n = self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        RequestId::Num(n)
    }
}
