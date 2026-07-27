//! Library crate for `zcode-acp`.
//!
//! An ACP (Agent Client Protocol) adapter that drives the [ZCode] CLI through
//! its stdio `app-server` (the "ZCode Protocol"). Mirrors the role
//! `zed-industries/codex-acp` plays for Codex: an ACP **Agent** that a host
//! such as Buzz's `buzz-acp` spawns and talks JSON-RPC to.
//!
//! [ZCode]: https://z.ai
//!
//! # Architecture
//!
//! ```text
//! buzz-acp ──stdio(ACP)──► zcode-acp ──spawn──► node zcode.cjs app-server
//!   (host)                   (this shim)            (one per ACP session)
//!      ◄──session/update──              ◄──NDJSON events──
//!      ──session/requestPermission──►   ──►interaction/requestPermission──►
//! ```
//!
//! Each ACP `session/new` spawns a dedicated `app-server` subprocess. Each
//! `session/prompt` becomes a `session/send` to that subprocess, after a
//! `session/subscribe` so events stream back. Tool calls and permissions are
//! translated bidirectionally.
//!
//! See `src/main.rs` for the binary entry point.

pub mod acp_translator;
pub mod agent;
pub mod zcode;

use std::path::PathBuf;

use agent_client_protocol as acp;
use clap::Args;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

pub use agent::ZcodeAgent;

/// Tunables exposed via the CLI that are not about locating ZCode itself.
#[derive(Args, Debug, Clone)]
pub struct CliOptions {
    /// Per-prompt hard timeout in seconds. If a turn exceeds it, the active
    /// ZCode prompt is stopped and the turn ends with `StopReason::Cancelled`.
    #[arg(long, env = "ZCODE_ACP_PROMPT_TIMEOUT_SECS", default_value_t = 900)]
    pub prompt_timeout_secs: u64,

    /// Disable interactive OAuth (browser/device flow). When set, the adapter
    /// only accepts an already-configured provider in
    /// `~/.zcode/cli/config.json` and refuses `authenticate` requests that
    /// would require a browser.
    #[arg(long, env = "ZCODE_ACP_NO_BROWSER", default_value_t = false)]
    pub no_browser: bool,

    /// Override the ZCode data base directory (default `$HOME`, giving
    /// `~/.zcode`). Mirrors ZCode's own `ZCODE_DATA_BASE_DIR`.
    #[arg(long, env = "ZCODE_DATA_BASE_DIR")]
    pub data_base_dir: Option<PathBuf>,
}

/// Everything [`run`] needs to start the agent.
pub struct RunConfig {
    /// Path to `zcode.cjs`.
    pub zcode_path: PathBuf,
    /// Executable to invoke it (normally `node`).
    pub node: String,
    /// Extra args appended to every `app-server` spawn (e.g. `--no-color`).
    pub extra_args: Vec<String>,
    /// Working directory used for spawned app-servers when a session does not
    /// supply one. A session's `cwd` (from ACP `session/new`) always wins.
    pub fallback_cwd: PathBuf,
    /// Misc tunables.
    pub options: CliOptions,
}

/// Run the ACP agent over stdio until the host disconnects.
///
/// stdout is the JSON-RPC transport; nothing else may write to it (enforced by
/// the `clippy::print_stdout` lint). All diagnostics go through `tracing` to
/// stderr.
pub async fn run(cfg: RunConfig) -> acp::Result<()> {
    let agent = std::sync::Arc::new(ZcodeAgent::new(cfg));
    let stdin = tokio::io::stdin().compat();
    let stdout = tokio::io::stdout().compat_write();
    // ByteStreams::new(outgoing_write, incoming_read) = (stdout, stdin).
    let transport = acp::ByteStreams::new(stdout, stdin);

    tracing::info!("zcode-acp serving ACP over stdio");
    agent.serve(transport).await
}
