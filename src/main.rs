//! Binary entry point for `zcode-acp`.
//!
//! Parses CLI flags (where to find `zcode.cjs`, how to invoke `node`), sets up
//! `tracing` to stderr (stdout is reserved for the ACP JSON-RPC protocol — see
//! the `print_stdout` clippy lint), and hands control to `zcode_acp::run()`.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use zcode_acp::CliOptions;

/// An ACP adapter that bridges Buzz relay events to the ZCode CLI via its
/// stdio `app-server`. Mirrors `zed-industries/codex-acp` for Codex.
#[derive(Parser, Debug)]
#[command(name = "zcode-acp", version, about, long_about = None)]
struct Cli {
    /// Path to the ZCode CLI bundle (the `zcode.cjs` file shipped inside the
    /// ZCode desktop app, or a copy of it).
    #[arg(long, env = "ZCODE_ACP_ZCODE_PATH")]
    zcode_path: Option<PathBuf>,

    /// Executable used to run `zcode_path`. Defaults to `node` from PATH.
    #[arg(long, env = "ZCODE_ACP_NODE", default_value = "node")]
    node: String,

    /// Extra args appended to every ZCode invocation (e.g. `--no-color`).
    /// Comma-separated.
    #[arg(long, env = "ZCODE_ACP_ZCODE_ARGS", value_delimiter = ',')]
    zcode_args: Vec<String>,

    /// Working directory used when spawning app-servers, when a session does
    /// not supply one. A session's `cwd` (from ACP `session/new`) always wins.
    /// Defaults to the current directory of `zcode-acp`.
    #[arg(long, env = "ZCODE_ACP_FALLBACK_CWD")]
    fallback_cwd: Option<PathBuf>,

    #[command(flatten)]
    options: CliOptions,
}

fn main() -> ExitCode {
    // Logs go to stderr only. stdout is the ACP protocol channel.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_ansi(false)
        .init();

    let cli = Cli::parse();

    let zcode_path = match cli.zcode_path.clone().or_else(default_zcode_path) {
        Some(p) => p,
        None => {
            tracing::error!(
                "Could not locate zcode.cjs. Pass --zcode-path or set ZCODE_ACP_ZCODE_PATH."
            );
            return ExitCode::from(1);
        }
    };
    // Resolve to an absolute path now: app-server subprocesses are spawned with
    // the session's cwd, so a relative --zcode-path would break once we chdir.
    let zcode_path = match zcode_path.canonicalize() {
        Ok(abs) => abs,
        Err(_) => zcode_path,
    };

    let fallback_cwd = cli
        .fallback_cwd
        .clone()
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));

    if !cli.zcode_args.is_empty() {
        tracing::debug!(args = ?cli.zcode_args, "extra args will be appended to every app-server spawn");
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!("Failed to start tokio runtime: {e}");
            return ExitCode::from(1);
        }
    };

    let result = runtime.block_on(zcode_acp::run(zcode_acp::RunConfig {
        zcode_path,
        node: cli.node,
        extra_args: cli.zcode_args,
        fallback_cwd,
        options: cli.options,
    }));

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("zcode-acp exited with error: {e:?}");
            ExitCode::from(1)
        }
    }
}

/// Best-effort default location of `zcode.cjs` inside the macOS ZCode.app bundle.
fn default_zcode_path() -> Option<PathBuf> {
    let candidates = [
        "/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs",
        "/Applications/ZCode.app/Contents/Resources/zcode.cjs",
    ];
    for c in candidates {
        let p = PathBuf::from(c);
        if p.exists() {
            return Some(p);
        }
    }
    None
}
