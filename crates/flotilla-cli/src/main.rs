//! `flotilla`: CLI for the fleet. Talks to the local daemon on loopback for
//! replicated state, and directly to peers for exec and job logs.

mod client;
mod commands;
mod install;

use anyhow::Result;
use clap::{Parser, Subcommand};
use flotilla_core::api::DEFAULT_PORT;

#[derive(Parser, Debug)]
#[command(
    name = "flotilla",
    version,
    about = "Coordinate a fleet of machines over Tailscale"
)]
struct Cli {
    /// Base URL of the local daemon (default: loopback on the port in ~/.config/flotilla/config.toml, else 7400)
    #[arg(long, global = true, env = "FLOTILLA_DAEMON")]
    daemon: Option<String>,
    /// Emit JSON instead of tables
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Show every node the fleet knows about
    Status,
    /// Show this node's identity as seen by the daemon
    Whoami,
    /// List Tailscale peers as seen by the local daemon
    Peers,
    /// Run a command on selected nodes and stream the output
    Run(commands::RunArgs),
    /// Durable jobs: submitted into the replicated store, run by whichever eligible node claims them
    #[command(subcommand)]
    Job(commands::JobCmd),
    /// Desired state per node
    #[command(subcommand)]
    Desired(commands::DesiredCmd),
    /// Raw access to the replicated record store
    #[command(subcommand)]
    Records(commands::RecordsCmd),
    /// Install flotillad as a user service (launchd on macOS, systemd on Linux)
    Install(install::InstallArgs),
    /// Remove the user service
    Uninstall,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let daemon = cli
        .daemon
        .clone()
        .unwrap_or_else(|| format!("http://127.0.0.1:{}", configured_port()));
    let client = client::Client::new(&daemon);
    match cli.cmd {
        Cmd::Status => commands::status(&client, cli.json).await,
        Cmd::Whoami => commands::whoami(&client, cli.json).await,
        Cmd::Peers => commands::peers(&client, cli.json).await,
        Cmd::Run(args) => commands::run(&client, args).await,
        Cmd::Job(cmd) => commands::job(&client, cmd, cli.json).await,
        Cmd::Desired(cmd) => commands::desired(&client, cmd, cli.json).await,
        Cmd::Records(cmd) => commands::records(&client, cmd).await,
        Cmd::Install(args) => install::install(args),
        Cmd::Uninstall => install::uninstall(),
    }
}

/// Port from the daemon config file, so the CLI follows a non-default port
/// without flags. Only the `port` key is read.
fn configured_port() -> u16 {
    let path = std::env::var_os("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| directories::BaseDirs::new().map(|b| b.home_dir().join(".config")))
        .map(|d| d.join("flotilla").join("config.toml"));
    path.and_then(|p| std::fs::read_to_string(p).ok())
        .and_then(|t| t.parse::<toml::Table>().ok())
        .and_then(|t| t.get("port").and_then(|v| v.as_integer()))
        .and_then(|p| u16::try_from(p).ok())
        .unwrap_or(DEFAULT_PORT)
}
