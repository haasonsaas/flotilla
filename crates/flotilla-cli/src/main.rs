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
    /// Base URL of the local daemon
    #[arg(long, global = true, env = "FLOTILLA_DAEMON", default_value_t = format!("http://127.0.0.1:{DEFAULT_PORT}"))]
    daemon: String,
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
    let client = client::Client::new(&cli.daemon);
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
