# flotilla

Coordinate a fleet of machines over Tailscale with no leader. Written in Rust.

Every node runs the same daemon, `flotillad`. Membership and identity come from
Tailscale. Fleet state lives in a small last-writer-wins record store that nodes
replicate by anti-entropy sync, so an always-on peer keeps state alive while
laptops are closed or away, but no node is special. Everything you can do with
the fleet is a thin layer over four primitives:

| Primitive | What it is |
|---|---|
| identity | Tailscale node IDs, peer list, and `whois` on every inbound connection |
| transport | HTTP/JSON over the tailnet, newline-delimited JSON for streams |
| store | replicated LWW map with hybrid logical clocks and version-vector sync |
| exec | run a process on a node and stream its output |

And the layers built on them:

| Layer | Command | How it works |
|---|---|---|
| status | `flotilla status` | each node publishes a facts record every 15s |
| fleet exec | `flotilla run --all -- uptime` | CLI fans out to peers directly and streams output |
| jobs | `flotilla job submit -l os=macos -- make test` | job/claim/result records; every node runs the same claim loop |
| desired state | `flotilla desired set mac-mini -f state.toml` | files and check/apply pairs, reconciled every 60s |
| discovery | `-l gpu=yes` | labels in facts, matched by selectors |
| files | `flotilla push ./bin mac-mini:~/.local/bin/bin --all` | streamed upload with atomic rename; `pull` the other way |
| upgrade | `flotilla upgrade --all` | installs the latest release on every node, or `--local` pushes this machine's build |
| events | `flotilla events job/` | server-sent stream of store changes; `status --watch`, `job ls --watch` |
| sessions | `flotilla session start -n olympus --name work --cwd ~/proj -- claude` | tmux sessions per node, listed in facts, `send`/`tail`/`attach` |
| wake | `flotilla wake mac-mini` | wake-on-LAN sent from every online node on that LAN |
| agents | `flotilla agent run -l os=linux --cwd ~/proj -- grok -p "fix the tests"` | a job that runs inside a tmux session on the least loaded node; `agent ls/attach/logs/stop` |
| dashboard | `open http://127.0.0.1:7400/` | live nodes and jobs page served by every daemon, refreshed from the event stream |
| batches | `flotilla batch submit -n 6 --then "cargo test --doc" -- cargo test --shard {i}/{n}` | shard a command across the fleet, chain a final job on all shards, `batch wait` |
| retries | `flotilla job submit --retries 2 -- ...` | failed (not cancelled) jobs are resubmitted with backoff; lost jobs are taken over |
| notify | `[notify] ntfy_url, topic` in config | push on failed / lost (or any outcome) from the node that ran the job |
| artifacts | `flotilla job pull 3fa1 ./out` | files a job wrote to `$FLOTILLA_ARTIFACTS`, fetched from the executor through the local daemon |
| wake-then-run | `flotilla job submit --wake -n mac-mini -- ...` | if no eligible node is online, wake the matching ones and wait |
| watchdog | `flotilla alerts` | nodes publish disk, load, build-process and tailscale login facts; `[alerts]` thresholds write `alert/` records and POST to ntfy or a webhook |
| snapshots | `flotilla records dump fleet.json` / `restore fleet.json` | every raw record out to a file and merged back in |

## Install

Prebuilt binaries for Apple Silicon and Intel Macs and for x86_64 and arm64
Linux are attached to every tagged release. This installs the latest one into
`~/.local/bin` and registers the daemon as a user service:

```sh
curl -fsSL https://raw.githubusercontent.com/haasonsaas/flotilla/main/scripts/install.sh | sh
```

Or build it yourself:

```sh
cargo build --release
cp target/release/flotilla target/release/flotillad ~/.local/bin/   # or anywhere on PATH
flotilla install        # launchd user agent on macOS, systemd --user unit on Linux
flotilla status
```

Or with Nix: `nix run github:haasonsaas/flotilla -- status`, or add the flake
as an input and put `flotilla.packages.${system}.default` in your profile.

Do that on every node. The daemon binds loopback and each Tailscale IP on port
7400. Nothing else needs to be opened; WireGuard is the wire encryption.

## Several tailnets, userspace tailscaled

A node can be on more than one tailnet, each served by its own tailscaled.
List them as `[[tailnet]]` tables; the first is the primary and its node id is
the node's fleet identity. With no `[[tailnet]]` table nothing changes.

```toml
name = "mac-mini"            # optional; default is the primary's node name

[[tailnet]]
name   = "evalops"
socket = "/var/run/tailscaled-evalops.sock"   # tailscale --socket=...
bin    = "/opt/homebrew/bin/tailscale"        # optional, per tailnet

[[tailnet]]
name          = "homelab"
allowed_users = ["me@example.com"]  # replaces the global lists for this tailnet
```

- Peers from every tailnet are merged. The same machine has a different node
  id on each tailnet; its facts list them all (`tailnets`), so it appears once.
- The daemon binds `:7400` on each tailnet's addresses. A connection is
  identified with `whois` on the tailnet it arrived on, so overlapping
  100.x addresses on two tailnets cannot be confused. `allowed_users` /
  `allowed_tags` under a `[[tailnet]]` replace the global lists for callers on
  that tailnet.
- A node on two tailnets carries the fleet between them by ordinary
  anti-entropy. Outbound requests use an address on a tailnet both nodes share.
- `flotilla status` gains a `tailnets` column, and `flotilla peers` a
  `tailnet` column, when any node is on more than one.

A tailscaled started with `--tun=userspace-networking` has no local
interfaces for its addresses, so flotillad cannot bind them or dial out
directly. Two more keys handle that:

```toml
[[tailnet]]
name         = "evalops"
socket       = "/var/run/tailscaled-evalops.sock"
proxy        = "socks5://127.0.0.1:1056"   # tailscaled --socks5-server, or
                                           # http://127.0.0.1:1057 (--outbound-http-proxy-listen)
proxy_listen = "127.0.0.1:7411"            # loopback, expects PROXY protocol
```

Inbound, forward the tailnet port with the caller's address preserved:

```sh
tailscale --socket=/var/run/tailscaled-evalops.sock serve --bg \
  --tcp=7400 --proxy-protocol=2 tcp://127.0.0.1:7411
```

The PROXY header carries the caller's real tailnet address, which is what
`whois` needs. A plain TCP forward would make every caller look like
loopback, which the daemon trusts, so `proxy_listen` refuses connections that
do not start with a PROXY header. If the tailnet policy admits only one port
(say 50051), set `port = 50051` and use it in `--tcp=` too. Outbound requests
to that tailnet's peers, including configured `seeds` in the Tailscale
address ranges, go through `proxy`. The daemon reads the tailnet list once at
startup: a secondary tailnet that is down is skipped after 30s, so restart
the daemon after logging one in.

## Watchdog

Every facts record carries health data: free space on `/` and on every other
mount of 50 GB or more (`disks`), load per core, the number of running
`cargo` and `rustc` processes (`build_procs`), and the login state of each
configured tailnet's tailscaled (`tailscale`: `Running`, `NeedsLogin`,
`Stopped`, or `unreachable` when the daemon does not answer).

`[alerts]` sets the thresholds. These are the defaults; `ntfy_url`, `topic`
and `token` fall back to `[notify]` when unset:

```toml
[alerts]
disk_free_pct_min    = 10      # any watched volume below 10% free; 0 disables
tailscale_logged_out = true    # any tailnet not Running
load_per_core_max    = 0       # off; e.g. 2.0 alerts above 2 load per core
for_secs             = 60      # condition must hold this long before it fires
ntfy_url             = "https://ntfy.sh"
topic                = "fleet-alerts"
webhook_url          = "https://example.com/hook"   # JSON: {"event","alert"}
notify_resolved      = false
renotify_hours       = 0       # 0 sends once per incident
```

A tripped threshold writes `alert/<node id>/<id>` with `firing = true` and
sends one notification. When the condition clears the record is rewritten
with `firing = false` (and a notice goes out if `notify_resolved`). The
record is written before the POST, so a node whose network is the problem
still records it and replicates the alert when it reconnects. `flotilla
alerts` lists firing alerts across the fleet, `--all` includes cleared ones.

## Auth

Inbound requests from the tailnet are resolved with `tailscale whois`. A caller
is allowed if its login name is in `allowed_users` or one of its node tags is in
`allowed_tags`. If neither list is configured the daemon allows only its own
login, which is the right default for one person's machines. Loopback is
trusted. Tagged nodes (like a CI box) need a tag entry:

```toml
# ~/.config/flotilla/config.toml
allowed_users = ["you@example.com"]
allowed_tags  = ["tag:fleet"]

[labels]
gpu = "yes"
role = "builder"

# Peers to bootstrap from when discovery alone isn't enough, e.g. a node whose
# Tailscale ACL only admits a non-default port. Known peers are always dialed
# on the port they advertise.
seeds = ["100.100.185.44:50051"]
```

Finer-grained access comes from your tailnet policy instead of per-node
config. A caller not on the allow-lists gets exactly the roles named in the
`haasonsaas.com/cap/flotilla` application grant (rename it with `grant_cap`):

```jsonc
"grants": [
  {
    "src": ["group:ops"],
    "dst": ["tag:fleet"],
    "ip":  ["tcp:7400"],
    "app": { "haasonsaas.com/cap/flotilla": [{ "roles": ["read", "exec"] }] }
  }
]
```

Roles: `read` (status, records, events, logs, file downloads), `write`
(records, so jobs and desired state), `exec` (run processes, upload files),
`sync` (the peer protocol), `admin` (all). Allow-listed users and tags hold
every role.

An `app`-only grant like the one above carries no `ip` rule, so it changes
nothing about reachability; it only shows up in `whois` as a capability. A
node that needs to sync with its peers must be granted `sync` as well as
`read`, or the anti-entropy rounds towards it fail with 403. Verified against
a live tailnet: with `["read", "sync"]` the daemon accepted sync and refused
`run` and `push` with `403 ... lacks the exec role`; adding `exec` made them
pass without restarting anything (the whois cache is 20s).

All keys and defaults are in `crates/flotillad/src/config.rs`.

## Usage

```sh
flotilla status                              # every node, online or not, with last sync per peer
flotilla sync                                # force a sync round with every candidate peer
flotilla run -n mac-mini -n olympus -- df -h /
flotilla run -l os=macos --timeout 60 -- brew outdated
flotilla job submit --wait -l arch=x86_64 -- cargo test
flotilla job ls
flotilla job logs 3fa1                       # prefix match, fetched from the executor
flotilla job cancel 3fa1
flotilla desired set mac-mini -f desired.toml
flotilla desired status
flotilla records ls job/                     # raw store access
flotilla push ./script.sh ~/bin/script.sh -l os=macos --mode 0755
flotilla pull dev-desktop-1 /var/log/syslog ./syslog
flotilla upgrade --all                       # latest GitHub release everywhere; --local to push this build
flotilla events job/                         # NDJSON stream of changes
flotilla status --watch
flotilla session ls
flotilla session start -n dev-desktop-1 --name codex --cwd ~/proj -- codex
flotilla session tail -n dev-desktop-1 codex --lines 40
flotilla session send -n dev-desktop-1 codex -- "run the tests"
flotilla session attach -n dev-desktop-1 codex   # ssh -t ... tmux attach
flotilla wake mac-mini
flotilla agent run --cwd ~/code/mono -- grok -p "resolve the conflict in PR 9138"
flotilla agent ls                              # every agent run, node, elapsed, last output line
flotilla agent attach 3fa1                     # ssh -t into its tmux session on the executor
flotilla job submit --pick least-load -- cargo test   # any job can ask for the idlest node
flotilla web                                   # open the dashboard; `flotilla web dev-desktop-1` opens that node's
```

Agent runs are ordinary jobs with a `tmux` session name and the `least-load`
placement hint. The executor starts the command in a detached tmux session
whose shell writes the job log and exit status to files and then signals a
tmux wait channel; the daemon waits on that channel, so cancelling the job
kills the session, `job logs` returns the full output, and you can attach to
the live session at any time.

On macOS, jobs run under `caffeinate -i` so a laptop on power does not doze
mid-build (`caffeinate_jobs = false` to disable), and facts carry `xcode=<ver>`
and `tmux=yes` labels when those tools are present, so `-l xcode=16.2` works
as a selector.

Nodes can advertise build caches they hold warm. In the node's config:

```toml
[[warm_cache]]
name = "mono-rust"
path = "/builds/mono/target"
key_cmd = "git -C /builds/mono rev-parse --short HEAD"   # optional
```

Every facts refresh (`facts_interval_secs`) publishes a label
`warm.mono-rust=<key>@<age>` (for example `3fa9c1e@12m`; age is time since the
newest change under the directory) and a structured `warm` field in the node's
facts with the path, size (recomputed every 5 minutes in the background) and
last-used time. A cache whose directory does not exist is not advertised.
`key_cmd` runs under `sh -c` with a 5s limit; its first output line is the key.

A desired-state file:

```toml
[[files]]
path = "~/.config/foo/config.toml"
content = "enabled = true\n"
mode = "0600"

[[ensure]]
name = "homebrew-jq"
check = ["brew", "list", "jq"]
apply = ["brew", "install", "jq"]
```

### Recipe: build node

`docs/recipes/build-node.toml` (Linux, systemd user timer) and
`build-node-macos.toml` (launchd agent) turn a machine into a build node:

```sh
flotilla desired set olympus -f docs/recipes/build-node.toml
flotilla desired status
```

- `~/.local/bin/flotilla-build-clean` deletes each `~/builds/<name>/` that has
  had no file written in 7 days, every 6 hours. `BUILD_ROOT` and
  `BUILD_MAX_AGE_DAYS` are set in the unit or plist. The script refuses `/`,
  `$HOME`, relative paths and a non-numeric age, and leaves loose files alone.
- `rust-toolchain` installs rustup (minimal profile) if `~/.cargo/bin/cargo`
  is missing.
- `sccache` is `cargo install`ed if absent, then `cargo-sccache-wrapper` adds
  `rustc-wrapper = "sccache"` and `SCCACHE_CACHE_SIZE = "50G"` to
  `~/.cargo/config.toml`. It does this only once the binary exists, and it
  fails visibly (instead of editing) when the file already has a `[build]`
  table.

Every entry is a file or a check/apply pair, so a converged node changes
nothing on later passes. The reconcile loop runs ensures one at a time, so a
first `cargo install sccache` holds up that node's next pass for a few
minutes.

## How jobs get scheduled without a leader

1. The submitter writes `job/<id>` with a command and either a node pin or a label selector.
2. Every node's scheduler loop sees the job. Eligible nodes with spare capacity write `claim/<id>` naming themselves.
3. Each claimant waits one settle window (two sync intervals) and re-reads the claim. Last-writer-wins has picked exactly one record by then; everyone else stands down.
4. The winner runs the job, streams the log to a local file, and writes `result/<id>` with the exit code and the last 4 KiB of output.
5. `job show` reads the result from any node. `job logs` fetches the full log from the executor.

Every job gets `FLOTILLA_ARTIFACTS` (a directory on the executor),
`FLOTILLA_JOB_ID` and `FLOTILLA_NODE` in its environment. Whatever it writes
under the artifacts directory is listed by `/v1/jobs/{id}/artifacts` and
downloaded by `flotilla job pull`, and is removed with the job after
`job_retention_hours`.

Dependencies: `--after <id>` makes a job eligible only once those jobs
succeeded; if one of them ends any other way, the dependent is cancelled with
a reason. `--retries N` clears a failed result after 10s, 20s, 40s ... and
lets the job be claimed again; a cancelled job is never retried. Batches are
just a label plus `{i}`/`{n}` substitution, and `--then` submits one more job
depending on every shard.

Job states, derived from the replicated records rather than stored:

| state | meaning |
|---|---|
| queued | submitted, no claim yet |
| claimed | a node wrote a claim and is settling |
| running | the executor recorded a start time on the claim |
| succeeded / failed | a result exists with exit 0 / non-zero (or a timeout) |
| cancelled | terminated on request, or cancelled before it ran |
| lost | the claim's lease lapsed with no result: the executor went away and the outcome is unknown until another node takes it over |

Every daemon's dashboard opens a drawer per job with the timeline (created,
claimed, started, exited), the log (fetched from the executor through the
local daemon), and the raw records with author and clock, so the replicated
state itself is inspectable.

Claims carry a lease (`job_lease_secs`, default 60). The executor renews it at a
third of that interval while the job runs. If a lease lapses with no result,
because the executor died or lost its network, any eligible node takes the job
over with a new claim (`attempt` increments, and `job ls` shows the gap as
`orphaned` until then). An executor that sees another node's claim on its job
kills the process and writes no result, so ownership converges even after a
partition heals.

This is at-least-once: a node partitioned for longer than the settle window can
also run the job, and a takeover can overlap with an executor that is alive but
unreachable. That's the trade for having no coordinator.

A daemon that is stopped (SIGTERM, `launchctl kickstart -k`, `systemctl
restart`) kills its running jobs' process groups and leaves their claims in
place, so it resumes them itself on restart or another node takes them over
once the lease lapses. No result is written for a job interrupted this way.

## Store hygiene

Deleted records become tombstones so the delete replicates. After
`gc_horizon_days` (default 30) a tombstone is collected down to a `(key, hlc)`
marker; a stale live copy of that key arriving later from a node that missed
the delete is refused instead of resurrecting it, while keys that were never
deleted are never affected, so a node that was away for months, or a new
node, still converges. Markers are forgotten after `gc_forget_days` (365).
Records stamped more than `max_clock_skew_secs` (3600) in the future are
refused, so one machine with a wrong clock cannot win every write. Finished
jobs are retired after `job_retention_hours` (72).

## Layout

- `crates/flotilla-core`: clock, record, store, sync, schemas, selectors, API types. No network.
- `crates/flotillad`: the daemon.
- `crates/flotilla-cli`: the `flotilla` binary.
- `docs/superpowers/specs/`: design spec.

## Development

```sh
cargo test --workspace      # unit tests plus in-process two-node integration tests
cargo clippy --workspace --all-targets
```

The integration tests start two daemons on loopback with a static identity
provider, so nothing in the test suite touches Tailscale.

## License

MIT
