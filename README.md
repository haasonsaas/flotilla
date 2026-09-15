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
`haasonsaas.dev/cap/flotilla` application grant (rename it with `grant_cap`):

```jsonc
"grants": [
  {
    "src": ["group:ops"],
    "dst": ["tag:fleet"],
    "ip":  ["tcp:7400"],
    "app": { "haasonsaas.dev/cap/flotilla": [{ "roles": ["read", "exec"] }] }
  }
]
```

Roles: `read` (status, records, events, logs, file downloads), `write`
(records, so jobs and desired state), `exec` (run processes, upload files),
`sync` (the peer protocol), `admin` (all). Allow-listed users and tags hold
every role.

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
```

On macOS, jobs run under `caffeinate -i` so a laptop on power does not doze
mid-build (`caffeinate_jobs = false` to disable), and facts carry `xcode=<ver>`
and `tmux=yes` labels when those tools are present, so `-l xcode=16.2` works
as a selector.

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

## How jobs get scheduled without a leader

1. The submitter writes `job/<id>` with a command and either a node pin or a label selector.
2. Every node's scheduler loop sees the job. Eligible nodes with spare capacity write `claim/<id>` naming themselves.
3. Each claimant waits one settle window (two sync intervals) and re-reads the claim. Last-writer-wins has picked exactly one record by then; everyone else stands down.
4. The winner runs the job, streams the log to a local file, and writes `result/<id>` with the exit code and the last 4 KiB of output.
5. `job show` reads the result from any node. `job logs` fetches the full log from the executor.

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
