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

## Install

```sh
cargo build --release
cp target/release/flotilla target/release/flotillad ~/.local/bin/   # or anywhere on PATH
flotilla install        # launchd user agent on macOS, systemd --user unit on Linux
flotilla status
```

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
```

All keys and defaults are in `crates/flotillad/src/config.rs`.

## Usage

```sh
flotilla status                              # every node, online or not
flotilla run -n mac-mini -n olympus -- df -h /
flotilla run -l os=macos --timeout 60 -- brew outdated
flotilla job submit --wait -l arch=x86_64 -- cargo test
flotilla job ls
flotilla job logs 3fa1                       # prefix match, fetched from the executor
flotilla job cancel 3fa1
flotilla desired set mac-mini -f desired.toml
flotilla desired status
flotilla records ls job/                     # raw store access
```

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

This is at-least-once: a node partitioned for longer than the settle window can
also run the job. That's the trade for having no coordinator.

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
