# Flotilla design

Date: 2026-09-13. Status: approved in conversation, implementing.

## Goal

Coordinate Jonathan's Macs (plus dev-desktop, an always-on Linux box) as a
leaderless fleet over Tailscale, in Rust. Build a small set of primitives so
that status, fleet-wide exec, a job runner, desired-state convergence, and
capability discovery are all thin layers over the same core.

## Non-goals (v1)

- Consensus or exactly-once job execution. Jobs are at-least-once.
- Working off-tailnet. Tailscale is the network and the identity provider.
- Running as root. The daemon runs as the logged-in user on each node.

## Topology

Every node runs the same daemon, `flotillad`. There is no leader. Membership
comes from Tailscale's peer list. dev-desktop is an ordinary peer whose uptime
makes it the de facto anchor for replicated state.

## Primitives

### 1. Identity (`identity`)

- Node ID = Tailscale stable node ID. Node name = short DNS name.
- Peers = `tailscale status --json`, cached for 5s. Online flag and
  Tailscale IPs come from there.
- Inbound auth = `tailscale whois --json <src-ip>` on the connection's
  source address. Allowed if the login name is in `allowed_users` or any
  node tag is in `allowed_tags`. Loopback connections are trusted (same
  user on the same machine).
- The identity provider is a trait so tests use a static implementation.

### 2. Transport

HTTP/1.1 + JSON over the tailnet (axum server, reqwest client). WireGuard
already encrypts and authenticates the wire, so no TLS. Streaming responses
are newline-delimited JSON. The daemon binds loopback plus every Tailscale IP
on port 7400.

### 3. Replicated record store (`store`)

A last-writer-wins key/value map replicated by anti-entropy sync.

- `Record { key, value: JSON, author: NodeId, hlc: Hlc, deleted }`
- `Hlc` = 48-bit wall-clock ms + 16-bit counter. On merge the local clock
  observes the remote HLC. Ties break on author ID.
- Merge rule: incoming record replaces local iff `(hlc, author)` is greater.
- Each store maintains a version vector: max HLC seen per author.
- Sync (push-pull) between two nodes A -> B:
  1. A sends its version vector and no records.
  2. B replies with every record whose `(author, hlc)` exceeds A's vector,
     plus B's own vector.
  3. A merges, then sends B the records B lacks.
- Sync runs every 3s against a random online peer. Convergence follows from
  LWW being commutative, associative and idempotent.
- Deletes are tombstones. Garbage collection is future work.
- Persistence: redb, one file per node under the data dir.

### 4. Exec (`exec`)

`POST /v1/exec {cmd, cwd, env, timeout_secs}` runs a process as the daemon's
user and streams `{stream, data}` frames followed by `{exit}`. Used directly
by the CLI for fan-out and by the job runner.

## Layers

| Layer | Records | Loop |
|---|---|---|
| Status | `node/<id>/facts` written by that node every 15s | facts collector |
| Fleet exec | none | CLI fans out `/v1/exec` to selected peers |
| Jobs | `job/<id>`, `claim/<id>`, `result/<id>` | scheduler on every node |
| Desired state | `desired/<node>`, `reconcile/<node>` | reconciler on every node |
| Discovery | labels inside facts | selectors on jobs and exec |

### Job scheduling

1. Submitter writes `job/<id>` with command, selector labels, and settings.
2. Every node runs the scheduler every 3s: for each job with no claim and no
   result whose selector matches the node's facts, and while under
   `max_concurrent_jobs`, write `claim/<id> {node}`.
3. Wait one settle window (2 x sync interval), re-read `claim/<id>`. LWW
   resolves concurrent claims to a single winner. If the winner is not me,
   drop it.
4. Winner runs the job, streams logs to a local file, writes `result/<id>`
   with exit code and the last 4 KiB of output.
5. `job logs` fetches the full log from the executor via `/v1/jobs/<id>/log`.
6. Cancel = rewrite `job/<id>` with `cancelled: true`; the executor polls it.

### Desired state

`desired/<node>` holds `files: [{path, content, mode}]` and
`ensure: [{name, check, apply}]`. The reconciler runs every 60s, writes files
whose content differs, runs `apply` for every `check` that exits non-zero, and
records the outcome in `reconcile/<node>`.

## Crates

- `flotilla-core`: hlc, record, store, sync, facts/job/desired schemas,
  selector matching, API types. No I/O beyond redb.
- `flotillad`: daemon. identity, auth, HTTP server, facts/sync/scheduler/
  reconcile loops, exec runner.
- `flotilla-cli`: `flotilla` binary. Talks to the local daemon on loopback for
  state; talks to peers directly for exec and logs.

## Testing

- Unit: HLC monotonicity, LWW merge, version-vector deltas.
- Convergence: three in-memory stores, random writes, sync in random pair
  order, assert identical contents.
- Integration: two daemons on loopback with static identity; verify sync,
  exec streaming, job claim and result.
- Live: jonathan-air, macbook-air, dev-desktop.
