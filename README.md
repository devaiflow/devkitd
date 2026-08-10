# devkitd

A privileged daemon that executes scripts on a host server on behalf of external orchestrators and coding agents, exposed as an MCP server.

## Context

DevAIFlow runs AI coding agents in isolated containers with no host access. When a task requires interacting with Docker or the host itself, the orchestrator (or an MCP-capable agent) talks to `devkitd` — the only component with host-level permissions.

## Architecture

- **MCP server** (`rmcp`, Streamable HTTP transport, stateless) mounted at `/mcp` (default `127.0.0.1:9000`).
- **Plugins** — JSON configs in `./plugins/` that define which scripts (in `./scripts/`) are allowed to run, their required arguments, and an optional per-plugin timeout.
- **Jobs** — every plugin call is fire-and-track, not blocking: `tools/call` returns a `job_id` immediately, and the script keeps running in the background under a `JobRegistry`. Poll it with `job-status`, stop it with `job-cancel`.

This async-job model exists because a blocking `tools/call` in a stateless MCP session has no reliable way to be cancelled from the outside (no shared session to carry `notifications/cancelled`, and a dropped connection isn't observable from inside the handler). See `PLANREF.md` for the full history of that decision.

## Tools

- **`<plugin-name>`** (one tool per file in `./plugins/`, args from `args_mapping`) — starts the script, returns `{"job_id": "..."}` immediately.
- **`job-status`** (arg `job_id`) — current state: `received` / `running` / `cancelled`, or `done` with `stdout`, `stderr`, `exit_code`.
- **`job-cancel`** (arg `job_id`) — cancels a pending or running job; kills the whole process group, not just the top-level script.

## Plugin Config

Place `.json` files in `./plugins/`, with the referenced script in `./scripts/`:

```json
{
  "name": "containers-up",
  "executable": "containers-start.sh",
  "description": "Starts the project containers",
  "args_mapping": {
    "project": { "flag": "-p", "required": true },
    "feature": { "flag": "-f", "required": true }
  },
  "timeout_seconds": 300
}
```

Plugins are live-reloaded on every `tools/list` call — no restart needed to pick up a new or edited plugin.

## Config (`devkitd.toml`)

```toml
[server]
bind_address = "127.0.0.1:9000"
allowed_hosts = ["localhost", "127.0.0.1", "::1"]   # default; see "Reaching devkitd from Docker" below

[paths]
plugins_dir = "./plugins"
scripts_dir = "./scripts"

[execution]
timeout_seconds = 600       # default per-call timeout; a per-call `_timeout_seconds` or per-plugin `timeout_seconds` overrides it (authoritatively — can extend)
max_parallel = 5
kill_grace_seconds = 5      # SIGTERM -> grace period -> SIGKILL, on timeout or cancel
job_retention_seconds = 86400   # how long a finished job stays queryable via `job-status` (default 24h)
```

Loaded from (first match wins): the OS config dir (`~/.config/devkitd/config.toml` on Linux), then `./devkitd.toml`, then built-in defaults.

## Auth

Bearer token via the `DEVKITD_MCP_TOKEN` environment variable — **never** put it in `devkitd.toml`. If `bind_address` is not loopback and no token is set, devkitd refuses to start (it executes privileged scripts, so it can't be left reachable without a credential once it's off `127.0.0.1`/`::1`/`localhost`).

```bash
export DEVKITD_MCP_TOKEN=your-secret
```

Requests then need `Authorization: Bearer your-secret`.

## Reaching devkitd from Docker

rmcp (the MCP library devkitd is built on) validates the inbound `Host` header
to prevent DNS-rebinding attacks against locally running servers — by default
it only accepts `localhost` / `127.0.0.1` / `::1`, so a request whose `Host`
is `host.docker.internal:9000` (the usual way a container reaches the host)
gets rejected before it hits any devkitd logic.

Fix it on the server, not by fighting the client's HTTP library over the
`Host` header (some HTTP clients — anything built on `fetch`/undici — refuse
to let you set `Host` manually at all, since it's a forbidden header name per
the WHATWG fetch spec). Add the extra hostname to `allowed_hosts`:

```toml
[server]
bind_address = "0.0.0.0:9000"
allowed_hosts = ["localhost", "127.0.0.1", "::1", "host.docker.internal"]
```

Once `bind_address` isn't loopback, the [Auth](#auth) guard requires
`DEVKITD_MCP_TOKEN` to be set — devkitd refuses to start otherwise.

## Running

```bash
cargo run
```
