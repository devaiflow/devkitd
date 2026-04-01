# devkitd

A privileged daemon that executes scripts on a host server on behalf of external orchestrators and coding agents.

## Context

DevAIFlow runs AI coding agents in isolated containers with no host access. When a task requires interacting with Docker or the host itself, the orchestrator sends a command to `devkitd` — the only component with host-level permissions.

## Architecture

- **Broker** — TCP listener (default `127.0.0.1:9000`) that receives JSON commands from the orchestrator.
- **Handler** — Validates commands against loaded plugin configs and sends responses back via TCP callback.
- **Plugins** — JSON configs in `./scripts/` that define which scripts are allowed to run, their required arguments, and their executable path.

## Message Format

```json
{
  "message_id": "uuid",
  "reply_to": "host:port",
  "command": "script_name",
  "args": { "key": "value" }
}
```

## Plugin Config

Place `.json` files in `./scripts/`:

```json
{
  "name": "build_container",
  "executable": "/opt/scripts/build.sh",
  "description": "Builds a Docker container for a feature branch",
  "args_mapping": {
    "branch": { "flag": "--branch", "required": true }
  }
}
```

## Running

```bash
cargo run
```
