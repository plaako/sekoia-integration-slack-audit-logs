# Slack Audit Logs — Sekoia integration

Sekoia automation module collecting audit events from the
[Slack Audit Logs API](https://docs.slack.dev/admins/audit-logs-api/) and forwarding them to a
Sekoia intake.

> **Architecture and design rationale:** [`doc/architecture.html`](doc/architecture.html) — why the
> collection walks time forward in windows, what the two state files do, and a walkthrough of every
> source file. Open it in a browser.

## Requirements

- Slack Enterprise Grid — the Audit Logs API does not exist on other plans
- A Slack **user** token (`xoxp-…`) with the `auditlogs:read` scope, from an app installed on the
  **organization** (not on a workspace)

## Module settings

| Setting | Default | Description |
| --- | --- | --- |
| `token` | — | Slack user token (`xoxp-…`), secret |
| `base_url` | `https://api.slack.com/audit/v1` | Base URL of the Slack Audit Logs API |

## Connector settings

| Setting | Default | Range | Description |
| --- | --- | --- | --- |
| `intake_server` | — | — | Leave empty to use the platform intake URL |
| `intake_key` | — | required | Sekoia intake receiving the events |
| `frequency` | 60 | 10–3600 | Minimum seconds between collection cycles |
| `limit` | 1000 | 1–9999 | Events per API page. The right knob for catching up on a backlog |
| `ratelimit_per_minute` | 30 | 1–50 | Requests per minute. Slack's quota is **organization-wide**, shared with every other Slack app |
| `timebuffer` | 60 | 1–3600 | Events younger than this are left for the next cycle, covering Slack's indexing lag |
| `lookback_seconds` | 3600 | ≥ 60 | How far back the first run starts. **Only applies when no state exists** |

A credentials test is available in the console: it runs the account validator, which makes one cheap
authenticated call and distinguishes a rejected token from a plan restriction from an API outage.

## What it guarantees

Events reach the intake **exhaustively and without duplicates**, across pagination truncation,
container restarts, rejected cursors, and outages of any length. Slack returns `/audit/v1/logs`
newest-first with no sort parameter, so collection walks time **forward** in one-hour windows and the
watermark only advances over a window that was fully drained *and* confirmed pushed.

Two state files under the connector's data path carry that:

| File | Contents |
| --- | --- |
| `context.json` | the watermark — end of the last fully drained window (SDK `CheckpointTimestamp`) |
| `pending.json` | the window in flight — frozen bounds, Slack cursor, and the ids already forwarded |

The paths where an event can still be lost are enumerated in `doc/architecture.html`; each one is
either announced in a `critical` log or documented as unobservable.

## Repository layout

```text
.                                   # repo root == module root (Sekoia imports by repo URL)
├── main.py                         # generated: registers the connector and the account validator
├── manifest.json                   # generated from the pydantic models
├── connector_slack_audit_logs.json # generated from the pydantic models
├── Dockerfile                      # python:3.11-slim + uv sync --frozen
├── pyproject.toml / uv.lock        # uv project; the lock file is committed and copied into the image
├── logo.svg
├── doc/architecture.html           # design and code walkthrough
├── slack_audit_logs_modules/
│   ├── __init__.py                 # SlackAuditLogsModule
│   ├── models.py                   # module configuration (pydantic v2)
│   ├── errors.py                   # SlackAuditLogsError / AuthenticationError / PlanError
│   ├── client.py                   # requests.Session + LimiterAdapter, pagination, error mapping
│   ├── connector.py                # SlackAuditLogsConnector — iterate() and frequency
│   └── validator.py                # SlackAuditLogsAccountValidator
└── tests/                          # 82 tests, 98 % branch coverage
```

## Development

```bash
uv sync
uv run pytest                       # 82 tests, coverage gate at 80 %

# regenerate manifest.json, connector_*.json and main.py from the models
uv run sekoia-automation generate-files-from-code
```

`generate-files-from-code` rewrites `main.py` **without** the `register_account_validator` call —
check it is still there after any regeneration. `tests/test_main.py` guards this.

Built against `sekoia-automation-sdk` 1.24.0. The module implements only `iterate()` and
`frequency`; the SDK owns the run loop, chunking, the intake push, the frequency sleep, and the
metrics.

## Licence

MIT — see [LICENSE](LICENSE).
