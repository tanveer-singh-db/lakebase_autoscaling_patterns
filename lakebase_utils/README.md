# lakebase_utils

Python clients, provisioning SQL, and docs for **Databricks Lakebase
Autoscaling** (managed Postgres). Exercises the same patterns whether
you connect over the Data API (REST / PostgREST) or directly to Postgres
(psycopg2).

## What you get

| Module                                                | Class                        | Transport             |
|-------------------------------------------------------|------------------------------|-----------------------|
| `src/lakebase_utils/lakebase_api.py`                  | `LakebaseDataApiClient`      | HTTPS (sync, `requests`) |
| `src/lakebase_utils/lakebase_api_async.py`            | `AsyncLakebaseDataApiClient` | HTTPS (asyncio, `aiohttp`) — with token-bucket rate limiting |
| `src/lakebase_utils/lakebase_connect.py`              | `LakebaseAutoscalingClient`  | Postgres wire (psycopg2 + pool) |
| `src/lakebase_utils/provisioning/`                    | `lakebase-provision` CLI     | Config-driven roles/grants/exposed-schemas (data-layer gap-filler for Asset Bundles) |

All three clients:

- Accept the **same auth modes** — `oauth_token`, `user_oauth`,
  `sp_oauth`, or SDK-ambient (auto).
- Are safe in notebooks (ambient `WorkspaceClient()` auth) and outside
  Databricks (CLI profile or SP env vars).
- Target **DBR 16.4 LTS / Serverless** with no extra installs, plus
  Python 3.11 locally.

## Repo layout

```
.
├── CLAUDE.md                        project context for Claude Code
├── databricks.yml                   bundle config
├── pyproject.toml                   pytest config
├── src/
│   ├── lakebase_utils/              the package itself
│   │   ├── _common.py               shared auth / URL resolution
│   │   ├── lakebase_api.py          sync Data API client
│   │   ├── lakebase_api_async.py    async Data API client (+ rate limiter)
│   │   ├── lakebase_connect.py      direct-Postgres client
│   │   └── provisioning/            YAML-driven roles/grants/exposed-schemas (CLI: lakebase-provision)
│   ├── create_widgets.sql           sample table DDL + grants
│   ├── provision_data_api_role.sql  provision user/SP as Postgres role
│   ├── test_*.py                    runnable demo scripts (not pytest)
│   └── test_user_authentication_flow.ipynb
├── tests/
│   ├── unit/                        93 pytest tests, no network (~5s)
│   └── integration/                 5 tests, gated on LAKEBASE_API_URL / _LIVE
└── docs/                            ← see below
```

## Docs — pick your starting point

| I want to…                                                   | Read                                                                     |
|--------------------------------------------------------------|--------------------------------------------------------------------------|
| Look something up fast (FAQ, task-oriented index)            | [`docs/quick_reference.md`](docs/quick_reference.md)                     |
| Understand the two-layer auth model & full enterprise flow   | [`docs/data_access_patterns.md`](docs/data_access_patterns.md)           |
| Use the REST client (sync or async)                          | [`docs/lakebase_api.md`](docs/lakebase_api.md)                           |
| Use the direct-Postgres client                               | [`docs/lakebase_connect.md`](docs/lakebase_connect.md)                   |
| Debug `PGRST301` / `42501` from the Data API                 | [`docs/fix_data_api_auth.md`](docs/fix_data_api_auth.md)                 |
| Provision roles / grants / exposed-schemas from YAML         | [`docs/lakebase_provisioning.md`](docs/lakebase_provisioning.md)         |

## The two gotchas everyone hits

1. **Never use the project owner as the Data API identity.** Owners
   inherit `databricks_superuser`; `authenticator` can't assume elevated
   roles, so OAuth as the owner fails. Use a non-owner user or SP.
2. **Provision Data API roles via SQL, not the UI.** The UI's *Add Role
   → OAuth* flow doesn't grant the project owner `ADMIN OPTION`, so the
   follow-up `GRANT "<role>" TO authenticator` fails with
   `42501 permission denied to grant role`. Use
   `databricks_create_role('<identity>', 'USER' | 'SERVICE_PRINCIPAL')`
   from [`src/provision_data_api_role.sql`](src/provision_data_api_role.sql).

More in [`docs/fix_data_api_auth.md`](docs/fix_data_api_auth.md).

## Getting started

Install deps locally (uv or pip — choose one):

```bash
# with uv (matches pyproject.toml)
uv sync --dev

# or with pip
pip install psycopg2-binary databricks-sdk requests aiohttp \
            pytest pytest-asyncio aioresponses pyyaml
```

Quick smoke test (Data API, REST):

```python
from lakebase_utils.lakebase_api import LakebaseDataApiClient

with LakebaseDataApiClient() as c:   # reads LAKEBASE_API_URL, ambient auth
    print(c.fetch_all("public", "widgets"))
```

Quick smoke test (direct Postgres):

```python
from lakebase_utils.lakebase_connect import LakebaseAutoscalingClient

client = LakebaseAutoscalingClient(
    host="ep-xxxx.database.<region>.azuredatabricks.net",
    database="databricks_postgres",
    auth_mode="user_oauth",
    oauth_user="me@example.com",
    endpoint_path="projects/<proj>/branches/<branch>/endpoints/<endpoint>",
)
print(client.fetch("SELECT current_user, now()"))
```

## Tests

```bash
# Unit tests — no network, ~5s
.venv/bin/pytest tests/unit/ -v

# Integration tests — needs LAKEBASE_API_URL + SDK auth
LAKEBASE_API_URL=... .venv/bin/pytest tests/integration/ -v
```

Or copy `tests/test_config.yaml.template` → `tests/test_config.yaml`
(gitignored) and run without env vars.

## Databricks Asset Bundle

`databricks.yml` is provided so this directory can also be deployed as a
bundle. See the upstream Databricks docs for bundle workflow —
[Asset Bundles](https://docs.databricks.com/dev-tools/bundles/),
[VS Code extension](https://docs.databricks.com/dev-tools/vscode-ext.html),
[CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html).
