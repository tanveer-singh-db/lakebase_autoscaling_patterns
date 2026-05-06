# `lakebase-provision` — config-driven data-layer provisioning

A Python module + CLI that takes a YAML config and reconciles three
things Databricks Asset Bundles can't manage:

- **Postgres roles** — for Databricks users / service principals
  (via `databricks_create_role`).
- **GRANTs** — schema `USAGE`, table `SELECT`/`INSERT`/`UPDATE`/`DELETE`,
  with optional drift `REVOKE`.
- **Data API exposed schemas** + schema-cache refresh.

Source: `src/lakebase_utils/provisioning/`.

## Why this exists (and what it deliberately doesn't do)

Databricks Asset Bundles natively support the **platform layer** of
Lakebase — the bundle schema (`databricks bundle schema`) defines:

| Resource           | Bundle key                                   |
|--------------------|----------------------------------------------|
| Project            | `resources.postgres_projects`                |
| Branch             | `resources.postgres_branches`                |
| Endpoint (compute) | `resources.postgres_endpoints`               |
| Synced table       | `resources.synced_database_tables`           |
| Database instance  | `resources.database_instances`               |
| Database catalog   | `resources.database_catalogs`                |
| Project ACLs       | `permissions:` on any of the above           |

What bundles **cannot** do is run SQL against the Postgres database.
Roles, GRANTs, and Data API exposed-schemas live inside the database
itself and must be applied with `psycopg2` or via an SQL editor.
That's the gap this module fills, and it's all this module fills.

> If you're asking yourself "should I provision the project here?" —
> the answer is no. Put projects, branches, endpoints, and synced
> tables in `databricks.yml`. Run this module after.

## Quick start

```bash
# 1. write the YAML (see fixtures/provisioning_full.yaml for a worked example)
# 2. validate it (no network)
lakebase-provision validate -f provisioning.yaml

# 3. preview what would change
lakebase-provision plan -f provisioning.yaml

# 4. apply
lakebase-provision apply -f provisioning.yaml --auto-approve
```

`plan` and `apply` need the same auth as the rest of `lakebase_utils` —
SDK ambient (notebook), `DATABRICKS_CONFIG_PROFILE` + `databricks auth login`,
or `DATABRICKS_HOST` / `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`.

## Config shape

```yaml
target:
  project: cust360                         # Lakebase project id
  branch: production
  endpoint: primary
  database: databricks_postgres
  host: ep-xxxx.database.eastus2.azuredatabricks.net
  workspace_host: https://<ws>.azuredatabricks.net
  auth_mode: user_oauth                    # user_oauth | sp_oauth | oauth_token
  oauth_user: data-eng@example.com         # required for user_oauth
  profile: ${env:DATABRICKS_CONFIG_PROFILE:-DEFAULT}

roles:
  - identity: api-user@example.com
    type: USER
  - identity: 00000000-0000-0000-0000-000000000001
    type: SERVICE_PRINCIPAL

grants:
  - to: api-user@example.com
    usage_on_schemas: [public, analytics]
    select_on_tables:
      - public.widgets
      - analytics.*                        # '*' expands at plan time
    extra_privileges_on_tables: [INSERT]   # added alongside SELECT
    revoke_existing: false                 # opt-in drift REVOKE

exposed_schemas: [public, analytics]
refresh_schema_cache: true
```

`${env:VAR}` and `${env:VAR:-default}` are substituted at load time.
The full JSON Schema lives at
`src/lakebase_utils/provisioning/schemas/lakebase_provisioning_spec.json`.

## How reconciliation works

For every section, the corresponding reconciler:

1. Reads current state from Postgres or the SDK.
2. Diffs against config.
3. Returns a list of `Action` records (`create_role`, `grant`, `revoke`,
   `set_exposed_schemas`, `refresh_cache`, `manual`).
4. `--dry-run` / `plan` prints the actions and exits without writing.

Re-running `apply` after a clean apply produces an empty plan — the
module reads `pg_roles` / `pg_auth_members` /
`information_schema.role_table_grants` to know what's already there.

The `revoke_existing: true` knob is opt-in per grant block: when set,
table privileges present in the database but missing from the config
are surfaced as `REVOKE` actions, scoped to the schemas that block
already manages. Default is off so accidentally narrowing a role
during a refactor doesn't silently strip prod grants.

## Two-layer auth — what this module assumes

The module needs **both** auth layers wired up before it can do its
job:

1. **Workspace / project layer** — the identity running the CLI must
   have `CAN_MANAGE` on the Lakebase project. Granted via the bundle
   `permissions:` block on `postgres_projects`.
2. **Database layer** — the identity must be the project owner *for
   provisioning calls*. The owner has DDL rights even though they
   can't themselves serve Data API traffic (that gotcha is in
   [`fix_data_api_auth.md`](fix_data_api_auth.md)).

So a typical pipeline is: a CI service principal owns the project (and
runs the CLI); separate SPs / users get provisioned by the CLI as
non-owner Data API identities.

## DAB integration — single deploy for both layers

`databricks.yml`:

```yaml
bundle:
  name: cust360-lakebase

resources:
  postgres_projects:
    cust360:
      project_id: cust360
      display_name: Customer 360 Serving
      pg_version: 17
      permissions:
        - level: CAN_MANAGE
          user_name: data-eng@example.com

  postgres_endpoints:
    primary:
      endpoint_id: primary
      parent: ${resources.postgres_projects.cust360.name}/branches/production
      endpoint_type: ENDPOINT_TYPE_READ_WRITE
      autoscaling_limit_min_cu: 0.25
      autoscaling_limit_max_cu: 4

  synced_database_tables:
    orders_synced:
      name: main.sales.orders_synced
      database_instance_name: cust360
      logical_database_name: databricks_postgres
      spec:
        # ... full spec per Databricks docs

  jobs:
    provision_data_layer:
      name: Lakebase data layer
      tasks:
        - task_key: provision
          python_wheel_task:
            package_name: lakebase_utils
            entry_point: lakebase-provision
            parameters: ["apply", "-f", "${workspace.file_path}/provisioning.yaml", "--auto-approve"]
          libraries:
            - whl: ./dist/lakebase_utils-*.whl
```

`databricks bundle deploy` deploys all resources, then `databricks bundle run provision_data_layer` runs the Python task that applies the data layer. Wire the two together in your CI script and you have a single command for the full stack.

## What NOT to put in this YAML

| Thing                                       | Where it goes              |
|---------------------------------------------|----------------------------|
| Project create / pg_version / budget policy | `postgres_projects`        |
| Branch create / source_branch / TTL         | `postgres_branches`        |
| Compute autoscaling limits                  | `postgres_endpoints`       |
| Synced table spec                           | `synced_database_tables`   |
| `CAN_USE` / `CAN_MANAGE` on project         | `permissions:` on bundle resource |
| Database catalog (UC ↔ Lakebase mapping)    | `database_catalogs`        |

If a future bundle release ships a first-class resource for roles or
grants, this module gets retired — for now, this is the stable seam.

## Exit codes

| Code | Meaning                                    |
|------|--------------------------------------------|
| 0    | Success (or empty plan with nothing to do) |
| 1    | Partial apply (one or more reconcilers failed mid-run) |
| 2    | Invalid config / file not found            |

## See also

- [`quick_reference.md`](quick_reference.md) — task-oriented FAQ.
- [`fix_data_api_auth.md`](fix_data_api_auth.md) — `PGRST301` / `42501`
  diagnostic flow; closely related to what this module sets up.
- `../src/provision_data_api_role.sql` — the SQL template the role
  reconciler emits, with comments explaining the gotchas.
