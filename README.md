# lakebase_autoscaling_patterns

Reference implementation and playbook for serving lakehouse data through
**Databricks Lakebase Autoscaling** (managed Postgres) in an enterprise
setup — project provisioning, table sync, compute/autoscaling, the
two-layer permission model, and connection patterns for the two surfaces
apps actually reach for: the **Data API** (REST) and **direct Postgres**
(JDBC / psycopg).

## What's here

- [`lakebase_utils/`](lakebase_utils/) — a Python package + docs + tests
  exercising every pattern end-to-end. Start with
  [`lakebase_utils/README.md`](lakebase_utils/README.md).

The package gives you three clients over the same identity model:

| Client                       | Transport              | Use when                                               |
|------------------------------|------------------------|--------------------------------------------------------|
| `LakebaseDataApiClient`      | HTTPS / PostgREST      | Browsers, mobile, serverless, or no outbound 5432.     |
| `AsyncLakebaseDataApiClient` | HTTPS / PostgREST      | Same as above, asyncio apps; has a client-side rate limiter. |
| `LakebaseAutoscalingClient`  | Postgres wire (psycopg2) | Complex SQL, transactions, existing JDBC code.        |

All three share one auth story (Databricks OAuth → Postgres role) and
one provisioning story (`databricks_create_role` SQL), so you can swap
surfaces without re-thinking permissions.

## Documentation map

Everything is under [`lakebase_utils/docs/`](lakebase_utils/docs/):

- [`quick_reference.md`](lakebase_utils/docs/quick_reference.md) —
  task-oriented lookup + FAQ. **Read this first.**
- [`data_access_patterns.md`](lakebase_utils/docs/data_access_patterns.md) —
  narrative playbook: two-layer auth model, project creation, UC→Lakebase
  sync, compute/autoscaling, network patterns (public internet vs
  PrivateLink), enterprise recommendations.
- [`lakebase_api.md`](lakebase_utils/docs/lakebase_api.md) — Data API
  client reference (sync + async).
- [`lakebase_connect.md`](lakebase_utils/docs/lakebase_connect.md) —
  direct-Postgres client reference.
- [`fix_data_api_auth.md`](lakebase_utils/docs/fix_data_api_auth.md) —
  diagnostic playbook for `PGRST301` / `42501` errors from the Data API.

## The mental model in one paragraph

Lakebase has **two independent permission layers**. The workspace layer
(`CAN_USE` / `CAN_MANAGE`) controls who can manage the project. The
database layer (Postgres roles + `GRANT`) controls who can read data.
Neither implies the other — a `CAN_MANAGE` user gets zero rows out of
`SELECT *` until a Postgres role exists with the right grants. Full
diagram and failure modes in
[`data_access_patterns.md`](lakebase_utils/docs/data_access_patterns.md#authentication-model-at-a-glance).

## License

See [`LICENSE`](LICENSE).
