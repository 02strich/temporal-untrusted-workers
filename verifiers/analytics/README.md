# analytics-verifier

The tool-call policy of the [dogwood-analytics](../../demos/dogwood-analytics) demo, written as plain
Go instead of [Dogwood](../dogwood) policies. It is a temporal-proxy tool-call verifier
(`TEMPORAL_PROXY_TOOL_VERIFIER=nexus`): a Nexus service with the `VerifyToolCalls` and
`RecordInputEvents` operations, hosted by a Temporal worker.

It decides the calls of an analytics agent with two tools, activities on the `analytics` task queue:
- `analytics/RunQuery({query})` may read the `metrics` and `customers` tables, and never write
  (`queries-are-read-only`). A query the verifier can't analyze is denied.
- `analytics/PublishResults(...)` is allowed once the run has queried `metrics` (`publish-metrics`),
  but never once it has read `customers` (`no-customer-data-in-published-results`).
- Every other tool is denied.

## Dogwood policy and Go code

| Dogwood ([`analytics.dw`](../../demos/dogwood-analytics/policies/analytics.dw)) | Go |
|---|---|
| `@sql("arg0.query")` on the action, and the verifier's SQL analysis | [`sql.go`](sql.go): `Analyze` with the [TiDB parser](https://github.com/pingcap/tidb/tree/master/pkg/parser) |
| `queries-are-read-only`: `context.sql.writes.isEmpty() && ["metrics", "customers"].containsAll(context.sql.reads)` | `decideQuery` in [`policy.go`](policy.go) |
| the `read_table` history events of the run, in a durable store | `runTables`: per run, the tables its allowed queries read, in memory |
| `publish-metrics` / `no-customer-data-in-published-results`: `formerly … read_table{ table: … }` | `decidePublish` |
| Dogwood's default deny | the `default` case, and every `verdict{}` without `allowed` |

What the Go version gives up:
- **Durable history.** What each run read lives in this process, bounded to the
  `VERIFIER_MAX_RUNS` (default 10000) runs seen most recently. After a restart, or for a run it has
  forgotten, `PublishResults` is denied until the run queries `metrics` again. That fails closed, but
  a long-running workflow may need to re-run its query.
- **Policy edits without code.** Changing a rule means changing, testing and redeploying Go.

What it gains: no policy engine or schema to learn, and the rules are ordinary code with ordinary tests
([`policy_test.go`](policy_test.go) runs the demo's cases).

## SQL analysis

`Analyze` mirrors the Dogwood verifier's analysis:
- Only `SELECT` (including `UNION` and friends), `INSERT`, `UPDATE` and `DELETE` are understood.
  Anything else (DDL, `TRUNCATE`, `SELECT … INTO`, SQL that doesn't parse) is an error, and the query
  is denied.
- `SELECT` reads every table it names, in joins and subqueries too. Names of common table
  expressions aren't tables.
- `INSERT`, `UPDATE` and `DELETE` write their target (every table of a multi-table `UPDATE` or
  `DELETE`), and read the other tables they name.
- Several statements in one query are combined.

The parser speaks MySQL with `ANSI_QUOTES`, so `"x"` is an identifier. It doesn't record how an
identifier was quoted, so names are compared case-insensitively (`"Customers"` counts as `customers`),
and a qualified name stays qualified (`public.customers` is not `customers`).

## Running

```bash
temporal operator nexus endpoint create --name tool-verifier \
  --target-namespace default --target-task-queue tool-verifier
go run .   # also TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY, VERIFIER_TASK_QUEUE, VERIFIER_SERVICE, VERIFIER_MAX_RUNS
```

Every evaluation is logged to stderr as an `analytics_request: {json}` line (the tool, its context and
the run's remembered tables, the decision and the rule), which the demo shows under each case. Run the
demo with `VERIFIER=go ./up.sh` (see its README). Tests: `go test ./...` or `make test-analytics-verifier`.

This is its own Go module, so the SQL parser stays out of temporal-proxy's dependencies. It imports
the generated contract types from [`gen/toolpolicy/v1`](../../gen/toolpolicy/v1).
