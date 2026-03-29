# CLAUDE.md — DBIO::PostgreSQL::Async

## Overview

Async PostgreSQL storage for DBIO using EV::Pg. Bypasses DBI entirely —
speaks libpq's async protocol directly for maximum performance.

## Namespace

- `DBIO::PostgreSQL::Async` — Schema integration entry point
- `DBIO::PostgreSQL::Async::Storage` — Async storage implementation (extends DBIO::Storage::Async)
- `DBIO::PostgreSQL::Async::Pool` — EV::Pg connection pool

## Key Architecture

- Uses EV::Pg (libpq XS wrapper), NOT DBI/DBD::Pg
- Connection info is libpq conninfo format, not DBI DSN
- Returns Future.pm objects from all async methods
- Sync methods (select, insert, etc.) work by blocking via ->get
- Pipeline mode for batching queries in single round-trip
- LISTEN/NOTIFY via dedicated connection

## Dependencies

- EV::Pg >= 0.02, < 0.03 (0.02.x is the verified line on libpq 15)
- Future >= 0.49
- DBIO core

## Testing

- `t/00-load.t` — skips if EV::Pg not installed
- `t/01-storage-api.t` — skips if EV::Pg not installed
- `t/02-access-broker.t` — AccessBroker unit tests (no real DB)
- `t/10-integration.t` — EV::Pg live tests; requires `DBIOTEST_PG_DSN` + EV::Pg
- `t/11-access-broker-live.t` — AccessBroker live tests with real PG; requires `DBIOTEST_PG_DSN` + EV::Pg

### Kubernetes setup for integration tests

```bash
# Deploy and port-forward
kubectl --kubeconfig ~/.kube/rexdemo.yaml apply -f maint/k8s/pg-pod.yaml
kubectl --kubeconfig ~/.kube/rexdemo.yaml wait --for=condition=Ready pod/dbio-async-pg --timeout=60s
kubectl --kubeconfig ~/.kube/rexdemo.yaml port-forward svc/dbio-async-pg-svc 5432:5432 &

# Run all integration tests
DBIOTEST_PG_DSN='dbi:Pg:dbname=dbio_async;host=127.0.0.1;port=5432' \
DBIOTEST_PG_USER=dbio \
DBIOTEST_PG_PASS=dbio \
  prove -l t/10-integration.t t/11-access-broker-live.t

# Teardown
kubectl --kubeconfig ~/.kube/rexdemo.yaml delete -f maint/k8s/pg-pod.yaml
```
