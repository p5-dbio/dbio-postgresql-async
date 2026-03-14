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

- EV::Pg >= 0.02 (requires libpq >= 14, some features need >= 17)
- Future >= 0.49
- DBIO core

## Testing

- `t/00-load.t` — skips if EV::Pg not installed
- `t/01-storage-api.t` — skips if EV::Pg not installed
- `t/10-integration.t` — requires `DBIOTEST_PG_DSN` + EV::Pg
