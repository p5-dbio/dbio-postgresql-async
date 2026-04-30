---
name: dbio-development-driver
description: "How to develop a DBIO database driver: registry, storage class, SQLMaker, capabilities, async, Cake integration — developer guide"
user-invocable: false
allowed-tools: Read, Grep, Glob
model: sonnet
---

# DBIO Driver Development

DBIO database drivers are separate CPAN distributions. Each driver connects
DBIO's generic ORM to a specific database engine via the component system.

## Architecture Overview

```
User code
  → DBIO::Schema (connection)
    → Driver Registry (auto-detect from DSN)
      → Storage class (DB-specific DBI logic)
        → SQLMaker (SQL dialect + operator extensions)
        → Capability system (feature detection)
```

Two driver families exist:

| Family | Base Class | Protocol | Returns |
|--------|-----------|----------|---------|
| **DBI-based** (Pg, SQLite, MySQL) | `DBIO::Storage::DBI` | DBD driver | Blocking values |
| **Async** (PostgreSQL::Async) | `DBIO::Storage::Async` | libpq (EV::Pg) | Future objects |

## Driver Registry & Auto-Detection

Drivers register themselves in a global registry at load time:

```perl
package DBIO::PostgreSQL::Storage;
use base 'DBIO::Storage::DBI';

__PACKAGE__->register_driver('Pg' => __PACKAGE__);
```

**Auto-detection flow:**

1. User connects: `$schema->connect('dbi:Pg:dbname=myapp')`
2. First DB operation triggers `_determine_driver()` (lazy)
3. Extracts DBD name from DSN (`Pg` from `dbi:Pg:...`)
4. Looks up `$_driver_registry{'Pg'}` → `DBIO::PostgreSQL::Storage`
5. Reblesses storage object into that class
6. Calls `_rebless()` hook for driver-specific init

**Manual override** (skips auto-detection, used in Schema component):

```perl
sub connection {
  my $self = shift;
  $self->storage_type('+DBIO::PostgreSQL::Storage');
  return $self->next::method(@_);
}
```

## Driver Structure

A driver distribution provides up to 4 components:

### 1. Schema Component (`DBIO::DriverName`)

The entry point — users load this into their schema class.

```perl
package DBIO::DriverName;
# ABSTRACT: DriverName support for DBIO
use base 'DBIO::Schema';

sub connection {
  my $self = shift;
  $self->storage_type('+DBIO::DriverName::Storage');
  return $self->next::method(@_);
}

1;
```

### 2. Storage Class (`DBIO::DriverName::Storage`) — required

The core of the driver. Extends `DBIO::Storage::DBI` with database-specific behavior.

```perl
package DBIO::DriverName::Storage;
# ABSTRACT: Storage for DriverName databases
use base 'DBIO::Storage::DBI';

# Register for auto-detection
__PACKAGE__->register_driver('DriverName' => __PACKAGE__);

# Set defaults via class data
__PACKAGE__->sql_quote_char('"');
__PACKAGE__->datetime_parser_type('DateTime::Format::DriverName');
__PACKAGE__->sql_maker_class('DBIO::DriverName::SQLMaker');  # if custom

# Capability declarations (Tier 1: force on/off)
__PACKAGE__->_use_multicolumn_in(1);
__PACKAGE__->_use_insert_returning(1);

sub _rebless { ... }           # Post-detection init hook
sub last_insert_id { ... }     # Auto-increment retrieval
sub sqlt_type { 'DriverName' } # SQL::Translator type name

# Savepoint support
sub _svp_begin { ... }
sub _svp_release { ... }
sub _svp_rollback { ... }

# FK constraint deferral
sub with_deferred_fk_checks { ... }

# Connection-time setup
sub connect_call_set_encoding { ... }

# Bind attributes for data types
sub bind_attribute_by_data_type { ... }

1;
```

### 3. SQLMaker (`DBIO::DriverName::SQLMaker`) — optional

Override SQL dialect differences or register custom operators via `special_ops`.

```perl
package DBIO::DriverName::SQLMaker;
# ABSTRACT: SQL dialect for DriverName
use base 'DBIO::SQLMaker';

# Disable unsupported syntax
sub _lock_select { '' }   # e.g. SQLite has no SELECT ... FOR UPDATE

# Custom operators via special_ops (see below)
sub new {
  my $class = shift;
  my %opts = ref $_[0] eq 'HASH' ? %{$_[0]} : @_;
  push @{ $opts{special_ops} }, {
    regex   => qr/^my_op$/i,
    handler => '_where_op_my_op',
  };
  $class->next::method(\%opts);
}

sub _where_op_my_op {
  my ($self, $col, $op, $val) = @_;
  my $quoted = $self->_quote($col);
  return ("$quoted MY_OP ?", $val);
}

1;
```

**`special_ops` handler signature:** `($self, $col_unquoted, $op, $val)`
— call `$self->_quote($col)` yourself. Return `($sql, @bind)`.

**`special_ops` regex** matches against the operator string (the key inside
`{ op => val }`), NOT the field name.

Real driver examples:

| Driver | SQLMaker | What it adds |
|--------|----------|-------------|
| PostgreSQL | `DBIO::PostgreSQL::SQLMaker` | JSONB operators (`@>`, `?`, `@?`, ...) via `special_ops` |
| SQLite | `DBIO::SQLite::SQLMaker` | Disables `SELECT ... FOR UPDATE` |
| Oracle | `DBIO::Oracle::SQLMaker` | `CONNECT BY`, `PRIOR`, identifier shortening, `RETURNING INTO` |

### 4. Result Component (`DBIO::DriverName::Result`) — optional

Database-specific column/table features for Result classes.

```perl
__PACKAGE__->load_components('DriverName::Result');
# Now has access to database-specific annotations
```

## Capability System (2-tier)

Drivers declare capabilities through a two-tier system:

```perl
# Tier 1: Force enable/disable (class data, set in driver)
__PACKAGE__->_use_insert_returning(1);      # "I definitely support this"
__PACKAGE__->_use_multicolumn_in(1);

# Tier 2: Detect at runtime (only checked if Tier 1 is undef)
sub _determine_supports_insert_returning {
  return shift->_server_info->{normalized_dbms_version} >= 8.002 ? 1 : 0;
}
```

The result is cached in `_supports_insert_returning` (computed once).

Real examples:

```perl
# PostgreSQL: INSERT ... RETURNING since 8.2
sub _determine_supports_insert_returning {
  return shift->_server_info->{normalized_dbms_version} >= 8.002 ? 1 : 0;
}

# SQLite: multicolumn IN since 3.14
sub _determine_supports_multicolumn_in {
  ( shift->_server_info->{normalized_dbms_version} < '3.014' ) ? 0 : 1
}
```

## Key Storage Methods

| Method | Purpose | Must Override? |
|--------|---------|----------------|
| `register_driver()` | Auto-detection registry | Yes (at class load) |
| `_rebless()` | Post-detection init hook | Optional |
| `last_insert_id()` | Auto-increment retrieval | Usually yes |
| `sqlt_type()` | SQL::Translator type name | Yes |
| `_svp_begin/release/rollback()` | Savepoint support | If DB supports it |
| `with_deferred_fk_checks()` | Defer FK constraints | If DB supports it |
| `connect_call_*()` | Connection-time setup | Optional |
| `bind_attribute_by_data_type()` | DBI bind attrs per type | Optional |
| `datetime_parser_type` | DateTime parser class | Set via class data |
| `sql_quote_char` | Identifier quoting | Set via class data |
| `sql_maker_class` | Custom SQLMaker | Set via class data if needed |
| `cake_defaults()` | Driver-recommended Cake flags | Optional (enables `-Pg` etc.) |

### cake_defaults()

Optional method that returns driver-recommended options for `DBIO::Cake`.
Activated by driver shortcut flags like `use DBIO::Cake '-Pg'`:

```perl
sub cake_defaults {
  return (
    inflate_jsonb     => 1,   # jsonb only, not json (leaves json() free)
    inflate_datetime  => 1,
    retrieve_defaults => 1,   # PostgreSQL generates UUIDs, serials, NOW()
  );
}
```

Cake uses `DBIO::Storage::DBI->driver_storage_class($name)` to look up
the storage class from the registry, then calls `cake_defaults()` on it.

**Inherited for free** (no override needed):
- Connection/disconnection management
- SQL generation (via SQLMaker)
- Transaction management (`txn_begin`, `txn_commit`, `txn_rollback`)
- Query execution (`insert`, `update`, `delete`, `select`)
- Handle caching, prepared statement support
- DBH attribute binding

## Async Drivers

Async drivers bypass DBI entirely, using native async database protocols.

| Aspect | DBI Driver | Async Driver |
|--------|-----------|--------------|
| Base class | `DBIO::Storage::DBI` | `DBIO::Storage::Async` |
| Protocol | DBD (DBI) | Native async (e.g., EV::Pg/libpq) |
| Returns | Blocking values | Future objects |
| Connection | Single DBH | Connection pool |
| Batching | No | Pipeline mode (multiple queries/round-trip) |

**Async driver must implement:**

```perl
sub future_class { ... }        # Event-loop-specific Future class
sub pool { ... }                # Connection pool
sub select_async { ... }        # Non-blocking query → Future
sub select_single_async { ... } # Non-blocking single-row → Future
```

Async features (PostgreSQL::Async):
- LISTEN/NOTIFY (event-driven, not polling)
- COPY IN/OUT (bulk data transfer)
- Prepared statement caching
- Transaction pinning (pin to pool connection)
- Sync methods still work (block event loop — for compatibility)

## Distribution Layout

```
DBIO-DriverName/
  lib/
    DBIO/
      DriverName.pm                # Schema component
      DriverName/
        Storage.pm                 # Storage class (core)
        SQLMaker.pm                # SQL dialect (optional)
        Result.pm                  # Result component (optional)
  t/
    00-load.t                      # Load tests (no DB needed)
    20-sqlmaker.t                  # SQL generation tests (no DB needed)
    10-integration.t               # Requires live DB
  dist.ini                         # [@DBIO] plugin bundle
  cpanfile                         # Dependencies
  .proverc                         # -Ilib -I../dbio/lib (workspace-local)
```

## Naming Convention

| Part | Pattern | Example |
|------|---------|---------|
| Distribution | `DBIO-DriverName` | `DBIO-PostgreSQL` |
| Schema component | `DBIO::DriverName` | `DBIO::PostgreSQL` |
| Storage class | `DBIO::DriverName::Storage` | `DBIO::PostgreSQL::Storage` |
| SQLMaker | `DBIO::DriverName::SQLMaker` | `DBIO::PostgreSQL::SQLMaker` |
| DBD driver used | `DBD::X` | `DBD::Pg`, `DBD::mysql` |
| Async variant | `DBIO::DriverName::Async` | `DBIO::PostgreSQL::Async` |

## Testing

- **Offline tests** (no DB): SQLMaker SQL generation, module loading — always required
- **Integration tests**: require a live database connection via env vars (see the driver's `CLAUDE.md` or `t/` for the exact var names)
- Always provide offline tests — they run in CI without database setup
- `.proverc` in each driver repo adds `-I../dbio/lib` automatically

```perl
# Offline SQLMaker test pattern:
my $schema = DBIO::Test->init_schema(
  no_deploy    => 1,
  storage_type => 'DBIO::DriverName::Storage',
);
is_same_sql_bind( $rs->search(...)->as_query, $expected_sql, \@bind, 'description' );
```

## Build System

All drivers use `[@DBIO]` Dist::Zilla bundle:

```ini
name = DBIO-DriverName

[@DBIO]
```
