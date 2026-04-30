---
name: dbio-postgresql
description: "DBIO::PostgreSQL driver — component API, JSONB querying, DDL/Deploy, enums, indexes, introspection"
user-invocable: false
allowed-tools: Read, Grep, Glob
model: sonnet
---

# DBIO::PostgreSQL

Covers PostgreSQL hierarchy: cluster → database → schema (namespace) → table/type/function/index/trigger/policy.

## Component Loading

```perl
package MyApp::DB;
use base 'DBIO::Schema';
__PACKAGE__->load_components('PostgreSQL');
# → sets storage_type to +DBIO::PostgreSQL::Storage automatically
```

## Three-Layer Architecture

| Layer | Class | Purpose |
|-------|-------|---------|
| Database | `DBIO::PostgreSQL` (Schema component) | extensions, search_path, settings |
| Namespace | `DBIO::PostgreSQL::PgSchema` subclass | enums, composite types, functions |
| Table | `DBIO::PostgreSQL::Result` (Result component) | indexes, triggers, RLS, pg_schema |

### Database layer

```perl
__PACKAGE__->pg_schemas(qw( public auth api ));
__PACKAGE__->pg_extensions(qw( pgcrypto uuid-ossp pgvector ));
__PACKAGE__->pg_search_path(qw( public ));
__PACKAGE__->pg_settings({ 'default_text_search_config' => 'pg_catalog.german' });
```

### PgSchema layer

```perl
package MyApp::DB::PgSchema::Auth;
use base 'DBIO::PostgreSQL::PgSchema';
__PACKAGE__->pg_schema_name('auth');
__PACKAGE__->pg_enum('role_type' => [qw( admin moderator user guest )]);
__PACKAGE__->pg_type('address_type' => { street => 'text', city => 'text', zip => 'varchar(10)' });
```

### Result layer

```perl
package MyApp::DB::Result::User;
use base 'DBIO::Core';
__PACKAGE__->load_components('PostgreSQL::Result');
__PACKAGE__->pg_schema('auth');    # → auth.users
__PACKAGE__->table('users');

__PACKAGE__->add_columns(
  id       => { data_type => 'uuid', default_value => \'gen_random_uuid()' },
  role     => { data_type => 'enum', pg_enum_type => 'role_type' },
  tags     => { data_type => 'text[]' },
  metadata => { data_type => 'jsonb', default_value => '{}' },
);

__PACKAGE__->pg_index('idx_users_tags'   => { using => 'gin', columns => ['tags'] });
__PACKAGE__->pg_index('idx_users_active' => { columns => ['role'], where => "role != 'suspended'" });
```

## JSONB Querying

Operators work in any `search()` automatically via `DBIO::PostgreSQL::SQLMaker` `special_ops`.

### Containment `@>` / `<@`

Hashref/arrayref → JSON-encoded, bound with `::jsonb` cast:

```perl
$rs->search({ 'me.data' => { '@>' => { status => 'active' } } });
# WHERE "me"."data" @> '{"status":"active"}'::jsonb

$rs->search({ 'me.tags' => { '@>' => ['admin', 'user'] } });
$rs->search({ 'me.data' => { '<@' => { role => 'guest' } } });

# Pre-encoded JSON string — passed through as-is
$rs->search({ 'me.data' => { '@>' => '{"status":"active"}' } });

# Scalar ref — literal SQL, no binding
$rs->search({ 'me.data' => { '@>' => \'other_col' } });
```

### Key existence `?` / `?|` / `?&`

Rewritten as `jsonb_exists*()` (avoids DBI `?` placeholder conflict):

```perl
$rs->search({ 'me.data' => { '?'  => 'email' } });
# WHERE jsonb_exists("me"."data", ?)
$rs->search({ 'me.data' => { '?|' => [qw(email phone)] } });
# WHERE jsonb_exists_any("me"."data", ARRAY[?, ?])
$rs->search({ 'me.data' => { '?&' => [qw(name email)] } });
# WHERE jsonb_exists_all("me"."data", ARRAY[?, ?])
```

### JSONPath `@?` / `@@` (PostgreSQL 12+)

```perl
$rs->search({ 'me.data' => { '@?' => '$.status == "active"' } });
# WHERE "me"."data" @? '$.status == "active"'::jsonpath
$rs->search({ 'me.data' => { '@@' => '$.score > 10' } });
```

### Path extraction DSL

```perl
use DBIO::PostgreSQL::JSONB qw(jsonb);

# Single key uses ->>, nested path uses #>>
jsonb('me.data', 'status')->eq('active');           # (me.data->>'status') = ?
jsonb('me.config', 'theme', 'color')->eq('dark');   # (me.config#>>'{theme,color}') = ?

# Comparison: eq, ne, gt, ge, lt, le, like, ilike, is_null, is_not_null
jsonb('me.stats', 'score')->gt(100);
jsonb('me.data', 'name')->ilike('%smith%');
jsonb('me.data', 'email')->is_not_null;

# ORDER BY
$rs->search({}, { order_by => jsonb('me.score', 'total')->as_order });
$rs->search({}, { order_by => { -desc => jsonb('me.score', 'total')->as_order } });

# Combine containment + path (OR)
$rs->search([
  jsonb('me.data', 'status')->eq('published'),
  { 'me.data' => { '@>' => { featured => \1 } } },
]);
```

## DDL & Deploy

```perl
my $ddl = $schema->pg_install_ddl;     # DBIO::PostgreSQL::DDL
print $ddl->as_sql;

my $deploy = $schema->pg_deploy;
$deploy->install;                       # fresh install

# Diff: temp DB deploy + pg_catalog compare
my $diff = $deploy->diff;
say $diff->as_sql;       # ALTER statements
say $diff->summary;      # human-readable changes
$deploy->apply($diff);

$deploy->upgrade;        # one-step
```

## Introspection

All via `pg_catalog`:

```perl
my $introspect = DBIO::PostgreSQL::Introspect->new(schema => $schema);
$introspect->schemas;     # list of DBIO::PostgreSQL::Introspect::Schema
$introspect->tables;      # columns, constraints
$introspect->types;       # enums, composites, ranges
$introspect->indexes;     # btree/gin/gist/brin/ivfflat with full def
$introspect->triggers;
$introspect->functions;
$introspect->extensions;
$introspect->policies;    # RLS
$introspect->sequences;
```

## Testing

Live integration:

```bash
export DBIO_TEST_PG_DSN='dbi:Pg:dbname=dbio_test;host=localhost'
export DBIO_TEST_PG_USER='dbio'
export DBIO_TEST_PG_PASS='secret'
prove -l t/
```

PostgreSQL must have **pgvector** installed.

Offline (SQL generation via fake storage):

```perl
my $schema = DBIO::Test->init_schema(
  no_deploy    => 1,
  storage_type => 'DBIO::PostgreSQL::Storage',
);
```

## Key Modules

| Module | Purpose |
|--------|---------|
| `DBIO::PostgreSQL` | Schema component (database layer) |
| `DBIO::PostgreSQL::Storage` | DBI storage: RETURNING, savepoints, BYTEA, JSONB inflate |
| `DBIO::PostgreSQL::SQLMaker` | JSONB operators, `special_ops` |
| `DBIO::PostgreSQL::JSONB` | `jsonb()` path-expression DSL |
| `DBIO::PostgreSQL::Result` | Result component: indexes, triggers, RLS, pg_schema |
| `DBIO::PostgreSQL::PgSchema` | Base for PG namespace classes |
| `DBIO::PostgreSQL::DDL` | Generates CREATE statements |
| `DBIO::PostgreSQL::Deploy` | Orchestrates install/diff/upgrade |
| `DBIO::PostgreSQL::Diff` | Compares two introspected models |
| `DBIO::PostgreSQL::Introspect` | Reads live DB via pg_catalog |
| `DBIO::PostgreSQL::Loader` | Reverse-engineer live DB into DBIO classes |
