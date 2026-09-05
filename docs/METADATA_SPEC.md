# Metadata Specification

This document answers the open question in STORAGE_SPEC.md §18: *"What
metadata fields are required by the downstream system on day one?"*

It provides the column-level contract for every domain record and SQLite
table in the storage layer: type, nullable status, which implementation
step introduces it, and why it is required. It also documents which
fields the downstream pipeline consumer (`opx-strategy`) depends on and
must be present before that consumer can function.

See STORAGE_SPEC.md for record descriptions, the storage port protocol,
and implementation ordering. See EXTERNAL_INTERFACE_SPEC.md for the
stable public surface exposed to downstream consumers.

---

## 1. `RunRecord`

One record per fetch run. Created by `create_run`, updated by
`finalize_run` and `fail_run`. Persisted by both the filesystem backend
(as a JSON sidecar at `runs/{run_id}/run.json`) and the SQLite backend (`runs` table).

| Field | Type | Nullable | Step | Purpose |
|---|---|---|---|---|
| `run_id` | `str` | NO | 2 | Primary key; unique identifier for this fetch run |
| `started_at` | `datetime` | NO | 2 | UTC timestamp when `create_run` was called |
| `finished_at` | `datetime` | YES | 2 | UTC timestamp when `finalize_run` or `fail_run` was called; `None` while running |
| `status` | `str` | NO | 2 | `running` / `complete` / `failed` / `interrupted`; `create_run` sets `running` immediately; `pending` is reserved |
| `provider` | `str` | NO | 2 | Data provider name (e.g., `marketdata`, `yfinance`); required for dataset provenance |
| `script_version` | `str` | NO | 2 | opx-chain package version that opened the run; legacy records without this field read back as `unknown` |
| `tickers` | `tuple[str, ...]` | NO | 2 | Effective fetch universe for this run, including configured tickers and stock tickers expanded from the positions file; used by ticker-filtered dataset discovery |
| `config_fingerprint` | `str` | NO | 2 | SHA-256 of the resolved config fields that affect output, including provider, tickers, expiration cap, filters, validation, scoring weights, Greek/HV constants, freshness threshold, provider modes, retry/backoff settings, cache settings, and storage/export settings; excludes credentials, local runtime paths, viewer bind settings, config warnings, and the transient `today` value; two runs with the same fingerprint and positions fingerprint should produce structurally comparable datasets |
| `positions_fingerprint` | `str` | NO | 2 | SHA-256 of the canonical parsed positions payload; empty string when no positions file is present; changes when held stock or option contracts change, but not for cosmetic CSV rewrites such as line endings, column order, BOMs, or quoting |
| `dataset_id` | `str` | YES | 2 | FK to `DatasetRecord`; `None` until `write_dataset` succeeds; a run may complete without a dataset if all tickers fail |
| `error_summary` | `str` | YES | 2 | Short error description when `status = failed` or `interrupted`; `None` otherwise |

**Required by downstream consumer**: `run_id`, `status`, `provider`,
`tickers`, `positions_fingerprint`. The pipeline reads `positions_fingerprint` to
detect whether the chain was collected against the same parsed portfolio that is
being processed in the current pipeline run.

---

## 2. `DatasetRecord`

One record per successfully written canonical dataset. Created by
`write_dataset` after the artifact file is written and its hash is
computed. This is the central metadata record that inspection surfaces and
downstream consumers discover and reference. Metadata discovery is not proof
that a dataset is safe for semantic use.

| Field | Type | Nullable | Step | Purpose |
|---|---|---|---|---|
| `dataset_id` | `str` | NO | 2 | Primary key; stable identifier for this dataset snapshot |
| `run_id` | `str` | NO | 2 | FK to `RunRecord`; links the dataset to the fetch run that produced it |
| `created_at` | `datetime` | NO | 2 | UTC timestamp when the artifact was written; used by the downstream consumer for freshness assessment |
| `provider` | `str` | NO | 2 | Data provider that produced this dataset |
| `script_version` | `str` | NO | 2 | opx-chain package version that wrote the dataset metadata; legacy records without this field read back as `unknown` |
| `schema_version` | `int` | NO | 1 | Value of `SCHEMA_VERSION` at write time; consumer validates this before reading the artifact to detect schema drift |
| `row_count` | `int` | NO | 2 | Total rows in the artifact; used for basic sanity validation by the consumer |
| `format` | `str` | NO | 2 | `csv` (default) / `parquet`; tells the consumer which reader to use |
| `location` | `str` | NO | 2 | Absolute path to the artifact file; raw inspection/export code must use this field rather than construct or infer a path independently |
| `content_hash` | `str` | NO | 2 | SHA-256 of artifact bytes, computed after write completes; used by the downstream consumer for integrity verification and deduplication |
| `integrity_status` | `OptionChainIntegrityStatus` | NO | integrity | Effective state: `valid`, `invalid`, or `unknown`; only `valid` is eligible for semantic use |
| `integrity_schema_version` | `int` | YES | integrity | Serialized integrity-summary schema version |
| `integrity_validator_version` | `int` | YES | integrity | Validator semantic version used for the exact bytes |
| `integrity_checked_at` | `datetime` | YES | integrity | UTC time the exact artifact bytes were validated |
| `integrity_content_hash` | `str` | YES | integrity | Hash the integrity summary covers; must equal `content_hash` when valid |
| `integrity_summary` | `OptionChainIntegritySummary` | YES | integrity | Bounded aggregate and sample findings for the exact bytes |
| `dataset_facts_status` | `OptionChainDatasetFactsStatus` | NO | integrity | `available` or `unknown`; only `available` is eligible for semantic use |
| `dataset_facts` | `OptionChainDatasetFacts` | YES | integrity | Versioned, content-bound neutral ticker/time/expiration projection |
| `row_scope_status` | `OptionChainRowScopeStatus` | NO | publication | `available` or `unknown`; legacy absence is explicitly unknown |
| `row_scope` | `OptionChainRowScope` | YES | publication | Versioned provider-neutral filter state, acquisition horizon, and conserved row/ticker totals |

Current integrity and dataset-facts fields are required for semantic use, but
the metadata model supplies backward-compatible `unknown`/null defaults when
reading legacy records. Semantic consumers do not decide safety from individual
fields or open `location` directly; they call
`load_validated_option_chain_dataset(dataset_id)`, which validates the complete
record and exact artifact bytes.

Legacy metadata defaults the two status fields to `unknown` and leaves the
versioned projections null. Unfiltered metadata lookup can still return that
record for history, diagnostics, and raw inspection, but the validated loader
rejects it until it is revalidated and republished.

---

## 3. `DatasetHandle`

Returned by `get_dataset`. The stable external reference passed to
downstream consumers. Defined by EXTERNAL_INTERFACE_SPEC.md §4 as the
public contract — these fields may not be removed or renamed without a
`SCHEMA_VERSION` bump.

| Field | Type | Nullable | Source |
|---|---|---|---|
| `dataset_id` | `str` | NO | `DatasetRecord.dataset_id` |
| `run_id` | `str` | NO | `DatasetRecord.run_id` |
| `provider` | `str` | NO | `DatasetRecord.provider` |
| `location` | `str` | NO | `DatasetRecord.location` |
| `schema_version` | `int` | NO | `DatasetRecord.schema_version` |
| `script_version` | `str` | NO | `DatasetRecord.script_version` |
| `row_count` | `int` | NO | `DatasetRecord.row_count` |
| `format` | `str` | NO | `DatasetRecord.format` |
| `content_hash` | `str` | NO | `DatasetRecord.content_hash` |
| `created_at` | `datetime` | NO | `DatasetRecord.created_at` |
| `integrity_status` | `OptionChainIntegrityStatus` | NO | `DatasetRecord.integrity_status` |
| `integrity_schema_version` | `int` | YES | `DatasetRecord.integrity_schema_version` |
| `integrity_validator_version` | `int` | YES | `DatasetRecord.integrity_validator_version` |
| `integrity_checked_at` | `datetime` | YES | `DatasetRecord.integrity_checked_at` |
| `integrity_content_hash` | `str` | YES | `DatasetRecord.integrity_content_hash` |
| `integrity_summary` | `OptionChainIntegritySummary` | YES | `DatasetRecord.integrity_summary` |
| `dataset_facts_status` | `OptionChainDatasetFactsStatus` | NO | `DatasetRecord.dataset_facts_status` |
| `dataset_facts` | `OptionChainDatasetFacts` | YES | `DatasetRecord.dataset_facts` |
| `row_scope_status` | `OptionChainRowScopeStatus` | NO | `DatasetRecord.row_scope_status` |
| `row_scope` | `OptionChainRowScope` | YES | `DatasetRecord.row_scope` |

`run_id`, `provider`, `content_hash`, and `created_at` are required additions to
`DatasetHandle` (they were previously only on `DatasetRecord`). Downstream
consumers need them to preserve dataset provenance, avoid paginated
`list_datasets()` lookups, perform integrity checks, and assess freshness
without fetching the full `DatasetRecord`. See EXTERNAL_INTERFACE_SPEC.md §7.2.

---

## 4. `TickerRunRecord`

One record per ticker per run. Written during the per-ticker fetch loop.
Used for run-level diagnostics and to attribute row count changes to
normalization losses vs. filter losses.

| Field | Type | Nullable | Step | Purpose |
|---|---|---|---|---|
| `run_id` | `str` | NO | 3 | FK to `RunRecord` |
| `ticker` | `str` | NO | 3 | Underlying symbol |
| `raw_row_count` | `int` | NO | 3 | Rows received from provider before any processing |
| `normalized_row_count` | `int` | NO | 3 | Rows after normalize/enrich and before filter step; isolates normalization losses |
| `kept_row_count` | `int` | NO | 3 | Rows after filters are applied; rows that reach the canonical export |
| `filtered_row_count` | `int` | NO | 3 | Rows removed by filters (`normalized_row_count - kept_row_count`) |
| `expiration_count` | `int` | NO | 3 | Distinct expiration dates in the kept rows |
| `status` | `str` | NO | 3 | `ok` / `skipped` / `error` |
| `error_summary` | `str` | YES | 3 | Short error description when `status = error`; `None` otherwise |

Not part of the downstream consumer's external interface. Used internally
for run diagnostics and the `opx-check` summary.

---

## 5. `ValidationRecord`

One record per validation finding per run. Written during the validate
step within `fetcher.py`. Multiple records with the same `code` may exist
for a single run (one per affected ticker or condition).

| Field | Type | Nullable | Step | Purpose |
|---|---|---|---|---|
| `run_id` | `str` | NO | 3 | FK to `RunRecord` |
| `severity` | `str` | NO | 3 | `error` / `warning` / `info` |
| `code` | `str` | NO | 3 | Machine-readable validation code (e.g., `STALE_QUOTE`, `MISSING_GREEKS`) |
| `count` | `int` | NO | 3 | Number of rows or tickers affected |
| `sample` | `str` | YES | 3 | Optional JSON-encoded detail for the first affected row; `None` when count is the only useful signal |

Not part of the downstream consumer's external interface.

---

## 6. `ArtifactRecord`

One record per auxiliary artifact. Written by `write_artifact` for debug
payloads, run logs, and optional sidecars. Sidecars may live under the owning
run directory instead of the debug artifact directory.

| Field | Type | Nullable | Step | Purpose |
|---|---|---|---|---|
| `artifact_id` | `str` | NO | 3 | Primary key |
| `run_id` | `str` | NO | 3 | FK to `RunRecord` |
| `artifact_type` | `str` | NO | 3 | `debug_payload` / `run_log` / `sidecar` |
| `location` | `str` | NO | 3 | Path to the artifact file |
| `content_hash` | `str` | NO | 3 | SHA-256 of artifact bytes |

Not part of the downstream consumer's external interface.

---

## 7. SQLite Schema (Step 5)

When `backend = sqlite` is configured, the following tables are created
by schema initialization. Schema version is tracked in a `_schema_meta`
table. Existing databases with an older version are upgraded by numbered
migrations before use; a schema-version bump without a migration fails
startup instead of silently reusing an outdated layout.

Migrations run as individual idempotent statements and advance `_schema_meta`
after each numbered migration. This lets startup recover from interrupted
`ALTER TABLE ... ADD COLUMN` migrations: an already-added column is skipped,
remaining columns are applied, and the version advances only after the whole
numbered migration succeeds.

```sql
CREATE TABLE _schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
-- seed: INSERT INTO _schema_meta VALUES ('schema_version', '1');

CREATE TABLE runs (
    run_id               TEXT PRIMARY KEY,
    started_at           TEXT NOT NULL,   -- ISO 8601 UTC
    finished_at          TEXT,            -- NULL while running
    status               TEXT NOT NULL,
    provider             TEXT NOT NULL,
    script_version       TEXT NOT NULL DEFAULT 'unknown',
    tickers              TEXT NOT NULL DEFAULT '[]',
    config_fingerprint   TEXT NOT NULL,
    positions_fingerprint TEXT NOT NULL,
    dataset_id           TEXT,            -- NULL until write_dataset succeeds
    error_summary        TEXT
);

CREATE TABLE datasets (
    dataset_id      TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES runs(run_id),
    created_at      TEXT NOT NULL,   -- ISO 8601 UTC
    provider        TEXT NOT NULL,
    script_version  TEXT NOT NULL DEFAULT 'unknown',
    schema_version  INTEGER NOT NULL,
    row_count       INTEGER NOT NULL,
    format          TEXT NOT NULL,   -- 'csv' | 'parquet'
    location        TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    integrity_status            TEXT NOT NULL DEFAULT 'unknown',
    integrity_schema_version    INTEGER,
    integrity_validator_version INTEGER,
    integrity_checked_at        TEXT,
    integrity_content_hash      TEXT,
    integrity_summary_json      TEXT,
    dataset_facts_status        TEXT NOT NULL DEFAULT 'unknown',
    dataset_facts_json          TEXT,
    row_scope_status            TEXT NOT NULL DEFAULT 'unknown',
    row_scope_json              TEXT
);

CREATE TABLE ticker_results (
    run_id               TEXT NOT NULL REFERENCES runs(run_id),
    ticker               TEXT NOT NULL,
    raw_row_count        INTEGER NOT NULL,
    normalized_row_count INTEGER NOT NULL,
    kept_row_count       INTEGER NOT NULL,
    filtered_row_count   INTEGER NOT NULL,
    expiration_count     INTEGER NOT NULL,
    status               TEXT NOT NULL,
    error_summary        TEXT,
    PRIMARY KEY (run_id, ticker)
);

CREATE TABLE validations (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id   TEXT NOT NULL REFERENCES runs(run_id),
    severity TEXT NOT NULL,
    code     TEXT NOT NULL,
    count    INTEGER NOT NULL,
    sample   TEXT    -- JSON-encoded; NULL when not applicable
);

CREATE TABLE artifacts (
    artifact_id   TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL REFERENCES runs(run_id),
    artifact_type TEXT NOT NULL,
    location      TEXT NOT NULL,
    content_hash  TEXT NOT NULL
);

CREATE INDEX idx_datasets_created_at ON datasets(created_at DESC);
CREATE INDEX idx_datasets_run_id     ON datasets(run_id);
CREATE INDEX idx_runs_status         ON runs(status);
```

All writes use `INSERT OR REPLACE` (or equivalent upsert) so re-running
a fetch after an interrupted run overwrites the prior incomplete record
rather than accumulating duplicates.

---

## 8. Fields Required by the Downstream Consumer

The `opx-strategy` pipeline reads opx storage through `StorageBackend`
as a read-only consumer. These are the fields it depends on from day one:

| Field | Record | Why required |
|---|---|---|
| `dataset_id` | `DatasetRecord` / `DatasetHandle` | Stable reference stored in the pipeline's `runs` table to link every pipeline run to the exact chain it consumed |
| `provider` | `DatasetRecord` / `DatasetHandle` | Provider provenance for a specific dataset id; downstream consumers should not need paginated `list_datasets()` scans to recover it |
| `location` | `DatasetHandle` | Artifact path for raw inspection/export; semantic consumers do not open it directly |
| `schema_version` | `DatasetHandle` | Provenance exposed for assessment; the validated loader rejects an unsupported schema before returning a frame |
| `script_version` | `RunRecord`, `DatasetRecord`, `DatasetHandle` | Carries the opx-chain package version that produced the run/dataset so downstream provenance does not depend on grepping `opx_runs.log` |
| `content_hash` | `DatasetHandle` | Stored in `runs.chain_content_hash`; the validated loader binds it to the exact bytes and downstream consumers retain it as provenance |
| `created_at` | `DatasetHandle` | Used for chain freshness assessment against the staleness thresholds in STRATEGY.md DATA AUTHORITY |
| `row_count` | `DatasetHandle` | Declared row count checked by the validated loader and retained as provenance |
| `format` | `DatasetHandle` | Declared serialization format dispatched and checked by the validated loader |
| `positions_fingerprint` | `RunRecord` | Cross-checked against the pipeline's own positions fingerprint to detect chain/positions mismatch |

**`SCHEMA_VERSION`** (from `opx_chain/__init__.py`) is the most critical
single field. The downstream consumer imports it directly:

```python
from opx_chain import SCHEMA_VERSION
assert handle.schema_version == SCHEMA_VERSION
```

A mismatch means either the opx-chain package has been updated without
re-fetching, or a stale chain is being reused across a schema boundary.
Both cases are fatal — the pipeline stops with a clear error message
before any data is read.

---

## 9. Fields That Must Be Present Before `write_dataset` Returns

These fields must be successfully written or the storage backend must
raise before returning. A `DatasetRecord` with any of these fields absent
or zero is a storage bug, not an acceptable null.

- `dataset_id` — must be a non-empty unique string
- `schema_version` — must equal the current `SCHEMA_VERSION` constant
- `location` — must point to an existing file
- `content_hash` — must be a 64-character hex string (SHA-256)
- `row_count` — must be greater than zero; a zero-row dataset indicates
  a failed fetch that should not have called `write_dataset`
- `created_at` — must be a valid UTC timestamp
