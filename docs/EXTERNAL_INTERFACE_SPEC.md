# External Interface Specification

This document specifies the stable external interface that `opx-chain` exposes to
downstream consumers. It covers the CLI invocation contract, the Python package
interface, and the schema versioning contract.

`opx-chain` does not own any downstream system and has no dependency on them. This
document describes what `opx-chain` commits to stabilizing so that consumers can
integrate without coupling to internal implementation details.

---

## Boundary and Duplication Policy

`opx-chain` owns public contracts for option-chain market data, canonical
exports, storage/dataset discovery, provider-owned metadata, and positions
parsing. Downstream packages may depend on those contracts only through the
public surfaces documented here.

Generic helpers are not external contracts. Similar JSON helpers, timestamp
formatting, timestamp-age guards, XDG/path helpers, logging helpers, CLI/test
guards, display formatting, and error wording may remain package-local in
`opx-chain` and downstream packages. If one package has a concrete bug in those
areas, fix that package without creating a shared abstraction.

Promote behavior into this external interface only when downstream consumers
must rely on the same `opx-chain` domain semantics. Good candidates are storage
dataset handles and schema metadata, option-chain reuse and freshness fields,
provider/fetcher defaults that define exported rows, and option or ticker
parsing when it governs chain/positions boundary behavior. Strategy-layer
policy remains outside this interface.

## 1. Scope

Three integration points are in scope:

1. **CLI invocation** — a downstream orchestrator can invoke `opx-fetch` as a
   subprocess to trigger a fresh chain fetch
2. **Programmatic fetch** — a downstream consumer running in the same process can call
   `opx_chain.fetcher.run_fetch()` to trigger a fetch without spawning a subprocess
3. **Storage interface** — a downstream consumer can import `opx_chain` as a Python
   package and use `StorageBackend` to discover and read the latest chain dataset

Everything else — internal storage layout, provider adapters, scoring weights,
normalization logic — is internal to `opx-chain` and may change without notice.

---

## 2. CLI Invocation Contract

### 2.1 `opx-fetch`

`opx-fetch` is the entry point for triggering a fresh option-chain fetch.

A downstream orchestrator invokes it as a subprocess:

```
opx-fetch [--positions <path>] [--dry-run] [--enable-filters | --disable-filters]
```

The orchestrator must:
- wait for the process to exit before querying storage for the new dataset
- treat any non-zero exit code as a fetch failure
- not parse stdout or stderr for structured data; those streams are for logging only

**`--positions <path>` (optional)**

Overrides the default positions file path (`$XDG_DATA_HOME/opx-chain/positions.csv`,
default `~/.local/share/opx-chain/positions.csv`). When provided,
`opx-fetch` uses this file to determine which option contracts must survive hard
filters regardless of screening criteria. When absent, behaviour is unchanged.

A downstream orchestrator that manages a per-run positions file passes the
run-specific path here:

```
opx-fetch --positions /path/to/runs/<run_id>/positions.csv
```

See `docs/PROJECT_SPEC.md` §7.3 for the full behaviour specification.

**`--dry-run` (optional)**

Validates the resolved config, parses the positions file, and checks storage
backend reachability without provider API calls or output writes. If parquet
storage is configured, the dry run also verifies that the optional `pyarrow`
dependency is installed before any provider call can be made. A dry run exits
`0` when those preflight checks pass. It is safe for operator diagnostics; a
downstream orchestrator should not treat it as producing a new dataset.

**Exit codes**

| Code | Meaning |
|---|---|
| `0` | Fetch completed; at least one dataset was written to storage |
| non-zero | Fetch failed or was interrupted; no new dataset should be assumed |

### 2.2 No other CLI arguments are part of the external interface

`--enable-filters` and `--disable-filters` are internal operational flags, not part
of the stable downstream interface. A downstream orchestrator should not set them.

---

## 3. Python Package Interface

**Prerequisite:** the Python package interface is only available when storage is
enabled in the `opx-chain` config (`[storage] enable = true`). When storage is
disabled (the default), `opx_chain.storage` modules are importable but
`get_storage_backend()` returns `None` and `list_datasets` is not meaningful.
A downstream consumer must ensure the `opx-chain` instance it connects to has storage
enabled before using this interface.

A downstream consumer may import `opx_chain` as a Python dependency to query the storage
layer without shelling out or scanning the filesystem directly.

### 3.1 Public surface

The stable public surface is:

```python
from opx_chain.fetcher import run_fetch
from opx_chain.storage.base import StorageBackend
from opx_chain.storage.models import DatasetHandle, DatasetRecord, RunRecord
from opx_chain.storage.factory import get_storage_backend
from opx_chain.utils import read_dataset_file
from opx_chain.positions import (
    OptionPositionKey,
    PositionSet,
    load_positions,
    positions_fingerprint,
)
from opx_chain.price_context import (
    PRICE_CONTEXT_RECORD_FIELDS,
    PRICE_CONTEXT_SCHEMA_VERSION,
    PriceContextStatus,
    blank_price_context,
)
from opx_chain.option_types import (
    OPTION_TYPE_CALL,
    OPTION_TYPE_PUT,
    OPTION_TYPES,
    normalize_option_type,
    option_type_label,
)
from opx_chain.paths import get_runs_dir
from opx_chain.config import DEFAULT_PRICE_CONTEXT_MAX_AGE_DAYS, US_MARKET_TIMEZONE
from opx_chain import SCHEMA_VERSION
```

`read_dataset_file` is the only stable public import from `opx_chain.utils`;
other helpers in that module remain internal.

`load_positions`, `positions_fingerprint`, `PositionSet`, and
`OptionPositionKey` are the stable positions parsing surface for downstream
consumers that need the same stock ticker expansion, held-option contract keys,
and parsed-position fingerprint as `opx-fetch`.

`PRICE_CONTEXT_SCHEMA_VERSION`, `PRICE_CONTEXT_RECORD_FIELDS`,
`PriceContextStatus`, and `blank_price_context` are the stable price-context
artifact vocabulary for downstream consumers that join optional daily-OHLCV
levels to option-chain rows.

`OPTION_TYPE_CALL`, `OPTION_TYPE_PUT`, `OPTION_TYPES`,
`normalize_option_type`, and `option_type_label` are the stable option-type
normalization vocabulary for downstream consumers that need to compare option
rows, positions, and generated candidate identifiers using the same canonical
`call` / `put` values as `opx-chain`.

`get_runs_dir`, `DEFAULT_PRICE_CONTEXT_MAX_AGE_DAYS`, and
`US_MARKET_TIMEZONE` are stable runtime-environment helpers for locating
`opx-chain` run artifacts and applying the same US-market calendar boundary as
the fetcher.

All other names within `opx_chain.fetcher`, `opx_chain.normalize`, `opx_chain.provider`,
and other internal modules are not part of the stable interface and may change across
releases.

### 3.2 Triggering a fresh fetch programmatically

```python
from opx_chain.fetcher import run_fetch

run_fetch(positions_path=Path("/path/to/runs/<run_id>/positions.csv"))
run_fetch(tickers=("TSLA", "NVDA"))
run_fetch(
    positions_path=Path.home() / ".local" / "share" / "opx-chain" / "positions.csv",
    tickers=("AAPL",),
    max_expiration_weeks=34,
    stale_quote_seconds=86_400,
    data_provider="marketdata",
)
run_fetch(dry_run=True)
run_fetch(price_context_only=True)
```

`run_fetch()` is the in-process equivalent of invoking `opx-fetch` as a subprocess.
It acquires the same exclusive lock, runs the full fetch pipeline, and writes the result
to storage. The caller blocks until the fetch completes.

**`positions_path` (optional `Path`)** — overrides the default positions file, identical
in semantics to the `--positions` CLI flag. When absent, the configured default is used.

**`tickers` (optional `tuple[str, ...]`)** — overrides the ticker list from config for
this run only. The override replaces `settings.tickers` entirely; the positions file
can still add additional tickers via stock-ticker expansion. When absent, the configured
`settings.tickers` is used unchanged.

**`max_expiration_weeks` (optional `int`)** — overrides the configured maximum
expiration window for this run only. `0` disables the max-expiration filter for the
fetch. When absent, the configured `settings.max_expiration_weeks` is used unchanged.

**`stale_quote_seconds` (optional `int`)** — overrides the configured stale quote
threshold for this run only. Downstream callers use this to align fetch freshness with
their own run policy without editing opx-chain config. When absent, the configured
`settings.stale_quote_seconds` is used unchanged.

**`data_provider` (optional `str`)** — overrides `settings.data_provider` for this
run only. Supported values are `marketdata`, `massive`, and `yfinance`. Downstream
callers use this to select a provider per experimental or production run without
mutating the opx-chain config file. When absent, the configured provider is used
unchanged.

**`dry_run` (optional `bool`)** — when `True`, validates config loading, positions
parsing, lock acquisition, and storage reachability without making provider API calls
or writing run artifacts. This is the in-process equivalent of `opx-fetch --dry-run`.

**`price_context_only` (optional `bool`)** — when `True`, reconciles only the
optional daily-OHLCV price-history store, writes the derived price-context
artifact, and skips option-chain export. This also enables price-context fetching
for the run, regardless of the config default.
The result is written as a standalone versioned JSON artifact under the runs
directory and does not change the option-chain dataset schema.

**Errors:**

| Condition | Raised |
|---|---|
| Another fetch is already active (lock held) | `RuntimeError` |
| Fetch produces no data | `RuntimeError` |
| Provider or storage failure | provider-specific exception |

After `run_fetch()` returns without error, the result is available via `get_storage_backend()`
exactly as it would be after a successful `opx-fetch` subprocess exit.

### 3.3 Obtaining a backend instance

```python
backend: StorageBackend = get_storage_backend()
```

`get_storage_backend()` returns the configured backend (filesystem or SQLite) based
on the `opx-chain` config. No arguments are required. The consumer must not construct a
backend directly.

### 3.4 Discovering the latest dataset

```python
records: list[DatasetRecord] = backend.list_datasets(limit=1)
```

Returns the most recent successfully written dataset. Returns an empty list if no
datasets exist.

The consumer should validate:
- the list is non-empty (no datasets available → cannot proceed)
- `records[0].schema_version == SCHEMA_VERSION` (schema drift → must re-fetch or
  update the consumer to handle the new schema before proceeding)

### 3.5 Obtaining a dataset handle

```python
handle: DatasetHandle = backend.get_dataset(dataset_id)
```

Returns a `DatasetHandle` for the given `dataset_id`. The consumer reads the chain
artifact at `handle.location`.

### 3.6 Retrieving a run record

```python
run: RunRecord = backend.get_run(run_id)
```

Returns the `RunRecord` for the given `run_id`. Raises `KeyError` when the run
does not exist. Downstream consumers use this to retrieve
`RunRecord.tickers` — the effective fetch universe for that run — and
`RunRecord.positions_fingerprint` — the SHA-256 of the canonical parsed
positions payload active when the chain was fetched — for cross-checking against
the consumer's own positions fingerprint. Cosmetic positions CSV rewrites, such
as line-ending or column-order changes, do not change this fingerprint when the
parsed stock and option positions are unchanged.

`status="running"` is transient. If a previous fetch process terminates without
running cleanup, the next real fetch that acquires the fetcher lock marks stale
`running` records as `interrupted` before creating a new run. Dry runs do not
perform this recovery because they are read-only diagnostics.

`run_id` is available on `DatasetRecord.run_id` (returned by `list_datasets`).

```python
records = backend.list_datasets(limit=1)
run = backend.get_run(records[0].run_id)
assert "TSLA" in run.tickers
assert run.positions_fingerprint == pipeline_positions_fingerprint
```

### 3.7 Reading the chain artifact

```python
from opx_chain.utils import read_dataset_file
df = read_dataset_file(handle.location)  # dispatches on .csv / .parquet extension
```

`read_dataset_file` is the recommended reader. It selects `pd.read_parquet` or
`pd.read_csv` based on the file extension, matching `handle.format`, then
normalizes format-sensitive canonical dtypes. Whole-number fields such as
`days_to_expiration` read back as nullable `Int64`, boolean fields read back as
nullable `boolean`, and quote timestamp fields read back as UTC
`datetime64[ns, UTC]` for both CSV and parquet artifacts. Parquet requires the
optional `pyarrow` dependency (`pip install 'opx-chain[parquet]'`).

### 3.8 Parsing positions consistently

```python
from opx_chain.positions import (
    OptionPositionKey,
    PositionSet,
    load_positions,
    positions_fingerprint,
)

positions: PositionSet = load_positions(Path("/path/to/positions.csv"))
held_contracts: frozenset[OptionPositionKey] = positions.option_keys
fingerprint: str = positions_fingerprint(Path("/path/to/positions.csv"), positions)
```

`load_positions()` parses the same Fidelity positions CSV format used by
`opx-fetch`. The file must be UTF-8 or UTF-8-with-BOM and include a `Symbol`
column. Missing files return an empty `PositionSet` silently; existing files
that cannot be parsed print a warning to stderr before returning an empty
`PositionSet`. It returns a `PositionSet` with:

- `stock_tickers`: stock symbols parsed from held stock rows
- `option_keys`: held option contracts as `OptionPositionKey` values with `ticker`,
  `expiration_date`, `option_type`, and `strike`
- `tickers`: the union of stock symbols and option-underlying symbols; this expands
  the effective fetch universe and controls same-day expiration retention

Missing, malformed, or unsupported files return an empty `PositionSet` instead of
raising, matching the fetch pipeline's graceful fallback behavior.

---

## 4. `DatasetHandle` Contract

`DatasetHandle` is the stable reference returned by `get_dataset`. The following
fields are part of the external interface contract:

```python
@dataclass
class DatasetHandle:
    dataset_id: str       # stable identifier for this dataset
    run_id: str           # fetch run that produced this dataset
    provider: str         # provider that produced this dataset
    location: str         # absolute or relative path to the artifact file
    schema_version: int   # matches SCHEMA_VERSION at write time
    script_version: str   # opx-chain package version that wrote the dataset
    row_count: int        # total rows in the artifact
    format: str           # "csv" | "parquet"
    content_hash: str     # SHA-256 of artifact bytes; use for integrity checks
    created_at: datetime  # UTC timestamp when the dataset was written
```

**Change from STORAGE_SPEC §6:** `run_id`, `provider`, `content_hash`,
`created_at`, and `script_version` are exposed on `DatasetHandle`. Downstream
consumers need these for dataset provenance, direct provider lookup by
`dataset_id`, chain integrity verification, freshness checks, and
producer-version provenance without having to fetch the full `DatasetRecord` or
scan a paginated dataset listing.

`location` is an absolute path when the filesystem backend is active. Downstream
consumers must not construct or infer artifact paths independently — always use the
`location` field from the handle.

---

## 5. Schema Version Contract

### 5.1 `SCHEMA_VERSION` constant

```python
# opx_chain/__init__.py
SCHEMA_VERSION: int = 2   # incremented on every breaking schema change
```

This integer is the join key between the chain artifact and the consumer's field
expectations. It is written into every `DatasetRecord` and `DatasetHandle` at write
time.

### 5.2 Breaking vs non-breaking changes

| Change type | Version bump required |
|---|---|
| Column removed | Yes |
| Column renamed | Yes |
| Column order changed | Yes |
| Column added (appended) | Yes — downstream must handle unknown columns gracefully, but version still bumps |
| Value format change (e.g., date string format) | Yes |
| Internal scoring weight change | No |
| New provider added | No |

### 5.3 Consumer responsibility

A consumer that detects `schema_version != SCHEMA_VERSION` must not read the
artifact. It should surface a clear error: `chain schema version mismatch:
expected {expected}, got {actual}`. The operator must either re-fetch with the
current `opx-chain` version or update the consumer to support the new schema.

Backward compatibility across schema versions is not guaranteed.

### 5.4 `PRICE_CONTEXT_SCHEMA_VERSION` constant

Optional price context is a separate artifact contract, not part of the
option-chain CSV schema.

```python
# opx_chain.price_context
PRICE_CONTEXT_SCHEMA_VERSION = 1
PriceContextStatus.FRESH.value == "FRESH"
```

The latest standalone artifact is written as `price_context_latest.json` under
the runs directory, with timestamped copies named
`price_context_YYYYMMDD_HHMMSS.json`.
It is derived from the local daily-bar history store (`price-history.db`) after
incremental reconciliation with the active provider; old stored bars are reused
and only missing/backfill/tail history is fetched.

```json
{
  "artifact_type": "price_context",
  "schema_version": 1,
  "provider": "marketdata",
  "fetched_at": "2026-05-06T20:00:00Z",
  "tickers": ["TSLA"],
  "records": [
    {
      "ticker": "TSLA",
      "support_1": 100.0,
      "price_context_as_of": "2026-05-06",
      "price_context_staleness_status": "FRESH"
    }
  ]
}
```

Consumers join `records[].ticker` to option-chain `underlying_symbol` when they
need row-level price context.

`records[].price_context_staleness_status` uses the stable
`PriceContextStatus` vocabulary: `FRESH`, `STALE`, `MISSING`, and `ERROR`.

---

## 6. Staleness Contract

A downstream consumer is responsible for determining whether the latest dataset
is fresh enough for its purposes. `opx-chain` does not enforce freshness on behalf of
consumers.

The consumer should use `DatasetHandle.created_at` as the dataset-level timestamp.
For per-ticker freshness, the chain artifact includes `underlying_price_time` per
row. Optional price context is a separate JSON artifact with its own
`PRICE_CONTEXT_SCHEMA_VERSION`, `price_context_as_of`, `price_context_age_days`,
and `price_context_staleness_status`; consumers should apply a separate
freshness policy to those slower-moving daily-OHLCV fields rather than treating
them like intraday option quotes.

`opx-chain` does not expose a staleness API. The consumer decides what "fresh enough"
means and blocks its own pipeline when the threshold is exceeded.

---

## 7. Implemented Interface Notes

The following notes record the implemented behavior behind this interface. They
are present-tense contract notes, not pending TODOs.

### 7.1 `SCHEMA_VERSION` public constant

`SCHEMA_VERSION` lives in `opx_chain/__init__.py` and is the canonical public
constant for option-chain artifact schema compatibility. Export and storage code
read this constant rather than defining a second schema version value.
`DatasetRecord.schema_version` and `DatasetHandle.schema_version` carry the
writer's schema value for downstream compatibility checks.

### 7.2 `DatasetHandle` provenance and integrity fields

`DatasetHandle` includes `run_id`, `provider`, `content_hash`, `created_at`,
and `script_version` in addition to artifact location, format, row count, and
schema version. `get_dataset()` populates those fields from the persisted
`DatasetRecord` so consumers do not need to scan paginated dataset listings for
basic provenance and integrity checks.

### 7.3 Add `--positions` argument to `opx-fetch`

Implemented. Behaviour is specified in `docs/PROJECT_SPEC.md` §7.3.

### 7.4 `get_storage_backend()` public factory function

Implemented. `opx_chain.storage.factory.get_storage_backend()` returns a
`StorageBackend` instance configured from the `opx-chain` config, or `None` when
storage is disabled. Enabled backends are memoized within the process by the
storage-affecting config values, so repeated calls return the same backend
instance until storage config changes or the cache is cleared.

### 7.5 `also_write_csv` config option

When `[storage] also_write_csv = false` (default `true`), `opx-fetch` skips
writing the timestamped
`<data-dir>/runs/options_engine_output_<ts>.csv` file. `<data-dir>` is
`[storage].dir` when configured and otherwise `$XDG_DATA_HOME/opx-chain`. Only the
storage-managed artifact is written. Downstream orchestrators that read the
timestamped filename pattern must either keep `also_write_csv = true` or switch to
reading through `get_storage_backend().list_datasets()`.

### 7.6 `opx-view --data-dir` and `--csv`

`opx-view` accepts a `--data-dir DIR` argument that overrides all dataset
discovery — it scans `DIR` for `.csv` and `.parquet` files ordered by
modification time. The `--csv` flag skips the storage backend and reads
timestamped CSV exports directly from the output directory. The default
behavior queries the storage backend first, falling back to the timestamped
CSV glob when no storage records exist.

### 7.7 `get_run()` on `StorageBackend`

`StorageBackend.get_run(run_id: str) -> RunRecord` is part of the formal storage
protocol and is implemented by the filesystem, SQLite, and memory backends.
Downstream consumers may call it through the typed interface to retrieve
run-level provenance such as effective tickers and positions fingerprint.

---

## 8. What Does Not Change

- CSV output format and column order (governed by `SCHEMA_VERSION`)
- output directory layout
- `opx-fetch` fetch logic, provider adapters, scoring, or normalization
- `StorageBackend` write interface — consumers are read-only; they never call
  `create_run`, `write_dataset`, or any write method
- `opx-chain` config file format

---

## 9. Relationship to STORAGE_SPEC

This document and `docs/STORAGE_SPEC.md` are complementary:

- `STORAGE_SPEC.md` specifies the full internal storage architecture, all backends,
  the implementation order, and the testing strategy
- this document specifies the external-facing subset of that architecture that
  downstream consumers may depend on

When STORAGE_SPEC changes affect the public surface (e.g., a new field on
`DatasetHandle`), this document must be updated in the same commit.
