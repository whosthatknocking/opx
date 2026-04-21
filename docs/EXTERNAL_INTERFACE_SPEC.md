# External Interface Specification

This document specifies the stable external interface that `opx` exposes to
downstream consumers. It covers the CLI invocation contract, the Python package
interface, and the schema versioning contract.

`opx` does not own any downstream system and has no dependency on them. This
document describes what `opx` commits to stabilizing so that consumers can
integrate without coupling to internal implementation details.

---

## 1. Scope

Two integration points are in scope:

1. **CLI invocation** — a downstream orchestrator can invoke `opx-fetcher` as a
   subprocess to trigger a fresh chain fetch
2. **Storage interface** — a downstream consumer can import `opx` as a Python
   package and use `StorageBackend` to discover and read the latest chain dataset

Everything else — internal storage layout, provider adapters, scoring weights,
normalization logic — is internal to `opx` and may change without notice.

---

## 2. CLI Invocation Contract

### 2.1 `opx-fetcher`

`opx-fetcher` is the entry point for triggering a fresh option-chain fetch.

A downstream orchestrator invokes it as a subprocess:

```
opx-fetcher [--positions <path>] [--enable-filters | --disable-filters]
```

The orchestrator must:
- wait for the process to exit before querying storage for the new dataset
- treat any non-zero exit code as a fetch failure
- not parse stdout or stderr for structured data; those streams are for logging only

**`--positions <path>` (optional)**

Overrides the default positions file path (`data/positions.csv`). When provided,
`opx-fetcher` uses this file to determine which option contracts must survive hard
filters regardless of screening criteria. When absent, behaviour is unchanged.

A downstream orchestrator that manages a per-run positions file passes the
run-specific path here:

```
opx-fetcher --positions data/runs/<run_id>/positions.csv
```

See `docs/PROJECT_SPEC.md` §7.3 for the full behaviour specification.

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

A downstream consumer may import `opx` as a Python dependency to query the storage
layer without shelling out or scanning the filesystem directly.

### 3.1 Public surface

The stable public surface is limited to:

```python
from opx.storage.base import StorageBackend
from opx.storage.models import DatasetHandle, DatasetRecord
from opx.storage.factory import get_storage_backend
from opx import SCHEMA_VERSION
```

All other modules are internal. Importing from `opx.fetcher`, `opx.normalize`,
`opx.provider`, or any other internal module is not supported and may break across
releases.

### 3.2 Obtaining a backend instance

```python
backend: StorageBackend = get_storage_backend()
```

`get_storage_backend()` returns the configured backend (filesystem or SQLite) based
on the `opx` config. No arguments are required. The consumer must not construct a
backend directly.

### 3.3 Discovering the latest dataset

```python
records: list[DatasetRecord] = backend.list_datasets(limit=1)
```

Returns the most recent successfully written dataset. Returns an empty list if no
datasets exist.

The consumer should validate:
- the list is non-empty (no datasets available → cannot proceed)
- `records[0].schema_version == SCHEMA_VERSION` (schema drift → must re-fetch or
  update the consumer to handle the new schema before proceeding)

### 3.4 Obtaining a dataset handle

```python
handle: DatasetHandle = backend.get_dataset(dataset_id)
```

Returns a `DatasetHandle` for the given `dataset_id`. The consumer reads the chain
artifact at `handle.location`.

### 3.5 Reading the chain artifact

```python
import pandas as pd
df = pd.read_csv(handle.location)  # when handle.format == "csv"
```

The consumer is responsible for choosing the correct reader based on `handle.format`.
When Parquet support is added (STORAGE_SPEC §9, step 4), `handle.format` will be
`"parquet"` for new datasets.

---

## 4. `DatasetHandle` Contract

`DatasetHandle` is the stable reference returned by `get_dataset`. The following
fields are part of the external interface contract:

```python
@dataclass
class DatasetHandle:
    dataset_id: str       # stable identifier for this dataset
    location: str         # absolute or relative path to the artifact file
    schema_version: int   # matches SCHEMA_VERSION at write time
    row_count: int        # total rows in the artifact
    format: str           # "csv" | "parquet"
    content_hash: str     # SHA-256 of artifact bytes; use for integrity checks
    created_at: datetime  # UTC timestamp when the dataset was written
```

**Change from STORAGE_SPEC §6:** `content_hash` and `created_at` are added to
`DatasetHandle`. They were previously only on `DatasetRecord`. Downstream consumers
need both for chain integrity verification and freshness checks without having to
fetch the full `DatasetRecord`.

`location` is an absolute path when the filesystem backend is active. Downstream
consumers must not construct or infer artifact paths independently — always use the
`location` field from the handle.

---

## 5. Schema Version Contract

### 5.1 `SCHEMA_VERSION` constant

```python
# opx/__init__.py  (or opx/export.py)
SCHEMA_VERSION: int = 1   # incremented on every breaking schema change
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
current `opx` version or update the consumer to support the new schema.

Backward compatibility across schema versions is not guaranteed.

---

## 6. Staleness Contract

A downstream consumer is responsible for determining whether the latest dataset
is fresh enough for its purposes. `opx` does not enforce freshness on behalf of
consumers.

The consumer should use `DatasetHandle.created_at` as the dataset-level timestamp.
For per-ticker freshness, the chain artifact includes `underlying_price_time` per
row — the consumer applies its own staleness policy against that field.

`opx` does not expose a staleness API. The consumer decides what "fresh enough"
means and blocks its own pipeline when the threshold is exceeded.

---

## 7. Changes Required

The following changes to `opx` implement this interface. They are ordered by
dependency.

### 7.1 Add `SCHEMA_VERSION` public constant

- add `SCHEMA_VERSION: int = 1` to `opx/__init__.py` (or `opx/export.py`)
- write it into `DatasetRecord.schema_version` on every `write_dataset` call
- this is already described in STORAGE_SPEC §3.4 and §16 step 1; this spec
  makes it a named public constant importable from `opx` directly

### 7.2 Add `content_hash` and `created_at` to `DatasetHandle`

Current `DatasetHandle` (STORAGE_SPEC §6):
```python
dataset_id, location, schema_version, row_count, format
```

Required addition:
```python
content_hash: str     # already on DatasetRecord; copy here
created_at: datetime  # already on DatasetRecord; copy here
```

`get_dataset` must populate both fields from the underlying `DatasetRecord`.
No storage schema change is required — both values are already persisted.

### 7.3 Add `--positions` argument to `opx-fetcher`

Implemented. Behaviour is specified in `docs/PROJECT_SPEC.md` §7.3.

### 7.4 Expose `get_storage_backend()` as a public factory function

`opx.storage.factory.get_storage_backend()` must be importable and return a
`StorageBackend` instance configured from the `opx` config. If this function does
not yet exist, it should be created as part of STORAGE_SPEC step 2.

No arguments; reads config from the standard `opx` config path.

---

## 8. What Does Not Change

- CSV output format and column order (governed by `SCHEMA_VERSION`)
- output directory layout
- `opx-fetcher` fetch logic, provider adapters, scoring, or normalization
- `StorageBackend` write interface — consumers are read-only; they never call
  `create_run`, `write_dataset`, or any write method
- `opx` config file format

---

## 9. Relationship to STORAGE_SPEC

This document and `docs/STORAGE_SPEC.md` are complementary:

- `STORAGE_SPEC.md` specifies the full internal storage architecture, all backends,
  the implementation order, and the testing strategy
- this document specifies the external-facing subset of that architecture that
  downstream consumers may depend on

When STORAGE_SPEC changes affect the public surface (e.g., a new field on
`DatasetHandle`), this document must be updated in the same commit.
