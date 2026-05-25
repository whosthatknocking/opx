# AGENTS.md

This file gives project-specific guidance to AI agents working in this repository.

## Project Context

- Project: `opx-chain`
- Purpose: fetch near-term option chains from one configured provider, normalize them into a canonical CSV schema, enrich them with shared analytics, and serve a local viewer for inspection
- Runtime: Python `3.10+`
- Main entrypoints:
  - `opx-fetch` for data collection and CSV export
  - `opx-view` for the local HTTP viewer
  - `opx-check` for validating positions against the latest exported dataset
  - `opx-price-history-backfill` for refreshing provider-scoped daily OHLCV history
  - `opx-iv-history-backfill` for replaying or seeding durable IV history
- Packaging:
  - install with `python -m pip install -e .`
  - dev install with `python -m pip install -e ".[dev]"`

## Source of Truth

When behavior, naming, or scope is unclear, use these files in this order:

1. `AGENTS.md`
2. `docs/PROJECT_SPEC.md`
3. `docs/USER_GUIDE.md`
4. `docs/FIELD_REFERENCE.md`
5. `docs/STORAGE_SPEC.md`
6. `docs/METADATA_SPEC.md`
7. `docs/EXTERNAL_INTERFACE_SPEC.md`
8. `docs/DEVELOPMENT.md`
9. `README.md`
10. `docs/DESIGN_SPEC.md` for UI direction

Keep those files aligned with the implementation. If you change canonical fields, provider behavior, config keys, CLI behavior, viewer behavior, or validation semantics, update the docs in the same task.

## Architecture Map

- `opx_chain/fetcher.py`
  - CLI entrypoint for fetch runs
  - runtime config reporting
  - fetch lock handling
  - export writing and run-level validation
- `opx_chain/fetch.py`
  - per-ticker fetch orchestration
  - expiration filtering
  - provider execution, normalization, filtering, and progress logging
- `opx_chain/config.py`
  - config loading from `$XDG_CONFIG_HOME/opx-chain/config.toml` (default `~/.config/opx-chain/config.toml`)
  - defaults, fallback warnings, provider selection, and runtime override support
- `opx_chain/paths.py`
  - XDG config, data, state, cache, positions, and viewer preference path resolution
- `opx_chain/positions.py`
  - Fidelity positions parsing, ticker expansion, option-key extraction, and default positions path
- `opx_chain/check_positions.py`
  - `opx-check` CLI entrypoint for confirming portfolio tickers are represented in the latest dataset
- `opx_chain/providers/`
  - provider contract in `base.py`
  - vendor implementations in `yfinance.py`, `massive.py`, and `marketdata.py`
- `opx_chain/normalize.py`
  - canonical field normalization
  - shared post-download filters
  - enrichment handoff into pricing and freshness metrics
- `opx_chain/metrics.py` and `opx_chain/greeks.py`
  - derived analytics, scoring, and options math
- `opx_chain/price_history.py` and `opx_chain/price_history_backfill.py`
  - durable daily OHLCV history storage and provider-scoped backfill CLI
- `opx_chain/iv_history.py` and `opx_chain/iv_history_backfill.py`
  - durable aggregate implied-volatility history and retained/historical backfill CLI
- `opx_chain/volatility_features.py`
  - public volatility feature builders for downstream advisory consumers
- `opx_chain/export.py`
  - canonical export column handling and CSV writing
- `opx_chain/validate.py`
  - row-level and export-level validation
- `opx_chain/schema.py`
  - shared canonical schema constants, quality flags, and boolean-field vocabulary
- `opx_chain/storage/`
  - storage backend protocol, metadata models, serializers, provider cache, and filesystem/SQLite/memory implementations
- `opx_chain/runlog.py`
  - shared fetch-run logging and log path reporting
- `opx_chain/utils.py`
  - stable dataset reader and small scalar/file utility helpers shared across fetch, storage, and downstream consumers
- `opx_chain/version.py`
  - package version lookup used by runtime reporting
- `opx_chain/viewer.py`
  - local HTTP server
  - CSV discovery and serialization
  - dataset summaries and reference content wiring
- `opx_chain/viewer_static/`
  - frontend assets for the local viewer

## Non-Negotiable Design Rules

- Preserve the canonical CSV schema as the primary product contract unless the docs are updated deliberately.
- Keep exactly one active provider per run. Do not mix rows from multiple providers in one export.
- Prefer mapping provider-native values into canonical columns over adding provider-specific scratch fields.
- Keep shared metrics provider-agnostic once rows have been normalized.
- Do not add secrets to tracked files, logs, docs, or debug dumps.
- Do not silently reinterpret provider data when semantics do not match. Leave fields blank rather than map misleading values.
- Keep the viewer as an inspection tool, not a trading terminal or decision engine.
- Maintain stable output and behavior across fetch, export, validation, and viewer layers together.

## Provider and Pipeline Conventions

- Add or change provider-specific market-data logic under `opx_chain/providers/`.
- Route provider payloads through the shared `DataProvider` contract; do not bypass it from fetch orchestration.
- Normalize vendor frames through `normalize_provider_frame(...)` or equivalent provider methods before enrichment.
- Keep provider debug dumps representative of the raw upstream payload shape.
- Respect config-driven pacing, retry, credential, and mode behavior already implemented in the provider layer.
- Use shared post-download filters as the main tradability gate unless there is a documented reason to narrow data earlier.
- If a provider plan or upstream API is delayed, sparse, or unreliable, document that caveat clearly instead of presenting the data as fresher or more complete than it is.

## Config and Runtime Rules

- Runtime settings come from `$XDG_CONFIG_HOME/opx-chain/config.toml` (default `~/.config/opx-chain/config.toml`); `config/example.toml` is the tracked template.
- Defaults and fallback warnings in `opx_chain/config.py` are part of the product behavior. Keep startup reporting accurate if config handling changes.
- If the selected provider is misconfigured, preserve the current clear fallback or failure behavior rather than failing ambiguously.
- Secrets must stay redacted in any user-facing output.

## Viewer and Export Conventions

- Keep exported CSVs under `$XDG_DATA_HOME/opx-chain/runs/`, logs under `$XDG_STATE_HOME/opx-chain/logs/`, optional provider payload dumps under `$XDG_DATA_HOME/opx-chain/debug/`, and filesystem provider cache files under `$XDG_CACHE_HOME/opx-chain/cache/`.
- If you change exported columns, also update the viewer serialization assumptions and field-reference docs.
- Keep viewer endpoints and payloads aligned with the current tab model: `Dataset`, `Overview`, `Chain View`, and `Reference`.
- Do not reintroduce rich portfolio/positions browsing in opx-chain. The viewer may show only lightweight positions counts, parsed-position fingerprint, and chain coverage metadata; portfolio-specific browsing belongs in opx-strategy.
- Use JSON-serializable payloads only when sending data to the browser.

## Package Boundary and Downstream Consumers

`opx-chain` is the market-data collection, normalization, storage, and export
package. Downstream packages such as `opx-strategy` may import only documented
public APIs for option-chain datasets, storage handles, provider-owned
market-data artifacts, and positions parsing.

Boundary rules:

- Never import `opx_strategy` or add strategy-layer policy here. Prompt rules,
  strategy constraints, decision schemas, trade validation policy, run
  lifecycle, server behavior, UI behavior, and rendered strategy output belong
  to `opx-strategy`.
- Generic helper duplication across packages is acceptable when the behavior is
  package-local: JSON strict/sanitize helpers, timestamp formatting,
  timestamp-age handling, XDG/path helpers, logger helpers, CLI/test guards,
  display formatting, or error wording. Fix concrete defects locally instead of
  creating shared glue.
- Promote behavior to an `opx-chain` public contract only when it defines the
  market-data, storage, export, dataset, option-chain, or positions-parsing
  surface downstream consumers rely on. Document that contract in
  `docs/EXTERNAL_INTERFACE_SPEC.md` and expose it through a stable module.
- Do not create an `opx_common` package or shared cross-repo module to remove
  small duplication unless the operator explicitly approves that architecture
  change and the public contract is documented.

## Error Handling and Stability

- Raise clear project-appropriate errors for config, authentication, mapping, and validation failures.
- Do not leak raw provider exceptions to users when the app can normalize them into a clearer failure mode.
- Preserve fetch locking, validation reporting, and run logging unless there is a strong reason to change them.
- Be careful with market-open and stale-quote edge cases. Freshness fields are user-facing and should remain trustworthy.

## Local Style Contracts

- Route opx-chain package loggers through `opx_chain.runlog.get_logger(...)`
  / `logger_name(...)`; do not repeat `opx_chain.*` logger-name literals in
  production code or tests.
- Keep production logger handles module-local and named `_LOGGER` when a module
  needs a reusable logger.
- Route UTC timestamp displays and artifact/run-id filename timestamps through
  `opx_chain.timestamps` helpers/constants. Do not repeat canonical
  second-precision, microsecond-precision, or compact UTC `strftime(...)`
  format strings at call sites.

## Testing Expectations

Run the smallest relevant test set first, then broaden if needed.

- Main suite: `pytest`
- Lint: `pylint $(git ls-files '*.py')`

Testing guidance:

- Add or update tests for any behavior change in provider mappings, normalization, metrics, validation, export shape, or viewer payloads.
- Prefer offline, deterministic tests by default.
- If a change depends on live upstream provider behavior, say so explicitly and note what was not verified locally.
- If you change docs-visible output fields or viewer summaries, add or update focused tests where practical.

## Documentation Expectations

Update docs when any of these change:

- canonical field names or meanings
- provider selection or credential behavior
- config keys or defaults
- filter or validation behavior
- CLI flags or run instructions
- viewer tabs, summaries, or reference behavior
- supported or unsupported provider capabilities

Common files to update:

- `README.md`
- `docs/PROJECT_SPEC.md`
- `docs/USER_GUIDE.md`
- `docs/FIELD_REFERENCE.md`
- `docs/DEVELOPMENT.md`

## Practical Workflow

1. Read the affected code and the matching contract docs first.
2. Make the smallest coherent change that keeps fetch, export, validation, and viewer behavior aligned.
3. Update tests with the code change.
4. Update docs if user-facing behavior changed.
5. Run targeted verification, then broaden if warranted.

## Commit and PR Guidance

- Use imperative commit subjects, for example `docs: add provider-mapping guidance`.
- Keep commits small, single-purpose, and easy to review.
- Include tests with behavior changes in the same commit when practical.
- Avoid mixing unrelated refactors with schema or provider behavior changes.
- In PRs, summarize intent briefly and list the validation steps actually run.
- If validation was skipped or limited, say so explicitly.

## Repository-Specific Notes

- The package version is defined in `pyproject.toml`.
- Current supported providers are `yfinance`, `massive`, and `marketdata`.
- `opx-view` remains the stable viewer CLI entrypoint; implementation lives in
  `opx_chain.viewer`.
- This project is the data and screening layer. The portfolio decision engine is downstream and should not be collapsed into the fetch/viewer runtime.

## Good Changes

- tightening a provider-to-canonical field mapping with tests
- improving normalization, freshness, or scoring logic while preserving documented schema intent
- making config fallback behavior clearer and better documented
- fixing CSV serialization or viewer payload edge cases
- updating docs so they match the actual provider and viewer behavior

## Bad Changes

- mixing providers in one export file
- adding undocumented columns casually to the canonical CSV
- exposing secrets or raw credentials in config examples, logs, or dumps
- bypassing the provider contract from fetch orchestration
- changing viewer or export behavior without updating docs and tests

## Commit conventions

- Do not add `Co-Authored-By` trailers to commit messages.
- Do not include assistant/tool attribution in commit messages, PR bodies, or other artifacts.
- Commit subject line: ≤72 characters, imperative mood.
- Commit body: explain what changed and the motivation. Do not describe the task or reference issue numbers.

## No Personal Information in Tracked Files

Never write local machine paths, usernames, or host identifiers into any tracked file.

Rules:
- Markdown links must use repo-relative paths, never absolute filesystem paths.
  - From the repo root: `[text](docs/FILE.md)`, `[text](config/example.toml)`
  - From `docs/`: `[text](../config/example.toml)`, `[text](OTHER.md)`
- Do not embed hostnames, local usernames, or machine-specific paths in docs, config examples, scripts, or test fixtures.
- `127.0.0.1` is acceptable as a documented default bind address; it is not personal information.

Before committing any `.md`, `.toml`, or `.py` file that contains a path or link, verify it does not start with `/Users/`, `/home/`, or any other absolute filesystem prefix.
