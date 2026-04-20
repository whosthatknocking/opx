"""CLI tool to verify that every option position appears in the latest output CSV."""
from pathlib import Path
from numbers import Real

import pandas as pd

from opx.config import get_runtime_config
from opx.positions import DEFAULT_POSITIONS_PATH, load_positions

OUTPUTS_DIR = Path("output")


def find_latest_output(outputs_dir: Path = OUTPUTS_DIR) -> Path | None:
    """Return the most recently modified CSV in the outputs directory."""
    csvs = sorted(outputs_dir.glob("options_engine_output_*.csv"), key=lambda p: p.stat().st_mtime)
    return csvs[-1] if csvs else None


def check_positions(positions_path: Path | None = None, output_path: Path | None = None):
    """Check every option position against the given (or latest) output CSV.

    Returns a tuple of (found, missing) lists where each element is an OptionPositionKey.
    """
    resolved_positions = (positions_path or DEFAULT_POSITIONS_PATH).expanduser()
    position_set = load_positions(resolved_positions)

    if position_set.empty:
        return [], []

    resolved_output = output_path or find_latest_output()
    if resolved_output is None or not resolved_output.exists():
        return [], list(position_set.option_keys)

    df = pd.read_csv(resolved_output, low_memory=False)

    found, missing = [], []
    for key in sorted(
        position_set.option_keys,
        key=lambda k: (k.ticker, k.expiration_date, k.option_type),
    ):
        mask = (
            (df["underlying_symbol"] == key.ticker)
            & (df["expiration_date"] == key.expiration_date)
            & (df["option_type"] == key.option_type)
            & ((df["strike"] - key.strike).abs() < 0.01)
        )
        if df[mask].empty:
            missing.append(key)
        else:
            found.append((key, df[mask].iloc[0]))
    return found, missing


def _is_true_like(value) -> bool:
    """Interpret common boolean-like CSV values."""
    return str(value).strip().lower() in {"true", "1", "yes"}


def _format_filter_value(value) -> str:
    """Format row and threshold values for concise CLI output."""
    if value is None or pd.isna(value):
        return "missing"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, Real):
        return f"{float(value):.4f}"
    return str(value)


def _append_filter_failure(
    failures: list[str],
    *,
    filter_name: str,
    row_value,
    threshold_value,
    operator: str,
) -> None:
    """Append one failed filter note using canonical config naming."""
    failures.append(
        f"{filter_name}({_format_filter_value(row_value)}"
        f"{operator}{_format_filter_value(threshold_value)})"
    )


def _get_failed_primary_screen_filters(row: pd.Series) -> list[str]:
    """Return the configured primary-screen filters that the row fails."""
    config = get_runtime_config()
    failures: list[str] = []

    if config.min_bid is not None:
        bid = pd.to_numeric(row.get("bid"), errors="coerce")
        if pd.isna(bid) or bid < config.min_bid:
            _append_filter_failure(
                failures,
                filter_name="filters_min_bid",
                row_value=bid,
                threshold_value=config.min_bid,
                operator="<",
            )

    spread_pct = pd.to_numeric(row.get("bid_ask_spread_pct_of_mid"), errors="coerce")
    if pd.isna(spread_pct) or spread_pct > config.max_spread_pct_of_mid:
        _append_filter_failure(
            failures,
            filter_name="filters_max_spread_pct_of_mid",
            row_value=spread_pct,
            threshold_value=config.max_spread_pct_of_mid,
            operator=">",
        )

    open_interest = pd.to_numeric(row.get("open_interest"), errors="coerce")
    if pd.isna(open_interest) or open_interest < config.min_open_interest:
        _append_filter_failure(
            failures,
            filter_name="filters_min_open_interest",
            row_value=open_interest,
            threshold_value=config.min_open_interest,
            operator="<",
        )

    volume = pd.to_numeric(row.get("volume"), errors="coerce")
    if pd.isna(volume) or volume < config.min_volume:
        _append_filter_failure(
            failures,
            filter_name="filters_min_volume",
            row_value=volume,
            threshold_value=config.min_volume,
            operator="<",
        )

    return failures


def _format_found_position_line(key, row: pd.Series) -> str:
    """Build one CLI output line for a found portfolio position."""
    passes = row.get("passes_primary_screen")
    screen_status = f"passes_primary_screen={'true' if _is_true_like(passes) else 'false'}"
    failed_filters = (
        _get_failed_primary_screen_filters(row) if not _is_true_like(passes) else []
    )
    failed_filters_note = (
        f"  failed_filters={','.join(failed_filters)}"
        if failed_filters
        else ""
    )
    return (
        f"  FOUND    {key.ticker:<6} {key.expiration_date}  {key.option_type:<4}  "
        f"strike={key.strike:>7.1f}  bid={row['bid']}  ask={row['ask']}  "
        f"{screen_status}{failed_filters_note}"
    )


def main(argv=None):
    """Print a position coverage report for the latest output CSV."""
    import argparse  # pylint: disable=import-outside-toplevel

    parser = argparse.ArgumentParser(
        prog="opx-check",
        description=(
            "Check that every option position in the portfolio positions CSV "
            "appears in the latest output."
        ),
    )
    parser.add_argument("--positions", type=Path, default=None, help="Path to positions CSV.")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path to output CSV (default: latest).",
    )
    args = parser.parse_args(argv)

    positions_path = (args.positions or DEFAULT_POSITIONS_PATH).expanduser()
    output_path = args.output

    if not positions_path.exists():
        print(f"Positions file not found: {positions_path}")
        return 1

    resolved_output = output_path or find_latest_output()
    if resolved_output is None:
        print(f"No output CSV found in {OUTPUTS_DIR}/")
        return 1

    print(f"Positions: {positions_path}")
    print(f"Output:    {resolved_output}")
    print()

    found, missing = check_positions(positions_path, resolved_output)
    total = len(found) + len(missing)

    if total == 0:
        print("No option positions found in positions file.")
        return 0

    for key, row in found:
        print(_format_found_position_line(key, row))

    for key in missing:
        print(
            f"  MISSING  {key.ticker:<6} {key.expiration_date}  {key.option_type:<4}  "
            f"strike={key.strike:>7.1f}"
        )

    print()
    print(
        f"Result: {len(found)}/{total} positions found"
        + (f"  ({len(missing)} missing)" if missing else "")
    )

    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
