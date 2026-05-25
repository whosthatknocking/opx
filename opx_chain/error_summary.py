"""Helpers for stable compact exception summaries."""

EXCEPTION_SUMMARY_MAX_LENGTH = 240
DEFAULT_EXCEPTION_SUMMARY = "unknown error"


def compact_exception_summary(
    exc: BaseException,
    *,
    include_type: bool = False,
    fallback: str = DEFAULT_EXCEPTION_SUMMARY,
    max_length: int = EXCEPTION_SUMMARY_MAX_LENGTH,
) -> str:
    """Return a single-line exception summary without assuming str(exc) is nonblank."""
    first_line = next((line.strip() for line in str(exc).splitlines() if line.strip()), "")
    if include_type:
        summary = f"{type(exc).__name__}: {first_line}" if first_line else type(exc).__name__
    else:
        summary = first_line or type(exc).__name__ or fallback
    return (summary or fallback)[:max_length]
