from pathlib import Path

import pandas as pd

import main


class StubLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None


def test_main_prints_rows_written_before_saved(monkeypatch, capsys, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "TICKERS", ["AAA", "BBB"])
    monkeypatch.setattr(main, "today", "2026-03-20")
    monkeypatch.setattr(main, "MAX_EXPIRATION", "2026-06-30")
    monkeypatch.setattr(main, "create_run_logger", lambda: (StubLogger(), Path("logs/run.log")))

    frames = {
        "AAA": pd.DataFrame([{"x": 1}, {"x": 2}]),
        "BBB": pd.DataFrame([{"x": 3}]),
    }
    monkeypatch.setattr(main, "fetch_ticker_option_chain", lambda ticker, logger=None: frames[ticker])

    written = {}

    def stub_write_options_csv(ticker_frames, output_path):
        written["rows"] = sum(len(frame) for frame in ticker_frames)
        written["path"] = output_path

    monkeypatch.setattr(main, "write_options_csv", stub_write_options_csv)

    main.main()

    stdout = capsys.readouterr().out
    assert "Rows written: 3" in stdout
    assert f"Saved: {written['path']}" in stdout
    assert stdout.index("Rows written: 3") < stdout.index(f"Saved: {written['path']}")
