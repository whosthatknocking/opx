from datetime import datetime

from options_fetcher_app.config import MAX_EXPIRATION, TICKERS, today
from options_fetcher_app.export import write_options_csv
from options_fetcher_app.fetch import fetch_ticker_option_chain


def main():
    print(f"Today: {today}")
    print(f"Max expiration: {MAX_EXPIRATION}")

    ticker_frames = []
    for ticker in TICKERS:
        print(f"Loading {ticker}")
        ticker_df = fetch_ticker_option_chain(ticker)
        if not ticker_df.empty:
            ticker_frames.append(ticker_df)

    if not ticker_frames:
        print("No data fetched.")
        raise SystemExit(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"options_engine_output_{timestamp}.csv"
    write_options_csv(ticker_frames, output_path=output_path)
    print(f"\nSaved: {output_path}")


if __name__ == "__main__":
    main()
