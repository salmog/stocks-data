# daily_stock_fetch_all_timeframe_data.py
# Fetches historical data for multiple stock tickers (e.g., QQQ, AAPL, MSFT) for multiple timeframes
# (daily, 4h, weekly) using yfinance, updates existing CSV files, and handles deduplication and time zone consistency.

import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta
import pytz

# ----------------------------------
# 1. Configuration and Setup
# ----------------------------------
# Define list of tickers to fetch data for (configurable)
tickers = ["QQQ", "AAPL", "MSFT"]  # Add or remove tickers as needed
output_folder = "./stock_data"  # Output directory for all tickers

# Define Eastern Time (US market time zone)
ET = pytz.timezone('US/Eastern')

# Configuration for each timeframe: interval, max days, filename template, overlap days
configs = {
    "daily_10y": {
        "interval": "1d",
        "max_days": 3650,  # ~10 years
        "filename_template": "{output_folder}/{ticker}_daily_10y.csv",
        "overlap_days": 2
    },
    "4h_729d": {
        "interval": "4h",
        "max_days": 729,  # Yahoo Finance limit for 4h
        "filename_template": "{output_folder}/{ticker}_4h_729d.csv",
        "overlap_days": 1
    },
    "weekly_10y": {
        "interval": "1wk",
        "max_days": 3650,  # ~10 years
        "filename_template": "{output_folder}/{ticker}_weekly_10y.csv",
        "overlap_days": 7
    }
}

# Create output directory if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# ----------------------------------
# 2. Utility Functions
# ----------------------------------
def format_date(dt, interval):
    """Format datetime to string for yfinance API (YYYY-MM-DD)."""
    return dt.strftime('%Y-%m-%d')

# ----------------------------------
# 3. File Reading and Date Checking
# ----------------------------------
def get_last_date_or_none(filename, interval):
    """
    Read the last date from a CSV file and check if it's up-to-date.
    Returns None if file is missing, corrupt, or up-to-date; otherwise, returns the last date (tz-aware).
    """
    if not os.path.exists(filename):
        print(f"⚠️ File {filename} does not exist.")
        return None

    try:
        # Check file permissions
        if not os.access(filename, os.R_OK):
            print(f"⚠️ No read permission for {filename}. Will re-download.")
            return None

        # Verify CSV structure with a small sample
        df = pd.read_csv(filename, nrows=5)
        expected_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in df.columns for col in expected_columns):
            print(f"⚠️ Invalid columns in {filename}. Expected: {expected_columns}, Found: {df.columns.tolist()}\nFile content preview:\n{df.head()}\nWill re-download.")
            return None

        # Read full file for date processing
        df = pd.read_csv(filename)
        if interval == "4h":
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce', format='%Y-%m-%d %H:%M:%S').dt.tz_localize(ET)
        else:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce', format='%Y-%m-%d').dt.tz_localize(ET)
        df.dropna(subset=['Date'], inplace=True)
        if df.empty:
            print(f"⚠️ No valid rows in {filename}. File content preview:\n{df.head()}\nWill re-download.")
            return None

        last_date = df['Date'].max()
        if last_date.year < 2000:
            print(f"⚠️ Suspicious last date in {filename}: {last_date}. File content preview:\n{df.head()}\nWill re-download.")
            return None

        # Check if data is up-to-date
        now = datetime.now(ET)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        market_open = today.replace(hour=9, minute=30, tzinfo=ET)

        if interval == "4h":
            if last_date.date() < today.date() or (last_date.date() == today.date() and now >= market_open):
                return last_date
            print(f"✅ {filename} is up-to-date (last date: {last_date}). Skipping fetch.")
            return None
        else:
            if last_date.date() < today.date():
                return last_date
            print(f"✅ {filename} is up-to-date (last date: {last_date}). Skipping fetch.")
            return None

    except Exception as e:
        print(f"⚠️ Failed to read {filename}: {e}\nFile content preview:\n{df.head() if 'df' in locals() else 'No data read'}\nWill re-download.")
        return None

# ----------------------------------
# 4. Data Fetching
# ----------------------------------
def fetch_new_data(ticker, start_date, end_date, interval):
    """
    Fetch data from Yahoo Finance for the given ticker, date range, and interval.
    Returns a DataFrame with cleaned data or an empty DataFrame if no data is fetched.
    """
    try:
        data = yf.download(
            ticker,
            start=format_date(start_date, interval),
            end=format_date(end_date, interval),
            interval=interval,
            auto_adjust=True,
            progress=False,
            threads=True,
            group_by='column'
        )
        if data.empty:
            return data

        # Flatten MultiIndex columns if present
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [col[0] if isinstance(col, tuple) else col for col in data.columns]
        data = data[["Open", "High", "Low", "Close", "Volume"]]
        data.index.name = "Date"
        data.reset_index(inplace=True)

        # Format dates for saving
        if interval == "4h":
            data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')

        return data
    except Exception as e:
        print(f"❌ Error downloading {ticker} ({interval}): {e}")
        return pd.DataFrame()

# ----------------------------------
# 5. Data Merging and Deduplication
# ----------------------------------
def merge_and_deduplicate(existing_df, new_data, interval):
    """
    Merge existing and new data, remove duplicates, and sort by date.
    Returns the combined DataFrame.
    """
    if existing_df is None:
        return new_data

    try:
        expected_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in existing_df.columns for col in expected_columns):
            print(f"⚠️ Corrupt file — invalid columns. Expected: {expected_columns}, Found: {existing_df.columns.tolist()}\nFile content preview:\n{existing_df.head()}\nOverwriting with fresh data.")
            return new_data

        existing_df['Date'] = existing_df['Date'].astype(str)
        new_data['Date'] = new_data['Date'].astype(str)
        print(f"🔍 Before concat: existing rows={len(existing_df)}, new rows={len(new_data)}")
        combined = pd.concat([existing_df, new_data])
        combined = combined.drop_duplicates(subset=['Date'], keep='last')
        combined.sort_values(by="Date", inplace=True)
        print(f"🔍 After concat: total rows={len(combined)}, duplicates removed: {len(existing_df) + len(new_data) - len(combined)}")
        return combined
    except Exception as e:
        print(f"⚠️ Error merging data: {e}. File content preview:\n{existing_df.head() if 'existing_df' in locals() else 'No data read'}\nOverwriting with fresh data.")
        return new_data

# ----------------------------------
# 6. Data Saving and Verification
# ----------------------------------
def save_and_verify_data(combined, filename, interval):
    """Save the combined DataFrame to CSV and verify the save operation."""
    try:
        combined.to_csv(filename, index=False)
        verify_df = pd.read_csv(filename)
        expected_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in verify_df.columns for col in expected_columns):
            print(f"⚠️ Failed to save {filename} correctly: invalid columns. Expected: {expected_columns}, Found: {verify_df.columns.tolist()}\nFile content preview:\n{verify_df.head()}")
        else:
            if interval == "4h":
                verify_last_date = pd.to_datetime(verify_df['Date'], errors='coerce', format='%Y-%m-%d %H:%M:%S').max()
            else:
                verify_last_date = pd.to_datetime(verify_df['Date'], errors='coerce', format='%Y-%m-%d').max()
            print(f"✅ Saved {len(combined) - (len(verify_df) - len(combined))} new rows to {filename}. Total rows: {len(combined)}, Last date: {verify_last_date}")
    except Exception as e:
        print(f"⚠️ Failed to verify {filename} after saving: {e}")

# ----------------------------------
# 7. Main Processing Loop
# ----------------------------------
print("\n📊 Starting historical data fetch for multiple tickers...\n")

for ticker in tickers:
    print(f"\n📈 Processing ticker: {ticker}\n")
    for label, cfg in configs.items():
        # Generate filename for this ticker and timeframe
        filename = cfg["filename_template"].format(output_folder=output_folder, ticker=ticker.lower())
        interval = cfg["interval"]
        max_days = cfg["max_days"]
        overlap_days = cfg["overlap_days"]

        # Get last date from existing file
        last_date = get_last_date_or_none(filename, interval)

        # Determine start date for fetching
        if last_date is None and not os.path.exists(filename):
            start_date = datetime.now(ET) - timedelta(days=max_days)
            print(f"📁 No existing data for {ticker} {label}. Fetching full {max_days}-day range from {format_date(start_date, interval)}.")
        elif last_date is None:
            print(f"📁 Existing file {filename} is corrupt or unreadable. Will re-download.")
            start_date = datetime.now(ET) - timedelta(days=max_days)
        else:
            start_date = last_date - timedelta(days=overlap_days)
            print(f"🔍 Existing data for {ticker} {label}. Last saved: {last_date}. Refetching from {format_date(start_date, interval)}.")

        # Enforce Yahoo 4h limit
        if interval == "4h":
            max_start = datetime.now(ET) - timedelta(days=729)
            if start_date < max_start:
                start_date = max_start
                print(f"⚠️ Adjusted 4h start date to Yahoo limit: {format_date(start_date, interval)}")

        # Extend end date to tomorrow
        end_date = datetime.now(ET) + timedelta(days=1)
        print(f"⏳ Downloading {ticker} {label} ({interval}) from {format_date(start_date, interval)} to {format_date(end_date, interval)}...")

        # Fetch new data
        new_data = fetch_new_data(ticker, start_date, end_date, interval)
        if new_data.empty:
            print(f"⚠️ No new data fetched for {ticker} {label}. Skipping save.\n")
            continue

        # Filter new data to only include dates beyond last_date
        new_data['Date'] = pd.to_datetime(new_data['Date'], errors='coerce')
        if last_date is not None and not new_data.empty:
            new_data = new_data[new_data['Date'].dt.tz_localize(None) > last_date.replace(tzinfo=None)]
            if new_data.empty:
                print(f"⚠️ No new dates beyond {last_date} for {ticker} {label}. Skipping save.\n")
                continue

        # Load existing data if file exists
        existing_df = None
        if os.path.exists(filename):
            if not os.access(filename, os.W_OK):
                print(f"⚠️ No write permission for {filename}. Skipping save.")
                continue
            try:
                existing_df = pd.read_csv(filename)
            except Exception as e:
                print(f"⚠️ Error reading existing {filename}: {e}. File content preview:\n{existing_df.head() if 'existing_df' in locals() else 'No data read'}\nOverwriting with fresh data.")

        # Merge and deduplicate
        combined = merge_and_deduplicate(existing_df, new_data, interval)

        # Save and verify
        save_and_verify_data(combined, filename, interval)

print("🎉 All tickers and timeframes updated successfully!\n")
