import os
import sys
import argparse
from datetime import datetime, timedelta

# Ensure we can import from src
# Assuming this script is located in fetch_data/utils/
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # fetch_data directory

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Try importing merge_parquet
try:
    from src.process.process_lob import merge_parquet
except ImportError:
    try:
        # Fallback for when running from outside fetch_data or different structure
        from fetch_data.src.process.process_lob import merge_parquet
    except ImportError:
        print("Error: Could not import 'merge_parquet' from 'src.process.process_lob'.")
        print(f"sys.path: {sys.path}")
        sys.exit(1)

def get_date_range(start_date: str, end_date: str):
    """Generate a list of date strings between start and end (inclusive)."""
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        delta = end - start
        return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta.days + 1)]
    except ValueError as e:
        print(f"Error parsing dates: {e}. Please use YYYY-MM-DD format.")
        sys.exit(1)

def merge_daily_data(date_str, data_dir):
    """
    Search for all parquet data (trades and LOB) for a specific date and merge them.
    Iterates through all symbols and subdirectories (trades, LOB variants).
    """
    day_dir = os.path.join(data_dir, date_str)
    if not os.path.exists(day_dir):
        print(f"[{date_str}] Directory not found: {day_dir}")
        return

    print(f"[{date_str}] Scanning {day_dir}...")
    
    # Walk through the directory structure to find all folders containing parquet files
    # Structure example: data/date/SYMBOL/trades/ or data/date/SYMBOL/nSigFigs=5.0/
    for root, dirs, files in os.walk(day_dir):
        # Filter for parquet files
        parquet_files = [f for f in files if f.endswith(".parquet")]
        
        if not parquet_files:
            continue
            
        # Optimization: If only 'merged.parquet' exists, we might skip to save time.
        # But to be safe (in case of re-runs or partial merges), we usually let merge_parquet handle it.
        # merge_parquet has a check: if len(files) == 1 and basename is merged, it returns.
        
        output_file = os.path.join(root, "merged.parquet")
        
        # Identify context for logging (e.g., BTC/trades or BTC/nSigFigs=5.0)
        rel_path = os.path.relpath(root, day_dir)
        print(f"[{date_str}] Merging {rel_path}...")
        
        try:
            merge_parquet(root, output_file)
        except Exception as e:
            print(f"[{date_str}] Error merging in {rel_path}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Merge LOB and Trades parquet files for a date range.")
    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--data-dir", type=str, default="data", help="Root data directory (default: ./data)")
    
    args = parser.parse_args()
    
    # Handle data_dir path
    # If not absolute, try to find it relative to CWD or Project Root
    if not os.path.isabs(args.data_dir):
        # 1. Try CWD/data_dir
        cwd_path = os.path.abspath(args.data_dir)
        if os.path.exists(cwd_path):
            args.data_dir = cwd_path
        else:
            # 2. Try ProjectRoot/data_dir
            proj_path = os.path.join(project_root, args.data_dir)
            if os.path.exists(proj_path):
                args.data_dir = proj_path
            else:
                # Default to CWD path and let logic fail gracefully if not found
                args.data_dir = cwd_path

    print(f"Data Directory: {args.data_dir}")
    
    dates = get_date_range(args.start, args.end)
    print(f"Processing {len(dates)} days from {args.start} to {args.end}...")
    print("-" * 50)
    
    for day in dates:
        merge_daily_data(day, args.data_dir)
        print("-" * 50)

if __name__ == "__main__":
    main()
