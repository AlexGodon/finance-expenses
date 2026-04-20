import os
import argparse
from datetime import datetime
import pandas as pd
from file_extraction import get_extractor
from categorization import Categorizer
from reporting import ExcelReporter


SOURCE_DIR = 'source_files'
CONFIG_PATH = 'config/category_mappings.yml'
OUTPUT_PATH = 'output/financial_report.xlsx'


def parse_args():
    parser = argparse.ArgumentParser(description='Finance Expenses ETL')
    parser.add_argument('--month', required=True,
                        help='Target month in YYYY-MM format (e.g. 2026-03)')
    parser.add_argument('--file', default=None,
                        help='Output filename in the output/ folder to accumulate into')
    parser.add_argument('--timestamp', action='store_true', default=False,
                        help='Create a new standalone file with timestamp instead of accumulating')
    return parser.parse_args()


def discover_files(directory):
    """Scan directory for CSV and XLSX files, ignoring everything else."""
    valid_extensions = ['.csv', '.xlsx', '.xls']
    files = []
    for name in sorted(os.listdir(directory)):
        full_path = os.path.join(directory, name)
        if not os.path.isfile(full_path):
            continue
        ext = os.path.splitext(name)[1].lower()
        if ext in valid_extensions:
            files.append(full_path.lower())
    return files


def load_all_transactions(files):
    """Run each file through its bank-specific extractor, concat into one DataFrame."""
    all_dfs = []
    for file_path in files:
        try:
            extractor = get_extractor(file_path)
            df = extractor.parse()
            print(f"      Loaded {len(df)} rows from {os.path.basename(file_path)} [{df['source'].iloc[0]}]")
            all_dfs.append(df)
        except ValueError as e:
            print(f"      Skipping {os.path.basename(file_path)}: {e}")

    if not all_dfs:
        raise RuntimeError("No transaction files found or all files failed to parse.")

    return pd.concat(all_dfs, ignore_index=True)


def filter_by_month(df, month_str):
    """Keep only transactions whose date falls within the given YYYY-MM month."""
    target = pd.Period(month_str, freq='M')
    mask = df['date'].dt.to_period('M') == target
    filtered = df[mask].copy()
    return filtered, target


def ensure_folders_exist():
    for folder in [SOURCE_DIR, 'output', 'past_ai_chats']:
        os.makedirs(folder, exist_ok=True)


def main():
    args = parse_args()
    ensure_folders_exist()

    print("=== Finance Expenses ETL ===\n")
    print(f"   Target month: {args.month}\n")

    print("1. Discovering source files...")
    files = discover_files(SOURCE_DIR)
    print(f"   Found {len(files)} file(s)\n")

    print("2. Extracting transactions...")
    df = load_all_transactions(files)
    print(f"   Total extracted: {len(df)} transactions\n")

    print("3. Filtering to target month...")
    df, target_period = filter_by_month(df, args.month)
    print(f"   Kept {len(df)} transactions for {target_period}\n")

    if df.empty:
        print("   No transactions found for this month. Exiting.")
        return

    print("4. Categorizing...")
    categorizer = Categorizer(CONFIG_PATH)
    df = categorizer.categorize(df)

    n_categorized = (df['category'] != 'other').sum()
    n_other = (df['category'] == 'other').sum()
    print(f"   Categorized: {n_categorized}, Uncategorized: {n_other}\n")

    print("5. Writing Excel report...")
    if args.timestamp:
        ts = datetime.now().strftime('%Y-%m-%d_%H%M')
        output_path = os.path.join('output', f'financial_report_{ts}.xlsx')
    elif args.file:
        output_path = os.path.join('output', args.file)
    else:
        output_path = OUTPUT_PATH
    category_order = list(categorizer.config.keys())
    reporter = ExcelReporter(output_path)
    reporter.write(df, category_order)
    print(f"   Report saved to {output_path}\n")

    print("=== Done ===")


if __name__ == '__main__':
    main()
