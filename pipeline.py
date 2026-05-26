import os
from datetime import datetime

import pandas as pd
import yaml

from extractor import get_extractor
from categorization import Categorizer
from reporting import ExcelReporter


SOURCE_DIR = 'source_files'
CATEGORY_CONFIG_PATH = 'config/category_mappings.yml'
BUDGET_CONFIG_PATH = 'config/monthly_mappings.yml'
DEFAULT_OUTPUT_PATH = 'output/financial_report.xlsx'
SHARED_FOLDERS = [SOURCE_DIR, 'output', 'past_ai_chats']


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
    """Run each file through its bank-specific extractor and concat into one DataFrame."""
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

    combined = pd.concat(all_dfs, ignore_index=True)

    # Safety net for overlapping same-account statements. Source is in the
    # subset so cross-account transfers aren't collapsed.
    before = len(combined)
    combined = combined.drop_duplicates(
        subset=['date', 'description', 'amount', 'source'],
        keep='first',
    ).reset_index(drop=True)
    dropped = before - len(combined)
    if dropped:
        print(f"      Dropped {dropped} exact-duplicate row(s) across source files")
    return combined


def filter_by_month(df, month_str):
    """Keep only transactions whose date falls within the given YYYY-MM month."""
    target = pd.Period(month_str, freq='M')
    mask = df['date'].dt.to_period('M') == target
    return df[mask].copy(), target


def ensure_folders_exist():
    for folder in SHARED_FOLDERS:
        os.makedirs(folder, exist_ok=True)


def resolve_output_path(file_arg, timestamp_arg):
    if timestamp_arg:
        ts = datetime.now().strftime('%Y-%m-%d_%H%M')
        return os.path.join('output', f'financial_report_{ts}.xlsx')
    if file_arg:
        return os.path.join('output', file_arg)
    return DEFAULT_OUTPUT_PATH


def load_budget_entries(path=BUDGET_CONFIG_PATH):
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    if not data or 'budget' not in data:
        return None
    return data['budget']


def build_category_map(categorizer):
    return {
        cat: list(rules.get('subcategories', {}).keys())
        for cat, rules in categorizer.config.items()
    }


def run_pipeline(month, output_file=None, timestamp=False, fresh_file=False):
    ensure_folders_exist()

    print("=== Finance Expenses ETL ===\n")
    print(f"   Target month: {month}\n")

    print("1. Discovering source files...")
    files = discover_files(SOURCE_DIR)
    print(f"   Found {len(files)} file(s)\n")

    print("2. Extracting transactions...")
    df = load_all_transactions(files)
    print(f"   Total extracted: {len(df)} transactions\n")

    print("3. Filtering to target month...")
    df, target_period = filter_by_month(df, month)
    print(f"   Kept {len(df)} transactions for {target_period}\n")

    if df.empty:
        print("   No transactions found for this month. Exiting.")
        return

    print("4. Categorizing...")
    categorizer = Categorizer(CATEGORY_CONFIG_PATH)
    df = categorizer.categorize(df)
    n_categorized = (df['category'] != 'other').sum()
    n_other = (df['category'] == 'other').sum()
    print(f"   Categorized: {n_categorized}, Uncategorized: {n_other}\n")

    print("5. Writing Excel report...")
    output_path = resolve_output_path(output_file, timestamp)
    category_map = build_category_map(categorizer)
    budget_entries = load_budget_entries()

    reporter = ExcelReporter(output_path)
    reporter.write(df, category_map=category_map, budget_entries=budget_entries, fresh_file=fresh_file)
    print(f"   Report saved to {output_path}\n")

    print("=== Done ===")
