import argparse

from pipeline import run_pipeline


def parse_args():
    parser = argparse.ArgumentParser(description='Finance Expenses ETL')
    parser.add_argument('--month', required=True,
                        help='Target month in YYYY-MM format (e.g. 2026-03)')
    parser.add_argument('--file', default=None,
                        help='Output filename in the output/ folder to accumulate into')
    parser.add_argument('--timestamp', action='store_true', default=False,
                        help='Create a new standalone file with timestamp instead of accumulating')
    parser.add_argument('--fresh-file', action='store_true', default=False,
                        help='Overwrite the output file from scratch instead of preserving prior month tabs')
    return parser.parse_args()


def main():
    args = parse_args()
    run_pipeline(
        month=args.month,
        output_file=args.file,
        timestamp=args.timestamp,
        fresh_file=args.fresh_file,
    )


if __name__ == '__main__':
    main()
