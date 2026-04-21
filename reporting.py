import os
import calendar
from datetime import date
import yaml
import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.properties import Outline
from helpers import col_range


DATE_DISPLAY_FORMAT = '%d %b %Y'

BOLD = Font(bold=True)
BOLD_LARGE = Font(bold=True, size=14)
HEADER_FONT = Font(bold=True, size=11)


def find_duplicates(df):
    """Find suspected cross-account duplicate pairs within a month.
    Matches transactions from different sources with equal absolute amounts
    and opposite signs (one positive, one negative)."""
    positives = df[df['amount'] > 0].copy().reset_index(drop=True)
    negatives = df[df['amount'] < 0].copy().reset_index(drop=True)

    used_pos = set()
    used_neg = set()
    pairs = []

    for i, pos_row in positives.iterrows():
        if i in used_pos:
            continue
        for j, neg_row in negatives.iterrows():
            if j in used_neg:
                continue
            if pos_row['source'] == neg_row['source']:
                continue
            if abs(pos_row['amount']) == abs(neg_row['amount']):
                pairs.append(pos_row)
                pairs.append(neg_row)
                used_pos.add(i)
                used_neg.add(j)
                break

    if not pairs:
        return pd.DataFrame(columns=df.columns)

    result = pd.DataFrame(pairs)
    result = result.sort_values(by='amount', key=lambda s: s.abs(), ascending=False)
    return result.reset_index(drop=True)


def _month_tab_name(period):
    """Convert a pandas Period to short tab name like 'Mar 26'."""
    return period.strftime('%b %y')


def _month_full_name(period):
    """Convert a pandas Period to full name like 'March 2026'."""
    return period.strftime('%B %Y')


class ExcelReporter:
    def __init__(self, file_path):
        self.file_path = file_path

    def write(self, df, category_map=None):
        os.makedirs(os.path.dirname(self.file_path) or '.', exist_ok=True)

        df = df.copy()
        df['month'] = df['date'].dt.to_period('M')
        self.category_map = category_map or {}

        if os.path.exists(self.file_path):
            wb = load_workbook(self.file_path)
        else:
            wb = Workbook()
            if 'Sheet' in wb.sheetnames:
                del wb['Sheet']

        months = sorted(df['month'].unique())

        data_ranges = {}
        for month in months:
            month_df = df[df['month'] == month].copy()
            data_ranges[month] = self._write_month_tab(wb, month, month_df)

        self._write_year_summary(wb, df, months, data_ranges)

        wb.save(self.file_path)

    def _read_all_month_tabs(self, wb):
        """Read transaction data from all existing monthly tabs in the workbook."""
        columns = ['date', 'description', 'amount', 'category', 'subcategory', 'source']
        all_dfs = []

        for tab_name in wb.sheetnames:
            if tab_name.isdigit():
                continue
            ws = wb[tab_name]
            if ws.max_row < 3:
                continue
            headers = [ws.cell(row=2, column=c).value for c in range(1, 7)]
            if headers != columns:
                continue

            rows = []
            for r in range(3, ws.max_row + 1):
                first_cell = ws.cell(row=r, column=1).value
                if first_cell is None or first_cell == 'Suspected Duplicates':
                    break
                rows.append([ws.cell(row=r, column=c).value for c in range(1, 7)])

            if rows:
                month_df = pd.DataFrame(rows, columns=columns)
                month_df['date'] = pd.to_datetime(month_df['date'], format=DATE_DISPLAY_FORMAT)
                month_df['amount'] = pd.to_numeric(month_df['amount'], errors='coerce').fillna(0.0)
                all_dfs.append(month_df)

        if not all_dfs:
            return pd.DataFrame(columns=columns)
        return pd.concat(all_dfs, ignore_index=True)

    def _build_month_summary(self, month_df):
        summary = (
            month_df.groupby(['category', 'subcategory'])['amount']
            .sum()
            .reset_index()
        )
        categories_with_data = set(summary['category'].unique())

        if self.category_map:
            full_rows = []
            for cat, subcats in self.category_map.items():
                if cat not in categories_with_data:
                    continue
                for sub in subcats:
                    full_rows.append({'category': cat, 'subcategory': sub, 'amount': 0.0})
            if full_rows:
                full_df = pd.DataFrame(full_rows)
                summary = pd.concat([summary, full_df], ignore_index=True)
                summary = (
                    summary.groupby(['category', 'subcategory'])['amount']
                    .sum()
                    .reset_index()
                )

        if self.category_map:
            order_map = {cat: i for i, cat in enumerate(self.category_map.keys())}
            summary = summary.sort_values(
                'category', key=lambda col: col.map(
                    lambda c: order_map.get(c, len(order_map))
                )
            )
        else:
            summary = summary.sort_values('category')
        return summary

    def _get_month_data_range(self, wb, month):
        """Return (first_row, last_row) of transaction data on a month tab."""
        tab = _month_tab_name(month)
        if tab not in wb.sheetnames:
            return 3, 3
        ws = wb[tab]
        last_row = 2
        for r in range(3, ws.max_row + 1):
            val = ws.cell(row=r, column=1).value
            if val is None or val == 'Suspected Duplicates':
                break
            last_row = r
        return 3, last_row

    def _write_month_block(self, ws, month, month_df, summary, start_row, data_range):
        """Write a single month's summary block. Returns next available row."""
        row_cursor = start_row
        tab = _month_tab_name(month)
        ref = f"'{tab}'"
        ds, de = data_range
        rc = col_range('C', ds, de)
        rd = col_range('D', ds, de)
        re_ = col_range('E', ds, de)

        ws.cell(row=row_cursor, column=1, value=_month_full_name(month)).font = BOLD_LARGE
        row_cursor += 1

        ws.cell(row=row_cursor, column=1, value='Total Income:')
        ws.cell(row=row_cursor, column=2,
                value=f'=SUMIFS({ref}!{rc},{ref}!{rc},">"&0)')
        income_row = row_cursor
        row_cursor += 1

        ws.cell(row=row_cursor, column=1, value='Total Expenses:')
        ws.cell(row=row_cursor, column=2,
                value=f'=SUMIFS({ref}!{rc},{ref}!{rc},"<"&0)')
        row_cursor += 1

        ws.cell(row=row_cursor, column=1, value='Net:')
        ws.cell(row=row_cursor, column=2,
                value=f'=B{income_row}+B{income_row + 1}').font = BOLD
        row_cursor += 2

        current_cat = None
        cat_header_row = None
        cat_first_row = None
        for _, srow in summary.iterrows():
            if srow['category'] != current_cat:
                if current_cat is not None:
                    ws.cell(row=cat_header_row, column=2,
                            value=f"=SUM(B{cat_first_row}:B{row_cursor - 1})").font = BOLD
                    ws.row_dimensions.group(cat_first_row, row_cursor - 1,
                                            outline_level=1, hidden=True)

                current_cat = srow['category']
                ws.cell(row=row_cursor, column=1, value=current_cat.capitalize()).font = BOLD
                cat_header_row = row_cursor
                row_cursor += 1
                cat_first_row = row_cursor

            ws.cell(row=row_cursor, column=1, value=f"  {srow['subcategory']}")
            ws.cell(row=row_cursor, column=2,
                    value=f'=SUMIFS({ref}!{rc},{ref}!{rd},"{srow["category"]}",{ref}!{re_},"{srow["subcategory"]}")')

            row_cursor += 1

        if current_cat is not None:
            ws.cell(row=cat_header_row, column=2,
                    value=f"=SUM(B{cat_first_row}:B{row_cursor - 1})").font = BOLD
            ws.row_dimensions.group(cat_first_row, row_cursor - 1,
                                    outline_level=1, hidden=True)

        row_cursor += 2
        return row_cursor

    def _write_year_summary(self, wb, df, months, data_ranges):
        current_months = set(df['month'].unique())

        existing = self._read_all_month_tabs(wb)
        if not existing.empty:
            existing['month'] = existing['date'].dt.to_period('M')
            preserved = existing[~existing['month'].isin(current_months)]
            combined = pd.concat([df, preserved], ignore_index=True)
        else:
            combined = df.copy()

        years = sorted(combined['month'].apply(lambda m: m.year).unique())

        for year in years:
            sheet_name = str(year)
            if sheet_name in wb.sheetnames:
                del wb[sheet_name]
            ws = wb.create_sheet(sheet_name, 0)
            ws.sheet_properties.outlinePr = Outline(summaryBelow=False)

            year_months = sorted(
                [m for m in combined['month'].unique() if m.year == year],
                reverse=True,
            )

            row_cursor = 1

            ws.cell(row=row_cursor, column=1, value=f'Grand Total {year}').font = BOLD_LARGE
            row_cursor += 1

            ws.cell(row=row_cursor, column=1, value='Interest Earned (YTD):').font = BOLD
            interest_parts = []
            for m in year_months:
                tab = _month_tab_name(m)
                dr = data_ranges.get(m) or self._get_month_data_range(wb, m)
                rc = col_range('C', dr[0], dr[1])
                rd = col_range('D', dr[0], dr[1])
                interest_parts.append(
                    f'SUMIFS(\'{tab}\'!{rc},\'{tab}\'!{rd},"interest-earned")'
                )
            ws.cell(row=row_cursor, column=2,
                    value='=' + '+'.join(interest_parts)).font = BOLD
            row_cursor += 2

            for month in year_months:
                dr = data_ranges.get(month) or self._get_month_data_range(wb, month)
                month_df = combined[combined['month'] == month]
                summary = self._build_month_summary(month_df)
                row_cursor = self._write_month_block(ws, month, month_df, summary, start_row=row_cursor, data_range=dr)

            ws.column_dimensions['A'].width = 30
            ws.column_dimensions['B'].width = 15

            instructions_col = 7  # column G
            instructions = [
                ("How to get monthly CSV files:", BOLD_LARGE),
                ("Wealthsimple:", BOLD),
                ("Click Settings (bottom left) > Accounts > Select Chequing >", None),
                ("Documents > Monthly statements > Download CSV of desired month", None),
                ("ScotiaBank:", BOLD),
                ("Click Credit Card > In transactions > Set Date Range > Download as CSV", None),
                ("CIBC:", BOLD),
                ("Left side menu bar > Download Transactions > Select Account >", None),
                ("Set Date Range > Download Transactions", None),
            ]
            for i, (text, font) in enumerate(instructions, start=1):
                cell = ws.cell(row=i, column=instructions_col, value=text)
                if font:
                    cell.font = font
            ws.column_dimensions['G'].width = 60

            ws.cell(row=1, column=8, value="Activate virtual environment:").font = BOLD
            ws.cell(row=2, column=8, value="source .venv/bin/activate")
            ws.column_dimensions['H'].width = 35

    def _load_monthly_mappings(self):
        path = os.path.join('config', 'monthly_mappings.yml')
        if not os.path.exists(path):
            return None
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        if not data or 'budget' not in data:
            return None
        return data['budget']

    def _build_sumifs_formula(self, entry, data_start, data_end):
        """Build the SUMIFS formula string for column C based on the entry's formula type."""
        formula = entry.get('formula', {})
        ftype = formula.get('type', 'none')
        cr = col_range

        if ftype == 'none':
            return 0

        if ftype in ('category', 'subcategory'):
            cat = formula.get('category', '')
            sub = formula.get('subcategory')
            if sub:
                return f'=SUMIFS({cr("C", data_start, data_end)},{cr("D", data_start, data_end)},"{cat}",{cr("E", data_start, data_end)},"{sub}")'
            return f'=SUMIFS({cr("C", data_start, data_end)},{cr("D", data_start, data_end)},"{cat}")'

        if ftype == 'lookups':
            parts = []
            for lk in formula.get('lookups', []):
                cat = lk.get('category', '')
                sub = lk.get('subcategory')
                if sub:
                    parts.append(f'SUMIFS({cr("C", data_start, data_end)},{cr("D", data_start, data_end)},"{cat}",{cr("E", data_start, data_end)},"{sub}")')
                else:
                    parts.append(f'SUMIFS({cr("C", data_start, data_end)},{cr("D", data_start, data_end)},"{cat}")')
            return '=' + '+'.join(parts) if parts else 0

        if ftype == 'source_negatives':
            sources = formula.get('sources', [])
            parts = [f'SUMIFS({cr("C", data_start, data_end)},{cr("F", data_start, data_end)},"{src}",{cr("C", data_start, data_end)},"<"&0)' for src in sources]
            return '=' + '+'.join(parts)

        return 0

    def _write_budget_section(self, ws, month, tbl_last_row, row_cursor):
        """Write the Monthly Budget section. Returns next available row.
        txn_last_row is the last row of transaction data (for scoping SUMIFS ranges)."""
        budget = self._load_monthly_mappings()
        if budget is None:
            return row_cursor

        data_start = 3  # row 1 is header, row 2 is table header
        data_end = tbl_last_row

        year = month.year
        mon = month.month
        last_day = calendar.monthrange(year, mon)[1]

        ws.cell(row=row_cursor, column=1, value='Monthly Budget').font = BOLD_LARGE
        row_cursor += 1

        for ci, header in enumerate(['date', 'Description', 'Default', 'Actual', 'Diff'], start=1):
            ws.cell(row=row_cursor, column=ci, value=header).font = HEADER_FONT
        row_cursor += 1

        first_data_row = row_cursor
        for entry in budget:
            day = min(int(entry.get('month-day', 1)), last_day)
            dt = date(year, mon, day)
            ws.cell(row=row_cursor, column=1, value=dt.strftime(DATE_DISPLAY_FORMAT))

            ws.cell(row=row_cursor, column=2, value=entry.get('description', ''))
            ws.cell(row=row_cursor, column=3, value=entry.get('default', 0))
            ws.cell(row=row_cursor, column=4,
                    value=self._build_sumifs_formula(entry, data_start, data_end))
            ws.cell(row=row_cursor, column=5, value=f'=D{row_cursor}-C{row_cursor}')
            row_cursor += 1

        ws.cell(row=row_cursor, column=2, value='Total').font = BOLD
        ws.cell(row=row_cursor, column=3,
                value=f'=SUM(C{first_data_row}:C{row_cursor - 1})').font = BOLD
        ws.cell(row=row_cursor, column=4,
                value=f'=SUM(D{first_data_row}:D{row_cursor - 1})').font = BOLD
        ws.cell(row=row_cursor, column=5,
                value=f'=SUM(E{first_data_row}:E{row_cursor - 1})').font = BOLD
        row_cursor += 1

        return row_cursor

    def _write_month_tab(self, wb, month, month_df):
        tab_name = _month_tab_name(month)

        if tab_name in wb.sheetnames:
            del wb[tab_name]
        ws = wb.create_sheet(tab_name)

        columns = ['date', 'description', 'amount', 'category', 'subcategory', 'source']
        display_df = month_df[columns].copy()
        display_df = display_df.sort_values('date')
        display_df['date'] = display_df['date'].dt.strftime(DATE_DISPLAY_FORMAT)

        row_cursor = 1
        ws.cell(row=row_cursor, column=1, value='All Transactions').font = BOLD_LARGE
        row_cursor += 1

        for ci, col in enumerate(columns, start=1):
            ws.cell(row=row_cursor, column=ci, value=col).font = HEADER_FONT
        row_cursor += 1

        for _, data_row in display_df.iterrows():
            for ci, col in enumerate(columns, start=1):
                ws.cell(row=row_cursor, column=ci, value=data_row[col])
            row_cursor += 1

        tbl_last_row = row_cursor - 1

        row_cursor += 2

        duplicates = find_duplicates(month_df)
        if not duplicates.empty:
            ws.cell(row=row_cursor, column=1, value='Suspected Duplicates').font = BOLD_LARGE
            row_cursor += 1

            for ci, col in enumerate(columns, start=1):
                ws.cell(row=row_cursor, column=ci, value=col).font = HEADER_FONT
            row_cursor += 1

            dup_display = duplicates[columns].copy()
            dup_display['date'] = pd.to_datetime(dup_display['date']).dt.strftime(DATE_DISPLAY_FORMAT)

            for _, data_row in dup_display.iterrows():
                for ci, col in enumerate(columns, start=1):
                    ws.cell(row=row_cursor, column=ci, value=data_row[col])
                row_cursor += 1

        row_cursor += 2
        self._write_budget_section(ws, month, tbl_last_row, row_cursor)

        ws.column_dimensions['A'].width = 14
        ws.column_dimensions['B'].width = 55
        ws.column_dimensions['C'].width = 12
        ws.column_dimensions['D'].width = 18
        ws.column_dimensions['E'].width = 25
        ws.column_dimensions['F'].width = 12

        return 3, tbl_last_row
