import calendar
from datetime import date

import pandas as pd

from helpers import month_tab_name, DATE_DISPLAY_FORMAT
from reporting.base import (
    BaseTabWriter,
    BOLD,
    BOLD_LARGE,
    HEADER_FONT,
    TXN_COLUMNS,
    MONTH_TAB_COLUMN_WIDTHS,
    find_duplicates,
    build_budget_sumifs,
)


class MonthTabWriter(BaseTabWriter):
    """Writes a single month's tab: transactions section, suspected duplicates,
    and the monthly budget section."""

    def write(self, month, month_df, budget_entries):
        """Returns (data_start, data_end) row range of transaction data."""
        tab_name = month_tab_name(month)
        ws = self._replace_sheet(tab_name)

        data_range = self._write_transactions(ws, month_df)
        next_row = data_range[1] + 3

        next_row = self._write_duplicates(ws, month_df, next_row)
        next_row += 2

        self._write_budget(ws, month, data_range[1], next_row, budget_entries)

        self._apply_column_widths(ws, MONTH_TAB_COLUMN_WIDTHS)

        return data_range

    def _write_transactions(self, ws, month_df):
        display_df = month_df[TXN_COLUMNS].copy()
        display_df = display_df.sort_values('date')
        display_df['date'] = display_df['date'].dt.strftime(DATE_DISPLAY_FORMAT)

        ws.cell(row=1, column=1, value='All Transactions').font = BOLD_LARGE

        for ci, col in enumerate(TXN_COLUMNS, start=1):
            ws.cell(row=2, column=ci, value=col).font = HEADER_FONT

        row_cursor = 3
        for _, data_row in display_df.iterrows():
            for ci, col in enumerate(TXN_COLUMNS, start=1):
                ws.cell(row=row_cursor, column=ci, value=data_row[col])
            row_cursor += 1

        return 3, row_cursor - 1

    def _write_duplicates(self, ws, month_df, start_row):
        duplicates = find_duplicates(month_df)
        if duplicates.empty:
            return start_row

        row_cursor = start_row
        ws.cell(row=row_cursor, column=1, value='Suspected Duplicates').font = BOLD_LARGE
        row_cursor += 1

        for ci, col in enumerate(TXN_COLUMNS, start=1):
            ws.cell(row=row_cursor, column=ci, value=col).font = HEADER_FONT
        row_cursor += 1

        dup_display = duplicates[TXN_COLUMNS].copy()
        dup_display['date'] = pd.to_datetime(dup_display['date']).dt.strftime(DATE_DISPLAY_FORMAT)

        for _, data_row in dup_display.iterrows():
            for ci, col in enumerate(TXN_COLUMNS, start=1):
                ws.cell(row=row_cursor, column=ci, value=data_row[col])
            row_cursor += 1

        return row_cursor

    def _write_budget(self, ws, month, txn_last_row, start_row, budget_entries):
        if not budget_entries:
            return start_row

        data_start = 3
        data_end = txn_last_row

        year = month.year
        mon = month.month
        last_day = calendar.monthrange(year, mon)[1]

        row_cursor = start_row
        ws.cell(row=row_cursor, column=1, value='Monthly Budget').font = BOLD_LARGE
        row_cursor += 1

        for ci, header in enumerate(['date', 'Description', 'Default', 'Actual', 'Diff'], start=1):
            ws.cell(row=row_cursor, column=ci, value=header).font = HEADER_FONT
        row_cursor += 1

        first_data_row = row_cursor
        for entry in budget_entries:
            day = min(int(entry.get('month-day', 1)), last_day)
            dt = date(year, mon, day)
            ws.cell(row=row_cursor, column=1, value=dt.strftime(DATE_DISPLAY_FORMAT))
            ws.cell(row=row_cursor, column=2, value=entry.get('description', ''))
            ws.cell(row=row_cursor, column=3, value=entry.get('default', 0))
            ws.cell(row=row_cursor, column=4,
                    value=build_budget_sumifs(entry, data_start, data_end))
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
