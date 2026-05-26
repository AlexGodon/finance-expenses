DATE_DISPLAY_FORMAT = '%d %b %Y'


def col_range(letter, start, end):
    """Build an Excel column range string, e.g. col_range('C', 3, 150) -> 'C3:C150'."""
    return f'{letter}{start}:{letter}{end}'


def month_tab_name(period):
    """Convert a pandas Period to short tab name like 'Mar 26'."""
    return period.strftime('%b %y')


def month_full_name(period):
    """Convert a pandas Period to full name like 'March 2026'."""
    return period.strftime('%B %Y')
