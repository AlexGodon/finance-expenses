def col_range(letter, start, end):
    """Build an Excel column range string, e.g. col_range('C', 3, 150) → 'C3:C150'."""
    return f'{letter}{start}:{letter}{end}'
