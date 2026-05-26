import os
import pandas as pd


class BaseExtractor:
    """Base class for bank-specific transaction extractors.
    Every subclass must produce a DataFrame with the standard schema:
    [date, description, amount, source]
    """

    def __init__(self, file_path):
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)

    def load_file(self, **kwargs):
        ext = os.path.splitext(self.file_path)[1]
        if ext == '.csv':
            return pd.read_csv(self.file_path, encoding='utf-8', **kwargs)
        elif ext in ('.xlsx', '.xls'):
            return pd.read_excel(self.file_path, **kwargs)
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

    def normalize(self, df):
        raise NotImplementedError

    def parse(self):
        df = self.load_file(**self._load_kwargs())
        df = self.normalize(df)
        return df

    def _load_kwargs(self):
        return {}


class WealthsimpleExtractor(BaseExtractor):
    """Parses Wealthsimple CSVs ('Future Us' chequing account).
    Header: date, transaction, description, amount, balance, currency
    Amounts are already correctly signed (negative=out, positive=in).
    """

    def _load_kwargs(self):
        return {'header': 0}

    def normalize(self, df):
        out = pd.DataFrame()
        out['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        out['description'] = df['description'].astype(str).str.strip()
        out['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
        out['source'] = 'WS'
        return out


class CIBCExtractor(BaseExtractor):
    """Parses CIBC Visa and Mastercard CSVs.
    No header row. Positional columns: date, description, debit, credit, card_number.
    amount = (credit or 0) - (debit or 0)
    """

    def _load_kwargs(self):
        return {
            'header': None,
            'names': ['date', 'description', 'debit', 'credit', 'card_number'],
        }

    def normalize(self, df):
        out = pd.DataFrame()
        out['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        out['description'] = df['description'].astype(str).str.strip()

        debit = pd.to_numeric(df['debit'], errors='coerce').fillna(0.0)
        credit = pd.to_numeric(df['credit'], errors='coerce').fillna(0.0)
        out['amount'] = credit - debit

        if 'visa' in self.file_name:
            out['source'] = 'CIBC-VISA'
        else:
            out['source'] = 'CIBC-MC'

        return out


class ScotiabankExtractor(BaseExtractor):
    """Parses Scotiabank Gold Amex CSVs.
    Header: Filter, Date, Description, Sub-description, Status, Type of Transaction, Amount
    Amount is inverted: CSV debits are positive, credits are negative. We multiply by -1.
    """

    def _load_kwargs(self):
        return {'header': 0}

    def normalize(self, df):
        out = pd.DataFrame()
        out['date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
        out['description'] = df['Description'].astype(str).str.strip()
        out['amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0.0) * -1
        out['source'] = 'SCOTIA'
        return out


EXTRACTOR_RULES = [
    ('future us', WealthsimpleExtractor),
    ('cibc', CIBCExtractor),
    ('scotiabank', ScotiabankExtractor),
]


def get_extractor(file_path):
    name = os.path.basename(file_path)
    ext = os.path.splitext(file_path)[1]

    if ext not in ('.csv', '.xlsx', '.xls'):
        raise ValueError(f"Unsupported file type: {file_path}")

    for pattern, cls in EXTRACTOR_RULES:
        if pattern in name:
            return cls(file_path)

    raise ValueError(f"Unknown bank source for file: {file_path}")
