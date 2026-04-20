import yaml


class Categorizer:
    def __init__(self, config_path):
        self.config_path = config_path
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f) or {}

    def _normalize_description(self, desc):
        """Lowercase, replace spaces with hyphens (matches v1 keyword convention)."""
        return str(desc).lower().strip().replace(' ', '-')

    def categorize(self, df):
        df = df.copy()
        df['category'] = 'other'
        df['subcategory'] = 'unknown'

        for idx, row in df.iterrows():
            normalized = self._normalize_description(row['description'])

            matched = False
            for category, rules in self.config.items():
                keywords_map = rules.get('keywords', {})
                if not isinstance(keywords_map, dict):
                    continue
                for subcategory, keyword_list in keywords_map.items():
                    terms = keyword_list if keyword_list else [subcategory]
                    for term in terms:
                        if isinstance(term, dict):
                            kw = term.get('keyword', '')
                            amt = term.get('amount')
                            if kw in normalized and amt is not None \
                                    and round(row['amount'], 2) == round(amt, 2):
                                matched = True
                        elif term in normalized:
                            matched = True

                        if matched:
                            df.at[idx, 'category'] = category
                            df.at[idx, 'subcategory'] = subcategory
                            break
                    if matched:
                        break
                if matched:
                    break

        return df

