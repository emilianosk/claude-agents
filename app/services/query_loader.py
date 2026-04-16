from __future__ import annotations

import json
from pathlib import Path


class QueryLoader:
    def __init__(self, mapping_file: str) -> None:
        self.mapping_file = Path(mapping_file)
        if not self.mapping_file.exists():
            raise FileNotFoundError(f'Query mapping file not found: {self.mapping_file}')
        self.mapping = json.loads(self.mapping_file.read_text(encoding='utf-8'))

    def get_query(self, dataset_label: str) -> str:
        config = self.mapping.get(dataset_label)
        if not config:
            raise KeyError(f'No query config for dataset: {dataset_label}')

        query_file = config.get('query_file')
        if not query_file:
            raise ValueError(f'Missing query_file for dataset: {dataset_label}')

        sql_path = Path(query_file)
        if not sql_path.exists():
            sql_path = self.mapping_file.parent / query_file

        if not sql_path.exists():
            raise FileNotFoundError(f'Query file not found for {dataset_label}: {query_file}')

        return sql_path.read_text(encoding='utf-8').strip()
