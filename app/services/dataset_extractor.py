from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.services.databricks_client import DatabricksClient
from app.services.query_loader import QueryLoader


class DatasetExtractor:
    def __init__(self, query_loader: QueryLoader, databricks_client: DatabricksClient) -> None:
        self.query_loader = query_loader
        self.databricks_client = databricks_client

    def extract_to_csv(self, dataset_label: str, output_dir: Path) -> tuple[Path, int]:
        query = self.query_loader.get_query(dataset_label)
        rows = self.databricks_client.execute_query(query)
        df = pd.DataFrame(rows)

        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f'{dataset_label}.csv'
        df.to_csv(output_file, index=False)
        return output_file, len(df)
