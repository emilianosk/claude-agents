from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


class CSVProfiler:
    def profile_csv(self, csv_path: Path, include_raw_preview: bool = False) -> dict:
        df = pd.read_csv(csv_path)

        numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
        summary = {
            'rows': int(df.shape[0]),
            'columns': df.columns.tolist(),
            'missing_values': {k: int(v) for k, v in df.isna().sum().to_dict().items()},
            'numeric_summary': self._numeric_summary(df, numeric_cols),
        }

        if include_raw_preview:
            summary['sample_rows'] = df.head(5).to_dict(orient='records')

        return summary

    def _numeric_summary(self, df: pd.DataFrame, numeric_cols: list[str]) -> dict:
        out: dict = {}
        for col in numeric_cols:
            series = df[col].dropna()
            if series.empty:
                out[col] = {'mean': None, 'min': None, 'max': None}
            else:
                out[col] = {
                    'mean': float(series.mean()),
                    'min': float(series.min()),
                    'max': float(series.max()),
                }
        return out
