from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

_MAX_TOP_VALUES = 10
_CARDINALITY_THRESHOLD = 50


class CSVProfiler:
    def profile_csv(
        self,
        csv_path: Path,
        include_raw_preview: bool = False,
        include_string_summary: bool = False,
    ) -> dict:
        df = pd.read_csv(csv_path)

        numeric_cols = df.select_dtypes(include=np.number).columns.tolist()

        summary = {
            'rows': int(df.shape[0]),
            'columns': df.columns.tolist(),
            'missing_values': {k: int(v) for k, v in df.isna().sum().to_dict().items()},
            'numeric_summary': self._numeric_summary(df, numeric_cols),
        }

        if include_string_summary:
            string_cols = df.select_dtypes(include='object').columns.tolist()
            summary['string_summary'] = self._string_summary(df, string_cols)

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

    def _string_summary(self, df: pd.DataFrame, string_cols: list[str]) -> dict:
        out: dict = {}
        for col in string_cols:
            series = df[col].dropna()
            unique_count = int(series.nunique())
            entry: dict = {'unique_count': unique_count}
            if unique_count <= _CARDINALITY_THRESHOLD:
                entry['values'] = sorted(series.unique().tolist())
            else:
                top = series.value_counts().head(_MAX_TOP_VALUES)
                entry['top_values'] = [
                    {'value': str(v), 'count': int(c)} for v, c in top.items()
                ]
            out[col] = entry
        return out
