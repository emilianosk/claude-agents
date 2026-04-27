from pathlib import Path

import pandas as pd
import pytest

from app.services.csv_profiler import CSVProfiler


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    df = pd.DataFrame([
        {'store': 'Store A', 'hour': 9, 'sku': 'ABC', 'tx_count': 5},
        {'store': 'Store A', 'hour': 10, 'sku': 'ABC', 'tx_count': 3},
        {'store': 'Store B', 'hour': 9, 'sku': 'XYZ', 'tx_count': 2},
        {'store': 'Store B', 'hour': 11, 'sku': 'ABC', 'tx_count': 7},
        {'store': 'Store C', 'hour': 9, 'sku': 'XYZ', 'tx_count': 1},
    ])
    path = tmp_path / 'test.csv'
    df.to_csv(path, index=False)
    return path


def test_profile_includes_string_summary(sample_csv: Path) -> None:
    result = CSVProfiler().profile_csv(sample_csv, include_string_summary=True)
    assert 'string_summary' in result
    assert 'store' in result['string_summary']
    assert 'sku' in result['string_summary']


def test_string_summary_not_included_by_default(sample_csv: Path) -> None:
    result = CSVProfiler().profile_csv(sample_csv)
    assert 'string_summary' not in result


def test_string_summary_low_cardinality_shows_values(sample_csv: Path) -> None:
    result = CSVProfiler().profile_csv(sample_csv, include_string_summary=True)
    store_summary = result['string_summary']['store']
    assert store_summary['unique_count'] == 3
    assert 'values' in store_summary
    assert set(store_summary['values']) == {'Store A', 'Store B', 'Store C'}


def test_string_summary_high_cardinality_shows_top_values(tmp_path: Path) -> None:
    df = pd.DataFrame({'store': [f'Store {i}' for i in range(100)], 'tx': range(100)})
    path = tmp_path / 'big.csv'
    df.to_csv(path, index=False)

    result = CSVProfiler().profile_csv(path, include_string_summary=True)
    store_summary = result['string_summary']['store']
    assert store_summary['unique_count'] == 100
    assert 'top_values' in store_summary
    assert 'values' not in store_summary


def test_profile_sample_rows_only_when_requested(sample_csv: Path) -> None:
    without = CSVProfiler().profile_csv(sample_csv, include_raw_preview=False)
    with_preview = CSVProfiler().profile_csv(sample_csv, include_raw_preview=True)

    assert 'sample_rows' not in without
    assert 'sample_rows' in with_preview
    assert len(with_preview['sample_rows']) == 5


def test_numeric_summary_still_present(sample_csv: Path) -> None:
    result = CSVProfiler().profile_csv(sample_csv)
    assert 'numeric_summary' in result
    assert 'hour' in result['numeric_summary']
    assert result['numeric_summary']['hour']['min'] == 9.0
    assert result['numeric_summary']['hour']['max'] == 11.0
