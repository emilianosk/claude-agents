from pathlib import Path

import pandas as pd

from app.services.derived_features import build_pos_hourly_demand, build_store_piercer_sid_map


def test_build_pos_hourly_demand_creates_derived_dataset(tmp_path: Path) -> None:
    pos = pd.DataFrame(
        [
            {
                'location_id': '1001',
                'sale_datetime': '2026-04-20T10:15:00+00:00',
                'sku': 'SKU-A',
                'is_piercing': True,
            },
            {
                'location_id': '1001',
                'sale_datetime': '2026-04-20T10:35:00+00:00',
                'sku': 'SKU-A',
                'is_piercing': True,
            },
            {
                'location_id': '1002',
                'sale_datetime': '2026-04-20T11:10:00+00:00',
                'sku': 'SKU-B',
                'is_piercing': False,
            },
        ]
    )
    loc = pd.DataFrame(
        [
            {'CompanyNumber': '1001', 'CompanyName': 'Store A'},
            {'CompanyNumber': '1002', 'CompanyName': 'Store B'},
        ]
    )

    pos.to_csv(tmp_path / 'DATALAKE.POS_TRANSACTIONS.csv', index=False)
    loc.to_csv(tmp_path / 'DEPUTY.LOCATIONS.csv', index=False)

    output_file = build_pos_hourly_demand(tmp_path)

    assert output_file.exists()
    out = pd.read_csv(output_file)
    assert 'tx_count' in out.columns
    assert int(out['tx_count'].sum()) == 3
    assert set(out['CompanyName'].dropna().tolist()) == {'Store A', 'Store B'}


def test_build_pos_hourly_demand_requires_source_files(tmp_path: Path) -> None:
    try:
        build_pos_hourly_demand(tmp_path)
        assert False, 'expected FileNotFoundError'
    except FileNotFoundError as exc:
        assert 'DATALAKE.POS_TRANSACTIONS.csv' in str(exc)


def test_build_store_piercer_sid_map_creates_expected_rows(tmp_path: Path) -> None:
    locations = pd.DataFrame(
        [
            {'Id': 10, 'CompanyName': 'Store A', 'CompanyNumber': 'LID_100AU'},
            {'Id': 11, 'CompanyName': 'Store B', 'CompanyNumber': 'LID_200AU'},
        ]
    )
    team = pd.DataFrame(
        [
            {'Id': 1001, 'Company': 10, 'DisplayName': 'Piercer A'},
            {'Id': 1002, 'Company': 11, 'DisplayName': 'Piercer B'},
        ]
    )
    employees_training = pd.DataFrame(
        [
            {'Employee': 1001, 'Module': 5001},
            {'Employee': 1001, 'Module': 5002},
            {'Employee': 1002, 'Module': 5003},
        ]
    )
    training_modules = pd.DataFrame(
        [
            {'Id': 5001, 'Provider': 'SID_ABC', 'Title': 'Nostril'},
            {'Id': 5002, 'Provider': 'NOT_SID', 'Title': 'General'},
            {'Id': 5003, 'Provider': 'SID_XYZ', 'Title': 'Conch'},
        ]
    )

    locations.to_csv(tmp_path / 'DEPUTY.LOCATIONS.csv', index=False)
    team.to_csv(tmp_path / 'DEPUTY.TEAM_AVAILABILITY.csv', index=False)
    employees_training.to_csv(tmp_path / 'DEPUTY.EMPLOYEES_TRAINING.csv', index=False)
    training_modules.to_csv(tmp_path / 'DEPUTY.TRAINING_MODULES.csv', index=False)

    output_file = build_store_piercer_sid_map(tmp_path)
    out = pd.read_csv(output_file)

    assert output_file.exists()
    assert list(out.columns) == ['store_lid', 'store_name', 'piercer_pid', 'piercer_name', 'sid', 'service_name']
    assert set(out['sid'].tolist()) == {'SID_ABC', 'SID_XYZ'}
    assert set(out['piercer_pid'].astype(str).tolist()) == {'1001', '1002'}
    assert set(out['piercer_name'].tolist()) == {'Piercer A', 'Piercer B'}
    assert set(out['service_name'].tolist()) == {'Nostril', 'Conch'}


def test_build_store_piercer_sid_map_requires_source_files(tmp_path: Path) -> None:
    try:
        build_store_piercer_sid_map(tmp_path)
        assert False, 'expected FileNotFoundError'
    except FileNotFoundError as exc:
        assert 'DEPUTY.LOCATIONS.csv' in str(exc)
