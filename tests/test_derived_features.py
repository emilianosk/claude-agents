from pathlib import Path

import pandas as pd

from app.services.derived_features import (
    build_frosters_hourly_patterns,
    build_frosters_last_4m,
    build_kepler_hourly_with_location,
    build_locations_with_operational_units,
    build_pos_hourly_demand,
    build_store_piercer_sid_map,
)


def test_build_locations_with_operational_units_creates_expected_rows(tmp_path: Path) -> None:
    locations = pd.DataFrame(
        [
            {'Id': 10, 'CompanyName': 'Store A', 'CompanyNumber': 'LID_100AU'},
            {'Id': 11, 'CompanyName': 'Store B', 'CompanyNumber': 'LID_200AU'},
        ]
    )
    store_units = pd.DataFrame(
        [
            {'Id': 100, 'Company': 10, 'OperationalUnitName': 'Store A (STORE)'},
            {'Id': 101, 'Company': 11, 'OperationalUnitName': 'Store B (STORE)'},
        ]
    )
    management_units = pd.DataFrame(
        [
            {'Id': 200, 'Company': 10, 'OperationalUnitName': 'Store A (MANAGEMENT)'},
        ]
    )

    locations.to_csv(tmp_path / 'DEPUTY.LOCATIONS.csv', index=False)
    store_units.to_csv(tmp_path / 'DEPUTY.OPERATIONAL_UNITS_FOR_ROSTERS_STORES.csv', index=False)
    management_units.to_csv(tmp_path / 'DEPUTY.OPERATIONAL_UNITS_FOR_ROSTERS_MANAGEMENT.csv', index=False)

    output_file = build_locations_with_operational_units(tmp_path)
    out = pd.read_csv(output_file)

    assert output_file.exists()
    assert list(out.columns) == [
        'location_id',
        'location_name',
        'location_lid',
        'operational_unit_id',
        'operational_unit_name',
        'operational_unit_type',
    ]
    assert len(out) == 3
    assert set(out['operational_unit_type'].tolist()) == {'STORE', 'MANAGEMENT'}
    assert set(out['location_lid'].tolist()) == {'LID_100AU', 'LID_200AU'}


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
    assert list(out.columns) == ['location_lid', 'store_name', 'hour', 'sku', 'is_piercing', 'tx_count']
    assert int(out['tx_count'].sum()) == 3
    assert set(out['store_name'].dropna().tolist()) == {'Store A', 'Store B'}
    assert set(out['location_lid'].dropna().astype(str).tolist()) == {'1001', '1002'}


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
    team.to_csv(tmp_path / 'DEPUTY.PIERCERS_TEAM.csv', index=False)
    employees_training.to_csv(tmp_path / 'DEPUTY.EMPLOYEES_TRAINING.csv', index=False)
    training_modules.to_csv(tmp_path / 'DEPUTY.TRAINING_MODULES.csv', index=False)

    output_file = build_store_piercer_sid_map(tmp_path)
    out = pd.read_csv(output_file)

    assert output_file.exists()
    assert list(out.columns) == ['store_lid', 'store_name', 'piercer_pid', 'piercer_name', 'sid', 'sku', 'service_name']
    assert set(out['sid'].tolist()) == {'SID_ABC', 'SID_XYZ'}
    assert set(out['sku'].tolist()) == {'ABC', 'XYZ'}
    assert set(out['piercer_pid'].astype(str).tolist()) == {'1001', '1002'}
    assert set(out['piercer_name'].tolist()) == {'Piercer A', 'Piercer B'}
    assert set(out['service_name'].tolist()) == {'Nostril', 'Conch'}


def test_build_store_piercer_sid_map_requires_source_files(tmp_path: Path) -> None:
    try:
        build_store_piercer_sid_map(tmp_path)
        assert False, 'expected FileNotFoundError'
    except FileNotFoundError as exc:
        assert 'DEPUTY.LOCATIONS.csv' in str(exc)


def test_build_frosters_last_4m_filters_and_enriches(tmp_path: Path) -> None:
    ou_feature = pd.DataFrame([
        {'location_id': '10', 'location_name': 'Store A', 'location_lid': 'LID_100AU', 'operational_unit_id': '100', 'operational_unit_name': 'Store A (STORE)', 'operational_unit_type': 'STORE'},
        {'location_id': '10', 'location_name': 'Store A', 'location_lid': 'LID_100AU', 'operational_unit_id': '200', 'operational_unit_name': 'Store A (MANAGEMENT)', 'operational_unit_type': 'MANAGEMENT'},
        {'location_id': '11', 'location_name': 'Store B', 'location_lid': 'LID_200AU', 'operational_unit_id': '101', 'operational_unit_name': 'Store B (STORE)', 'operational_unit_type': 'STORE'},
    ])
    rosters = pd.DataFrame([
        {'Id': 1, 'OperationalUnit': 100, 'Date': '2026-01-01', 'StartTimeLocalized': '2026-01-01T09:00:00', 'EndTimeLocalized': '2026-01-01T17:00:00', 'TotalTime': 8.0, 'Cost': 200.0, 'OnCost': 220.0, 'Employee': 501, 'Published': True, 'Open': False},
        {'Id': 2, 'OperationalUnit': 101, 'Date': '2026-01-02', 'StartTimeLocalized': '2026-01-02T10:00:00', 'EndTimeLocalized': '2026-01-02T18:00:00', 'TotalTime': 8.0, 'Cost': 200.0, 'OnCost': 220.0, 'Employee': 502, 'Published': True, 'Open': False},
        {'Id': 3, 'OperationalUnit': 999, 'Date': '2026-01-03', 'StartTimeLocalized': '2026-01-03T08:00:00', 'EndTimeLocalized': '2026-01-03T16:00:00', 'TotalTime': 8.0, 'Cost': 200.0, 'OnCost': 220.0, 'Employee': 503, 'Published': True, 'Open': False},
    ])

    ou_feature.to_csv(tmp_path / 'FEATURES.LOCATIONS_WITH_OPERATIONAL_UNITS.csv', index=False)
    rosters.to_csv(tmp_path / 'DEPUTY.ROSTERS_LAST_4M.csv', index=False)

    output_file = build_frosters_last_4m(tmp_path)
    out = pd.read_csv(output_file)

    assert output_file.exists()
    assert list(out.columns) == [
        'location_id', 'location_name', 'location_lid',
        'operational_unit_id', 'operational_unit_name', 'operational_unit_type',
        'roster_id', 'date', 'day_of_week', 'day_of_week_num', 'start_hour',
        'start_time', 'end_time', 'total_time', 'cost', 'on_cost',
        'employee_id', 'published', 'open',
    ]
    assert len(out) == 2
    assert set(out['roster_id'].tolist()) == {1, 2}
    assert set(out['location_name'].tolist()) == {'Store A', 'Store B'}
    assert set(out['operational_unit_type'].tolist()) == {'STORE'}
    assert set(out['day_of_week'].tolist()) == {'Thursday', 'Friday'}
    assert set(out['start_hour'].tolist()) == {9, 10}


def test_build_frosters_last_4m_requires_source_files(tmp_path: Path) -> None:
    try:
        build_frosters_last_4m(tmp_path)
        assert False, 'expected FileNotFoundError'
    except FileNotFoundError as exc:
        assert 'FEATURES.LOCATIONS_WITH_OPERATIONAL_UNITS.csv' in str(exc)


def test_build_frosters_hourly_patterns_aggregates_correctly(tmp_path: Path) -> None:
    frosters = pd.DataFrame([
        {'location_id': '10', 'location_name': 'Store A', 'location_lid': 'LID_100AU',
         'operational_unit_id': '100', 'operational_unit_name': 'Store A (STORE)', 'operational_unit_type': 'STORE',
         'roster_id': 1, 'date': '2026-01-05', 'day_of_week': 'Monday', 'day_of_week_num': 0, 'start_hour': 9,
         'start_time': '2026-01-05T09:00:00', 'end_time': '2026-01-05T17:00:00',
         'total_time': 8.0, 'cost': 200.0, 'on_cost': 220.0, 'employee_id': 501, 'published': True, 'open': False},
        {'location_id': '10', 'location_name': 'Store A', 'location_lid': 'LID_100AU',
         'operational_unit_id': '100', 'operational_unit_name': 'Store A (STORE)', 'operational_unit_type': 'STORE',
         'roster_id': 2, 'date': '2026-01-05', 'day_of_week': 'Monday', 'day_of_week_num': 0, 'start_hour': 9,
         'start_time': '2026-01-05T09:00:00', 'end_time': '2026-01-05T17:00:00',
         'total_time': 7.5, 'cost': 190.0, 'on_cost': 210.0, 'employee_id': 502, 'published': True, 'open': False},
        {'location_id': '11', 'location_name': 'Store B', 'location_lid': 'LID_200AU',
         'operational_unit_id': '101', 'operational_unit_name': 'Store B (STORE)', 'operational_unit_type': 'STORE',
         'roster_id': 3, 'date': '2026-01-06', 'day_of_week': 'Tuesday', 'day_of_week_num': 1, 'start_hour': 10,
         'start_time': '2026-01-06T10:00:00', 'end_time': '2026-01-06T18:00:00',
         'total_time': 8.0, 'cost': 200.0, 'on_cost': 220.0, 'employee_id': 503, 'published': True, 'open': False},
    ])

    frosters.to_csv(tmp_path / 'FEATURES.FROSTERS_LAST_4M.csv', index=False)

    output_file = build_frosters_hourly_patterns(tmp_path)
    out = pd.read_csv(output_file)

    assert output_file.exists()
    assert list(out.columns) == [
        'location_id', 'location_name', 'location_lid',
        'operational_unit_id', 'operational_unit_name', 'operational_unit_type',
        'day_of_week', 'day_of_week_num', 'start_hour',
        'shift_count', 'unique_employees', 'avg_total_time',
    ]
    assert len(out) == 2
    store_a_row = out[out['location_name'] == 'Store A'].iloc[0]
    assert store_a_row['shift_count'] == 2
    assert store_a_row['unique_employees'] == 2
    assert store_a_row['avg_total_time'] == 7.75


def test_build_kepler_hourly_with_location_enriches_with_location_id(tmp_path: Path) -> None:
    kepler = pd.DataFrame([
        {'id': 0, 'Name': 'SkinKandy Airport West', 'Date': '2026-01-01', 'Date_Time': '2026-01-01T09:00:00Z', 'Measures_Inside': 10, 'Measures_Transactions': 3},
        {'id': 0, 'Name': 'SkinKandy Bondi', 'Date': '2026-01-01', 'Date_Time': '2026-01-01T10:00:00Z', 'Measures_Inside': 8, 'Measures_Transactions': 2},
        {'id': 0, 'Name': 'SkinKandy Unknown', 'Date': '2026-01-01', 'Date_Time': '2026-01-01T11:00:00Z', 'Measures_Inside': 5, 'Measures_Transactions': 1},
    ])
    conversion = pd.DataFrame([
        {'location_id': 'LID_100AU', 'kepler_store_name': 'SkinKandy Airport West', 'hour_bucket': '2026-01-01T09:00:00Z', 'walk_in_conversion_rate_adjusted': 0.3},
        {'location_id': 'LID_100AU', 'kepler_store_name': 'SkinKandy Airport West', 'hour_bucket': '2026-01-01T10:00:00Z', 'walk_in_conversion_rate_adjusted': 0.4},
        {'location_id': 'LID_200AU', 'kepler_store_name': 'SkinKandy Bondi', 'hour_bucket': '2026-01-01T10:00:00Z', 'walk_in_conversion_rate_adjusted': 0.5},
    ])
    kepler.to_csv(tmp_path / 'DATALAKE.KEPLER_HOURLY_PAST_4M.csv', index=False)
    conversion.to_csv(tmp_path / 'DATALAKE.DATA_LAKE_CONVERSION.csv', index=False)

    output_file = build_kepler_hourly_with_location(tmp_path)
    out = pd.read_csv(output_file)

    assert output_file.exists()
    assert 'location_id' in out.columns
    assert list(out.columns).index('location_id') == list(out.columns).index('Name') + 1
    assert len(out) == 3
    assert out[out['Name'] == 'SkinKandy Airport West']['location_id'].iloc[0] == 'LID_100AU'
    assert out[out['Name'] == 'SkinKandy Bondi']['location_id'].iloc[0] == 'LID_200AU'
    assert pd.isna(out[out['Name'] == 'SkinKandy Unknown']['location_id'].iloc[0])


def test_build_kepler_hourly_with_location_requires_source_files(tmp_path: Path) -> None:
    try:
        build_kepler_hourly_with_location(tmp_path)
        assert False, 'expected FileNotFoundError'
    except FileNotFoundError as exc:
        assert 'DATALAKE.KEPLER_HOURLY_PAST_4M.csv' in str(exc)


def test_build_frosters_hourly_patterns_requires_source_file(tmp_path: Path) -> None:
    try:
        build_frosters_hourly_patterns(tmp_path)
        assert False, 'expected FileNotFoundError'
    except FileNotFoundError as exc:
        assert 'FEATURES.FROSTERS_LAST_4M.csv' in str(exc)
