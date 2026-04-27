from __future__ import annotations

from pathlib import Path

import pandas as pd


def build_locations_with_operational_units(run_upload_dir: Path) -> Path:
    loc_file = run_upload_dir / 'DEPUTY.LOCATIONS.csv'
    store_ou_file = run_upload_dir / 'DEPUTY.OPERATIONAL_UNITS_FOR_ROSTERS_STORES.csv'
    management_ou_file = run_upload_dir / 'DEPUTY.OPERATIONAL_UNITS_FOR_ROSTERS_MANAGEMENT.csv'

    required_files = [loc_file, store_ou_file, management_ou_file]
    missing = [p.name for p in required_files if not p.exists()]
    if missing:
        raise FileNotFoundError(f'Missing required source datasets: {missing}')

    locations = pd.read_csv(loc_file)
    store_units = pd.read_csv(store_ou_file)
    management_units = pd.read_csv(management_ou_file)

    req_locations = {'Id', 'CompanyName', 'CompanyNumber'}
    req_units = {'Id', 'Company', 'OperationalUnitName'}

    missing_locations = sorted(req_locations - set(locations.columns))
    missing_store_units = sorted(req_units - set(store_units.columns))
    missing_management_units = sorted(req_units - set(management_units.columns))
    if missing_locations:
        raise ValueError(f'DEPUTY.LOCATIONS missing required columns: {missing_locations}')
    if missing_store_units:
        raise ValueError(f'DEPUTY.OPERATIONAL_UNITS_FOR_ROSTERS_STORES missing required columns: {missing_store_units}')
    if missing_management_units:
        raise ValueError(
            f'DEPUTY.OPERATIONAL_UNITS_FOR_ROSTERS_MANAGEMENT missing required columns: {missing_management_units}'
        )

    store_units = store_units.copy()
    management_units = management_units.copy()
    store_units['operational_unit_type'] = 'STORE'
    management_units['operational_unit_type'] = 'MANAGEMENT'
    units = pd.concat([store_units, management_units], ignore_index=True).drop_duplicates()

    locations['Id'] = locations['Id'].astype(str).str.strip()
    units['Company'] = units['Company'].astype(str).str.strip()
    units['Id'] = units['Id'].astype(str).str.strip()

    # Drop location-side columns from units so the merge never creates ambiguous suffixes.
    units = units.drop(columns=[c for c in ['CompanyName', 'CompanyNumber'] if c in units.columns])

    merged = units.merge(
        locations[['Id', 'CompanyName', 'CompanyNumber']],
        left_on='Company',
        right_on='Id',
        how='left',
        suffixes=('_operational_unit', '_location'),
    )

    out = merged[['Company', 'CompanyName', 'CompanyNumber', 'Id_operational_unit', 'OperationalUnitName', 'operational_unit_type']].rename(
        columns={
            'Company': 'location_id',
            'CompanyName': 'location_name',
            'CompanyNumber': 'location_lid',
            'Id_operational_unit': 'operational_unit_id',
            'OperationalUnitName': 'operational_unit_name',
        }
    )
    out = out.dropna(subset=['location_id', 'location_name', 'operational_unit_id', 'operational_unit_name'])
    out = out.sort_values(
        by=['location_name', 'operational_unit_type', 'operational_unit_name', 'operational_unit_id'],
        ascending=[True, True, True, True],
    )

    out_file = run_upload_dir / 'FEATURES.LOCATIONS_WITH_OPERATIONAL_UNITS.csv'
    out.to_csv(out_file, index=False)
    return out_file


def build_frosters_last_4m(run_upload_dir: Path) -> Path:
    feature_file = run_upload_dir / 'FEATURES.LOCATIONS_WITH_OPERATIONAL_UNITS.csv'
    rosters_file = run_upload_dir / 'DEPUTY.ROSTERS_LAST_4M.csv'

    missing = [p.name for p in [feature_file, rosters_file] if not p.exists()]
    if missing:
        raise FileNotFoundError(f'Missing required source datasets: {missing}')

    ou_feature = pd.read_csv(feature_file)
    rosters = pd.read_csv(rosters_file)

    req_ou = {'location_id', 'location_name', 'location_lid', 'operational_unit_id', 'operational_unit_name', 'operational_unit_type'}
    req_rosters = {'Id', 'OperationalUnit', 'Date', 'StartTimeLocalized', 'EndTimeLocalized', 'TotalTime', 'Cost', 'OnCost', 'Employee', 'Published', 'Open'}

    missing_ou = sorted(req_ou - set(ou_feature.columns))
    missing_rosters = sorted(req_rosters - set(rosters.columns))
    if missing_ou:
        raise ValueError(f'FEATURES.LOCATIONS_WITH_OPERATIONAL_UNITS missing required columns: {missing_ou}')
    if missing_rosters:
        raise ValueError(f'DEPUTY.ROSTERS_LAST_4M missing required columns: {missing_rosters}')

    ou_feature['operational_unit_id'] = ou_feature['operational_unit_id'].astype(str).str.strip()
    rosters['OperationalUnit'] = rosters['OperationalUnit'].astype(str).str.strip()

    merged = rosters.merge(
        ou_feature,
        left_on='OperationalUnit',
        right_on='operational_unit_id',
        how='inner',
    )

    out = merged[[
        'location_id', 'location_name', 'location_lid',
        'operational_unit_id', 'operational_unit_name', 'operational_unit_type',
        'Id', 'Date', 'StartTimeLocalized', 'EndTimeLocalized',
        'TotalTime', 'Cost', 'OnCost', 'Employee', 'Published', 'Open',
    ]].rename(columns={
        'Id': 'roster_id',
        'Date': 'date',
        'StartTimeLocalized': 'start_time',
        'EndTimeLocalized': 'end_time',
        'TotalTime': 'total_time',
        'Cost': 'cost',
        'OnCost': 'on_cost',
        'Employee': 'employee_id',
        'Published': 'published',
        'Open': 'open',
    }).copy()

    st = pd.to_datetime(out['start_time'], errors='coerce', utc=True)
    out['day_of_week'] = st.dt.day_name()
    out['day_of_week_num'] = st.dt.dayofweek
    out['start_hour'] = st.dt.hour

    col_order = [
        'location_id', 'location_name', 'location_lid',
        'operational_unit_id', 'operational_unit_name', 'operational_unit_type',
        'roster_id', 'date', 'day_of_week', 'day_of_week_num', 'start_hour',
        'start_time', 'end_time', 'total_time', 'cost', 'on_cost',
        'employee_id', 'published', 'open',
    ]
    out = out[col_order]
    out = out.sort_values(by=['location_name', 'operational_unit_type', 'day_of_week_num', 'start_hour'])

    out_file = run_upload_dir / 'FEATURES.FROSTERS_LAST_4M.csv'
    out.to_csv(out_file, index=False)
    return out_file


def build_frosters_hourly_patterns(run_upload_dir: Path) -> Path:
    frosters_file = run_upload_dir / 'FEATURES.FROSTERS_LAST_4M.csv'

    if not frosters_file.exists():
        raise FileNotFoundError(f'Missing required source dataset: {frosters_file.name}')

    frosters = pd.read_csv(frosters_file)

    req = {'location_id', 'location_name', 'location_lid', 'operational_unit_id',
           'operational_unit_name', 'operational_unit_type', 'day_of_week', 'day_of_week_num',
           'start_hour', 'total_time', 'employee_id'}
    missing = sorted(req - set(frosters.columns))
    if missing:
        raise ValueError(f'FEATURES.FROSTERS_LAST_4M missing required columns: {missing}')

    group_keys = [
        'location_id', 'location_name', 'location_lid',
        'operational_unit_id', 'operational_unit_name', 'operational_unit_type',
        'day_of_week', 'day_of_week_num', 'start_hour',
    ]

    out = (
        frosters.groupby(group_keys, dropna=False)
        .agg(
            shift_count=('employee_id', 'count'),
            unique_employees=('employee_id', 'nunique'),
            avg_total_time=('total_time', 'mean'),
        )
        .reset_index()
        .sort_values(by=['location_name', 'operational_unit_type', 'day_of_week_num', 'start_hour'])
    )
    out['avg_total_time'] = out['avg_total_time'].round(2)

    out_file = run_upload_dir / 'FEATURES.FROSTERS_HOURLY_PATTERNS.csv'
    out.to_csv(out_file, index=False)
    return out_file


def build_pos_hourly_demand(run_upload_dir: Path) -> Path:
    pos_file = run_upload_dir / 'DATALAKE.POS_TRANSACTIONS.csv'
    loc_file = run_upload_dir / 'DEPUTY.LOCATIONS.csv'

    if not pos_file.exists():
        raise FileNotFoundError(f'Missing required source dataset: {pos_file.name}')
    if not loc_file.exists():
        raise FileNotFoundError(f'Missing required source dataset: {loc_file.name}')

    pos = pd.read_csv(pos_file)
    loc = pd.read_csv(loc_file)

    required_pos = {'location_id', 'sale_datetime', 'sku', 'is_piercing'}
    required_loc = {'CompanyNumber', 'CompanyName'}

    missing_pos = sorted(required_pos - set(pos.columns))
    missing_loc = sorted(required_loc - set(loc.columns))
    if missing_pos:
        raise ValueError(f'POS_TRANSACTIONS missing required columns: {missing_pos}')
    if missing_loc:
        raise ValueError(f'DEPUTY.LOCATIONS missing required columns: {missing_loc}')

    # Normalize join keys to avoid whitespace/type mismatches.
    pos['location_id'] = pos['location_id'].astype(str).str.strip()
    loc['CompanyNumber'] = loc['CompanyNumber'].astype(str).str.strip()

    merged = pos.merge(
        loc[['CompanyNumber', 'CompanyName']],
        left_on='location_id',
        right_on='CompanyNumber',
        how='left',
    )

    merged['sale_datetime'] = pd.to_datetime(merged['sale_datetime'], errors='coerce')
    merged['hour'] = merged['sale_datetime'].dt.hour
    merged['is_piercing'] = merged['is_piercing'].astype(str).str.lower().isin({'1', 'true', 'yes', 'y'})

    out = (
        merged.groupby(['CompanyNumber', 'CompanyName', 'hour', 'sku', 'is_piercing'], dropna=False)
        .size()
        .reset_index(name='tx_count')
        .rename(columns={'CompanyNumber': 'location_lid', 'CompanyName': 'store_name'})
        .sort_values(by=['store_name', 'hour', 'tx_count'], ascending=[True, True, False])
    )

    out_file = run_upload_dir / 'FEATURES.POS_HOURLY_DEMAND_BY_STORE.csv'
    out.to_csv(out_file, index=False)
    return out_file


def build_store_piercer_sid_map(run_upload_dir: Path) -> Path:
    loc_file = run_upload_dir / 'DEPUTY.LOCATIONS.csv'
    team_file = run_upload_dir / 'DEPUTY.PIERCERS_TEAM.csv'
    emp_training_file = run_upload_dir / 'DEPUTY.EMPLOYEES_TRAINING.csv'
    modules_file = run_upload_dir / 'DEPUTY.TRAINING_MODULES.csv'

    required_files = [loc_file, team_file, emp_training_file, modules_file]
    missing = [p.name for p in required_files if not p.exists()]
    if missing:
        raise FileNotFoundError(f'Missing required source datasets: {missing}')

    locations = pd.read_csv(loc_file)
    team = pd.read_csv(team_file)
    emp_training = pd.read_csv(emp_training_file)
    modules = pd.read_csv(modules_file)

    req_locations = {'Id', 'CompanyName', 'CompanyNumber'}
    req_team = {'Id', 'Company', 'DisplayName'}
    req_emp_training = {'Employee', 'Module'}
    req_modules = {'Id', 'Provider', 'Title'}

    missing_locations = sorted(req_locations - set(locations.columns))
    missing_team = sorted(req_team - set(team.columns))
    missing_emp_training = sorted(req_emp_training - set(emp_training.columns))
    missing_modules = sorted(req_modules - set(modules.columns))
    if missing_locations:
        raise ValueError(f'DEPUTY.LOCATIONS missing required columns: {missing_locations}')
    if missing_team:
        raise ValueError(f'DEPUTY.PIERCERS_TEAM missing required columns: {missing_team}')
    if missing_emp_training:
        raise ValueError(f'DEPUTY.EMPLOYEES_TRAINING missing required columns: {missing_emp_training}')
    if missing_modules:
        raise ValueError(f'DEPUTY.TRAINING_MODULES missing required columns: {missing_modules}')

    # Normalize key types for stable joins.
    locations['Id'] = locations['Id'].astype(str).str.strip()
    team['Company'] = team['Company'].astype(str).str.strip()
    team['Id'] = team['Id'].astype(str).str.strip()
    emp_training['Employee'] = emp_training['Employee'].astype(str).str.strip()
    emp_training['Module'] = emp_training['Module'].astype(str).str.strip()
    modules['Id'] = modules['Id'].astype(str).str.strip()
    modules['Provider'] = modules['Provider'].astype(str).str.strip()

    store_people = team.merge(
        locations[['Id', 'CompanyName', 'CompanyNumber']],
        left_on='Company',
        right_on='Id',
        how='left',
        suffixes=('', '_location'),
    )

    people_training = store_people.merge(
        emp_training[['Employee', 'Module']],
        left_on='Id',
        right_on='Employee',
        how='left',
    )

    full = people_training.merge(
        modules[['Id', 'Provider', 'Title']],
        left_on='Module',
        right_on='Id',
        how='left',
        suffixes=('', '_module'),
    )

    # Keep SID-based training providers only.
    full['sid'] = full['Provider'].where(full['Provider'].str.startswith('SID_'))

    out = full[['CompanyNumber', 'CompanyName', 'Id', 'DisplayName', 'sid', 'Title']].rename(
        columns={
            'CompanyNumber': 'store_lid',
            'CompanyName': 'store_name',
            'Id': 'piercer_pid',
            'DisplayName': 'piercer_name',
            'Title': 'service_name',
        }
    )
    out['sku'] = out['sid'].str.removeprefix('SID_')
    out = out[['store_lid', 'store_name', 'piercer_pid', 'piercer_name', 'sid', 'sku', 'service_name']]
    out = out.dropna(subset=['store_lid', 'store_name', 'piercer_pid', 'piercer_name', 'sid', 'service_name']).drop_duplicates()
    out = out.sort_values(
        by=['store_lid', 'store_name', 'piercer_pid', 'piercer_name', 'sid', 'sku', 'service_name'],
        ascending=[True, True, True, True, True, True, True],
    )

    out_file = run_upload_dir / 'FEATURES.STORE_PIERCER_SID_MAP.csv'
    out.to_csv(out_file, index=False)
    return out_file
