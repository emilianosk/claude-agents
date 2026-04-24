from __future__ import annotations

from pathlib import Path

import pandas as pd


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
        merged.groupby(['CompanyName', 'hour', 'sku', 'is_piercing'], dropna=False)
        .size()
        .reset_index(name='tx_count')
        .sort_values(by=['CompanyName', 'hour', 'tx_count'], ascending=[True, True, False])
    )

    out_file = run_upload_dir / 'FEATURES.POS_HOURLY_DEMAND_BY_STORE.csv'
    out.to_csv(out_file, index=False)
    return out_file


def build_store_piercer_sid_map(run_upload_dir: Path) -> Path:
    loc_file = run_upload_dir / 'DEPUTY.LOCATIONS.csv'
    team_file = run_upload_dir / 'DEPUTY.TEAM_AVAILABILITY.csv'
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
        raise ValueError(f'DEPUTY.TEAM_AVAILABILITY missing required columns: {missing_team}')
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
    out = out.dropna(subset=['store_lid', 'store_name', 'piercer_pid', 'piercer_name', 'sid', 'service_name']).drop_duplicates()
    out = out.sort_values(
        by=['store_lid', 'store_name', 'piercer_pid', 'piercer_name', 'sid', 'service_name'],
        ascending=[True, True, True, True, True, True],
    )

    out_file = run_upload_dir / 'FEATURES.STORE_PIERCER_SID_MAP.csv'
    out.to_csv(out_file, index=False)
    return out_file
