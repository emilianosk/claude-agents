from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class DeputyConfig:
    base_url: str
    access_token: str
    client_id: str = ''
    client_secret: str = ''
    redirect_uri: str = ''
    ssl_verify: bool = True
    timeout_seconds: int = 30


class DeputyService:
    """Lightweight Deputy API client.

    Mirrors key query patterns used in the existing PHP DeputyService,
    without project-specific cache/database side effects.
    """

    def __init__(self, config: DeputyConfig) -> None:
        self.config = config

    def is_configured(self) -> bool:
        return bool(self.config.base_url and self.config.access_token)

    def get_config_info(self) -> dict[str, Any]:
        token = self.config.access_token or ''
        return {
            'base_url': self.config.base_url,
            'has_access_token': bool(token),
            'has_client_id': bool(self.config.client_id),
            'redirect_uri': self.config.redirect_uri,
            'is_configured': self.is_configured(),
            'access_token_length': len(token),
            'access_token_preview': f'{token[:10]}...' if token else 'NOT SET',
        }

    def test_connection(self) -> dict[str, Any]:
        if not self.is_configured():
            return {'success': False, 'message': 'Deputy service not properly configured'}

        result = self.query_operational_units(search_criteria={}, max_results=1, start=0)
        if result is False:
            return {'success': False, 'message': 'Deputy API connection failed'}

        return {'success': True, 'message': 'Deputy API connection successful', 'data': result}

    def make_request(
        self,
        endpoint: str,
        data: dict[str, Any] | None = None,
        method: str = 'POST',
        additional_headers: dict[str, str] | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]] | bool:
        if not self.is_configured():
            return False

        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        headers = self._default_headers()
        if additional_headers:
            headers.update(additional_headers)

        kwargs: dict[str, Any] = {
            'headers': headers,
            'timeout': self.config.timeout_seconds,
            'verify': self.config.ssl_verify,
        }
        if method.upper() in {'POST', 'PUT', 'PATCH'} and data is not None:
            kwargs['json'] = data

        try:
            resp = requests.request(method.upper(), url, **kwargs)
            if 200 <= resp.status_code < 300:
                return resp.json()
            return False
        except (requests.RequestException, ValueError):
            return False

    def get_employee_by_id(self, employee_id: int) -> dict[str, Any] | bool:
        search = {
            's1': {'field': 'Active', 'type': 'eq', 'data': True},
            's2': {'field': 'Id', 'type': 'eq', 'data': str(employee_id)},
        }
        result = self.make_request(
            '/api/v1/supervise/employee/QUERY',
            {'search': search, 'max': 1, 'start': 0},
            method='POST',
        )
        if isinstance(result, list) and result:
            return result[0]
        return False

    def get_employees_by_ids(
        self,
        ids: int | list[int],
        max_results: int = 100,
        start: int = 0,
        active_only: bool | None = None,
    ) -> list[dict[str, Any]] | bool:
        id_list = [int(x) for x in (ids if isinstance(ids, list) else [ids])]
        search: dict[str, Any] = {
            's1': {'field': 'Id', 'type': 'in', 'data': id_list},
        }
        if active_only is not None:
            search['s2'] = {'field': 'Active', 'type': 'eq', 'data': active_only}

        result = self.make_request(
            '/api/v1/supervise/employee/QUERY',
            {'search': search, 'max': max_results, 'start': start},
            method='POST',
        )
        return result if isinstance(result, list) else False

    def get_custom_field_data_by_id(self, custom_field_data_id: int) -> dict[str, Any] | bool:
        search = {
            's1': {'field': 'System', 'type': 'eq', 'data': 'Employee'},
            's2': {'field': 'Id', 'type': 'eq', 'data': str(custom_field_data_id)},
        }
        result = self.make_request(
            '/api/v1/resource/CustomFieldData/QUERY',
            {'search': search, 'max': 1},
            method='POST',
        )
        if isinstance(result, list) and result:
            return result[0]
        return False

    def query_operational_units(
        self,
        search_criteria: dict[str, Any] | None = None,
        max_results: int = 1,
        start: int = 0,
    ) -> list[dict[str, Any]] | bool:
        payload = {'search': search_criteria or {}, 'max': max_results, 'start': start}
        result = self.make_request('/api/v1/resource/OperationalUnit/QUERY', payload, method='POST')
        return result if isinstance(result, list) else False

    def find_operational_unit_by_name(
        self,
        name: str,
        max_results: int = 1,
        start: int = 0,
    ) -> list[dict[str, Any]] | bool:
        search = {
            's1': {'field': 'OperationalUnitName', 'type': 'eq', 'data': name},
            's2': {'field': 'Active', 'type': 'eq', 'data': True},
        }
        return self.query_operational_units(search, max_results=max_results, start=start)

    def get_operational_units_by_ids(
        self,
        ids: int | list[int],
        max_results: int = 500,
        start: int = 0,
        active_only: bool = True,
    ) -> list[dict[str, Any]] | bool:
        id_list = [int(x) for x in (ids if isinstance(ids, list) else [ids])]
        search: dict[str, Any] = {'s1': {'field': 'Id', 'type': 'in', 'data': id_list}}
        if active_only:
            search['s2'] = {'field': 'Active', 'type': 'eq', 'data': True}
        return self.query_operational_units(search, max_results=max_results, start=start)

    def query_roster(
        self,
        search_criteria: dict[str, Any] | None = None,
        max_results: int = 500,
        start: int = 0,
    ) -> list[dict[str, Any]] | bool:
        payload = {'search': search_criteria or {}, 'max': max_results, 'start': start}
        result = self.make_request('/api/v1/supervise/roster/QUERY', payload, method='POST')
        return result if isinstance(result, list) else False

    def find_roster_by_date_and_location(
        self,
        date_local: str,
        operational_unit_ids: int | list[int] | str,
        max_results: int = 500,
        start: int = 0,
    ) -> list[dict[str, Any]] | bool:
        if isinstance(operational_unit_ids, str):
            id_list = [int(x.strip()) for x in operational_unit_ids.split(',') if x.strip()]
        elif isinstance(operational_unit_ids, list):
            id_list = [int(x) for x in operational_unit_ids]
        else:
            id_list = [int(operational_unit_ids)]

        search = {
            's1': {'field': 'Date', 'type': 'eq', 'data': date_local},
            's2': {'field': 'OperationalUnit', 'type': 'in', 'data': id_list},
        }
        return self.query_roster(search, max_results=max_results, start=start)

    def query_training_records(
        self,
        search_criteria: dict[str, Any] | None = None,
        max_results: int = 1000,
        start: int = 0,
    ) -> list[dict[str, Any]] | bool:
        payload = {'search': search_criteria or {}, 'max': max_results, 'start': start}
        result = self.make_request('/api/v1/resource/TrainingRecord/QUERY', payload, method='POST')
        return result if isinstance(result, list) else False

    def find_training_records_by_module_and_employee(
        self,
        employee_ids: int | list[int],
        training_module_ids: int | list[int] | None = None,
        max_results: int = 3000,
        start: int = 0,
    ) -> list[dict[str, Any]] | bool:
        eids = [int(x) for x in (employee_ids if isinstance(employee_ids, list) else [employee_ids])]
        search: dict[str, Any] = {
            's1': {'field': 'Employee', 'type': 'in', 'data': eids},
            's2': {'field': 'Deleted', 'type': 'eq', 'data': False},
            's3': {'field': 'Status', 'type': 'eq', 'data': 'a'},
        }
        if training_module_ids is not None:
            mids = [int(x) for x in (training_module_ids if isinstance(training_module_ids, list) else [training_module_ids])]
            search['s4'] = {'field': 'Module', 'type': 'in', 'data': mids}
        return self.query_training_records(search, max_results=max_results, start=start)

    def get_store_by_lid(
        self,
        lids: str | list[str],
        max_results: int = 500,
        start: int = 0,
    ) -> list[dict[str, Any]] | dict[str, Any] | bool:
        lid_list = lids if isinstance(lids, list) else [lids]
        search = {
            's1': {'field': 'CompanyNumber', 'type': 'in', 'data': lid_list},
            's2': {'field': 'Active', 'type': 'eq', 'data': True},
        }
        result = self.make_request(
            '/api/v1/resource/Company/QUERY',
            {'search': search, 'max': max_results, 'start': start},
            method='POST',
        )
        if not isinstance(result, list) or not result:
            return False
        if not isinstance(lids, list) and len(result) == 1:
            return result[0]
        return result

    def _default_headers(self) -> dict[str, str]:
        return {
            'Authorization': f'Bearer {self.config.access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
