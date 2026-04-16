from __future__ import annotations

import time
from dataclasses import dataclass

import requests


@dataclass
class DatabricksConfig:
    host: str
    token: str
    warehouse_id: str
    catalog: str
    schema: str
    wait_timeout: str
    ssl_verify: bool
    oauth_tenant_id: str
    oauth_client_id: str
    oauth_client_secret: str
    oauth_token_url: str


class DatabricksClient:
    def __init__(self, config: DatabricksConfig) -> None:
        self.config = config
        self._oauth_token: str | None = None
        self._oauth_expires_at: float = 0

    def is_configured(self) -> bool:
        return bool(self.config.host and self.config.warehouse_id and (self.config.token or self._oauth_enabled()))

    def execute_query(self, statement: str) -> list[dict]:
        if not self.is_configured():
            raise RuntimeError('Databricks client is not configured')

        payload = {
            'statement': statement,
            'warehouse_id': self.config.warehouse_id,
            'catalog': self.config.catalog or None,
            'schema': self.config.schema or None,
            'wait_timeout': self.config.wait_timeout or None,
            'disposition': 'INLINE',
            'format': 'JSON_ARRAY',
        }
        payload = {k: v for k, v in payload.items() if v not in (None, '')}

        response = self._request('POST', '/api/2.0/sql/statements', json=payload)
        result = response.get('result', {})
        data_array = result.get('data_array') or []
        manifest = result.get('manifest', {})

        schema = manifest.get('schema', {})
        columns = [c.get('name') for c in schema.get('columns', [])]

        if not columns:
            return []

        rows: list[dict] = []
        for row in data_array:
            row_dict = {columns[i]: row[i] if i < len(row) else None for i in range(len(columns))}
            rows.append(row_dict)
        return rows

    def _request(self, method: str, path: str, **kwargs):
        base_url = self._base_url()
        url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
        token = self._access_token()

        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

        res = requests.request(
            method,
            url,
            headers=headers,
            timeout=45,
            verify=self.config.ssl_verify,
            **kwargs,
        )

        if res.status_code < 200 or res.status_code >= 300:
            raise RuntimeError(f'Databricks request failed: {res.status_code} {res.text}')

        return res.json()

    def _base_url(self) -> str:
        if self.config.host.startswith('http://') or self.config.host.startswith('https://'):
            return self.config.host
        return f'https://{self.config.host}'

    def _oauth_enabled(self) -> bool:
        return bool(
            self.config.oauth_tenant_id
            and self.config.oauth_client_id
            and self.config.oauth_client_secret
        )

    def _oauth_url(self) -> str:
        if self.config.oauth_token_url:
            return self.config.oauth_token_url
        return f'https://login.microsoftonline.com/{self.config.oauth_tenant_id}/oauth2/v2.0/token'

    def _access_token(self) -> str:
        if self.config.token:
            return self.config.token

        if not self._oauth_enabled():
            raise RuntimeError('Databricks PAT or OAuth credentials are required')

        now = time.time()
        if self._oauth_token and now < self._oauth_expires_at:
            return self._oauth_token

        token_url = self._oauth_url()
        payload = {
            'client_id': self.config.oauth_client_id,
            'grant_type': 'client_credentials',
            'scope': '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
            'client_secret': self.config.oauth_client_secret,
        }
        res = requests.post(
            token_url,
            data=payload,
            headers={'Accept': 'application/json'},
            timeout=30,
            verify=self.config.ssl_verify,
        )

        if res.status_code < 200 or res.status_code >= 300:
            raise RuntimeError(f'Databricks OAuth failed: {res.status_code} {res.text}')

        body = res.json()
        token = body.get('access_token')
        expires_in = int(body.get('expires_in', 3600))
        if not token:
            raise RuntimeError('Databricks OAuth response did not include access_token')

        self._oauth_token = token
        self._oauth_expires_at = now + max(60, expires_in - 30)
        return self._oauth_token
