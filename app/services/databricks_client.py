from __future__ import annotations

import time
import logging
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
        logger.info('databricks.query.start statement_len=%d', len(statement or ''))

        payload = {
            'statement': statement,
            'warehouse_id': self.config.warehouse_id,
            'catalog': self.config.catalog or None,
            'schema': self.config.schema or None,
            'wait_timeout': self.config.wait_timeout or None,
            'disposition': 'EXTERNAL_LINKS',
            'format': 'JSON_ARRAY',
        }
        payload = {k: v for k, v in payload.items() if v not in (None, '')}

        response = self._request('POST', '/api/2.0/sql/statements', json=payload)
        status = response.get('status', {}) if isinstance(response, dict) else {}
        state = status.get('state')
        if state in {'FAILED', 'CANCELED', 'CLOSED'}:
            error_node = status.get('error', {}) if isinstance(status, dict) else {}
            error_code = error_node.get('error_code') or status.get('error_code')
            error_msg = error_node.get('message') or status.get('error_message') or 'unknown Databricks error'
            statement_id = response.get('statement_id') if isinstance(response, dict) else None
            logger.error(
                'databricks.query.failed state=%s statement_id=%s error_code=%s error=%s',
                state,
                statement_id,
                error_code,
                error_msg,
            )
            raise RuntimeError(
                f'Databricks statement failed ({state})'
                f'{" [" + str(error_code) + "]" if error_code else ""}: {error_msg}'
            )

        result = response.get('result', {}) if isinstance(response, dict) else {}
        data_array = result.get('data_array') or []
        if not data_array and isinstance(result, dict) and result.get('external_links'):
            data_array = self._collect_external_link_rows(response)

        # Databricks SQL statements API returns manifest at top-level (not under result).
        manifest = response.get('manifest') if isinstance(response, dict) else None
        if not isinstance(manifest, dict):
            manifest = result.get('manifest', {}) if isinstance(result, dict) else {}

        schema = manifest.get('schema', {}) if isinstance(manifest, dict) else {}
        columns = [c.get('name') for c in schema.get('columns', [])]

        if not columns:
            logger.warning('databricks.query.no_columns state=%s', state)
            return []

        rows: list[dict] = []
        for row in data_array:
            row_dict = {columns[i]: row[i] if i < len(row) else None for i in range(len(columns))}
            rows.append(row_dict)
        logger.info('databricks.query.done rows=%d columns=%d', len(rows), len(columns))
        return rows

    def _collect_external_link_rows(self, response: dict) -> list[list]:
        rows: list[list] = []
        result = response.get('result', {}) if isinstance(response, dict) else {}
        links = result.get('external_links') if isinstance(result, dict) else None
        if not isinstance(links, list):
            links = []

        while links:
            next_internal_link = None
            for item in links:
                if not isinstance(item, dict):
                    continue
                ext_url = item.get('external_link')
                if not ext_url:
                    continue

                ext_headers = item.get('http_headers', {})
                if not isinstance(ext_headers, dict):
                    ext_headers = {}
                ext_resp = requests.get(
                    ext_url,
                    headers=ext_headers,
                    timeout=45,
                    verify=self.config.ssl_verify,
                )
                if ext_resp.status_code < 200 or ext_resp.status_code >= 300:
                    raise RuntimeError(f'Databricks external chunk download failed: {ext_resp.status_code}')

                ext_json = ext_resp.json()
                if isinstance(ext_json, dict):
                    chunk_rows = ext_json.get('data_array')
                    if isinstance(chunk_rows, list):
                        rows.extend(chunk_rows)
                elif isinstance(ext_json, list):
                    rows.extend(ext_json)

                maybe_next = item.get('next_chunk_internal_link')
                if isinstance(maybe_next, str) and maybe_next:
                    next_internal_link = maybe_next

            if next_internal_link:
                next_chunk = self._request('GET', next_internal_link)
                if isinstance(next_chunk, dict):
                    next_links = next_chunk.get('external_links')
                    if not isinstance(next_links, list):
                        next_links = next_chunk.get('result', {}).get('external_links')
                    links = next_links if isinstance(next_links, list) else []
                else:
                    links = []
            else:
                links = []

        logger.info('databricks.query.external_links.done rows=%d', len(rows))
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

        logger.info('databricks.request.start method=%s path=%s', method.upper(), path)
        res = requests.request(
            method,
            url,
            headers=headers,
            timeout=45,
            verify=self.config.ssl_verify,
            **kwargs,
        )

        if res.status_code < 200 or res.status_code >= 300:
            logger.error('databricks.request.error method=%s path=%s status=%d', method.upper(), path, res.status_code)
            raise RuntimeError(f'Databricks request failed: {res.status_code} {res.text}')

        logger.info('databricks.request.done method=%s path=%s status=%d', method.upper(), path, res.status_code)
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
            logger.error('databricks.oauth.error status=%d', res.status_code)
            raise RuntimeError(f'Databricks OAuth failed: {res.status_code} {res.text}')

        body = res.json()
        token = body.get('access_token')
        expires_in = int(body.get('expires_in', 3600))
        if not token:
            raise RuntimeError('Databricks OAuth response did not include access_token')

        self._oauth_token = token
        self._oauth_expires_at = now + max(60, expires_in - 30)
        logger.info('databricks.oauth.done expires_in=%d', expires_in)
        return self._oauth_token


logger = logging.getLogger(__name__)
