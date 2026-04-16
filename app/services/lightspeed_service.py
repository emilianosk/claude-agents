from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class LightspeedRegionConfig:
    domain: str
    token: str


@dataclass
class LightspeedConfig:
    api_version: str = '2026-04'
    default_region: str = 'AU'
    regions: dict[str, LightspeedRegionConfig] | None = None
    ssl_verify: bool = True
    timeout_seconds: int = 30


class LightspeedService:
    """Lightspeed Retail (X-Series) API infrastructure.

    Focused service for authenticated multi-region API requests.
    """

    def __init__(self, config: LightspeedConfig) -> None:
        self.config = config
        if self.config.regions is None:
            self.config.regions = {}

    def validate_config(self, region: str | None = None) -> dict[str, Any]:
        r = self._normalize_region(region)
        cfg = self.config.regions.get(r)
        errors = []
        if not cfg:
            errors.append(f'Missing Lightspeed region configuration: {r}')
        else:
            if not cfg.domain:
                errors.append(f'Missing Lightspeed domain for region: {r}')
            if not cfg.token:
                errors.append(f'Missing Lightspeed token for region: {r}')

        return {'valid': len(errors) == 0, 'errors': errors, 'region': r}

    def is_configured(self, region: str | None = None) -> bool:
        result = self.validate_config(region)
        return bool(result['valid'])

    def request(
        self,
        region: str | None,
        method: str,
        path: str,
        query_params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        r = self._normalize_region(region)
        cfg_result = self.validate_config(r)
        if not cfg_result['valid']:
            raise RuntimeError('; '.join(cfg_result['errors']))

        region_cfg = self.config.regions[r]
        url = f"{self._build_base_url(region_cfg.domain).rstrip('/')}/{path.lstrip('/')}"

        response = requests.request(
            method.upper(),
            url,
            headers=self._default_headers(region_cfg.token),
            params=query_params or None,
            json=json_body if method.upper() in {'POST', 'PUT', 'PATCH'} else None,
            timeout=self.config.timeout_seconds,
            verify=self.config.ssl_verify,
        )
        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(f'Lightspeed request failed ({response.status_code}): {response.text}')

        return response.json() if response.text else {}

    def test_connection(self, region: str | None = None) -> dict[str, Any]:
        # Lightweight call to confirm auth/base URL is valid.
        payload = self.request(region, 'GET', '/outlets')
        count = len(payload) if isinstance(payload, list) else 1
        return {'success': True, 'region': self._normalize_region(region), 'count': count}

    def _default_headers(self, token: str) -> dict[str, str]:
        return {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

    def _build_base_url(self, domain: str) -> str:
        d = domain.strip()
        if d.startswith('http://') or d.startswith('https://'):
            return f"{d.rstrip('/')}/api/{self.config.api_version}"
        if '.retail.lightspeed.app' in d:
            return f'https://{d}/api/{self.config.api_version}'
        return f'https://{d}.retail.lightspeed.app/api/{self.config.api_version}'

    def _normalize_region(self, region: str | None) -> str:
        value = (region or self.config.default_region).strip().upper()
        return value or 'AU'
