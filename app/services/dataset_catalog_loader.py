from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator


class DatasetSpec(BaseModel):
    key: str
    service: str
    type: str
    query_file: str | None = None
    openapi_file: str | None = None
    endpoint: str | None = None
    method: str | None = None
    example_param: str | None = Field(default=None, alias='example-param')

    @field_validator('key')
    @classmethod
    def _validate_key(cls, value: str) -> str:
        key = value.strip()
        if not key:
            raise ValueError('Dataset key cannot be empty')
        return key

    @field_validator('service')
    @classmethod
    def _normalize_service(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator('type')
    @classmethod
    def _normalize_type(cls, value: str) -> str:
        out = value.strip().lower()
        if out not in {'sql', 'api'}:
            raise ValueError('Dataset type must be sql or api')
        return out


class DatasetCatalog(BaseModel):
    datasets: list[DatasetSpec] = Field(default_factory=list)


class DatasetCatalogLoader:
    def __init__(self, catalog_file: str) -> None:
        self.catalog_file = Path(catalog_file)

    def load(self) -> DatasetCatalog:
        if not self.catalog_file.exists():
            raise FileNotFoundError(f'Dataset catalog file not found: {self.catalog_file}')

        raw = yaml.safe_load(self.catalog_file.read_text(encoding='utf-8')) or {}
        catalog = DatasetCatalog.model_validate(raw)

        keys = [x.key for x in catalog.datasets]
        dupes = {x for x in keys if keys.count(x) > 1}
        if dupes:
            raise ValueError(f'Duplicate dataset keys in catalog: {sorted(dupes)}')

        return catalog

    def get_dataset(self, key: str) -> DatasetSpec:
        catalog = self.load()
        for item in catalog.datasets:
            if item.key == key:
                return item
        raise KeyError(f'Dataset key not found in catalog: {key}')

    def resolve_path(self, value: str) -> Path:
        path = Path(value)
        if path.exists():
            return path
        alt = self.catalog_file.parent / value
        if alt.exists():
            return alt
        raise FileNotFoundError(f'Path not found: {value}')


class OpenAPILoader:
    def __init__(self, openapi_file: Path) -> None:
        self.openapi_file = openapi_file
        self.spec = yaml.safe_load(openapi_file.read_text(encoding='utf-8')) or {}

    def get_request_example(self, endpoint: str, method: str, example_param: str | None = None) -> dict[str, Any] | None:
        paths = self.spec.get('paths', {})
        endpoint_node = paths.get(endpoint)
        if not isinstance(endpoint_node, dict):
            return None

        method_node = endpoint_node.get(method.lower())
        if not isinstance(method_node, dict):
            return None

        request_body = method_node.get('requestBody', {})
        content = request_body.get('content', {}) if isinstance(request_body, dict) else {}
        app_json = content.get('application/json', {}) if isinstance(content, dict) else {}
        if not isinstance(app_json, dict):
            return None

        if example_param:
            # Non-standard exported specs sometimes put named examples directly under application/json,
            # e.g. "example-piercers: {...}".
            if example_param in app_json:
                selected = self._coerce_example_to_dict(app_json.get(example_param))
                if selected is not None:
                    return selected

            examples = app_json.get('examples')
            if isinstance(examples, dict) and example_param in examples:
                selected_node = examples.get(example_param)
                if isinstance(selected_node, dict) and 'value' in selected_node:
                    selected = self._coerce_example_to_dict(selected_node.get('value'))
                else:
                    selected = self._coerce_example_to_dict(selected_node)
                if selected is not None:
                    return selected

            return None

        example = app_json.get('example')
        if example is None:
            examples = app_json.get('examples')
            if isinstance(examples, dict) and examples:
                first = next(iter(examples.values()))
                if isinstance(first, dict):
                    example = first.get('value')
        if example is None:
            # Fallback for non-standard keys like "example-foo" directly under application/json.
            for key, value in app_json.items():
                if isinstance(key, str) and key.startswith('example-'):
                    example = value
                    break

        return self._coerce_example_to_dict(example)

    def _coerce_example_to_dict(self, example: Any) -> dict[str, Any] | None:
        if example is None:
            return None
        if isinstance(example, dict):
            return example
        if not isinstance(example, str):
            return None

        text = example.strip()
        if not text:
            return None

        # Strip common malformed markers and inline comments from exported specs.
        text = text.replace('appapp{', '{')
        text = re.sub(r'//.*', '', text)
        text = text.strip()

        start = text.find('{')
        end = text.rfind('}')
        if start == -1 or end == -1 or end <= start:
            return None

        candidate = text[start : end + 1]
        candidate = re.sub(r',\s*([}\]])', r'\1', candidate)

        try:
            data = json.loads(candidate)
            return data if isinstance(data, dict) else None
        except json.JSONDecodeError:
            return None
