from __future__ import annotations

import logging

import pandas as pd

from app.models.schemas import ExtractDatasetResult
from app.services.databricks_client import DatabricksClient, DatabricksConfig
from app.services.dataset_catalog_loader import DatasetCatalogLoader, OpenAPILoader
from app.services.deputy_service import DeputyConfig, DeputyService
from app.services.run_store import RunStore
from app.settings import Settings

logger = logging.getLogger(__name__)


class ConfiguredDatasetExtractor:
    def __init__(self, settings: Settings, run_store: RunStore) -> None:
        self.settings = settings
        self.run_store = run_store

    def extract(self, run_id: str, dataset_keys: list[str]) -> list[ExtractDatasetResult]:
        self.run_store.ensure_run(run_id)
        catalog_loader = DatasetCatalogLoader(self.settings.datasets_config_file)
        catalog = catalog_loader.load()
        output_dir = self.run_store.get_run_upload_dir(run_id)
        labels = dataset_keys or [x.key for x in catalog.datasets]

        databricks_client = DatabricksClient(
            DatabricksConfig(
                host=self.settings.databricks_host,
                token=self.settings.databricks_token,
                warehouse_id=self.settings.databricks_sql_warehouse_id,
                catalog=self.settings.databricks_catalog,
                schema=self.settings.databricks_schema,
                wait_timeout=self.settings.databricks_wait_timeout,
                ssl_verify=self.settings.databricks_ssl_verify,
                oauth_tenant_id=self.settings.databricks_oauth_tenant_id,
                oauth_client_id=self.settings.databricks_oauth_client_id,
                oauth_client_secret=self.settings.databricks_oauth_client_secret,
                oauth_token_url=self.settings.databricks_oauth_token_url,
            )
        )
        deputy_service = DeputyService(
            DeputyConfig(
                base_url=self.settings.deputy_base,
                access_token=self.settings.deputy_access_token,
                client_id=self.settings.deputy_client_id,
                client_secret=self.settings.deputy_client_secret,
                redirect_uri=self.settings.deputy_redirect_uri,
                ssl_verify=self.settings.deputy_ssl_verify,
                timeout_seconds=self.settings.deputy_timeout_seconds,
            )
        )

        results: list[ExtractDatasetResult] = []
        for dataset_key in labels:
            try:
                dataset = next((x for x in catalog.datasets if x.key == dataset_key), None)
                if dataset is None:
                    raise KeyError(f'Dataset not found in catalog: {dataset_key}')

                output_file = output_dir / _dataset_to_filename(dataset.key)
                output_file.parent.mkdir(parents=True, exist_ok=True)
                logger.info(
                    'run.extract dataset.start run_id=%s dataset=%s type=%s service=%s',
                    run_id,
                    dataset.key,
                    dataset.type,
                    dataset.service,
                )

                if dataset.type == 'sql':
                    row_count = self._extract_sql_dataset(dataset, catalog_loader, databricks_client, output_file)
                elif dataset.type == 'api':
                    row_count = self._extract_api_dataset(dataset, catalog_loader, deputy_service, output_file)
                else:
                    raise ValueError(f'Unsupported dataset type: {dataset.type}')

                results.append(
                    ExtractDatasetResult(
                        dataset=dataset.key,
                        status='ok',
                        rows=row_count,
                        output_file=str(output_file),
                    )
                )
            except Exception as exc:
                logger.exception('run.extract dataset.error run_id=%s dataset=%s error=%s', run_id, dataset_key, exc)
                results.append(ExtractDatasetResult(dataset=dataset_key, status='error', error=str(exc)))

        return results

    def _extract_sql_dataset(self, dataset, catalog_loader, databricks_client, output_file) -> int:
        if dataset.service != 'databricks':
            raise ValueError(f'Unsupported SQL service: {dataset.service}')
        if not databricks_client.is_configured():
            raise RuntimeError('Databricks is not configured')
        if not dataset.query_file:
            raise ValueError(f'Missing query_file for dataset: {dataset.key}')

        query_path = catalog_loader.resolve_path(dataset.query_file)
        query = query_path.read_text(encoding='utf-8').strip()
        rows = databricks_client.execute_query(query)
        df = pd.DataFrame(rows)
        df.to_csv(output_file, index=False)
        logger.info('run.extract dataset.sql.done dataset=%s rows=%d file=%s', dataset.key, len(df), output_file)
        return len(df)

    def _extract_api_dataset(self, dataset, catalog_loader, deputy_service, output_file) -> int:
        if dataset.service != 'deputy':
            raise ValueError(f'Unsupported API service: {dataset.service}')
        if not deputy_service.is_configured():
            raise RuntimeError('Deputy is not configured')
        if not dataset.openapi_file or not dataset.endpoint or not dataset.method:
            raise ValueError(f'Missing API config for dataset: {dataset.key}')

        openapi_path = catalog_loader.resolve_path(dataset.openapi_file)
        openapi_loader = OpenAPILoader(openapi_path)
        request_body = openapi_loader.get_request_example(
            dataset.endpoint,
            dataset.method,
            dataset.example_param,
        ) or {}
        if dataset.example_param and not request_body:
            raise ValueError(f'Example param "{dataset.example_param}" not found for endpoint {dataset.endpoint}')

        logger.info(
            'run.extract dataset.api.request_example dataset=%s example_param=%s',
            dataset.key,
            dataset.example_param,
        )
        response = deputy_service.make_request(
            endpoint=dataset.endpoint,
            data=request_body,
            method=dataset.method.upper(),
        )
        if response is False:
            raise RuntimeError(f'Deputy API request failed for dataset {dataset.key}')

        if isinstance(response, list):
            df = pd.DataFrame(response)
        elif isinstance(response, dict):
            records = response.get('records')
            df = pd.DataFrame(records if isinstance(records, list) else [response])
        else:
            df = pd.DataFrame([])

        df.to_csv(output_file, index=False)
        logger.info('run.extract dataset.api.done dataset=%s rows=%d file=%s', dataset.key, len(df), output_file)
        return len(df)


def _dataset_to_filename(dataset_key: str) -> str:
    safe = dataset_key.replace('/', '_')
    return f'{safe}.csv'
