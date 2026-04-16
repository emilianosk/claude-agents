from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile

from app.models.schemas import (
    AnalyzeRequest,
    AnalyzeResponse,
    ExtractDatasetResult,
    ExtractRequest,
    ExtractResponse,
    RunInitResponse,
    SalesforceQueryRequest,
)
from app.services.agent_config_loader import AgentConfigLoader
from app.services.analysis_orchestrator import AnalysisOrchestrator
from app.services.claude_client import ClaudeClient
from app.services.csv_profiler import CSVProfiler
from app.services.databricks_client import DatabricksClient, DatabricksConfig
from app.services.dataset_catalog_loader import DatasetCatalogLoader, OpenAPILoader
from app.services.deputy_service import DeputyConfig, DeputyService
from app.services.run_store import RunStore
from app.services.salesforce_service import SalesforceConfig, SalesforceService
from app.settings import Settings, get_settings

router = APIRouter(prefix='/api/v1', tags=['roster-analysis'])


def get_run_store(settings: Settings = Depends(get_settings)) -> RunStore:
    return RunStore(settings.uploads_path, settings.results_path)


def get_salesforce_service(settings: Settings = Depends(get_settings)) -> SalesforceService:
    config = SalesforceConfig(
        base_login_url=settings.salesforce_base_login_url,
        base_url=settings.salesforce_base_url,
        client_id=settings.salesforce_client_id,
        client_secret=settings.salesforce_client_secret,
        refresh_token=settings.salesforce_refresh_token,
        api_version=settings.salesforce_api_version,
        timeout_seconds=settings.salesforce_timeout_seconds,
        ssl_verify=settings.salesforce_ssl_verify,
    )
    return SalesforceService(config)


def _dataset_to_filename(dataset_key: str) -> str:
    safe = dataset_key.replace('/', '_')
    return f'{safe}.csv'


@router.get('/health')
def health() -> dict:
    return {'status': 'ok'}


@router.get('/salesforce/test')
def salesforce_test(service: SalesforceService = Depends(get_salesforce_service)) -> dict:
    config_result = service.validate_config()
    if not config_result['valid']:
        raise HTTPException(status_code=400, detail='; '.join(config_result['errors']))

    try:
        return service.test_connection()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Salesforce test failed: {exc}') from exc


@router.post('/salesforce/query')
def salesforce_query(
    payload: SalesforceQueryRequest,
    service: SalesforceService = Depends(get_salesforce_service),
) -> dict:
    config_result = service.validate_config()
    if not config_result['valid']:
        raise HTTPException(status_code=400, detail='; '.join(config_result['errors']))

    if not payload.soql.strip():
        raise HTTPException(status_code=400, detail='SOQL is required')

    try:
        if payload.query_all:
            records = service.query_all(payload.soql, max_pages=payload.max_pages)
            return {'success': True, 'count': len(records), 'records': records}
        result = service.query(payload.soql)
        return {'success': True, 'result': result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Salesforce query failed: {exc}') from exc


@router.post('/runs/init', response_model=RunInitResponse)
def init_run(run_store: RunStore = Depends(get_run_store)) -> RunInitResponse:
    return RunInitResponse(run_id=run_store.create_run())


@router.post('/upload/{run_id}/{dataset_label}')
async def upload_dataset(
    run_id: str,
    dataset_label: str,
    file: UploadFile = File(...),
    run_store: RunStore = Depends(get_run_store),
) -> dict:
    run_store.ensure_run(run_id)
    upload_dir = run_store.get_run_upload_dir(run_id)
    output_file = upload_dir / _dataset_to_filename(dataset_label)

    content = await file.read()
    output_file.write_bytes(content)

    return {
        'run_id': run_id,
        'dataset': dataset_label,
        'stored_file': str(output_file),
    }


@router.post('/extract/{run_id}', response_model=ExtractResponse)
def extract_datasets(
    run_id: str,
    payload: ExtractRequest,
    settings: Settings = Depends(get_settings),
    run_store: RunStore = Depends(get_run_store),
) -> ExtractResponse:
    run_store.ensure_run(run_id)

    databricks_config = DatabricksConfig(
        host=settings.databricks_host,
        token=settings.databricks_token,
        warehouse_id=settings.databricks_sql_warehouse_id,
        catalog=settings.databricks_catalog,
        schema=settings.databricks_schema,
        wait_timeout=settings.databricks_wait_timeout,
        ssl_verify=settings.databricks_ssl_verify,
        oauth_tenant_id=settings.databricks_oauth_tenant_id,
        oauth_client_id=settings.databricks_oauth_client_id,
        oauth_client_secret=settings.databricks_oauth_client_secret,
        oauth_token_url=settings.databricks_oauth_token_url,
    )
    databricks_client = DatabricksClient(databricks_config)
    deputy_service = DeputyService(
        DeputyConfig(
            base_url=settings.deputy_base,
            access_token=settings.deputy_access_token,
            client_id=settings.deputy_client_id,
            client_secret=settings.deputy_client_secret,
            redirect_uri=settings.deputy_redirect_uri,
            ssl_verify=settings.deputy_ssl_verify,
            timeout_seconds=settings.deputy_timeout_seconds,
        )
    )
    catalog_loader = DatasetCatalogLoader(settings.datasets_config_file)
    catalog = catalog_loader.load()
    output_dir = run_store.get_run_upload_dir(run_id)

    labels = payload.datasets or [x.key for x in catalog.datasets]
    results: list[ExtractDatasetResult] = []

    for dataset_key in labels:
        try:
            dataset = next((x for x in catalog.datasets if x.key == dataset_key), None)
            if dataset is None:
                raise KeyError(f'Dataset not found in catalog: {dataset_key}')

            output_file = output_dir / _dataset_to_filename(dataset.key)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            if dataset.type == 'sql':
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
                row_count = len(df)
            elif dataset.type == 'api':
                if dataset.service != 'deputy':
                    raise ValueError(f'Unsupported API service: {dataset.service}')
                if not deputy_service.is_configured():
                    raise RuntimeError('Deputy is not configured')
                if not dataset.openapi_file or not dataset.endpoint or not dataset.method:
                    raise ValueError(f'Missing API config for dataset: {dataset.key}')

                openapi_path = catalog_loader.resolve_path(dataset.openapi_file)
                openapi_loader = OpenAPILoader(openapi_path)
                request_body = openapi_loader.get_request_example(dataset.endpoint, dataset.method) or {}

                response = deputy_service.make_request(
                    endpoint=dataset.endpoint,
                    data=request_body,
                    method=dataset.method.upper(),
                )
                if response is False:
                    raise RuntimeError(f'Deputy API request failed for dataset {dataset.key}')

                if isinstance(response, list):
                    df = pd.DataFrame(response)
                    row_count = len(df)
                elif isinstance(response, dict):
                    records = response.get('records')
                    if isinstance(records, list):
                        df = pd.DataFrame(records)
                        row_count = len(df)
                    else:
                        df = pd.DataFrame([response])
                        row_count = len(df)
                else:
                    df = pd.DataFrame([])
                    row_count = 0

                df.to_csv(output_file, index=False)
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
            results.append(
                ExtractDatasetResult(
                    dataset=dataset_key,
                    status='error',
                    error=str(exc),
                )
            )

    return ExtractResponse(run_id=run_id, results=results)


@router.post('/analyze/{run_id}', response_model=AnalyzeResponse)
def analyze_run(
    run_id: str,
    payload: AnalyzeRequest,
    settings: Settings = Depends(get_settings),
    run_store: RunStore = Depends(get_run_store),
) -> AnalyzeResponse:
    run_store.ensure_run(run_id)
    upload_dir = run_store.get_run_upload_dir(run_id)

    profiler = CSVProfiler()

    profiles = {}
    available_datasets: list[str] = []
    for csv_path in sorted(upload_dir.glob('*.csv')):
        if csv_path.exists() and csv_path.stat().st_size > 0:
            dataset_key = csv_path.stem
            profiles[dataset_key] = profiler.profile_csv(csv_path, include_raw_preview=payload.include_raw_preview)
            available_datasets.append(dataset_key)

    if not profiles:
        raise HTTPException(status_code=400, detail='No datasets available for analysis in this run')

    claude = ClaudeClient(
        api_key=settings.anthropic_api_key,
        model=settings.anthropic_model,
        max_tokens=settings.anthropic_max_tokens,
    )
    config_loader = AgentConfigLoader(
        config_file=settings.agents_config_file,
        prompts_dir=settings.agent_prompts_dir,
    )
    orchestrator = AnalysisOrchestrator(claude, config_loader)
    agent_outputs, consensus, final_decision = orchestrator.run(payload.question, profiles)

    result_dir = run_store.get_run_result_dir(run_id)
    result_file = orchestrator.write_result_markdown(result_dir, payload.question, agent_outputs, consensus)

    return AnalyzeResponse(
        run_id=run_id,
        available_datasets=available_datasets,
        profiles=profiles,
        agent_outputs=agent_outputs,
        consensus=consensus,
        final_decision=final_decision,
        result_file=str(result_file),
    )
