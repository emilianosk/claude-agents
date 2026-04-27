from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile

from app.models.schemas import (
    AnalyzeRequest,
    AnalyzeResponse,
    ExtractRequest,
    ExtractResponse,
    RunInitResponse,
    SalesforceQueryRequest,
)
from app.services.agent_config_loader import AgentConfigLoader
from app.services.analysis_orchestrator import AnalysisOrchestrator
from app.services.claude_client import ClaudeClient
from app.services.configured_dataset_extractor import ConfiguredDatasetExtractor, _dataset_to_filename
from app.services.csv_profiler import CSVProfiler
from app.services.derived_features import build_pos_hourly_demand, build_store_piercer_sid_map
from app.services.run_store import RunStore
from app.services.salesforce_service import SalesforceConfig, SalesforceService
from app.settings import Settings, get_settings

router = APIRouter(prefix='/api/v1', tags=['roster-analysis'])
logger = logging.getLogger(__name__)


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
    run_id = run_store.create_run()
    logger.info('run.init created run_id=%s', run_id)
    return RunInitResponse(run_id=run_id)


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
    logger.info(
        'run.upload stored run_id=%s dataset=%s bytes=%d file=%s',
        run_id,
        dataset_label,
        len(content),
        output_file,
    )

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
    logger.info('run.extract start run_id=%s requested_datasets=%s', run_id, payload.datasets)
    extractor = ConfiguredDatasetExtractor(settings, run_store)
    results = extractor.extract(run_id, payload.datasets)

    ok_count = sum(1 for x in results if x.status == 'ok')
    err_count = sum(1 for x in results if x.status == 'error')
    logger.info('run.extract finished run_id=%s ok=%d error=%d', run_id, ok_count, err_count)
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
    logger.info(
        'run.analyze start run_id=%s question_len=%d selected_agents=%s consensus_profile=%s',
        run_id,
        len(payload.question or ''),
        payload.selected_agents,
        payload.consensus_profile,
    )

    # Build derived datasets from already extracted CSVs to avoid re-querying source systems.
    try:
        derived_file = build_pos_hourly_demand(upload_dir)
        logger.info('run.analyze derived_dataset.created run_id=%s file=%s', run_id, derived_file)
    except FileNotFoundError as exc:
        logger.info('run.analyze derived_dataset.skipped run_id=%s reason=%s', run_id, exc)
    except Exception as exc:
        logger.warning('run.analyze derived_dataset.error run_id=%s error=%s', run_id, exc)

    try:
        derived_file = build_store_piercer_sid_map(upload_dir)
        logger.info('run.analyze derived_dataset.created run_id=%s file=%s', run_id, derived_file)
    except FileNotFoundError as exc:
        logger.info('run.analyze derived_dataset.skipped run_id=%s reason=%s', run_id, exc)
    except Exception as exc:
        logger.warning('run.analyze derived_dataset.error run_id=%s error=%s', run_id, exc)

    profiler = CSVProfiler()

    profiles = {}
    available_datasets: list[str] = []
    for csv_path in sorted(upload_dir.glob('*.csv')):
        if csv_path.exists() and csv_path.stat().st_size > 0:
            dataset_key = csv_path.stem
            profiles[dataset_key] = profiler.profile_csv(csv_path, include_raw_preview=payload.include_raw_preview)
            available_datasets.append(dataset_key)

    if not profiles:
        logger.warning('run.analyze no_datasets run_id=%s upload_dir=%s', run_id, upload_dir)
        raise HTTPException(status_code=400, detail='No datasets available for analysis in this run')
    logger.info('run.analyze datasets.ready run_id=%s datasets=%s', run_id, available_datasets)

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
    try:
        agent_outputs, consensus, final_decision = orchestrator.run(
            payload.question,
            profiles,
            selected_agents=payload.selected_agents,
            consensus_profile=payload.consensus_profile,
        )
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    logger.info(
        'run.analyze orchestrator.done run_id=%s agent_outputs=%d consensus_level=%s',
        run_id,
        len(agent_outputs),
        consensus.get('consensus_level'),
    )

    result_dir = run_store.get_run_result_dir(run_id)
    result_file = orchestrator.write_result_markdown(result_dir, payload.question, agent_outputs, consensus)
    logger.info('run.analyze result.saved run_id=%s file=%s', run_id, result_file)

    return AnalyzeResponse(
        run_id=run_id,
        available_datasets=available_datasets,
        profiles=profiles,
        agent_outputs=agent_outputs,
        consensus=consensus,
        final_decision=final_decision,
        result_file=str(result_file),
    )
