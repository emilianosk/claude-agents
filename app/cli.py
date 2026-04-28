from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

from app.services.agent_config_loader import AgentConfigLoader
from app.services.analysis_orchestrator import AnalysisOrchestrator
from app.services.claude_client import ClaudeClient
from app.services.configured_dataset_extractor import ConfiguredDatasetExtractor
from app.services.csv_profiler import CSVProfiler
from app.services.derived_features import (
    build_clinic_hourly_occupancy,
    build_frosters_hourly_patterns,
    build_frosters_last_4m,
    build_kepler_hourly_with_location,
    build_locations_with_operational_units,
    build_pos_hourly_demand,
    build_store_piercer_sid_map,
)
from app.services.run_store import RunStore
from app.settings import get_settings

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='claude-agents')
    subparsers = parser.add_subparsers(dest='command', required=True)

    analyze = subparsers.add_parser('analyze', help='Run analysis for an existing run directory')
    analyze.add_argument('--run-id', required=True)
    analyze.add_argument('--question', required=True)
    analyze.add_argument('--agents', default=None, help='Comma-separated agent names, e.g. researcher,skillset')
    analyze.add_argument('--consensus', default='default', help='Consensus profile name from agents.yaml')
    analyze.add_argument('--include-raw-preview', action='store_true')
    analyze.add_argument('--summary', action='store_true', help='Print compact output instead of the full API-style JSON')

    extract = subparsers.add_parser('extract', help='Extract configured datasets to CSV for a run')
    extract.add_argument('--run-id', required=True)
    extract.add_argument(
        '--datasets',
        default=None,
        help='Comma-separated dataset keys. Omit or pass empty value to extract all configured datasets.',
    )

    features = subparsers.add_parser('features', help='Generate derived feature CSVs for a run')
    features.add_argument('--run-id', required=True)
    features.add_argument(
        '--features',
        default=None,
        help='Comma-separated feature names. Omit or pass empty value to generate all supported features.',
    )

    args = parser.parse_args(argv)

    if args.command == 'analyze':
        return _analyze(args)
    if args.command == 'extract':
        return _extract(args)
    if args.command == 'features':
        return _features(args)

    parser.error(f'Unknown command: {args.command}')
    return 2


def _features(args: argparse.Namespace) -> int:
    settings = get_settings()
    run_store = RunStore(settings.uploads_path, settings.results_path)
    run_store.ensure_run(args.run_id)

    upload_dir = run_store.get_run_upload_dir(args.run_id)
    requested = set(_parse_csv_arg(args.features) or _feature_builders().keys())
    results = []

    for name, builder in _feature_builders().items():
        if name not in requested:
            continue
        try:
            output_file = builder(upload_dir)
            results.append({'feature': name, 'status': 'ok', 'output_file': str(output_file)})
        except Exception as exc:
            logger.exception('cli.features error run_id=%s feature=%s error=%s', args.run_id, name, exc)
            results.append({'feature': name, 'status': 'error', 'error': str(exc)})

    unknown = sorted(requested - set(_feature_builders().keys()))
    for name in unknown:
        results.append({'feature': name, 'status': 'error', 'error': f'Unknown feature: {name}'})

    print(json.dumps({'run_id': args.run_id, 'results': results}, ensure_ascii=True, indent=2))
    return 1 if any(x['status'] == 'error' for x in results) else 0


def _extract(args: argparse.Namespace) -> int:
    settings = get_settings()
    run_store = RunStore(settings.uploads_path, settings.results_path)
    dataset_keys = _parse_csv_arg(args.datasets) or []

    extractor = ConfiguredDatasetExtractor(settings, run_store)
    results = extractor.extract(args.run_id, dataset_keys)
    response = {
        'run_id': args.run_id,
        'results': [x.model_dump() for x in results],
    }
    print(json.dumps(response, ensure_ascii=True, indent=2))

    return 1 if any(x.status == 'error' for x in results) else 0


def _analyze(args: argparse.Namespace) -> int:
    settings = get_settings()
    run_store = RunStore(settings.uploads_path, settings.results_path)
    run_store.ensure_run(args.run_id)

    upload_dir = run_store.get_run_upload_dir(args.run_id)
    _build_derived_datasets(upload_dir)
    profiles, available_datasets = _load_profiles(upload_dir, include_raw_preview=args.include_raw_preview)
    if not profiles:
        print(f'No datasets available for analysis in run {args.run_id}', file=sys.stderr)
        return 1

    selected_agents = _parse_agents(args.agents)
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
            args.question,
            profiles,
            selected_agents=selected_agents,
            consensus_profile=args.consensus,
        )
    except (RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    result_dir = run_store.get_run_result_dir(args.run_id)
    result_file = orchestrator.write_result_markdown(result_dir, args.question, agent_outputs, consensus)
    response = {
        'run_id': args.run_id,
        'available_datasets': available_datasets,
        'profiles': profiles,
        'agent_outputs': [x.model_dump() for x in agent_outputs],
        'consensus': consensus,
        'final_decision': final_decision,
        'result_file': str(result_file),
    }
    if args.summary:
        response = {
            'run_id': args.run_id,
            'available_datasets': available_datasets,
            'agents': [x.agent for x in agent_outputs],
            'consensus_profile': args.consensus,
            'consensus_level': consensus.get('consensus_level'),
            'final_decision': final_decision,
            'result_file': str(result_file),
        }

    print(json.dumps(response, ensure_ascii=True, indent=2))
    return 0


def _parse_agents(value: str | None) -> list[str] | None:
    return _parse_csv_arg(value)


def _parse_csv_arg(value: str | None) -> list[str] | None:
    if value is None:
        return None
    items = [x.strip() for x in value.split(',') if x.strip()]
    return items or None


def _build_derived_datasets(upload_dir: Path) -> None:
    for builder in _feature_builders().values():
        try:
            builder(upload_dir)
        except FileNotFoundError as exc:
            logger.info('cli.analyze derived_dataset.skipped reason=%s', exc)
        except Exception as exc:
            logger.warning('cli.analyze derived_dataset.error error=%s', exc)


def _feature_builders():
    return {
        'FEATURES.KEPLER_HOURLY_PAST_4M': build_kepler_hourly_with_location,
        'FEATURES.LOCATIONS_WITH_OPERATIONAL_UNITS': build_locations_with_operational_units,
        'FEATURES.FROSTERS_LAST_4M': build_frosters_last_4m,
        'FEATURES.FROSTERS_HOURLY_PATTERNS': build_frosters_hourly_patterns,
        'FEATURES.POS_HOURLY_DEMAND_BY_STORE': build_pos_hourly_demand,
        'FEATURES.STORE_PIERCER_SID_MAP': build_store_piercer_sid_map,
        'FEATURES.CLINIC_HOURLY_OCCUPANCY': build_clinic_hourly_occupancy,
    }


def _load_profiles(upload_dir: Path, include_raw_preview: bool) -> tuple[dict[str, Any], list[str]]:
    profiler = CSVProfiler()
    profiles: dict[str, Any] = {}
    available_datasets: list[str] = []

    for csv_path in sorted(upload_dir.glob('*.csv')):
        if csv_path.exists() and csv_path.stat().st_size > 0:
            dataset_key = csv_path.stem
            is_feature = dataset_key.startswith('FEATURES.')
            profiles[dataset_key] = profiler.profile_csv(
                csv_path,
                include_raw_preview=include_raw_preview or is_feature,
                include_string_summary=is_feature,
            )
            available_datasets.append(dataset_key)

    return profiles, available_datasets


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    raise SystemExit(main())
