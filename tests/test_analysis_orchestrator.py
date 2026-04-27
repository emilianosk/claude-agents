from pathlib import Path

import pytest

from app.services.agent_config_loader import AgentConfigLoader
from app.services.analysis_orchestrator import AnalysisOrchestrator
from app.services.claude_client import ClaudeClient


def _write_config(tmp_path: Path) -> AgentConfigLoader:
    prompts = tmp_path / 'prompts'
    prompts.mkdir(parents=True, exist_ok=True)
    for name in ('researcher_prompt.md', 'skillset_prompt.md', 'consensus_prompt.md'):
        (prompts / name).write_text(name, encoding='utf-8')

    config_file = tmp_path / 'agents.yaml'
    config_file.write_text(
        'agents:\n'
        '  - name: researcher\n'
        '    enabled: true\n'
        '    input_datasets:\n'
        '      - DATALAKE.POS_TRANSACTIONS\n'
        '  - name: skillset\n'
        '    enabled: true\n'
        'consensus_profiles:\n'
        '  default:\n'
        '    prompt_file: consensus_prompt.md\n'
        '    min_agents: 1\n',
        encoding='utf-8',
    )
    return AgentConfigLoader(str(config_file), str(prompts))


def test_run_uses_selected_agents(tmp_path: Path) -> None:
    orchestrator = AnalysisOrchestrator(
        ClaudeClient(api_key='', model='x', max_tokens=10),
        _write_config(tmp_path),
    )

    outputs, _, _ = orchestrator.run(
        'question',
        {'DATALAKE.POS_TRANSACTIONS': {'rows': 1}},
        selected_agents=['researcher'],
        consensus_profile='default',
    )

    assert [x.agent for x in outputs] == ['researcher']


def test_run_rejects_unknown_selected_agent(tmp_path: Path) -> None:
    orchestrator = AnalysisOrchestrator(
        ClaudeClient(api_key='', model='x', max_tokens=10),
        _write_config(tmp_path),
    )

    with pytest.raises(ValueError, match='Unknown agent names'):
        orchestrator.run(
            'question',
            {'DATALAKE.POS_TRANSACTIONS': {'rows': 1}},
            selected_agents=['missing'],
            consensus_profile='default',
        )


def test_run_rejects_unknown_consensus_profile(tmp_path: Path) -> None:
    orchestrator = AnalysisOrchestrator(
        ClaudeClient(api_key='', model='x', max_tokens=10),
        _write_config(tmp_path),
    )

    with pytest.raises(ValueError, match='Unknown consensus profile'):
        orchestrator.run(
            'question',
            {'DATALAKE.POS_TRANSACTIONS': {'rows': 1}},
            selected_agents=['researcher'],
            consensus_profile='missing',
        )
