from pathlib import Path

from app.services.agent_config_loader import AgentConfigLoader


def test_load_agents_config_and_default_prompt_file(tmp_path: Path) -> None:
    prompts = tmp_path / 'prompts'
    prompts.mkdir(parents=True, exist_ok=True)
    (prompts / 'consensus_prompt.md').write_text('consensus prompt', encoding='utf-8')
    (prompts / 'researcher_prompt.md').write_text('researcher prompt', encoding='utf-8')

    config_file = tmp_path / 'agents.yaml'
    config_file.write_text(
        'agents:\n'
        '  - name: researcher\n'
        '    enabled: true\n'
        'consensus:\n'
        '  prompt_file: consensus_prompt.md\n',
        encoding='utf-8',
    )

    loader = AgentConfigLoader(str(config_file), str(prompts))
    cfg = loader.load()

    assert len(cfg.agents) == 1
    assert cfg.agents[0].name == 'researcher'
    assert cfg.agents[0].prompt_file == 'researcher_prompt.md'


def test_read_prompt_from_prompts_dir(tmp_path: Path) -> None:
    prompts = tmp_path / 'prompts'
    prompts.mkdir(parents=True, exist_ok=True)
    prompt_file = prompts / 'operator_prompt.md'
    prompt_file.write_text('operator instructions', encoding='utf-8')

    config_file = tmp_path / 'agents.yaml'
    config_file.write_text('agents: []\n', encoding='utf-8')

    loader = AgentConfigLoader(str(config_file), str(prompts))
    content = loader.read_prompt('operator_prompt.md')

    assert content == 'operator instructions'
