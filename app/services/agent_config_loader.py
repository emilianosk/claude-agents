from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator


class AgentSpec(BaseModel):
    name: str
    enabled: bool = True
    prompt_file: str | None = None
    input_datasets: list[str] = Field(default_factory=list)
    output_schema: dict[str, Any] | None = None
    model_override: str | None = None

    @field_validator('name')
    @classmethod
    def validate_name(cls, value: str) -> str:
        out = value.strip()
        if not out:
            raise ValueError('Agent name cannot be empty')
        return out


class ConsensusSpec(BaseModel):
    prompt_file: str = 'consensus_prompt.md'
    min_agents: int = 2
    output_schema: dict[str, Any] | None = None


class AgentsConfig(BaseModel):
    agents: list[AgentSpec]
    consensus: ConsensusSpec = Field(default_factory=ConsensusSpec)


class AgentConfigLoader:
    def __init__(self, config_file: str, prompts_dir: str) -> None:
        self.config_file = Path(config_file)
        self.prompts_dir = Path(prompts_dir)

    def load(self) -> AgentsConfig:
        if not self.config_file.exists():
            raise FileNotFoundError(f'Agents config file not found: {self.config_file}')

        raw = yaml.safe_load(self.config_file.read_text(encoding='utf-8')) or {}
        cfg = AgentsConfig.model_validate(raw)

        names = [x.name for x in cfg.agents]
        dupes = {x for x in names if names.count(x) > 1}
        if dupes:
            raise ValueError(f'Duplicate agent names found: {sorted(dupes)}')

        for agent in cfg.agents:
            if not agent.prompt_file:
                agent.prompt_file = f'{agent.name}_prompt.md'

        return cfg

    def read_prompt(self, prompt_file: str) -> str:
        candidates = [
            Path(prompt_file),
            self.prompts_dir / prompt_file,
            self.config_file.parent / prompt_file,
        ]

        for path in candidates:
            if path.exists() and path.is_file():
                content = path.read_text(encoding='utf-8').strip()
                if not content:
                    raise ValueError(f'Prompt file is empty: {path}')
                return content

        raise FileNotFoundError(
            f'Prompt file not found: {prompt_file}. Checked: '
            + ', '.join(str(x) for x in candidates)
        )
