from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from app.models.schemas import AgentOutput
from app.services.agent_config_loader import AgentConfigLoader, AgentSpec
from app.services.claude_client import ClaudeClient


class AnalysisOrchestrator:
    def __init__(self, claude_client: ClaudeClient, config_loader: AgentConfigLoader) -> None:
        self.claude_client = claude_client
        self.config_loader = config_loader

    def run(self, question: str, profiles: dict) -> tuple[list[AgentOutput], dict[str, Any], str]:
        return asyncio.run(self._run_async(question, profiles))

    async def _run_async(self, question: str, profiles: dict) -> tuple[list[AgentOutput], dict[str, Any], str]:
        config = self.config_loader.load()
        enabled_agents = [x for x in config.agents if x.enabled]
        if not enabled_agents:
            raise RuntimeError('No enabled agents found in agents config')
        logger.info(
            'orchestrator.start enabled_agents=%s profiles=%d question_len=%d',
            [x.name for x in enabled_agents],
            len(profiles),
            len(question or ''),
        )

        tasks = [
            asyncio.create_task(self._run_single_agent(spec=spec, question=question, profiles=profiles))
            for spec in enabled_agents
        ]
        outputs = await asyncio.gather(*tasks)
        logger.info('orchestrator.agents.done outputs=%d', len(outputs))
        if len(outputs) < config.consensus.min_agents:
            raise RuntimeError(
                f'Not enough agent outputs for consensus: got {len(outputs)}, '
                f'requires at least {config.consensus.min_agents}'
            )

        consensus = await self._run_consensus(
            question=question,
            profiles=profiles,
            outputs=outputs,
            prompt_file=config.consensus.prompt_file,
            schema=config.consensus.output_schema or self._default_consensus_schema(question),
        )

        final_decision = str(consensus.get('final_recommendation', 'No final recommendation generated.'))
        logger.info('orchestrator.consensus.done level=%s', consensus.get('consensus_level'))
        return outputs, consensus, final_decision

    async def _run_single_agent(self, spec: AgentSpec, question: str, profiles: dict) -> AgentOutput:
        logger.info('orchestrator.agent.start agent=%s', spec.name)
        prompt_text = self.config_loader.read_prompt(spec.prompt_file or f'{spec.name}_prompt.md')
        schema = spec.output_schema or self._default_agent_schema()

        filtered_profiles = self._filter_profiles(profiles, spec.input_datasets)
        payload = {
            'question': question,
            'agent_name': spec.name,
            'profiles': filtered_profiles,
        }

        user_prompt = (
            f'{json.dumps(payload, ensure_ascii=True)}\n\n'
            'Return only valid JSON with this exact schema:\n'
            f'{json.dumps(schema, ensure_ascii=True)}'
        )

        fallback = {
            'verdict': 'unavailable',
            'confidence': 0.0,
            'key_findings': ['Claude API key not configured or response failed'],
            'risks': [],
            'actions': [],
            'data_caveats': ['Fallback output used'],
        }

        content = await asyncio.to_thread(
            self.claude_client.ask_json,
            prompt_text,
            user_prompt,
            fallback,
            spec.model_override,
        )
        logger.info('orchestrator.agent.done agent=%s verdict=%s', spec.name, content.get('verdict'))
        return AgentOutput(agent=spec.name, content=content)

    async def _run_consensus(
        self,
        question: str,
        profiles: dict,
        outputs: list[AgentOutput],
        prompt_file: str,
        schema: dict[str, Any],
    ) -> dict[str, Any]:
        prompt_text = self.config_loader.read_prompt(prompt_file)
        payload = {
            'question': question,
            'profiles': profiles,
            'agent_outputs': [{'agent': x.agent, 'content': x.content} for x in outputs],
        }
        user_prompt = (
            f'{json.dumps(payload, ensure_ascii=True)}\n\n'
            'Return only valid JSON with this exact schema:\n'
            f'{json.dumps(schema, ensure_ascii=True)}'
        )

        fallback = {
            'question': question,
            'consensus_level': 'low',
            'agreement_count': 0,
            'dissenting_agents': [x.agent for x in outputs],
            'common_actions': [],
            'unresolved_risks': ['Consensus step used fallback output'],
            'final_recommendation': 'Unable to produce consensus recommendation.',
        }
        logger.info('orchestrator.consensus.start outputs=%d', len(outputs))
        return await asyncio.to_thread(self.claude_client.ask_json, prompt_text, user_prompt, fallback)

    def _filter_profiles(self, profiles: dict[str, Any], input_datasets: list[str]) -> dict[str, Any]:
        if not input_datasets:
            return profiles
        allowed = set(input_datasets)
        return {k: v for k, v in profiles.items() if k in allowed}

    def _default_agent_schema(self) -> dict[str, Any]:
        return {
            'verdict': 'string',
            'confidence': 0.0,
            'key_findings': ['string'],
            'risks': ['string'],
            'actions': ['string'],
            'data_caveats': ['string'],
        }

    def _default_consensus_schema(self, question: str) -> dict[str, Any]:
        return {
            'question': question,
            'consensus_level': 'high|medium|low',
            'agreement_count': 0,
            'dissenting_agents': ['string'],
            'common_actions': ['string'],
            'unresolved_risks': ['string'],
            'final_recommendation': 'string',
        }

    def write_result_markdown(
        self,
        run_result_dir: Path,
        question: str,
        outputs: list[AgentOutput],
        consensus: dict[str, Any],
    ) -> Path:
        lines = [
            '# SkinKandy Roster Analysis Result',
            '',
            '## Question',
            question,
            '',
        ]

        for item in outputs:
            content = item.content
            lines.extend([
                f'## Agent: {item.agent}',
                f"- verdict: {content.get('verdict')}",
                f"- confidence: {content.get('confidence')}",
                '- key_findings:',
                *[f'  - {x}' for x in content.get('key_findings', [])],
                '- risks:',
                *[f'  - {x}' for x in content.get('risks', [])],
                '- actions:',
                *[f'  - {x}' for x in content.get('actions', [])],
                '- data_caveats:',
                *[f'  - {x}' for x in content.get('data_caveats', [])],
                '',
            ])

        lines.extend([
            '## Consensus',
            f"- level: {consensus.get('consensus_level')}",
            f"- agreement_count: {consensus.get('agreement_count')}",
            '- dissenting_agents:',
            *[f'  - {x}' for x in consensus.get('dissenting_agents', [])],
            '- common_actions:',
            *[f'  - {x}' for x in consensus.get('common_actions', [])],
            '- unresolved_risks:',
            *[f'  - {x}' for x in consensus.get('unresolved_risks', [])],
            '',
            '## Final Recommendation',
            str(consensus.get('final_recommendation', 'No recommendation provided')),
            '',
        ])

        output_file = run_result_dir / 'analysis_result.md'
        output_file.write_text('\n'.join(lines), encoding='utf-8')
        return output_file


logger = logging.getLogger(__name__)
