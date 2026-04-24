from __future__ import annotations

import json
import logging
import re

from anthropic import Anthropic


class ClaudeClient:
    def __init__(self, api_key: str, model: str, max_tokens: int) -> None:
        self.api_key = api_key
        self.model = model
        self.max_tokens = max_tokens
        self.client = Anthropic(api_key=api_key) if api_key else None

    def available(self) -> bool:
        return self.client is not None

    def ask(self, system_prompt: str, user_prompt: str, model: str | None = None) -> str:
        if not self.client:
            return 'Claude API key not configured. Returning placeholder response.'

        msg = self.client.messages.create(
            model=model or self.model,
            max_tokens=self.max_tokens,
            system=system_prompt,
            messages=[{'role': 'user', 'content': user_prompt}],
        )

        chunks = []
        for block in msg.content:
            text = getattr(block, 'text', '')
            if text:
                chunks.append(text)
        return '\n'.join(chunks).strip()

    def ask_json(self, system_prompt: str, user_prompt: str, fallback: dict, model: str | None = None) -> dict:
        if not self.client:
            return fallback

        raw = self.ask(system_prompt=system_prompt, user_prompt=user_prompt, model=model)
        parsed = self._parse_json(raw)
        if parsed is None:
            logger.warning('claude.ask_json.invalid_json using_fallback raw_preview=%s', raw[:400])
            return fallback
        return parsed

    def _parse_json(self, content: str) -> dict | None:
        text = content.strip()
        if not text:
            return None

        fence_match = re.search(r'```(?:json)?\s*(\{.*\})\s*```', text, flags=re.DOTALL)
        if fence_match:
            text = fence_match.group(1).strip()

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            start = text.find('{')
            end = text.rfind('}')
            if start == -1 or end == -1 or end <= start:
                return None
            try:
                data = json.loads(text[start : end + 1])
            except json.JSONDecodeError:
                return None

        if isinstance(data, dict):
            return data
        return None


logger = logging.getLogger(__name__)
