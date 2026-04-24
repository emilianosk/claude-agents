from app.services.claude_client import ClaudeClient


def test_parse_json_plain_object() -> None:
    client = ClaudeClient(api_key='', model='x', max_tokens=10)
    parsed = client._parse_json('{"verdict":"ok"}')
    assert parsed == {'verdict': 'ok'}


def test_parse_json_fenced_block() -> None:
    client = ClaudeClient(api_key='', model='x', max_tokens=10)
    parsed = client._parse_json('```json\n{"verdict":"ok"}\n```')
    assert parsed == {'verdict': 'ok'}


def test_ask_json_returns_fallback_on_invalid_json(monkeypatch) -> None:
    client = ClaudeClient(api_key='x', model='x', max_tokens=10)
    fallback = {'verdict': 'fallback'}

    # simulate invalid/truncated response from model
    monkeypatch.setattr(client, 'ask', lambda **kwargs: '```json\n{"verdict":"ok"')
    parsed = client.ask_json(system_prompt='s', user_prompt='u', fallback=fallback)

    assert parsed == fallback
