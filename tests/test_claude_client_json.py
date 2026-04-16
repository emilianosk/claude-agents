from app.services.claude_client import ClaudeClient


def test_parse_json_plain_object() -> None:
    client = ClaudeClient(api_key='', model='x', max_tokens=10)
    parsed = client._parse_json('{"verdict":"ok"}')
    assert parsed == {'verdict': 'ok'}


def test_parse_json_fenced_block() -> None:
    client = ClaudeClient(api_key='', model='x', max_tokens=10)
    parsed = client._parse_json('```json\n{"verdict":"ok"}\n```')
    assert parsed == {'verdict': 'ok'}
