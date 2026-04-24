from app.services.databricks_client import DatabricksClient, DatabricksConfig


def _cfg() -> DatabricksConfig:
    return DatabricksConfig(
        host='https://example.databricks.com',
        token='abc123',
        warehouse_id='wh-1',
        catalog='',
        schema='',
        wait_timeout='30s',
        ssl_verify=True,
        oauth_tenant_id='',
        oauth_client_id='',
        oauth_client_secret='',
        oauth_token_url='',
    )


def test_execute_query_uses_top_level_manifest() -> None:
    client = DatabricksClient(_cfg())
    client._request = lambda *args, **kwargs: {
        'status': {'state': 'SUCCEEDED'},
        'manifest': {
            'schema': {
                'columns': [
                    {'name': 'ok'},
                ]
            }
        },
        'result': {
            'data_array': [
                ['1'],
            ]
        },
    }
    rows = client.execute_query('SELECT 1 AS ok')
    assert rows == [{'ok': '1'}]


def test_execute_query_falls_back_to_result_manifest() -> None:
    client = DatabricksClient(_cfg())
    client._request = lambda *args, **kwargs: {
        'status': {'state': 'SUCCEEDED'},
        'result': {
            'manifest': {
                'schema': {
                    'columns': [
                        {'name': 'value'},
                    ]
                }
            },
            'data_array': [
                [42],
            ],
        },
    }
    rows = client.execute_query('SELECT 42 AS value')
    assert rows == [{'value': 42}]


def test_execute_query_raises_on_failed_statement_state() -> None:
    client = DatabricksClient(_cfg())
    client._request = lambda *args, **kwargs: {
        'statement_id': 'abc-123',
        'status': {
            'state': 'FAILED',
            'error': {
                'error_code': 'BAD_REQUEST',
                'message': 'Syntax error near FROM',
            },
        },
    }
    try:
        client.execute_query('SELECT * FROM broken')
        assert False, 'expected RuntimeError'
    except RuntimeError as exc:
        assert 'Databricks statement failed (FAILED)' in str(exc)
        assert 'Syntax error near FROM' in str(exc)


def test_execute_query_downloads_external_links_chunks(monkeypatch) -> None:
    client = DatabricksClient(_cfg())
    calls = {'n': 0}

    def fake_request(method, path, **kwargs):
        if path == '/api/2.0/sql/statements':
            return {
                'status': {'state': 'SUCCEEDED'},
                'manifest': {
                    'schema': {'columns': [{'name': 'ok'}]},
                },
                'result': {
                    'external_links': [
                        {
                            'external_link': 'https://example/chunk0',
                            'http_headers': {},
                            'next_chunk_internal_link': '/api/2.0/sql/statements/id/result/chunks/1',
                        }
                    ]
                },
            }
        if path == '/api/2.0/sql/statements/id/result/chunks/1':
            return {
                'external_links': [
                    {
                        'external_link': 'https://example/chunk1',
                        'http_headers': {},
                    }
                ]
            }
        raise AssertionError(f'unexpected path: {path}')

    class FakeResponse:
        def __init__(self, payload):
            self.status_code = 200
            self._payload = payload

        def json(self):
            return self._payload

    def fake_get(url, headers=None, timeout=45, verify=True):
        calls['n'] += 1
        if url.endswith('chunk0'):
            return FakeResponse({'data_array': [['1']]})
        if url.endswith('chunk1'):
            return FakeResponse({'data_array': [['2']]})
        return FakeResponse({'data_array': []})

    client._request = fake_request
    monkeypatch.setattr('app.services.databricks_client.requests.get', fake_get)
    rows = client.execute_query('SELECT ok')
    assert rows == [{'ok': '1'}, {'ok': '2'}]
    assert calls['n'] == 2
