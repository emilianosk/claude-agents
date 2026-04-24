from pathlib import Path

from app.services.dataset_catalog_loader import DatasetCatalogLoader, OpenAPILoader


def test_dataset_catalog_loader_reads_yaml(tmp_path: Path) -> None:
    catalog_file = tmp_path / 'datasets.yaml'
    catalog_file.write_text(
        'datasets:\n'
        '  - key: DATALAKE.DATA_LAKE_CONVERSION\n'
        '    service: databricks\n'
        '    type: sql\n'
        '    query_file: queries/DATA_LAKE_CONVERSION.sql\n',
        encoding='utf-8',
    )

    loader = DatasetCatalogLoader(str(catalog_file))
    catalog = loader.load()

    assert len(catalog.datasets) == 1
    assert catalog.datasets[0].key == 'DATALAKE.DATA_LAKE_CONVERSION'


def test_dataset_catalog_loader_reads_api_example_param(tmp_path: Path) -> None:
    catalog_file = tmp_path / 'datasets.yaml'
    catalog_file.write_text(
        'datasets:\n'
        '  - key: DEPUTY.TEAM_AVAILABILITY\n'
        '    service: deputy\n'
        '    type: api\n'
        '    openapi_file: openapi/deputy.yaml\n'
        '    endpoint: /api/v1/supervise/employee/QUERY\n'
        '    method: POST\n'
        '    example-param: example-piercers\n',
        encoding='utf-8',
    )

    loader = DatasetCatalogLoader(str(catalog_file))
    catalog = loader.load()

    assert len(catalog.datasets) == 1
    assert catalog.datasets[0].example_param == 'example-piercers'


def test_openapi_loader_parses_request_example(tmp_path: Path) -> None:
    openapi_file = tmp_path / 'deputy.yaml'
    openapi_file.write_text(
        'openapi: 3.0.0\n'
        'paths:\n'
        '  /api/v1/supervise/employee/QUERY:\n'
        '    post:\n'
        '      requestBody:\n'
        '        content:\n'
        '          application/json:\n'
        '            example:\n'
        '              max: 100\n'
        '              start: 0\n',
        encoding='utf-8',
    )

    loader = OpenAPILoader(openapi_file)
    payload = loader.get_request_example('/api/v1/supervise/employee/QUERY', 'POST')

    assert payload == {'max': 100, 'start': 0}


def test_openapi_loader_parses_named_nonstandard_example(tmp_path: Path) -> None:
    openapi_file = tmp_path / 'deputy.yaml'
    openapi_file.write_text(
        'openapi: 3.0.0\n'
        'paths:\n'
        '  /api/v1/supervise/employee/QUERY:\n'
        '    post:\n'
        '      requestBody:\n'
        '        content:\n'
        '          application/json:\n'
        '            example-piercers:\n'
        '              max: 1000\n'
        '              start: 0\n'
        '            example-all-actives:\n'
        '              max: 500\n'
        '              start: 0\n',
        encoding='utf-8',
    )

    loader = OpenAPILoader(openapi_file)
    payload = loader.get_request_example('/api/v1/supervise/employee/QUERY', 'POST', 'example-piercers')

    assert payload == {'max': 1000, 'start': 0}
