from pathlib import Path

from app.services.dataset_catalog_loader import DatasetCatalogLoader, OpenAPILoader


def test_dataset_catalog_loader_reads_yaml(tmp_path: Path) -> None:
    catalog_file = tmp_path / 'datasets.yaml'
    catalog_file.write_text(
        'datasets:\n'
        '  - key: DATALAKE.DATA_LAKE_CONVERSION\n'
        '    service: databricks\n'
        '    type: sql\n'
        '    query_file: queries/dev/DATA_LAKE_CONVERSION.sql\n',
        encoding='utf-8',
    )

    loader = DatasetCatalogLoader(str(catalog_file))
    catalog = loader.load()

    assert len(catalog.datasets) == 1
    assert catalog.datasets[0].key == 'DATALAKE.DATA_LAKE_CONVERSION'


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
