import json
from pathlib import Path

from app.services.query_loader import QueryLoader


def test_query_loader_reads_sql(tmp_path: Path) -> None:
    sql_file = tmp_path / 'a.sql'
    sql_file.write_text('SELECT 1', encoding='utf-8')

    mapping_file = tmp_path / 'map.json'
    mapping_file.write_text(json.dumps({'DATA_LAKE_CONVERSION': {'query_file': str(sql_file)}}), encoding='utf-8')

    loader = QueryLoader(str(mapping_file))
    sql = loader.get_query('DATA_LAKE_CONVERSION')

    assert sql == 'SELECT 1'
