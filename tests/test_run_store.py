from pathlib import Path

from app.services.run_store import RunStore


def test_create_run(tmp_path: Path) -> None:
    store = RunStore(tmp_path / 'uploads', tmp_path / 'results')
    run_id = store.create_run()

    assert run_id
    assert (tmp_path / 'uploads' / run_id).exists()
    assert (tmp_path / 'results' / run_id).exists()
