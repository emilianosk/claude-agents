from __future__ import annotations

import uuid
from pathlib import Path


class RunStore:
    def __init__(self, uploads_path: Path, results_path: Path) -> None:
        self.uploads_path = uploads_path
        self.results_path = results_path
        self.uploads_path.mkdir(parents=True, exist_ok=True)
        self.results_path.mkdir(parents=True, exist_ok=True)

    def create_run(self) -> str:
        run_id = uuid.uuid4().hex[:12]
        self.get_run_upload_dir(run_id).mkdir(parents=True, exist_ok=True)
        self.get_run_result_dir(run_id).mkdir(parents=True, exist_ok=True)
        return run_id

    def get_run_upload_dir(self, run_id: str) -> Path:
        return self.uploads_path / run_id

    def get_run_result_dir(self, run_id: str) -> Path:
        return self.results_path / run_id

    def ensure_run(self, run_id: str) -> None:
        self.get_run_upload_dir(run_id).mkdir(parents=True, exist_ok=True)
        self.get_run_result_dir(run_id).mkdir(parents=True, exist_ok=True)
