"""
src/ingest/run_tracker.py
Track per-pipeline-run statistics and fire completion callbacks.
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)
_trackers: dict[str, "PipelineRunTracker"] = {}


class PipelineRunTracker:
    def __init__(self, pipeline_run_id: str) -> None:
        self.pipeline_run_id = pipeline_run_id
        self._files: list[dict] = []

    def record_file(
        self,
        source_id: str,
        file_name: str,
        file_type: str,
        folder_number: int,
        chunks_created: int,
        status: str,
        error: str = "",
    ) -> None:
        self._files.append({
            "source_id": source_id, "file_name": file_name,
            "file_type": file_type, "folder_number": folder_number,
            "chunks_created": chunks_created, "status": status, "error": error,
        })

    async def send_callback(self) -> None:
        logger.info(
            f"[run_tracker] pipeline_run_id={self.pipeline_run_id!r} "
            f"complete — {len(self._files)} file(s) tracked"
        )
        # TODO: POST results to a webhook or update your DB


async def get_tracker(pipeline_run_id: str) -> Optional[PipelineRunTracker]:
    return _trackers.get(pipeline_run_id)


async def remove_tracker(pipeline_run_id: str) -> None:
    _trackers.pop(pipeline_run_id, None)


def create_tracker(pipeline_run_id: str) -> PipelineRunTracker:
    tracker = PipelineRunTracker(pipeline_run_id)
    _trackers[pipeline_run_id] = tracker
    return tracker