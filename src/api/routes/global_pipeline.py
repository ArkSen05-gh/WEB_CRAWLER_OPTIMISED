"""
src/api/routes/global_pipeline.py
Global in-memory cancellation registry for active pipeline runs.
"""
import logging

logger = logging.getLogger(__name__)
_cancelled: set[str] = set()


def cancel(collection_id: str) -> None:
    logger.info(f"[global_pipeline] Cancel requested: '{collection_id}'")
    _cancelled.add(collection_id)


def is_cancelled(collection_id: str) -> bool:
    return collection_id in _cancelled


def clear_cancellation(collection_id: str) -> None:
    _cancelled.discard(collection_id)