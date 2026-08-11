"""
src/core/db/mongo_storage.py
MongoDB connection and structured document storage using Motor (async driver).

What gets stored per crawled page
----------------------------------
  title             : page <title> tag text
  meta_description  : <meta name="description"> content
  clean_text        : all visible text, tags stripped, whitespace collapsed
  word_count        : integer
  headings          : [ {level: "h1", text: "..."}, ... ]
  paragraphs        : [ "paragraph text", ... ]
  images            : [ {src, alt, width, height}, ... ]
  + all identity/metadata fields (url, depth, mime, size, etc.)

raw_content (the raw HTML dump) is intentionally NOT stored — it is
unreadable in Atlas and wastes space.
"""
import logging
import os
from typing import Any, Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_client: Optional[AsyncIOMotorClient] = None
_db: Optional[AsyncIOMotorDatabase]   = None
_folder_counter: int = 0


def _get_db() -> AsyncIOMotorDatabase:
    global _client, _db
    if _db is None:
        uri     = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        db_name = os.getenv("MONGO_DB_NAME", "rag_db")
        _client = AsyncIOMotorClient(uri)
        _db     = _client[db_name]
        logger.info(f"[mongo_storage] Connected → db={db_name!r}")
    return _db


async def save_source_file(
    source_id: str,
    metadata: dict[str, Any],
    raw_bytes: bytes,
    mime_type: str,
) -> int:
    """
    Upsert one crawled-page document into MongoDB.

    The caller (web_crawler._crawl_one) already builds the full structured
    metadata dict — we just attach the folder number and persist it.

    We deliberately do NOT store raw_content (the raw HTML) here.
    The clean, human-readable fields are already in `metadata`.
    """
    global _folder_counter
    _folder_counter += 1
    folder_number = _folder_counter

    db = _get_db()

    doc = {
        **metadata,               # all structured fields from the crawler
        "folder_number": folder_number,
        # raw_size_bytes kept for diagnostic purposes only
        "raw_size_bytes": len(raw_bytes),
        # raw_content intentionally omitted — store clean_text instead
    }

    try:
        await db["source_files"].update_one(
            {"source_id": source_id},
            {"$set": doc},
            upsert=True,
        )
        logger.debug(
            f"[mongo_storage] Saved source_id={source_id!r} "
            f"folder={folder_number} "
            f"words={metadata.get('word_count', 0)} "
            f"imgs={len(metadata.get('images', []))}"
        )
    except Exception as exc:
        logger.error(
            f"[mongo_storage] Save FAILED for {source_id!r}: {exc}",
            exc_info=True,
        )

    return folder_number







# """
# src/core/db/mongo_storage.py
# MongoDB connection and document storage using Motor (async driver).
# """
# import logging
# import os
# from typing import Any, Optional

# from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
# from dotenv import load_dotenv

# load_dotenv()

# logger = logging.getLogger(__name__)

# _client: Optional[AsyncIOMotorClient] = None
# _db: Optional[AsyncIOMotorDatabase] = None
# _folder_counter: int = 0


# def _get_db() -> AsyncIOMotorDatabase:
#     global _client, _db
#     if _db is None:
#         uri     = os.getenv("MONGO_URI", "mongodb://localhost:27017")
#         db_name = os.getenv("MONGO_DB_NAME", "rag_db")
#         _client = AsyncIOMotorClient(uri)
#         _db     = _client[db_name]
#         logger.info(f"[mongo_storage] Connected → db={db_name!r}")
#     return _db


# async def save_source_file(
#     source_id: str,
#     metadata: dict[str, Any],
#     raw_bytes: bytes,
#     mime_type: str,
# ) -> int:
#     """Upsert document into MongoDB. Returns auto-incremented folder number."""
#     global _folder_counter
#     _folder_counter += 1
#     folder_number = _folder_counter

#     db  = _get_db()
#     doc = {
#         **metadata,
#         "folder_number":  folder_number,
#         "mime_type":      mime_type,
#         "raw_content":    raw_bytes.decode("utf-8", errors="replace") if raw_bytes else "",
#         "raw_size_bytes": len(raw_bytes),
#     }

#     try:
#         await db["source_files"].update_one(
#             {"source_id": source_id},
#             {"$set": doc},
#             upsert=True,
#         )
#         logger.debug(f"[mongo_storage] Saved source_id={source_id!r} folder={folder_number}")
#     except Exception as exc:
#         logger.error(f"[mongo_storage] Save failed for {source_id!r}: {exc}", exc_info=True)

#     return folder_number