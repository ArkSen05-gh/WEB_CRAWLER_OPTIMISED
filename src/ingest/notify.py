"""
src/ingest/notify.py
Notify the RAG pipeline that a document is ready, then summarise via Groq LLM.
"""
import logging
import os
from typing import Any, Optional

from groq import AsyncGroq
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_groq_client: Optional[AsyncGroq] = None

def _get_groq_client() -> AsyncGroq:
    global _groq_client
    if _groq_client is None:
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "GROQ_API_KEY is not set. Add it to your .env: GROQ_API_KEY=gsk_..."
            )
        _groq_client = AsyncGroq(api_key=api_key)
    return _groq_client

_active_pipeline_config: Optional[dict[str, Any]] = None

def set_active_pipeline_config(config: dict[str, Any]) -> None:
    global _active_pipeline_config
    _active_pipeline_config = config

def get_active_pipeline_config() -> Optional[dict[str, Any]]:
    return _active_pipeline_config

GROQ_MODEL = os.getenv("GROQ_MODEL", "llama3-8b-8192")

async def summarise_with_groq(text: str, source_url: str = "") -> str:
    """Send page text to Groq and return a concise factual summary."""
    client = _get_groq_client()
    try:
        response = await client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a precise document summariser for a RAG pipeline. "
                        "Given a web page excerpt, produce a concise factual summary "
                        "in 3-5 sentences. Focus on key topics, entities, and facts."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Source URL: {source_url}\n\nContent:\n{text[:12000]}",
                },
            ],
            temperature=0.2,
            max_tokens=512,
        )
        summary: str = response.choices[0].message.content or ""
        logger.debug(f"[notify] Groq summary ({len(summary)} chars) for {source_url!r}")
        return summary
    except Exception as exc:
        logger.warning(f"[notify] Groq summarisation failed for {source_url!r}: {exc}")
        return ""


async def notify_rag_pipeline(
    folder_number: int,
    pipeline_run_id: Optional[str] = None,
    source_id: str = "",
    file_name: str = "",
    file_type: str = "",
    raw_bytes: bytes = b"",
    web_url: str = "",
) -> None:
    """Decode content, summarise via Groq, then dispatch to RAG pipeline."""
    logger.info(
        f"[notify] folder={folder_number} file={file_name!r} "
        f"type={file_type!r} run={pipeline_run_id!r}"
    )

    text_content = ""
    if raw_bytes:
        try:
            text_content = raw_bytes.decode("utf-8", errors="replace")
        except Exception as exc:
            logger.warning(f"[notify] Decode failed for {source_id!r}: {exc}")

    summary = ""
    text_types = {"html", "htm", "txt", "md", "pdf", "doc", "docx"}
    if text_content and file_type in text_types:
        summary = await summarise_with_groq(text_content, source_url=web_url or source_id)

    if summary:
        logger.info(f"[notify] Summary: {summary[:120]}...")
    # TODO: store chunks + summary in your vector store (FAISS / ChromaDB)
    # TODO: dispatch to task queue (Celery / Redis)