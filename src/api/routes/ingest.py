"""
src/api/routes/ingest.py
REST endpoints for triggering web crawl / ingestion jobs.

Two ways to start a crawl:

  1. Quick crawl — just paste a URL, smart defaults apply:
     GET /api/ingest/crawl?url=https://en.wikipedia.org/wiki/Web_page

  2. Full control — POST with a JSON body:
     POST /api/ingest/web  { "urls": [...], "max_depth": 2, ... }
"""
import asyncio
import logging
import uuid
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Query
from pydantic import BaseModel, field_validator

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ingest", tags=["Ingest"])

# ---------------------------------------------------------------------------
# Smart defaults — tuned for well-behaved crawling
# ---------------------------------------------------------------------------
DEFAULT_MAX_DEPTH          = 2
DEFAULT_MAX_PAGES          = 200
DEFAULT_BATCH_SIZE         = 5
DEFAULT_CRAWL_DELAY        = 1.5
DEFAULT_RESPECT_ROBOTS     = True
DEFAULT_STAY_ON_DOMAIN     = True


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class WebIngestRequest(BaseModel):
    urls: list[str]
    max_depth: int               = DEFAULT_MAX_DEPTH
    max_pages: int               = DEFAULT_MAX_PAGES
    batch_size: int              = DEFAULT_BATCH_SIZE
    crawl_delay: float           = DEFAULT_CRAWL_DELAY
    max_size_mb: Optional[int]   = None
    allowed_file_types: Optional[list[str]] = None
    respect_robots: bool         = DEFAULT_RESPECT_ROBOTS
    stay_on_seed_domains: bool   = DEFAULT_STAY_ON_DOMAIN
    collection_id: Optional[str] = None

    @field_validator("urls")
    @classmethod
    def urls_not_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("At least one URL is required.")
        return v


class WebIngestResponse(BaseModel):
    pipeline_run_id: str
    message: str
    seed_urls: list[str]
    settings: dict


# ---------------------------------------------------------------------------
# Background runner
# ---------------------------------------------------------------------------

def _run_crawl(request: WebIngestRequest, pipeline_run_id: str) -> None:
    from services.web_crawler import process_web_urls

    async def _crawl() -> None:
        await process_web_urls(
            urls=request.urls,
            max_size_mb=request.max_size_mb,
            pipeline_run_id=pipeline_run_id,
            batch_size=request.batch_size,
            allowed_file_types=request.allowed_file_types,
            collection_id=request.collection_id,
            max_depth=request.max_depth,
            max_pages=request.max_pages,
            crawl_delay=request.crawl_delay,
            respect_robots=request.respect_robots,
            stay_on_seed_domains=request.stay_on_seed_domains,
        )

    asyncio.run(_crawl())


def _build_response(request: WebIngestRequest, pipeline_run_id: str) -> WebIngestResponse:
    return WebIngestResponse(
        pipeline_run_id=pipeline_run_id,
        message=(
            f"Crawl started for: {', '.join(request.urls)} — "
            f"depth={request.max_depth}, pages={request.max_pages}, "
            f"batch={request.batch_size}, delay={request.crawl_delay}s"
        ),
        seed_urls=request.urls,
        settings={
            "max_depth":          request.max_depth,
            "max_pages":          request.max_pages,
            "batch_size":         request.batch_size,
            "crawl_delay":        request.crawl_delay,
            "respect_robots":     request.respect_robots,
            "stay_on_seed_domains": request.stay_on_seed_domains,
        },
    )


# ---------------------------------------------------------------------------
# ✅ SIMPLE endpoint — paste just the URL, nothing else needed
# ---------------------------------------------------------------------------

@router.get(
    "/crawl",
    response_model=WebIngestResponse,
    summary="Quick crawl — paste just a URL",
    description=(
        "Start a crawl by passing a single URL. "
        "All settings use smart defaults (depth=2, pages=200, delay=1.5s). "
        "No JSON body needed — just paste the URL in the box."
    ),
)
async def quick_crawl(
    background_tasks: BackgroundTasks,
    url: str = Query(
        ...,
        description="The seed URL to crawl",
        example="https://en.wikipedia.org/wiki/Web_page",
    ),
) -> WebIngestResponse:
    """
    Simplest possible crawl — one URL, smart defaults, no JSON required.

    Usage in Swagger:
      1. Click 'Try it out'
      2. Paste your URL in the 'url' box
      3. Click Execute
    """
    request = WebIngestRequest(urls=[url])
    pipeline_run_id = str(uuid.uuid4())
    logger.info(f"[ingest] Quick crawl started run={pipeline_run_id!r} url={url!r}")
    background_tasks.add_task(_run_crawl, request, pipeline_run_id)
    return _build_response(request, pipeline_run_id)


# ---------------------------------------------------------------------------
# Full control endpoint — POST with JSON body
# ---------------------------------------------------------------------------

@router.post(
    "/web",
    response_model=WebIngestResponse,
    summary="Full crawl — JSON body with all options",
)
async def ingest_web(
    request: WebIngestRequest,
    background_tasks: BackgroundTasks,
) -> WebIngestResponse:
    pipeline_run_id = str(uuid.uuid4())
    logger.info(f"[ingest] Full crawl started run={pipeline_run_id!r} seeds={request.urls}")
    background_tasks.add_task(_run_crawl, request, pipeline_run_id)
    return _build_response(request, pipeline_run_id)


# ---------------------------------------------------------------------------
# Cancel + health
# ---------------------------------------------------------------------------

@router.post("/cancel", summary="Cancel an active crawl")
async def cancel_crawl(collection_id: str) -> dict:
    from src.api.routes.global_pipeline import cancel
    cancel(collection_id)
    return {"message": f"Cancellation requested for '{collection_id}'."}


@router.get("/health", summary="Health check")
async def health() -> dict:
    return {"status": "ok"}