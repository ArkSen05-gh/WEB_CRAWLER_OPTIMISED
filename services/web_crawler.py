# Copyright (c) 2026 Indus Net Technologies Private Limited
# Licensed under the Business Source License 1.1 (BUSL-1.1)

"""
services/web_crawler.py
Web URL ingestion for the RAG pipeline — recursive BFS with full content extraction.

For each page this module:
  1. Fetches the raw HTML via aiohttp.
  2. Extracts:
       - clean visible text  (no HTML tags)
       - page title + meta description
       - all headings  (h1-h6) in order
       - all paragraphs
       - all images  (src, alt, width, height)
       - all internal links discovered (fed back into BFS frontier)
  3. Saves a structured, human-readable document to MongoDB.
  4. Notifies the RAG pipeline.
"""

import asyncio
import logging
import urllib.parse
import urllib.robotparser
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
import time as _time

import aiohttp
from bs4 import BeautifulSoup, Tag

from src.core.events import broadcast
from src.core.db.mongo_storage import save_source_file as save_file_pair
from src.ingest.notify import notify_rag_pipeline

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------
DEFAULT_MAX_DEPTH: int       = 2
DEFAULT_MAX_PAGES: int       = 200
DEFAULT_BATCH_SIZE: int      = 5
DEFAULT_REQUEST_TIMEOUT: int = 30
DEFAULT_CRAWL_DELAY: float   = 1.5

CRAWLABLE_MIME_PREFIXES = (
    "text/html",
    "text/plain",
    "application/xhtml+xml",
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats",
)

# Extensions we never crawl or enqueue
SKIP_EXTENSIONS = {
    "jpg", "jpeg", "png", "gif", "svg", "webp", "ico", "bmp", "tiff",
    "mp4", "mp3", "avi", "mov", "mkv", "webm",
    "zip", "tar", "gz", "rar", "7z",
    "exe", "dmg", "pkg", "deb", "rpm",
    "css", "js", "woff", "woff2", "ttf", "eot",
    "xml", "json", "php",
}

# HTML tags whose text we always discard (navigation clutter)
IGNORE_TAGS = {
    "script", "style", "noscript", "head", "meta",
    "header", "footer", "nav", "aside",
}

USER_AGENT = "RAGCrawler/1.0 (+internal)"

# ---------------------------------------------------------------------------
# robots.txt cache
# ---------------------------------------------------------------------------
_robots_cache: dict[str, urllib.robotparser.RobotFileParser] = {}
_robots_cache_lock = asyncio.Lock()


async def _get_robots(
    domain: str, session: aiohttp.ClientSession
) -> urllib.robotparser.RobotFileParser:
    async with _robots_cache_lock:
        if domain in _robots_cache:
            return _robots_cache[domain]

    rp = urllib.robotparser.RobotFileParser()
    robots_url = f"https://{domain}/robots.txt"
    try:
        async with session.get(
            robots_url, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status == 200:
                text = await resp.text(errors="replace")
                rp.set_url(robots_url)
                rp.parse(text.splitlines())
            else:
                rp.set_url(robots_url)
                rp.parse(["User-agent: *", "Allow: /"])
    except Exception:
        rp.set_url(robots_url)
        rp.parse(["User-agent: *", "Allow: /"])

    async with _robots_cache_lock:
        _robots_cache[domain] = rp
    return rp


def _is_allowed_by_robots(
    rp: urllib.robotparser.RobotFileParser, url: str
) -> bool:
    try:
        return rp.can_fetch(USER_AGENT, url)
    except Exception:
        return True


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def _normalise_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    clean = parsed._replace(fragment="")
    return urllib.parse.urlunparse(clean).rstrip("/")


def _ext(parsed: urllib.parse.ParseResult) -> str:
    return Path(parsed.path).suffix.lower().lstrip(".") or "html"


def _seed_domains(urls: list[str]) -> set[str]:
    domains: set[str] = set()
    for u in urls:
        try:
            domains.add(urllib.parse.urlparse(u).netloc)
        except Exception:
            pass
    return domains


# ---------------------------------------------------------------------------
# ★  Content extraction — the core new logic
# ---------------------------------------------------------------------------

def _extract_content(base_url: str, html_bytes: bytes) -> dict[str, Any]:
    """
    Parse raw HTML and return a structured dict with:
      - title           : page <title>
      - meta_description: <meta name="description">
      - clean_text      : all visible text joined, no HTML noise
      - headings        : list of {level, text}
      - paragraphs      : list of paragraph strings
      - images          : list of {src, alt, width, height}
      - links           : list of absolute hrefs (internal discovery)
      - word_count      : int
    """
    try:
        html = html_bytes.decode("utf-8", errors="replace")
        soup = BeautifulSoup(html, "html.parser")
    except Exception as exc:
        logger.warning(f"[extract] Parse failed for {base_url}: {exc}")
        return _empty_content()

    # ---- remove noise tags entirely ----
    for tag in soup.find_all(IGNORE_TAGS):
        tag.decompose()

    # ---- title ----
    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    # ---- meta description ----
    meta_desc = ""
    meta_tag = soup.find("meta", attrs={"name": "description"})
    if isinstance(meta_tag, Tag):
        meta_desc = (meta_tag.get("content") or "").strip()  # type: ignore[arg-type]

    # ---- headings (h1–h6) ----
    headings: list[dict[str, str]] = []
    for level in range(1, 7):
        for tag in soup.find_all(f"h{level}"):
            text = tag.get_text(separator=" ", strip=True)
            if text:
                headings.append({"level": f"h{level}", "text": text})

    # ---- paragraphs ----
    paragraphs: list[str] = []
    for p in soup.find_all("p"):
        text = p.get_text(separator=" ", strip=True)
        if text and len(text) > 20:        # skip one-word fragments
            paragraphs.append(text)

    # ---- images ----
    images: list[dict[str, str]] = []
    for img in soup.find_all("img"):
        src = img.get("src", "").strip()
        if not src:
            continue
        # Resolve relative URLs
        abs_src = urllib.parse.urljoin(base_url, src)
        images.append({
            "src":    abs_src,
            "alt":    img.get("alt", "").strip(),
            "width":  str(img.get("width", "")),
            "height": str(img.get("height", "")),
        })

    # ---- all visible text (clean, no tags) ----
    # Get text from the body if present, else the whole doc
    body = soup.find("body") or soup
    clean_text = body.get_text(separator="\n", strip=True)  # type: ignore[union-attr]
    # Collapse excessive blank lines
    lines = [ln.strip() for ln in clean_text.splitlines() if ln.strip()]
    clean_text = "\n".join(lines)

    # ---- internal links for BFS frontier ----
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        abs_url = urllib.parse.urljoin(base_url, href)
        if abs_url.startswith(("http://", "https://")):
            links.append(abs_url)

    word_count = len(clean_text.split())

    return {
        "title":            title,
        "meta_description": meta_desc,
        "clean_text":       clean_text,
        "headings":         headings,
        "paragraphs":       paragraphs,
        "images":           images,
        "links":            links,
        "word_count":       word_count,
    }


def _empty_content() -> dict[str, Any]:
    return {
        "title": "", "meta_description": "", "clean_text": "",
        "headings": [], "paragraphs": [], "images": [],
        "links": [], "word_count": 0,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def process_web_urls(
    urls: list[str],
    max_size_mb: Optional[int] = None,
    pipeline_run_id: Optional[str] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    allowed_file_types: Optional[list[str]] = None,
    collection_id: Optional[str] = None,
    max_depth: int = DEFAULT_MAX_DEPTH,
    max_pages: int = DEFAULT_MAX_PAGES,
    crawl_delay: float = DEFAULT_CRAWL_DELAY,
    respect_robots: bool = True,
    stay_on_seed_domains: bool = True,
) -> None:
    seed_urls = [u.strip() for u in urls if u and u.strip()]
    if not seed_urls:
        logger.warning("[web_crawler] No valid seed URLs provided.")
        return

    logger.info(
        f"[web_crawler] Starting crawl: {len(seed_urls)} seed URL(s), "
        f"max_depth={max_depth}, max_pages={max_pages}"
    )

    await broadcast({
        "type": "crawl_start", "mode": "web_urls_recursive",
        "seed_count": len(seed_urls),
        "max_depth": max_depth, "max_pages": max_pages,
    })

    allowed_domains = _seed_domains(seed_urls) if stay_on_seed_domains else None
    max_bytes = (max_size_mb * 1024 * 1024) if max_size_mb else None

    frontier: deque[tuple[str, int]] = deque(
        (_normalise_url(u), 0) for u in seed_urls
    )
    visited: set[str] = set(_normalise_url(u) for u in seed_urls)
    total_processed = 0

    sem = asyncio.Semaphore(batch_size)
    domain_last_request: dict[str, float] = {}
    domain_lock = asyncio.Lock()

    connector = aiohttp.TCPConnector(limit=batch_size, ssl=False)
    timeout = aiohttp.ClientTimeout(total=DEFAULT_REQUEST_TIMEOUT)

    try:
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": USER_AGENT},
        ) as session:

            while frontier and total_processed < max_pages:

                if collection_id:
                    from src.api.routes.global_pipeline import is_cancelled
                    if is_cancelled(collection_id):
                        logger.info(f"[web_crawler] Cancelled for '{collection_id}'")
                        await broadcast({"type": "cancelled", "message": "Sync cancelled"})
                        break

                wave: list[tuple[str, int]] = []
                while frontier and len(wave) < batch_size:
                    wave.append(frontier.popleft())

                tasks = [
                    _crawl_one(
                        url=url, depth=depth, session=session, sem=sem,
                        domain_last_request=domain_last_request,
                        domain_lock=domain_lock,
                        pipeline_run_id=pipeline_run_id,
                        allowed_file_types=allowed_file_types,
                        allowed_domains=allowed_domains,
                        max_depth=max_depth, max_bytes=max_bytes,
                        crawl_delay=crawl_delay,
                        respect_robots=respect_robots,
                    )
                    for url, depth in wave
                ]

                results: list[list[str] | BaseException] = await asyncio.gather(
                    *tasks, return_exceptions=True
                )

                for item in results:
                    if not isinstance(item, list):
                        logger.error(f"[web_crawler] Task error: {item}")
                        continue
                    total_processed += 1
                    for child_url in item:
                        norm = _normalise_url(child_url)
                        if norm not in visited:
                            visited.add(norm)
                            frontier.append((child_url, 0))

        logger.info(f"[web_crawler] Done — {total_processed} pages processed")
        await broadcast({"type": "crawl_complete", "pages_processed": total_processed})

        if pipeline_run_id:
            from src.ingest.run_tracker import get_tracker, remove_tracker
            tracker = await get_tracker(pipeline_run_id)
            if tracker:
                await tracker.send_callback()
                await remove_tracker(pipeline_run_id)

    except Exception as e:
        logger.error(f"[web_crawler] Crawl FAILED: {e}", exc_info=True)
        await broadcast({"type": "error", "message": str(e)})

    finally:
        _robots_cache.clear()


# ---------------------------------------------------------------------------
# Internal: crawl + extract one page
# ---------------------------------------------------------------------------

async def _crawl_one(
    url: str,
    depth: int,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    domain_last_request: dict[str, float],
    domain_lock: asyncio.Lock,
    pipeline_run_id: str | None,
    allowed_file_types: Optional[list[str]],
    allowed_domains: Optional[set[str]],
    max_depth: int,
    max_bytes: Optional[int],
    crawl_delay: float,
    respect_robots: bool,
) -> list[str]:
    """
    Fetch one URL, extract all content, save structured doc to MongoDB.
    Returns list of child URLs for the BFS frontier.
    """
    async with sem:
        parsed_url = urllib.parse.urlparse(url)
        domain    = parsed_url.netloc
        ext       = _ext(parsed_url)
        file_name = Path(parsed_url.path).name or "index.html"

        # ---- domain filter ----
        if allowed_domains and domain not in allowed_domains:
            return []

        # ---- extension filter ----
        if ext in SKIP_EXTENSIONS:
            return []

        is_html_page = ext in ("html", "htm", "")
        if allowed_file_types is not None and not is_html_page \
                and ext not in allowed_file_types:
            await broadcast({
                "type": "skipped", "path": url, "file_name": file_name,
                "reason": f"File type not in allowed list: .{ext}",
            })
            return []

        # ---- robots.txt ----
        if respect_robots:
            rp = await _get_robots(domain, session)
            if not _is_allowed_by_robots(rp, url):
                logger.info(f"[web_crawler] robots.txt blocked: {url}")
                await broadcast({
                    "type": "skipped", "path": url,
                    "file_name": file_name, "reason": "Blocked by robots.txt",
                })
                return []

        # ---- polite delay per domain ----
        async with domain_lock:
            last = domain_last_request.get(domain, 0.0)
            wait = crawl_delay - (_time.monotonic() - last)
            if wait > 0:
                await asyncio.sleep(wait)
            domain_last_request[domain] = _time.monotonic()

        await broadcast({"type": "file_found",   "path": url, "file_name": file_name})
        await broadcast({"type": "processing",   "path": url, "file_name": file_name})

        # ---- HTTP fetch ----
        try:
            async with session.get(
                url, allow_redirects=True, max_redirects=5
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"[web_crawler] HTTP {resp.status} — {url}")
                    await broadcast({
                        "type": "skipped", "path": url, "file_name": file_name,
                        "reason": f"HTTP {resp.status}",
                    })
                    return []

                content_type = resp.content_type or ""
                if not any(
                    content_type.startswith(p) for p in CRAWLABLE_MIME_PREFIXES
                ):
                    return []

                if max_bytes:
                    raw_bytes = await resp.content.read(max_bytes + 1)
                    if len(raw_bytes) > max_bytes:
                        await broadcast({
                            "type": "skipped", "path": url,
                            "file_name": file_name,
                            "reason": "Exceeds max_size_mb",
                        })
                        return []
                else:
                    raw_bytes = await resp.read()

                actual_mime = content_type.split(";")[0].strip()
                is_html = actual_mime in ("text/html", "application/xhtml+xml")

        except asyncio.TimeoutError:
            await broadcast({
                "type": "skipped", "path": url,
                "file_name": file_name, "reason": "Timeout",
            })
            return []
        except aiohttp.ClientError as exc:
            await broadcast({
                "type": "skipped", "path": url,
                "file_name": file_name, "reason": str(exc),
            })
            return []

        # ---- extract structured content ----
        content: dict[str, Any] = _empty_content()
        child_urls: list[str] = []

        if is_html:
            content = _extract_content(url, raw_bytes)
            # child links come from the extractor (already resolved to absolute)
            if depth < max_depth:
                for child in content["links"]:
                    child_parsed = urllib.parse.urlparse(child)
                    child_ext = _ext(child_parsed)
                    if child_ext in SKIP_EXTENSIONS:
                        continue
                    if allowed_domains and child_parsed.netloc not in allowed_domains:
                        continue
                    child_urls.append(child)

        # ---- build MongoDB document ----
        now_iso = datetime.now(timezone.utc).isoformat()
        normalized = {
            # ── identity ──────────────────────────────────────────────
            "source_id":         url,
            "source_type":       "web",
            "file_name":         file_name,
            "file_type":         ext,
            "path":              url,
            "web_url":           url,
            "parent_folder_id":  (
                "/".join(parsed_url.path.rstrip("/").split("/")[:-1])
                if parsed_url.path else ""
            ),
            "mime_type":         actual_mime,
            "size":              len(raw_bytes),
            "content_status":    "accessible",
            "connector_synced_at": now_iso,
            "crawl_depth":       depth,

            # ── human-readable extracted content ──────────────────────
            "title":             content["title"],
            "meta_description":  content["meta_description"],
            "clean_text":        content["clean_text"],   # ← full visible text
            "word_count":        content["word_count"],
            "headings":          content["headings"],     # ← [{level,text}]
            "paragraphs":        content["paragraphs"],   # ← [str]
            "images":            content["images"],       # ← [{src,alt,w,h}]
        }

        folder_number = await save_file_pair(url, normalized, raw_bytes, actual_mime)

        await broadcast({
            "type":           "stored",
            "path":           url,
            "file_name":      file_name,
            "folder_number":  folder_number,
            "title":          content["title"],
            "word_count":     content["word_count"],
            "image_count":    len(content["images"]),
            "mime_type":      actual_mime,
            "size":           len(raw_bytes),
        })

        logger.info(
            f"[web_crawler] ✓ {url} | "
            f"words={content['word_count']} "
            f"imgs={len(content['images'])} "
            f"children={len(child_urls)}"
        )

        # ---- notify RAG pipeline ----
        await notify_rag_pipeline(
            folder_number,
            pipeline_run_id=pipeline_run_id,
            source_id=url,
            file_name=file_name,
            file_type=ext,
            raw_bytes=content["clean_text"].encode("utf-8"),  # clean text only
            web_url=url,
        )

        return child_urls









# # Copyright (c) 2026 Indus Net Technologies Private Limited
# # Licensed under the Business Source License 1.1 (BUSL-1.1)
# # See LICENSE file in the project root for full licence terms.
# # Additional Use Grant: internal deployment and modification only.
# # Commercial licensing: licensing@intglobal.com

# """
# services/web_crawler.py
# Web URL ingestion for the RAG pipeline — with recursive sub-link discovery.

# For each seed URL this module:
#   1. Fetches the page and extracts every sub-link (BFS, depth-limited).
#   2. Saves normalised metadata + raw bytes to MongoDB.
#   3. Notifies the RAG pipeline so the parser can chunk/embed the content.

# Crawl boundaries
# ----------------
# * Only URLs whose *netloc* matches one of the seed domains are followed
#   (cross-domain links are skipped).
# * ``max_depth``   – how many hops from a seed URL (default 3).
# * ``max_pages``   – hard ceiling across the entire run (default 500).
# * ``batch_size``  – max concurrent in-flight HTTP requests (default 10).
# * ``robots.txt``  is fetched and honoured per domain.
# """

# import asyncio
# import logging
# import re
# import urllib.parse
# import urllib.robotparser
# from collections import deque
# from datetime import datetime, timezone
# from pathlib import Path
# from typing import Optional

# import aiohttp
# from bs4 import BeautifulSoup

# from src.core.events import broadcast
# from src.core.db.mongo_storage import save_source_file as save_file_pair
# from src.ingest.notify import notify_rag_pipeline

# logger = logging.getLogger(__name__)

# # ---------------------------------------------------------------------------
# # Tunables
# # ---------------------------------------------------------------------------
# DEFAULT_MAX_DEPTH: int = 3
# DEFAULT_MAX_PAGES: int = 500
# DEFAULT_BATCH_SIZE: int = 10
# DEFAULT_REQUEST_TIMEOUT: int = 30          # seconds per HTTP request
# DEFAULT_CRAWL_DELAY: float = 0.5          # polite delay between requests (seconds)

# # Content-types we are willing to store and forward to the RAG pipeline.
# CRAWLABLE_MIME_PREFIXES = (
#     "text/html",
#     "text/plain",
#     "application/xhtml+xml",
#     "application/pdf",
#     "application/msword",
#     "application/vnd.openxmlformats",      # .docx / .xlsx / .pptx
# )

# # We skip these extensions even when they appear as hrefs — they are
# # unlikely to contain text useful for a RAG index.
# SKIP_EXTENSIONS = {
#     "jpg", "jpeg", "png", "gif", "svg", "webp", "ico", "bmp", "tiff",
#     "mp4", "mp3", "avi", "mov", "mkv", "webm",
#     "zip", "tar", "gz", "rar", "7z",
#     "exe", "dmg", "pkg", "deb", "rpm",
#     "css", "js", "woff", "woff2", "ttf", "eot",
#     "xml", "json",          # rarely useful raw; add back if your pipeline handles them
#     "php",                   # resource loaders (Wikipedia load.php etc.) — not page content
# }

# # ---------------------------------------------------------------------------
# # robots.txt cache  (per domain, per crawler run)
# # ---------------------------------------------------------------------------
# _robots_cache: dict[str, urllib.robotparser.RobotFileParser] = {}
# _robots_cache_lock = asyncio.Lock()

# USER_AGENT = "RAGCrawler/1.0 (+internal)"


# async def _get_robots(domain: str, session: aiohttp.ClientSession) -> urllib.robotparser.RobotFileParser:
#     """Fetch and cache robots.txt for *domain*. Returns a permissive parser on failure."""
#     async with _robots_cache_lock:
#         if domain in _robots_cache:
#             return _robots_cache[domain]

#     rp = urllib.robotparser.RobotFileParser()
#     robots_url = f"https://{domain}/robots.txt"
#     try:
#         async with session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
#             if resp.status == 200:
#                 text = await resp.text(errors="replace")
#                 rp.set_url(robots_url)
#                 rp.parse(text.splitlines())
#             else:
#                 # Non-200 (404, 403 etc.) → treat as "allow everything"
#                 rp.set_url(robots_url)
#                 rp.parse(["User-agent: *", "Allow: /"])
#     except Exception:
#         # Network error → fail open (allow everything)
#         rp.set_url(robots_url)
#         rp.parse(["User-agent: *", "Allow: /"])

#     async with _robots_cache_lock:
#         _robots_cache[domain] = rp

#     return rp


# def _is_allowed_by_robots(rp: urllib.robotparser.RobotFileParser, url: str) -> bool:
#     try:
#         return rp.can_fetch(USER_AGENT, url)
#     except Exception:
#         return True


# # ---------------------------------------------------------------------------
# # URL helpers
# # ---------------------------------------------------------------------------

# def _normalise_url(url: str) -> str:
#     """Strip fragments and trailing slashes for dedup purposes."""
#     parsed = urllib.parse.urlparse(url)
#     # Remove fragment, normalise path (collapse // etc.)
#     clean = parsed._replace(fragment="")
#     return urllib.parse.urlunparse(clean).rstrip("/")


# def _ext(parsed: urllib.parse.ParseResult) -> str:
#     return Path(parsed.path).suffix.lower().lstrip(".") or "html"


# def _mime_for_ext(ext: str) -> str:
#     mapping = {
#         "pdf": "application/pdf",
#         "doc": "application/msword",
#         "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
#         "xls": "application/vnd.ms-excel",
#         "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#         "ppt": "application/vnd.ms-powerpoint",
#         "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
#         "txt": "text/plain",
#     }
#     if ext in ("html", "htm", ""):
#         return "text/html"
#     return mapping.get(ext, "application/octet-stream")


# def _extract_links(base_url: str, html: str) -> list[str]:
#     """
#     Return absolute URLs found in <a href>, <link href>, and canonical
#     tags inside *html*.  Relative URLs are resolved against *base_url*.
#     """
#     try:
#         soup = BeautifulSoup(html, "html.parser")
#     except Exception:
#         return []

#     found: list[str] = []
#     tags = soup.find_all(["a", "link"], href=True)
#     for tag in tags:
#         href = tag.get("href", "").strip()
#         if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
#             continue
#         abs_url = urllib.parse.urljoin(base_url, href)
#         # Keep only http/https
#         if abs_url.startswith(("http://", "https://")):
#             found.append(abs_url)

#     return found


# def _seed_domains(urls: list[str]) -> set[str]:
#     """Return the set of netloc values for all seed URLs."""
#     domains: set[str] = set()
#     for u in urls:
#         try:
#             domains.add(urllib.parse.urlparse(u).netloc)
#         except Exception:
#             pass
#     return domains


# # ---------------------------------------------------------------------------
# # Public entry point
# # ---------------------------------------------------------------------------

# async def process_web_urls(
#     urls: list[str],
#     max_size_mb: Optional[int] = None,
#     pipeline_run_id: Optional[str] = None,
#     batch_size: int = DEFAULT_BATCH_SIZE,
#     allowed_file_types: Optional[list[str]] = None,
#     collection_id: Optional[str] = None,
#     # ---- new crawl-control parameters ----
#     max_depth: int = DEFAULT_MAX_DEPTH,
#     max_pages: int = DEFAULT_MAX_PAGES,
#     crawl_delay: float = DEFAULT_CRAWL_DELAY,
#     respect_robots: bool = True,
#     stay_on_seed_domains: bool = True,
# ):
#     """
#     Ingest a list of seed URLs **and all reachable sub-pages**.

#     Parameters
#     ----------
#     urls : list[str]
#         Seed URLs to start crawling from.
#     max_size_mb : int, optional
#         Skip pages whose response body exceeds this size.
#     pipeline_run_id : str, optional
#         Forwarded to the RAG pipeline tracker.
#     batch_size : int
#         Max concurrent HTTP requests.
#     allowed_file_types : list[str], optional
#         If set, only pages with these extensions are stored.
#     collection_id : str, optional
#         Used for cancellation checks.
#     max_depth : int
#         BFS depth limit from each seed URL.
#     max_pages : int
#         Global hard cap on total pages crawled.
#     crawl_delay : float
#         Seconds to wait between requests to the same domain.
#     respect_robots : bool
#         Honour robots.txt directives (recommended).
#     stay_on_seed_domains : bool
#         If True, never follow links to domains not in the seed list.
#     """
#     seed_urls = [u.strip() for u in urls if u and u.strip()]
#     if not seed_urls:
#         logger.warning("[web_crawler] No valid seed URLs provided.")
#         return

#     logger.info(
#         f"[web_crawler] Starting crawl: {len(seed_urls)} seed URL(s), "
#         f"max_depth={max_depth}, max_pages={max_pages}, "
#         f"pipeline_run_id={pipeline_run_id!r}"
#     )

#     from src.ingest.notify import get_active_pipeline_config
#     _cfg = get_active_pipeline_config()
#     if _cfg:
#         logger.info(f"[web_crawler] Active pipeline config: collection_id={_cfg.get('collection_id')!r}")
#     else:
#         logger.warning("[web_crawler] No active pipeline config found")

#     await broadcast({
#         "type": "crawl_start",
#         "mode": "web_urls_recursive",
#         "seed_count": len(seed_urls),
#         "max_depth": max_depth,
#         "max_pages": max_pages,
#     })

#     allowed_domains = _seed_domains(seed_urls) if stay_on_seed_domains else None
#     max_bytes = (max_size_mb * 1024 * 1024) if max_size_mb else None

#     # BFS frontier: (url, depth)
#     frontier: deque[tuple[str, int]] = deque((u, 0) for u in seed_urls)
#     visited: set[str] = set(_normalise_url(u) for u in seed_urls)
#     total_processed = 0

#     sem = asyncio.Semaphore(batch_size)
#     # Per-domain rate-limit: track last-request timestamp
#     domain_last_request: dict[str, float] = {}
#     domain_lock = asyncio.Lock()

#     connector = aiohttp.TCPConnector(limit=batch_size, ssl=False)
#     timeout = aiohttp.ClientTimeout(total=DEFAULT_REQUEST_TIMEOUT)

#     try:
#         async with aiohttp.ClientSession(
#             connector=connector,
#             timeout=timeout,
#             headers={"User-Agent": USER_AGENT},
#         ) as session:

#             while frontier and total_processed < max_pages:
#                 # Check cancellation at top of each BFS wave
#                 if collection_id:
#                     from src.api.routes.global_pipeline import is_cancelled
#                     if is_cancelled(collection_id):
#                         logger.info(f"[web_crawler] Cancelled for collection '{collection_id}'")
#                         await broadcast({"type": "cancelled", "message": "Sync cancelled by user"})
#                         break

#                 # Pull a wave of URLs to process concurrently
#                 wave: list[tuple[str, int]] = []
#                 while frontier and len(wave) < batch_size:
#                     wave.append(frontier.popleft())

#                 tasks = [
#                     _crawl_one(
#                         url=url,
#                         depth=depth,
#                         session=session,
#                         sem=sem,
#                         domain_last_request=domain_last_request,
#                         domain_lock=domain_lock,
#                         pipeline_run_id=pipeline_run_id,
#                         allowed_file_types=allowed_file_types,
#                         allowed_domains=allowed_domains,
#                         max_depth=max_depth,
#                         max_bytes=max_bytes,
#                         crawl_delay=crawl_delay,
#                         respect_robots=respect_robots,
#                     )
#                     for url, depth in wave
#                 ]

#                 results: list[list[str] | BaseException] = await asyncio.gather(
#                     *tasks, return_exceptions=True
#                 )

#                 for item in results:
#                     if not isinstance(item, list):
#                         # item is a BaseException — task failed
#                         logger.error(f"[web_crawler] Unhandled task error: {item}")
#                         continue
#                     total_processed += 1
#                     # item is confirmed list[str] — discovered child URLs
#                     for child_url in item:
#                         norm = _normalise_url(child_url)
#                         if norm not in visited and total_processed + len(frontier) < max_pages:
#                             visited.add(norm)
#                             # depth for children = parent depth + 1; we
#                             # stored parent depth in `wave` above.
#                             # We rely on _crawl_one returning children only
#                             # when depth < max_depth, so just enqueue at
#                             # depth=current+1 — we don't need it here
#                             # because _crawl_one already gate-keeps depth.
#                             frontier.append((child_url, 0))  # depth tracking internal

#         logger.info(f"[web_crawler] Crawl complete: {total_processed} pages processed")
#         await broadcast({"type": "crawl_complete", "pages_processed": total_processed})

#         if pipeline_run_id:
#             from src.ingest.run_tracker import get_tracker, remove_tracker
#             tracker = await get_tracker(pipeline_run_id)
#             if tracker:
#                 await tracker.send_callback()
#                 await remove_tracker(pipeline_run_id)

#     except Exception as e:
#         logger.error(f"[web_crawler] Crawl FAILED: {e}", exc_info=True)
#         await broadcast({"type": "error", "message": str(e)})
#         if pipeline_run_id:
#             from src.ingest.run_tracker import get_tracker, remove_tracker
#             tracker = await get_tracker(pipeline_run_id)
#             if tracker:
#                 tracker.record_file(
#                     source_id="", file_name="__crawl_error__", file_type="",
#                     folder_number=0, chunks_created=0, status="failed",
#                     error=str(e),
#                 )
#                 await tracker.send_callback()
#                 await remove_tracker(pipeline_run_id)

#     finally:
#         _robots_cache.clear()


# # ---------------------------------------------------------------------------
# # Internal: crawl a single page
# # ---------------------------------------------------------------------------

# async def _crawl_one(
#     url: str,
#     depth: int,
#     session: aiohttp.ClientSession,
#     sem: asyncio.Semaphore,
#     domain_last_request: dict[str, float],
#     domain_lock: asyncio.Lock,
#     pipeline_run_id: str | None,
#     allowed_file_types: Optional[list[str]],
#     allowed_domains: Optional[set[str]],
#     max_depth: int,
#     max_bytes: Optional[int],
#     crawl_delay: float,
#     respect_robots: bool,
# ) -> list[str]:
#     """
#     Fetch *url*, store metadata + raw bytes, notify RAG pipeline.

#     Returns
#     -------
#     list[str]
#         Child URLs discovered on this page (empty if depth >= max_depth
#         or if the page is not HTML).
#     """
#     async with sem:
#         parsed_url = urllib.parse.urlparse(url)
#         domain = parsed_url.netloc
#         ext = _ext(parsed_url)
#         file_name = Path(parsed_url.path).name or "index.html"

#         # ---- domain filter ----
#         if allowed_domains and domain not in allowed_domains:
#             logger.debug(f"[web_crawler] Skipping off-domain URL: {url}")
#             return []

#         # ---- extension filter ----
#         if ext in SKIP_EXTENSIONS:
#             logger.debug(f"[web_crawler] Skipping non-text extension '.{ext}': {url}")
#             return []

#         # html/htm and no-extension URLs are ALWAYS crawled —
#         # most web pages have no file extension in their path
#         # (e.g. /wiki/Web_page resolves to html at runtime).
#         is_html_page = ext in ('html', 'htm', '')
#         if allowed_file_types is not None and not is_html_page and ext not in allowed_file_types:
#             await broadcast({
#                 "type": "skipped", "path": url, "file_name": file_name,
#                 "reason": f"File type not in allowed list: .{ext}",
#             })
#             return []

#         # ---- robots.txt ----
#         if respect_robots:
#             rp = await _get_robots(domain, session)
#             if not _is_allowed_by_robots(rp, url):
#                 logger.info(f"[web_crawler] Blocked by robots.txt: {url}")
#                 await broadcast({
#                     "type": "skipped", "path": url, "file_name": file_name,
#                     "reason": "Blocked by robots.txt",
#                 })
#                 return []

#         # ---- polite crawl delay (per domain) ----
#         import time
#         async with domain_lock:
#             last = domain_last_request.get(domain, 0.0)
#             wait = crawl_delay - (time.monotonic() - last)
#             if wait > 0:
#                 await asyncio.sleep(wait)
#             domain_last_request[domain] = time.monotonic()

#         # ---- HTTP fetch ----
#         await broadcast({"type": "file_found", "path": url, "file_name": file_name})
#         await broadcast({"type": "processing", "path": url, "file_name": file_name})

#         try:
#             async with session.get(url, allow_redirects=True, max_redirects=5) as resp:
#                 if resp.status != 200:
#                     logger.warning(f"[web_crawler] HTTP {resp.status} for {url} — skipping")
#                     await broadcast({
#                         "type": "skipped", "path": url, "file_name": file_name,
#                         "reason": f"HTTP {resp.status}",
#                     })
#                     return []

#                 content_type = resp.content_type or ""
#                 # Reject content types we can't use
#                 if not any(content_type.startswith(p) for p in CRAWLABLE_MIME_PREFIXES):
#                     logger.debug(f"[web_crawler] Skipping unhandled content-type '{content_type}': {url}")
#                     return []

#                 # Size guard — read up to max_bytes + 1 so we can detect overflow
#                 if max_bytes:
#                     raw_bytes = await resp.content.read(max_bytes + 1)
#                     if len(raw_bytes) > max_bytes:
#                         logger.info(f"[web_crawler] Skipping oversized page (>{max_bytes // 1024 // 1024} MB): {url}")
#                         await broadcast({
#                             "type": "skipped", "path": url, "file_name": file_name,
#                             "reason": "Page exceeds max_size_mb",
#                         })
#                         return []
#                 else:
#                     raw_bytes = await resp.read()

#                 actual_mime = content_type.split(";")[0].strip()
#                 is_html = actual_mime in ("text/html", "application/xhtml+xml")

#         except asyncio.TimeoutError:
#             logger.warning(f"[web_crawler] Timeout fetching {url}")
#             await broadcast({"type": "skipped", "path": url, "file_name": file_name, "reason": "Timeout"})
#             return []
#         except aiohttp.ClientError as exc:
#             logger.warning(f"[web_crawler] Client error fetching {url}: {exc}")
#             await broadcast({"type": "skipped", "path": url, "file_name": file_name, "reason": str(exc)})
#             return []

#         # ---- persist to MongoDB ----
#         now_iso = datetime.now(timezone.utc).isoformat()
#         normalized = {
#             "source_id": url,
#             "source_type": "web",
#             "file_name": file_name,
#             "file_type": ext,
#             "path": url,
#             "parent_folder_id": (
#                 "/".join(parsed_url.path.rstrip("/").split("/")[:-1])
#                 if parsed_url.path else ""
#             ),
#             "web_url": url,
#             "mime_type": actual_mime,
#             "size": len(raw_bytes),
#             "content_status": "accessible",
#             "connector_synced_at": now_iso,
#             "crawl_depth": depth,
#         }

#         folder_number = await save_file_pair(url, normalized, raw_bytes, actual_mime)

#         await broadcast({
#             "type": "stored",
#             "path": url,
#             "file_name": file_name,
#             "folder_number": folder_number,
#             "content_status": "accessible",
#             "mime_type": actual_mime,
#             "size": len(raw_bytes),
#         })

#         # ---- notify RAG pipeline ----
#         logger.info(f"[web_crawler] Notifying RAG for folder {folder_number}: {url}")
#         await notify_rag_pipeline(
#             folder_number,
#             pipeline_run_id=pipeline_run_id,
#             source_id=url,
#             file_name=file_name,
#             file_type=ext,
#             raw_bytes=raw_bytes,          # actual content now, not empty bytes
#             web_url=url,
#         )

#         # ---- extract child links (HTML only, within depth limit) ----
#         if not is_html or depth >= max_depth:
#             return []

#         try:
#             html_text = raw_bytes.decode("utf-8", errors="replace")
#         except Exception:
#             return []

#         child_urls = _extract_links(url, html_text)
#         # Filter to same-domain and non-skippable extensions right here
#         # so we don't even enqueue junk into the frontier.
#         filtered: list[str] = []
#         for child in child_urls:
#             child_parsed = urllib.parse.urlparse(child)
#             child_ext = _ext(child_parsed)
#             if child_ext in SKIP_EXTENSIONS:
#                 continue
#             if allowed_domains and child_parsed.netloc not in allowed_domains:
#                 continue
#             filtered.append(child)

#         logger.debug(f"[web_crawler] Discovered {len(filtered)} child URLs from {url}")
#         return filtered