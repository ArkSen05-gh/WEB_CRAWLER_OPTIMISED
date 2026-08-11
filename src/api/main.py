"""
src/api/main.py
FastAPI application entry point.

Run with:
    uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
"""
import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes.ingest import router as ingest_router

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="RAG Web Crawler API",
    description="Recursively crawls URLs, stores content in MongoDB, summarises via Groq.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ingest_router)


@app.on_event("startup")
async def on_startup() -> None:
    logger.info("=" * 55)
    logger.info("RAG Web Crawler API — starting up")
    logger.info(f"  MONGO_URI  : {os.getenv('MONGO_URI','NOT SET')[:45]}...")
    logger.info(f"  MONGO_DB   : {os.getenv('MONGO_DB_NAME','NOT SET')}")
    logger.info(f"  GROQ_MODEL : {os.getenv('GROQ_MODEL','llama3-8b-8192')}")
    logger.info("  Docs       : http://localhost:8000/docs")
    logger.info("=" * 55)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    logger.info("RAG Web Crawler API — shut down")


@app.get("/", tags=["Root"])
async def root() -> dict:
    return {"service": "RAG Web Crawler API", "docs": "/docs", "health": "/api/ingest/health"}