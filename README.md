# 🕷️ Web Crawler (RAG)

A production-ready asynchronous web crawler built for **Retrieval-Augmented Generation (RAG)** pipelines. It recursively crawls any website from a single seed URL, extracts clean structured content from every page, and stores it in **MongoDB Atlas** — ready to be consumed by an LLM (powered by **Groq**).

Built by **Indus Net Technologies Pvt. Ltd.**

---

## ✨ Features

- 🔁 **Recursive BFS crawling** — give it one URL, it finds every sub-page automatically
- 🧹 **Clean text extraction** — strips all HTML tags, scripts, and navigation noise
- 🖼️ **Image extraction** — captures every image with `src`, `alt`, `width`, `height`
- 📑 **Structured content** — extracts title, meta description, headings (h1–h6), and paragraphs separately
- ⚡ **Async & concurrent** — built on `aiohttp` with configurable batch size
- 🤖 **robots.txt compliance** — respects crawl rules per domain
- 🚦 **Rate limiting** — configurable per-domain crawl delay to avoid HTTP 429s
- 🌐 **Domain boundary control** — stays on the seed domain by default
- 🗃️ **MongoDB storage** — upserts structured documents via Motor (async driver)
- 🧠 **Groq LLM summarisation** — generates a factual summary of each page via Groq API
- ❌ **Cancellation support** — stop any active crawl mid-run via API
- 📡 **Real-time broadcast events** — every crawl action is logged and broadcast

---

## 🗂️ Project Structure

```
Web_crawler_optimised/
│
├── .env                              # your secrets (never commit this)
├── .env.example                      # template — copy to .env
├── pyrightconfig.json                # VS Code / Pylance type-checker config
├── requirements.txt                  # all Python dependencies
│
├── services/
│   └── web_crawler.py                # core crawler — BFS, fetch, extract, store
│
└── src/
    ├── api/
    │   ├── main.py                   # FastAPI app entry point
    │   └── routes/
    │       ├── ingest.py             # POST /api/ingest/web  |  GET /api/ingest/crawl
    │       └── global_pipeline.py    # in-memory cancellation registry
    │
    ├── core/
    │   ├── events.py                 # broadcast() — real-time event logging
    │   └── db/
    │       └── mongo_storage.py      # Motor/MongoDB connection + upsert logic
    │
    └── ingest/
        ├── notify.py                 # Groq LLM summarisation + RAG pipeline notify
        └── run_tracker.py            # per-run statistics tracker
```

---

## 📦 What Gets Stored in MongoDB

Every crawled page is saved as one document in the `source_files` collection:

```json
{
  "source_id":        "https://example.com/about",
  "title":            "About Us | Example",
  "meta_description": "Learn more about our team and mission.",
  "clean_text":       "About Us\nWe are a team of passionate engineers...",
  "word_count":       432,
  "headings": [
    { "level": "h1", "text": "About Us" },
    { "level": "h2", "text": "Our Mission" }
  ],
  "paragraphs": [
    "We are a team of passionate engineers building the future.",
    "Founded in 2015, we have helped over 500 clients..."
  ],
  "images": [
    { "src": "https://example.com/team.jpg", "alt": "Our team", "width": "800", "height": "400" }
  ],
  "crawl_depth":      1,
  "file_type":        "html",
  "mime_type":        "text/html",
  "size":             94821,
  "content_status":   "accessible",
  "connector_synced_at": "2026-08-11T07:00:35.257261+00:00"
}
```

---

## ⚙️ Setup

### 1. Clone the repository

```bash
git clone https://github.com/your-username/Web_crawler_optimised.git
cd Web_crawler_optimised
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in your real values:

```env
# Groq LLM — get your free key at https://console.groq.com
GROQ_API_KEY=gsk_your_key_here
GROQ_MODEL=llama-3.1-8b-instant

# MongoDB Atlas — connection string from Atlas → Connect → Drivers → Python
MONGO_URI=mongodb+srv://youruser:yourpassword@cluster0.xxxxx.mongodb.net/
MONGO_DB_NAME=rag_db
```

> ⚠️ **Important:** your MongoDB password must not contain special characters like `@`, `:`, `/`.  
> If it does, reset it in Atlas → Security → Database Access to a simple alphanumeric password.

### 5. MongoDB Atlas setup

1. Go to [cloud.mongodb.com](https://cloud.mongodb.com) and create a free **M0** cluster
2. **Security → Database Access** → Add a database user with a simple password
3. **Security → Network Access** → Add IP `0.0.0.0/0` (allow all, for development)
4. **Connect → Drivers → Python** → copy the connection string into `MONGO_URI`

> The database (`rag_db`) and collection (`source_files`) are created **automatically** on first crawl — no manual setup needed.

---

## 🚀 Running the Project

### Start the server

```bash
uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
```

You should see:

```
=======================================================
RAG Web Crawler API — starting up
  MONGO_URI  : mongodb+srv://youruser:***...
  MONGO_DB   : rag_db
  GROQ_MODEL : llama-3.1-8b-instant
  Docs       : http://localhost:8000/docs
=======================================================
```

### Open the interactive API docs

```
http://localhost:8000/docs
```

---

## 🕸️ Starting a Crawl

### Option 1 — Simple (just paste a URL)

In Swagger UI → **GET /api/ingest/crawl** → Try it out → paste your URL → Execute.

Or directly in your browser:

```
http://localhost:8000/api/ingest/crawl?url=https://example.com
```

All settings use smart defaults automatically.

### Option 2 — Full control (POST with JSON)

```bash
curl -X POST http://localhost:8000/api/ingest/web \
  -H "Content-Type: application/json" \
  -d '{
    "urls": ["https://example.com"],
    "max_depth": 2,
    "max_pages": 100,
    "batch_size": 5,
    "crawl_delay": 1.5,
    "respect_robots": true,
    "stay_on_seed_domains": true
  }'
```

### Option 3 — Run directly from Python

```python
import asyncio
from services.web_crawler import process_web_urls

asyncio.run(process_web_urls(
    urls=["https://example.com"],
    max_depth=2,
    max_pages=100,
    batch_size=5,
    crawl_delay=1.5,
))
```

---

## 🎛️ Crawl Parameters

| Parameter | Default | Description |
|---|---|---|
| `urls` | required | List of seed URLs to start crawling from |
| `max_depth` | `2` | How many hops from the seed URL |
| `max_pages` | `200` | Hard cap on total pages crawled |
| `batch_size` | `5` | Max concurrent HTTP requests |
| `crawl_delay` | `1.5` | Seconds between requests to the same domain |
| `respect_robots` | `true` | Honour `robots.txt` directives |
| `stay_on_seed_domains` | `true` | Never follow links off the seed site |
| `max_size_mb` | `null` | Skip pages larger than this (in MB) |
| `allowed_file_types` | `null` | Whitelist extensions e.g. `["html","pdf"]` |

### Recommended settings by site type

| Site | `max_depth` | `max_pages` | `crawl_delay` |
|---|---|---|---|
| Wikipedia | `2` | `100–200` | `1.5s` |
| Corporate site (e.g. intglobal.com) | `2` | `50–100` | `1.0s` |
| E-commerce | `3` | `500–2000` | `1.0s` |
| Quick test | `1` | `10–20` | `1.5s` |

---

## ❌ Cancelling a Crawl

```bash
curl -X POST "http://localhost:8000/api/ingest/cancel?collection_id=YOUR_COLLECTION_ID"
```

---

## 🔍 Viewing Crawled Data in MongoDB Atlas

1. Go to [cloud.mongodb.com](https://cloud.mongodb.com)
2. Left sidebar → **Data Explorer**
3. Select **Cluster0** → **rag_db** → **source_files**
4. Each document is one crawled page with full structured content

---

## 🧠 Groq LLM Models

Set `GROQ_MODEL` in your `.env` to any of:

| Model | Speed | Context | Best for |
|---|---|---|---|
| `llama-3.1-8b-instant` | Fastest | 8K | Default, free tier |
| `llama-3.3-70b-versatile` | Slower | 8K | Higher quality summaries |
| `mixtral-8x7b-32768` | Medium | 32K | Long documents |

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| API framework | FastAPI + Uvicorn |
| HTTP crawling | aiohttp |
| HTML parsing | BeautifulSoup4 + lxml |
| Database | MongoDB Atlas + Motor |
| LLM | Groq (llama-3.1-8b-instant) |
| Type checking | Pyright / Pylance |
| Language | Python 3.12 |

---

## 📄 License

Business Source License 1.1 (BUSL-1.1)  
Additional Use Grant: internal deployment and modification only.  
Commercial licensing: licensing@intglobal.com
