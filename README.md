<h1 align="center">AIOPicks</h1>

<p align="center">
  <strong>AI-powered personalized recommendations for your next binge.</strong><br />
  AIOPicks generates dynamic movie and TV show catalogs for Stremio using your Trakt history and
  OpenRouter's <code>google/gemini-2.5-flash-lite</code> model.
</p>

---

## ✨ What is AIOPicks?

AIOPicks is a FastAPI-powered Stremio addon that turns your Trakt watch history into AI-curated, ever-changing
catalogs. Every refresh uses the Gemini 2.5 Flash Lite model on OpenRouter to craft brand-new themes, names, and
recommendations so you never scroll the same rows twice.

Because everything runs on your own server, your data never leaves your control. Connect your Trakt account, provide an
OpenRouter API key, and enjoy endlessly fresh discovery playlists.

## 🚀 Key Features

### 🤖 AI-Powered Personalization
- **Trakt Integration**: Pulls your watch history (movies & series) with extended metadata
- **OpenRouter AI**: Uses `google/gemini-2.5-flash-lite` for imaginative yet grounded catalog ideas
- **Randomized Catalogs**: Each refresh injects a random seed so names and picks are always surprising
- **Privacy-Focused**: All history processing and AI prompts happen on your self-hosted instance
- **Catalog-Only Manifest**: Advertises just the catalog resource so Stremio can merge in metadata from Cinemeta or other providers you install.

### 📊 User-Configurable Dynamic Catalogs
AIOPicks invents themed rows with bespoke names and contents:

- **🌙 Midnight Mystery Flights** – *Atmospheric thrillers for after dark*
- **🎭 Seoulful Stories** – *Emotional Korean dramas aligned with your taste*
- **🔥 Weekend Questline** – *Series primed for marathon sessions*
- **✨ Critics' Curveballs** – *Awarded picks you somehow missed*

### 🧰 Flexible Configuration
- **Catalog Count**: Choose how many movie/series rows to generate (1-12)
- **Manifest Name**: Override the add-on title shown inside Stremio without exposing model details
- **Refresh Interval**: Control how often the AI regenerates catalogs
- **Caching**: Lightweight in-memory cache keeps Stremio responses snappy between refreshes
- **Fallbacks**: If the AI call fails, the addon gracefully falls back to history-based mixes

## 🛠️ Prerequisites
- Python 3.10+
- A Trakt account with viewing history (OAuth device authentication recommended)
- OpenRouter API key with access to `google/gemini-2.5-flash-lite`
- (Optional) Docker if you prefer container deployment

## ⚙️ Configuration

Create a `.env` file (or copy `.env.sample`) with your credentials:

```env
OPENROUTER_API_KEY=your-openrouter-key
OPENROUTER_MODEL=google/gemini-2.5-flash-lite
TRAKT_CLIENT_ID=your-trakt-client-id
TRAKT_CLIENT_SECRET=your-trakt-client-secret
TRAKT_ACCESS_TOKEN=your-trakt-access-token
# Optional: override the detected redirect URL if you proxy through a custom domain
TRAKT_REDIRECT_URI=https://your-domain.example/api/trakt/callback
CATALOG_COUNT=6
REFRESH_INTERVAL=43200  # seconds
CACHE_TTL=1800          # seconds
```

> ℹ️ You can obtain a Trakt access token by creating a personal application and using the device code flow. Store the
> long-lived access token for this addon. The `/config` helper automatically uses `TRAKT_CLIENT_ID` and
> `TRAKT_CLIENT_SECRET` from the server environment when minting device codes—secrets never touch the browser.

## 🧪 Local Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
cp .env.sample .env  # then edit with your keys
uvicorn app.main:app --reload --port 3000
```

Open `http://localhost:3000/manifest.json` to confirm the addon is running. Install the manifest URL in Stremio to see
the AI-generated catalogs.

### Running Tests

```bash
pytest
```

## 🐳 Docker Quickstart

### Using Docker Compose (recommended)

```bash
cp .env.sample .env  # then edit with your keys
docker compose up -d --build
```

This command builds the image, starts the `aiopicks` service, and automatically loads
environment variables from `.env`. Visit
`http://localhost:3000/manifest.json` to verify the addon is running. Tail
logs with `docker compose logs -f aiopicks` and stop the stack when you are
finished with `docker compose down`.

### Manual Docker commands

```bash
docker build -t aiopicks .
docker run -d \
  --name aiopicks \
  -p 3000:3000 \
  --env-file .env \
  aiopicks
```

## 🏗️ Architecture Overview

- **FastAPI Server** (`app/main.py`): Implements Stremio manifest and catalog endpoints (metadata endpoint remains available for debugging but is not advertised)
- **Catalog Service** (`app/services/catalog_generator.py`): Orchestrates Trakt ingestion, AI prompting, caching, and
  background refresh
- **Trakt Client** (`app/services/trakt.py`): Fetches and summarizes history with optional fallbacks
- **OpenRouter Client** (`app/services/openrouter.py`): Calls Gemini 2.5 Flash Lite with structured prompts and parses
  the JSON response
- **Pydantic Models** (`app/models.py`): Validates AI output and converts it into Stremio-friendly payloads

The service keeps a short-lived cache of the last generated catalogs. A background coroutine refreshes them on the
interval you configure. If OpenRouter is unavailable, it falls back to simple mixes derived from your watch history.

## 📦 API Surface

| Endpoint | Description |
|----------|-------------|
| `/manifest.json` | Advertises AI-generated catalogs to Stremio (metadata comes from Cinemeta or other addons) |
| `/catalog/{type}/{id}.json` | Returns the metas array for a specific catalog |
| `/meta/{type}/{id}.json` | (Optional) Internal metadata endpoint for debugging |
| `/healthz` | Lightweight readiness probe |

## ⚠️ Disclaimer

AIOPicks is a discovery tool. It does not host or stream content—only suggests what to watch next based on your own
history. Always access content through legal providers and comply with applicable laws.

---

**Built for self-hosting enthusiasts chasing endlessly fresh watchlists.**
