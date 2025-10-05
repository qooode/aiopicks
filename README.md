<h1 align="center">AIOPicks</h1>

<p align="center">
  <strong>AI or Local personalised Stremio discovery built from your Trakt history.</strong><br />
  AIOPicks keeps a curated set of streaming lanes filled with fresh picks generated via OpenRouter or a fast Local engine.
</p>

---

## ✨ What is AIOPicks?

AIOPicks is a FastAPI service for Stremio that turns your own viewing footprint into a living discovery feed. On every refresh the service summarises your Trakt history, generates lane-matched titles using either an OpenRouter-powered AI generator or a Local offline engine, enriches the results with metadata, and serves them as standard Stremio catalogs. The catalogs stay consistent—only the items rotate—so you can pin them on the Stremio home screen without surprises.

Behind the scenes the service stores catalog payloads in SQLite (or any SQLAlchemy-compatible database you point it at) and refreshes them on a schedule. If the AI path isn’t configured, the Local engine uses Trakt listings (trending/popular) plus your taste signals to propose fresh, unseen titles—so your catalogs never disappear.

## 🎬 Current catalog line-up

AIOPicks currently generates 20 fixed lanes. Movies and series are requested separately so Stremio can merge in rich metadata from Cinemeta or another compatible service you configure.

| Lane | Type | What the AI looks for |
|------|------|-----------------------|
| Movies For You | Movie | Movies that represent your overall taste profile across favourite genres and moods. |
| Series For You | Series | Series that represent your overall taste profile across favourite genres and moods. |
| Your Comfort Zone | Movie | Safe film picks that align perfectly with the patterns you already love to revisit. |
| Expand Your Horizons | Movie | Quality films just outside your normal rotation, ready to broaden your taste without losing your vibe. |
| Your Next Obsession | Series | Binge-ready series poised to become your newest favourites based on deep taste analysis. |
| You Missed These | Movie | Noteworthy films that slipped past you the first time but match what you already enjoy. |
| Critics Love, You'll Love | Movie | Critically adored movies curated to mirror your personal preferences and pacing. |
| International Picks | Movie | Foreign films matched to your favourite genres and storytelling moods. |
| Your Guilty Pleasures Extended | Movie | More of the indulgent movies you watch on repeat—even if you never mention them. |
| Starring Your Favorite Actors | Movie | Films featuring the actors who dominate your watch history and never disappoint. |
| Visually Stunning For You | Movie | Cinematography showcases that match your genre preferences and appetite for lush visuals. |
| Background Watching | Series | Easy-flowing series perfect for multitasking without losing the narrative thread. |
| Same Universe, Different Story | Series | Spin-offs and related series expanding the franchises you already follow. |
| Animation Worth Your Time | Series | Animated series that transcend age brackets while still fitting your preferred tones. |
| Documentaries You’ll Like | Movie | Feature documentaries tied to the crime, sports, and history stories you revisit often. |
| Your Top Genre | Movie | Essential films from the genre you stream most, dialled in to your signature moods. |
| Your Second Genre | Series | Series highlights from the runner-up genre in your history, tuned to familiar beats. |
| Your Third Genre | Movie | Hand-picked films exploring the third pillar of your taste profile with fresh twists. |
| Franchises You Started | Series | Series sequels, prequels, and spin-offs tied to universes you've begun but not finished. |
| Independent Films That Mirror Your Taste | Movie | Indie standouts with daring storytelling and strong buzz that align with your preferences. |

## ⚙️ How it works right now

1. **Trakt ingestion** – The service pulls your configured amount of movie and series history (all plays by default, or the limit you set up to 10,000 entries per type) together with statistics that help the UI surface watch-time totals.
2. **Taste summary** – AIOPicks builds prompts summarising your favourite genres, people, and recent standouts while passing fingerprints of everything you have already logged so repeats can be filtered out.
3. **Discovery generation (AI or Local)** –
   - AI mode: lanes are requested in parallel through OpenRouter, seeded with a random token so results rotate between refreshes while keeping the lane title stable.
   - Local mode: lanes are filled by remixing your taste profile with fresh, unseen picks sourced from Trakt listings (trending/popular), filtered per lane.
4. **Metadata enrichment** – When a Cinemeta-compatible metadata service URL is configured, missing posters, backgrounds, IDs, and release years are filled in before storing the catalogs.
5. **Persistence & refresh** – Catalogs are written to the database and served straight from storage. Background jobs refresh them according to your configured interval, and `/api/profile/prepare` can be called (or triggered from the config UI) to force a rebuild.
6. **Graceful fallback** – If AI is selected but unavailable, Local mode keeps your catalogs populated with fresh, unseen picks; if the network fails entirely, history-based mixes keep lanes alive until the next successful refresh.

## 🚀 Feature highlights

- **Stable discovery lanes** – A fixed manifest of 20 catalogs keeps Stremio shelves predictable while still rotating the items inside each lane.
- **Dual discovery engine** – Choose AI via OpenRouter or a fast Local engine that works with only Trakt + a metadata add-on.
- **OpenRouter + Trakt intelligence** – In AI mode, the model receives genre/language counters, recent highlights, and a deduplication index to recommend true first-time watches.
- **Metadata bridge** – Optional lookups against Cinemeta (or any compatible service) fill in posters, backgrounds, and canonical IDs for cleaner Stremio grids.
- **Profile-aware config** – Manifest parameters, refresh cadence, and overrides are stored per profile in the database, and the `/config` UI lets you trigger refreshes, sign into Trakt, and copy ready-to-use manifest URLs.
- **Resilient caching** – Catalogs persist in SQLite by default and survive restarts; background refreshes can be forced via API or will run automatically on the interval you specify.
- **Path-based overrides** – Manifest URLs can encode OpenRouter keys, catalog selections, cache timing, and more without touching environment variables, making it easy to generate multiple tailored profiles.

## 🛠️ Prerequisites

- Python 3.10+
- A Trakt application (client ID/secret) and an access token with history scope
- An OpenRouter API key (optional if you use Local mode)
- (Recommended) A Cinemeta-compatible metadata endpoint, e.g. `https://v3-cinemeta.strem.io`
- (Optional) Docker if you prefer container deployment

## 🔧 Configuration

1. Copy `.env.sample` to `.env` and fill in your credentials. Only the variables listed below are currently used by the application.
2. Provide the minimum secrets:
   - Local mode (no AI accounts): `TRAKT_CLIENT_ID`, `TRAKT_CLIENT_SECRET`, and a long-lived `TRAKT_ACCESS_TOKEN`
   - AI mode: add `OPENROUTER_API_KEY` (and optionally choose a different `OPENROUTER_MODEL`)
3. Optional but recommended settings:
   - `OPENROUTER_MODEL` (defaults to `google/gemini-2.5-flash-lite`)
   - `TRAKT_HISTORY_LIMIT` (default `0` = full history, max `10000` per type)
   - `CATALOG_ITEM_COUNT` (items per lane, default `8`)
   - `REFRESH_INTERVAL` (seconds between automatic refreshes, default `43200`)
   - `CACHE_TTL` (how long cached catalog responses stay valid, default `1800`)
   - `GENERATION_RETRY_LIMIT` (extra AI attempts if a lane comes back short, default `3`; AI mode only)
   - `CATALOG_KEYS` (comma-separated lane keys if you want to trim the manifest or change the order)
   - `METADATA_ADDON_URL` (Cinemeta or another metadata service; omit `/manifest.json`)
   - `DATABASE_URL` (SQLAlchemy URL; defaults to `sqlite+aiosqlite:///./aiopicks.db`)
   - `GENERATOR_MODE` (choose `openrouter` or `local`; defaults to `local`. You can still switch per-profile in the UI.)

### Manifest overrides and multi-profile use

- Use `/manifest/<key>/<value>/manifest.json` to override runtime settings without query strings. Values must be URL encoded and key/value pairs must be balanced. Examples:
  - `/manifest/catalogItems/12/manifest.json` – request 12 items per catalog.
  - `/manifest/catalogKeys/movies-for-you%2Cyou-missed-these/manifest.json` – restrict the manifest to the listed lanes.
  - `/manifest/openrouterKey/sk_live_xxx/refreshInterval/21600/manifest.json` – inject an API key override and reduce the refresh window to 6 hours.
  - `/manifest/engine/local/manifest.json` – switch the discovery engine to Local without touching `.env`.
- `/profiles/<profile>/manifest.json` and matching catalog routes expose explicit profile IDs (handy for multi-user hosting).
- The config page surfaces copyable manifest links that already include any overrides stored for the profile.

Open `http://localhost:3000/config` after the server starts to:

- Verify your environment configuration and profile status
- Trigger catalog generation (`Force refresh`)
- Launch the Trakt OAuth helper if you prefer short-lived tokens
- Choose the discovery engine (AI via OpenRouter or Local offline). In Local mode, OpenRouter fields are hidden.

### Local mode at a glance (no AI accounts)

- Uses your Trakt history to compute taste signals (genres, languages, recency) and a fingerprint set of watched titles.
- Pulls fresh, unseen candidates from Trakt listings (trending/popular) and filters them per-lane (e.g., non-English for “International”, short runtimes for “Background Watching”, top genres for “Top/Second/Third Genre”).
- Retries with progressively relaxed filters until each lane reaches your item target; de-duplicates across lanes within the same refresh.
- Enriches items via your configured metadata add-on (title-based lookups only). No TMDb required.
- Copy a manifest URL scoped to a specific profile and override set

## 🧪 Local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
cp .env.sample .env  # update with your keys
uvicorn app.main:app --reload --port 3000
```

Visit `http://localhost:3000/manifest.json` to confirm the service is live and the manifest lists all 20 catalogs. Install that URL in Stremio once your profile shows as "Ready" on the config page.

### Running tests

```bash
pytest
```

## 🐳 Docker quickstart

### Using Docker Compose (recommended)

```bash
cp .env.sample .env  # then edit with your keys
docker compose up -d --build
```

This builds the image, starts the `aiopicks` service, and pulls environment variables from `.env`. Check `http://localhost:3000/config` for status, tail logs with `docker compose logs -f aiopicks`, and stop everything with `docker compose down`.

### Manual Docker commands

```bash
docker build -t aiopicks .
docker run -d \
  --name aiopicks \
  -p 3000:3000 \
  --env-file .env \
  aiopicks
```

## 🏗️ Architecture overview

- **FastAPI application** (`app/main.py`) wires HTTP routes, OAuth helpers, and the background refresh lifecycle.
- **Catalog service** (`app/services/catalog_generator.py`) orchestrates Trakt ingestion, AI prompting or Local selection, metadata enrichment, caching, and persistence.
- **OpenRouter client** (`app/services/openrouter.py`) formats prompts and manages per-lane retries when AI is selected.
- **Trakt client** (`app/services/trakt.py`) fetches history, basic stats, and lightweight listings (trending/popular) used for Local mode and taste summaries.
- **Metadata bridge** (`app/services/metadata_addon.py`) talks to Cinemeta-compatible services to fill in artwork and IDs.
- **Database layer** (`app/database.py`, `app/db_models.py`) persists profiles, catalog payloads, and refresh bookkeeping using SQLAlchemy async sessions.

## 📡 API surface

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/manifest.json` | GET | Returns the manifest containing the 20 catalog lanes for the resolved profile. |
| `/catalog/{type}/{id}.json` | GET | Returns the metas array for a catalog (`type` is `movie` or `series`). |
| `/profiles/{profile}/manifest.json` | GET | Manifest scoped to an explicit profile ID (useful for multi-user setups). |
| `/profiles/{profile}/catalog/{type}/{id}.json` | GET | Catalog payload for a specific profile/content combination. |
| `/config` | GET | Interactive configuration UI, including Trakt sign-in and manual refresh controls. |
| `/api/profile/status` | GET | Inspect or resolve profile state (used by the config UI). |
| `/api/profile/prepare` | POST | Trigger catalog generation; supports `force` and `waitForCompletion`. |
| `/api/trakt/login-url` | POST | Start the Trakt OAuth authorisation flow for the config UI helper. |
| `/api/trakt/callback` | GET | OAuth redirect handler that relays tokens back to the config page. |
| `/healthz` | GET | Lightweight readiness probe. |

## ⚠️ Disclaimer

AIOPicks suggests what to watch based on your own history—it does not host or stream any content. Use legitimate sources and respect all applicable laws.

---

**Built for self-hosters who want fresh, personalised Stremio shelves without surrendering their data.**
