<h1 align="center">AIOPicks</h1>

<p align="center">
  <strong>AI-personalised Stremio discovery built from your Trakt history.</strong><br />
  AIOPicks keeps a curated set of streaming lanes filled with fresh picks generated through OpenRouter.
</p>

---

## ✨ What is AIOPicks?

AIOPicks is a FastAPI service for Stremio that turns your own viewing footprint into a living discovery feed. On every refresh the service summarises your Trakt history, requests new titles from an OpenRouter-powered generator that match each lane, enriches the results with metadata, and serves them as standard Stremio catalogs. The catalogs stay consistent—only the items rotate—so you can pin them on the Stremio home screen without surprises.

Behind the scenes the service stores catalog payloads in SQLite (or any SQLAlchemy-compatible database you point it at) and refreshes them on a schedule. If the AI call fails it falls back to "recently loved" history mixes, so your catalogs never disappear.

## 🎬 Current catalog line-up

AIOPicks currently generates 19 fixed lanes. Movies and series are requested separately so Stremio can merge in rich metadata from Cinemeta or another compatible service you configure.

| Lane | Type | What the AI looks for |
|------|------|-----------------------|
| Movies For You | Movie | Primary personalised movie lane scoring unseen films against your genre affinity, favourite talent, and active streaks to surface the highest-propensity recommendations. |
| Series For You | Series | Primary personalised series lane ranking in-season runs and upcoming debuts by how closely they match your binge cadence, preferred talent, and franchise momentum. |
| Because You Watched | Series | Similar series to your recent watches, extending the moods you just binged. |
| Your Top Genre Picks | Movie | Fresh films expanding on the genres you play most—thrillers, comedies, and more. |
| Actors You Love | Movie | Movies headlined by the performers you return to again and again. |
| Directors You Return To | Movie | Films from directors already in your rotation, including acclaimed deep cuts. |
| Franchises You Started | Series | Series sequels, prequels, and spin-offs tied to universes you've begun but not finished. |
| Hidden Gems (Last 5 Years) | Movie | Critically praised films from the past five years that align with your taste yet slipped by. |
| Classics You Missed | Movie | 70s–90s films that fit your profile but never made it into your history. |
| Best of the Last Decade | Movie | Standout 2010s films matching your vibe and still waiting in your queue. |
| Seasonal Picks for You | Movie | Rotating films for the current season—holiday comfort, Halloween chills, or summer heat. |
| International Favorites | Movie | Foreign films in your preferred genres that global fans rave about. |
| Cult Classics in Your Taste | Movie | Famous cult films that match your sensibilities but never hit your watch history. |
| Indie Discoveries | Movie | Independent films that mirror your taste with daring storytelling and strong buzz. |
| Mini-Series Matches | Series | Short, high-impact limited series tuned to your favourite tones and genres. |
| Docs You’ll Like | Movie | Feature documentaries linked to the interests—crime, sports, history—you revisit often. |
| Animated Worlds | Series | Animated and anime series that echo the flavours you already love. |
| Missed While Binging | Movie | Films released while you were deep into other shows—worthy catch-ups for your queue. |
| Forgotten Favorites Expanded | Series | Series related to movies or shows you adored years ago—spiritual sequels and continuations. |

## ⚙️ How it works right now

1. **Trakt ingestion** – The service pulls your configured amount of movie and series history (up to 10,000 entries each) together with statistics that help the UI surface watch-time totals.
2. **Taste summary** – AIOPicks builds prompts summarising your favourite genres, people, and recent standouts while passing fingerprints of everything you have already logged so repeats can be filtered out.
3. **AI generation** – Each lane is requested in parallel through OpenRouter, seeded with a random token so results rotate between refreshes while keeping the lane title stable.
4. **Metadata enrichment** – When a Cinemeta-compatible metadata service URL is configured, missing posters, backgrounds, IDs, and release years are filled in before storing the catalogs.
5. **Persistence & refresh** – Catalogs are written to the database and served straight from storage. Background jobs refresh them according to your configured interval, and `/api/profile/prepare` can be called (or triggered from the config UI) to force a rebuild.
6. **Graceful fallback** – If the discovery engine cannot be reached, history-based mixes keep your catalogs populated until the next successful refresh.

## 🚀 Feature highlights

- **Stable discovery lanes** – A fixed manifest of 19 catalogs keeps Stremio shelves predictable while still rotating the items inside each lane.
- **OpenRouter + Trakt intelligence** – The AI receives rich context including genre/people counters and a deduplication index so it can recommend true first-time watches.
- **Metadata bridge** – Optional lookups against Cinemeta (or any compatible service) fill in posters, backgrounds, and canonical IDs for cleaner Stremio grids.
- **Profile-aware config** – Manifest parameters, refresh cadence, and overrides are stored per profile in the database, and the `/config` UI lets you trigger refreshes, sign into Trakt, and copy ready-to-use manifest URLs.
- **Resilient caching** – Catalogs persist in SQLite by default and survive restarts; background refreshes can be forced via API or will run automatically on the interval you specify.

## 🛠️ Prerequisites

- Python 3.10+
- A Trakt application (client ID/secret) and an access token with history scope
- An OpenRouter API key
- (Recommended) A Cinemeta-compatible metadata endpoint, e.g. `https://v3-cinemeta.strem.io`
- (Optional) Docker if you prefer container deployment

## 🔧 Configuration

1. Copy `.env.sample` to `.env` and fill in your credentials. Only the variables listed below are currently used by the application.
2. Provide the minimum secrets:
   - `OPENROUTER_API_KEY`
   - `TRAKT_CLIENT_ID`, `TRAKT_CLIENT_SECRET`, and a long-lived `TRAKT_ACCESS_TOKEN`
3. Optional but recommended settings:
   - `OPENROUTER_MODEL` (defaults to `google/gemini-2.5-flash-lite`)
   - `TRAKT_HISTORY_LIMIT` (default `1000`, max `10000` per type)
   - `CATALOG_ITEM_COUNT` (items per lane, default `8`)
   - `REFRESH_INTERVAL` (seconds between automatic refreshes, default `43200`)
   - `CACHE_TTL` (how long cached catalog responses stay valid, default `1800`)
   - `GENERATION_RETRY_LIMIT` (extra AI attempts if a lane comes back short)
   - `METADATA_ADDON_URL` (Cinemeta or another metadata service; omit `/manifest.json`)
   - `DATABASE_URL` (SQLAlchemy URL; defaults to `sqlite+aiosqlite:///./aiopicks.db`)

Open `http://localhost:3000/config` after the server starts to:

- Verify your environment configuration and profile status
- Trigger catalog generation (`Force refresh`)
- Launch the Trakt OAuth helper if you prefer short-lived tokens
- Copy a manifest URL scoped to a specific profile and override set

## 🧪 Local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
cp .env.sample .env  # update with your keys
uvicorn app.main:app --reload --port 3000
```

Visit `http://localhost:3000/manifest.json` to confirm the service is live and the manifest lists all 19 catalogs. Install that URL in Stremio once your profile shows as "Ready" on the config page.

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
- **Catalog service** (`app/services/catalog_generator.py`) orchestrates Trakt ingestion, AI prompting, metadata enrichment, caching, and persistence.
- **OpenRouter client** (`app/services/openrouter.py`) formats prompts and manages per-lane retries against the configured AI engine.
- **Trakt client** (`app/services/trakt.py`) fetches history batches and statistics used to build prompts and UI summaries.
- **Metadata bridge** (`app/services/metadata_addon.py`) talks to Cinemeta-compatible services to fill in artwork and IDs.
- **Database layer** (`app/database.py`, `app/db_models.py`) persists profiles, catalog payloads, and refresh bookkeeping using SQLAlchemy async sessions.

## 📡 API surface

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/manifest.json` | GET | Returns the manifest containing the 19 catalog lanes for the resolved profile. |
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
