<h1 align="center">AIOPicks</h1>

<p align="center">
  <strong>AI-personalised discovery rows for your Stremio library.</strong><br />
  AIOPicks turns your Trakt history into a rotating set of themed catalogs powered by
  large language models accessed via OpenRouter's routing network.
</p>

---

## ✨ What is AIOPicks?

AIOPicks is a FastAPI-powered Stremio add-on that keeps a **stable collection of curated lanes**
up to date with the help of generative AI. Every profile stores its own preferences and history
snapshot. When a refresh runs, the service asks your configured OpenRouter model to refill each lane with
new titles that match the theme while avoiding anything you've already logged on Trakt.

Because everything runs on your own server, your data never leaves your control. Connect your
Trakt account, provide an OpenRouter API key, and install the manifest in Stremio to receive
fresh recommendations in seconds.

## 🔁 How the add-on works

1. **History ingestion** – the add-on fetches your recent Trakt movie and series history (up to
   the configured limit) and stores a lightweight snapshot in a local SQLite database.
2. **Taste summarisation** – AIOPicks builds a condensed brief of your viewing patterns to send
   alongside the AI prompt.
3. **Lane generation** – The LLM creates new picks for every predefined catalog lane while
   respecting exclusions, minimum rating rules, and retry limits.
4. **Metadata enrichment** – Optional lookups against your preferred Stremio-compatible metadata
   service fill in artwork and IDs so Stremio can merge external metadata.
5. **Caching & refresh** – Generated catalogs are cached for the configured TTL. Background
   jobs refresh each profile on a schedule, and a manual `/config` trigger can force new runs.

If the AI call fails, AIOPicks gracefully falls back to history-based mixes so the manifest
never goes empty.

## 📚 Stable catalog lanes

AIOPicks always publishes the full set of 17 themed rows below. The AI refreshes the contents
while the lane identities remain consistent, making the add-on easy to browse inside Stremio.

| Lane | Type | Intent |
|---|---|---|
| Because You Watched | Series | Similar series to your recent watches, extending the moods you just binged. |
| Your Top Genre Picks | Movies | Fresh films expanding on the genres you play most—thrillers, comedies, and more. |
| Actors You Love | Movies | Movies headlined by the performers you return to again and again. |
| Directors You Return To | Movies | Films from directors already in your rotation, including acclaimed deep cuts. |
| Franchises You Started | Series | Series sequels, prequels, and spin-offs tied to universes you've begun but not finished. |
| Hidden Gems (Last 5 Years) | Movies | Critically praised films from the past five years that align with your taste yet slipped by. |
| Classics You Missed | Movies | 70s–90s films that fit your profile but never made it into your history. |
| Best of the Last Decade | Movies | Standout 2010s films matching your vibe and still waiting in your queue. |
| Seasonal Picks for You | Movies | Rotating films for the current season—holiday comfort, Halloween chills, or summer heat. |
| International Favorites | Movies | Foreign films in your preferred genres that global fans rave about. |
| Cult Classics in Your Taste | Movies | Famous cult films that match your sensibilities but never hit your watch history. |
| Indie Discoveries | Movies | Independent films that mirror your taste with daring storytelling and strong buzz. |
| Mini-Series Matches | Series | Short, high-impact limited series tuned to your favourite tones and genres. |
| Docs You’ll Like | Movies | Feature documentaries linked to the interests—crime, sports, history—you revisit often. |
| Animated Worlds | Series | Animated and anime series that echo the flavours you already love. |
| Missed While Binging | Movies | Films released while you were deep into other shows—worthy catch-ups for your queue. |
| Forgotten Favorites Expanded | Series | Series related to movies or shows you adored years ago—spiritual sequels and continuations. |

## 🚀 Key capabilities

- **Trakt integration** – Device authentication from the `/config` page stores long-lived tokens
  for each profile, and the service keeps track of how many movies and shows are available for
  exclusion on every refresh.
- **OpenRouter orchestration** – Each lane request uses structured prompts, retry controls, and
  deterministic seeds while OpenRouter routes the call to partner-hosted models through its
  unified API.
- **Metadata bridging** – Connect the add-on to whichever Stremio-compatible metadata service you
  prefer so returned catalogs include posters, backdrops, and ID fields Stremio understands.
- **Profile persistence** – Configuration overrides, AI seeds, and generated catalogs are stored
  in SQLite so the add-on survives restarts without losing personalisation.
- **Graceful degradation** – When the AI is unavailable, fallback catalogs based solely on your
  history keep the manifest populated until the next successful generation.

## 🛠️ Prerequisites

- Python 3.10+
- A Trakt account with viewing history (device code login recommended)
- OpenRouter API key with access to at least one supported model
- (Optional) Docker if you prefer container deployment

## ⚙️ Configuration workflow

1. **Create a `.env` file** – copy `.env.sample` and fill in at least your OpenRouter key plus
   Trakt API client credentials. The catalog count is fixed at 17 lanes, so no tuning is required.
2. **Start the server** – run `uvicorn app.main:app --reload --port 3000` (or use Docker).
3. **Visit `/config`** – open `http://localhost:3000/config` to finish setup. The assistant can:
   - guide you through Trakt device authentication and store the resulting access token;
   - override OpenRouter model/key per profile;
   - choose the metadata source you want to enrich catalogs with;
   - adjust refresh cadence, cache duration, and item counts per lane;
   - trigger an immediate regeneration and check profile status.
4. **Install in Stremio** – once the status indicator shows healthy catalogs, add the manifest URL
   (`http://localhost:3000/manifest.json`) to Stremio.

## 🧩 Runtime tuning

All configuration values can be provided via environment variables or the `.env` file. The table
below lists the most relevant options:

| Setting | Default | Purpose |
|---|---|---|
| `OPENROUTER_API_KEY` | – | API key used for catalog generation. Required unless supplied per profile via `/config`. |
| `OPENROUTER_MODEL` | – | Model identifier requested from OpenRouter (configurable per profile). |
| `TRAKT_CLIENT_ID` / `TRAKT_CLIENT_SECRET` | – | Credentials needed for device authentication from the config UI. |
| `TRAKT_ACCESS_TOKEN` | – | Optional long-lived token if you prefer to preconfigure the profile without using the UI. |
| `TRAKT_HISTORY_LIMIT` | `1000` | Maximum Trakt history items stored for exclusions (10–2000). |
| `CATALOG_ITEM_COUNT` | `8` | Number of items the AI should return for each lane. |
| `GENERATION_RETRY_LIMIT` | `3` | Extra attempts allowed when lanes return too few results. |
| `REFRESH_INTERVAL` | `43200` (12h) | How often the background worker re-generates catalogs per profile. |
| `CACHE_TTL` | `1800` (30m) | How long Stremio reuses the last catalog response before asking the server again. (Catalogs still regenerate on the `REFRESH_INTERVAL` schedule.) |
| `METADATA_ADDON_URL` | – | Base URL for the Stremio-compatible metadata source used to enrich items. |
| `DATABASE_URL` | `sqlite+aiosqlite:///./aiopicks.db` | Location of the SQLite database that stores profiles and catalogs. |
| `APP_NAME` | `AIOPicks` | Display name surfaced in the manifest and config UI. |

Advanced variables such as `HOST`, `PORT`, `TRAKT_API_URL`, and `OPENROUTER_API_URL` are also
available for bespoke deployments.

## 🧪 Local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
cp .env.sample .env  # then edit with your keys
uvicorn app.main:app --reload --port 3000
```

Visit `http://localhost:3000/manifest.json` to confirm the add-on is running and explore the
configurator at `/config`.

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

This command builds the image, starts the `aiopicks` service, and automatically loads environment
variables from `.env`. Visit `http://localhost:3000/manifest.json` to verify the add-on is running.
Tail logs with `docker compose logs -f aiopicks` and stop the stack with `docker compose down`.

### Manual Docker commands

```bash
docker build -t aiopicks .
docker run -d \
  --name aiopicks \
  -p 3000:3000 \
  --env-file .env \
  aiopicks
```

## 📦 API surface

| Endpoint | Description |
|----------|-------------|
| `/manifest.json` | Advertises the AI-personalised catalogs to Stremio. |
| `/catalog/{type}/{id}.json` | Returns the metas array for a specific catalog. |
| `/meta/{type}/{id}.json` | (Optional) Internal metadata endpoint for debugging. |
| `/profiles/{profile}/manifest.json` | Resolve a manifest scoped to a stored profile. |
| `/config` | Interactive configuration assistant and status dashboard. |
| `/api/profile/prepare` | Prepare or refresh a profile from the config UI. |
| `/api/profile/status` | Inspect the current profile state and refresh health. |
| `/healthz` | Lightweight readiness probe. |

## ⚠️ Disclaimer

AIOPicks is a discovery tool. It does not host or stream content—only suggests what to watch next
based on your own history. Always access content through legal providers and comply with applicable
laws.

---

**Built for self-hosters chasing endlessly fresh watchlists—now with dependable themed lanes.**
