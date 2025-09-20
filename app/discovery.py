"""Discovery lane blueprints that guide catalog generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from .utils import slugify


@dataclass(frozen=True)
class DiscoveryBlueprint:
    """Represents a curated discovery lane configuration."""

    id: str
    label: str
    description: str
    movie_prompt: str | None = None
    series_prompt: str | None = None
    priority: int = 0

    def supports(self, content_type: str) -> bool:
        """Return whether the blueprint can produce catalogs for the type."""

        if content_type == "movie":
            return self.movie_prompt is not None
        if content_type == "series":
            return self.series_prompt is not None
        return False

    def prompt_for(self, content_type: str) -> str | None:
        """Return the instruction text for the requested content type."""

        if content_type == "movie":
            return self.movie_prompt
        if content_type == "series":
            return self.series_prompt
        return None


DISCOVERY_BLUEPRINTS: Mapping[str, DiscoveryBlueprint] = {
    blueprint.id: blueprint
    for blueprint in [
        DiscoveryBlueprint(
            id="streaming-launchpad",
            label="Streaming launchpad",
            description=(
                "Brand-new streaming exclusives and day-and-date drops arriving right now"
                " so viewers can press play the moment they open the app."
            ),
            movie_prompt=(
                "Surface feature-length streaming premieres released in the past eight weeks,"
                " focusing on originals or day-and-date debuts that just hit major platforms"
                " in the viewer's region. Skip library pickups or titles that have circulated"
                " for months."
            ),
            series_prompt=(
                "Highlight streaming-exclusive series with seasons launching in the last eight"
                " weeks, prioritising originals debuting all episodes or rolling out now."
                " Avoid long-running back catalogue arrivals."
            ),
            priority=110,
        ),
        DiscoveryBlueprint(
            id="fresh-premieres",
            label="Fresh premieres",
            description=(
                "Festival darlings and prestige premieres that landed on streaming or digital"
                " platforms within the last twelve months."
            ),
            movie_prompt=(
                "Spotlight films that premiered in the past 12 months (festival debuts count"
                " if they're just hitting VOD or streaming). Prioritise critically approved"
                " releases that haven't had time to become catalog staples."
            ),
            series_prompt=(
                "Feature series that launched or dropped a new season within the past six"
                " months, favouring titles that only recently became bingeable on major"
                " services."
            ),
            priority=90,
        ),
        DiscoveryBlueprint(
            id="award-season-radar",
            label="Award season radar",
            description=(
                "Buzz-heavy contenders from the current awards cycle that only just became"
                " easy to watch at home."
            ),
            movie_prompt=(
                "Curate films from the ongoing awards season (released within the past nine"
                " months) earning major nominations or critics' prizes and newly available"
                " to rent or stream."
            ),
            series_prompt=(
                "Select limited series or prestige seasons premiering in this awards window"
                " (within nine months) that critics celebrate yet remain fresh to general"
                " audiences."
            ),
            priority=95,
        ),
        DiscoveryBlueprint(
            id="festival-circuit",
            label="Festival circuit heat",
            description=(
                "Award-courting films fresh from Sundance, TIFF, Cannes, or Berlinale now"
                " rolling into general release."
            ),
            movie_prompt=(
                "Curate recent festival standouts (last 18 months) that either secured awards"
                " or exceptional buzz and only just became available to rent or stream."
            ),
            priority=85,
        ),
        DiscoveryBlueprint(
            id="sleeper-streamer-hits",
            label="Sleeper streamer hits",
            description=(
                "Under-the-radar originals that debuted recently and are building organic"
                " buzz despite limited promotion."
            ),
            movie_prompt=(
                "Identify streamer-original films released in the past six months with strong"
                " audience chatter or critical praise yet low mainstream awareness, ensuring"
                " they only just landed on the platform."
            ),
            series_prompt=(
                "Pick new streamer-original series (last six months) seeing high completion"
                " or social buzz inside fandoms but little top-list exposure; focus on first"
                " seasons or soft reboots."
            ),
            priority=88,
        ),
        DiscoveryBlueprint(
            id="global-breakouts",
            label="Global breakouts",
            description=(
                "International sensations and under-the-radar imports drawing fresh chatter"
                " among enthusiasts."
            ),
            movie_prompt=(
                "Select international films (released within ~24 months) building momentum"
                " with Western critics or fan communities but unlikely to be in their history."
            ),
            series_prompt=(
                "Highlight new or returning series from outside their primary region that"
                " critics and global fandoms are buzzing about this year."
            ),
            priority=80,
        ),
        DiscoveryBlueprint(
            id="future-cult-classics",
            label="Future cult classics",
            description=(
                "Bold storytelling swings from the past two years destined to become cult"
                " favourites."
            ),
            movie_prompt=(
                "Pick daring genre or auteur-driven films from the last two years that built"
                " passionate early audiences yet remain largely undiscovered."
            ),
            series_prompt=(
                "Serve up ambitious serialized stories (2023 onward) gaining strong word of"
                " mouth among superfans but still early in their cultural ascent."
            ),
            priority=70,
        ),
        DiscoveryBlueprint(
            id="breakout-debut-filmmakers",
            label="Breakout debut filmmakers",
            description=(
                "First or second features from emerging directors that critics say you should"
                " catch early."
            ),
            movie_prompt=(
                "Spotlight first or second narrative features released in the past 18 months"
                " whose directors are being hailed as new voices, and which only recently"
                " arrived on VOD or streaming."
            ),
            priority=78,
        ),
        DiscoveryBlueprint(
            id="documentary-pulse",
            label="Documentary pulse",
            description=(
                "Edge-of-your-seat true stories premiering recently across film festivals and"
                " prestige streamers."
            ),
            movie_prompt=(
                "Gather cinematic documentaries released in the past 18 months that deliver"
                " gripping narratives, investigative scoops, or cultural revelations."
            ),
            series_prompt=(
                "Spotlight limited docu-series or anthology seasons debuting in the last year"
                " with buzzy journalism or true-crime hooks."
            ),
            priority=65,
        ),
        DiscoveryBlueprint(
            id="family-premiere-night",
            label="Family premiere night",
            description=(
                "All-ages crowd-pleasers that just premiered so households can discover them"
                " together for the first time."
            ),
            movie_prompt=(
                "Select new family-friendly films (last 12 months) rated PG or PG-13 that"
                " just debuted on streaming, prioritising four-quadrant adventures and"
                " animated standouts that haven't saturated the zeitgeist yet."
            ),
            series_prompt=(
                "Highlight new seasons or debuts of family and YA series from the past six"
                " months that are fresh arrivals on major platforms and easy to start from"
                " episode one."
            ),
            priority=62,
        ),
        DiscoveryBlueprint(
            id="animation-vanguard",
            label="Animation vanguard",
            description=(
                "Inventive animated storytelling from worldwide studios making noise right"
                " now."
            ),
            movie_prompt=(
                "Recommend animated films from the past two release cycles pushing visual or"
                " narrative boundaries and only recently landing on home platforms."
            ),
            series_prompt=(
                "Curate new-season or freshly launched animated series for adults and older"
                " teens that critics are celebrating in the current cycle."
            ),
            priority=55,
        ),
        DiscoveryBlueprint(
            id="genre-heatwave",
            label="Genre heatwave",
            description=(
                "High-energy thrillers, horror, and speculative adventures lighting up feeds"
                " over the last year."
            ),
            movie_prompt=(
                "Deliver propulsive thrillers, horror, or sci-fi releases from the past 18"
                " months that genre tastemakers champion yet remain fresh to the viewer."
            ),
            series_prompt=(
                "Surface bingeable genre series (new launches or seasons since 2023) that fan"
                " communities rave about but aren't mainstream staples yet."
            ),
            priority=50,
        ),
        DiscoveryBlueprint(
            id="true-story-sagas",
            label="True-story sagas",
            description=(
                "Recent fact-based dramas and limited series exploring gripping real events."
            ),
            movie_prompt=(
                "Find dramatic features from the past two years rooted in true events that"
                " critics praise for fresh perspective or daring execution."
            ),
            series_prompt=(
                "Recommend limited or returning series (past 18 months) dramatizing real"
                " figures or incidents with strong reviews yet limited saturation."
            ),
            priority=45,
        ),
        DiscoveryBlueprint(
            id="fresh-season-rush",
            label="Fresh season rush",
            description=(
                "Brand-new seasons or debuts exploding across conversation this quarter."
            ),
            series_prompt=(
                "Zero in on series with seasons premiering in the last 3-4 months whose buzz"
                " is still climbing, ensuring at least half are brand-new debuts."
            ),
            priority=75,
        ),
        DiscoveryBlueprint(
            id="short-binge-series",
            label="Weekend limited binges",
            description=(
                "Tight limited series or mini-seasons that can be finished in a weekend and"
                " premiered recently."
            ),
            series_prompt=(
                "Select newly released (past 12 months) limited or mini-series totalling 10"
                " episodes or fewer, perfect for a single-weekend dive."
            ),
            priority=60,
        ),
        DiscoveryBlueprint(
            id="weekly-warmers",
            label="Weekly warmers",
            description=(
                "Easy-going network and streamer comedies or dramas that premiered this"
                " season and make a breezy weeknight watch."
            ),
            series_prompt=(
                "Highlight feel-good or procedural-leaning shows that premiered in the last"
                " year, emphasising fresh comfort watches the viewer hasn't sampled."
            ),
            priority=40,
        ),
        DiscoveryBlueprint(
            id="sci-fi-frontier",
            label="Sci-fi frontier",
            description=(
                "Cutting-edge speculative stories from the past year pushing daring futuristic"
                " ideas into the spotlight."
            ),
            movie_prompt=(
                "Curate science-fiction films from the last 12 months exploring bold concepts"
                " or technology, favouring fresh festival-to-streaming arrivals over legacy"
                " franchise sequels."
            ),
            series_prompt=(
                "Feature serialized sci-fi shows with seasons premiering since 2023 that"
                " lean into ambitious world-building and only recently hit streaming."
            ),
            priority=52,
        ),
    ]
}


DEFAULT_MOVIE_BLUEPRINTS: Sequence[str] = (
    "streaming-launchpad",
    "fresh-premieres",
    "award-season-radar",
    "global-breakouts",
    "sleeper-streamer-hits",
    "future-cult-classics",
    "festival-circuit",
    "documentary-pulse",
    "genre-heatwave",
    "animation-vanguard",
    "true-story-sagas",
    "breakout-debut-filmmakers",
    "family-premiere-night",
    "sci-fi-frontier",
)

DEFAULT_SERIES_BLUEPRINTS: Sequence[str] = (
    "streaming-launchpad",
    "fresh-season-rush",
    "award-season-radar",
    "global-breakouts",
    "sleeper-streamer-hits",
    "future-cult-classics",
    "genre-heatwave",
    "documentary-pulse",
    "short-binge-series",
    "weekly-warmers",
    "family-premiere-night",
    "fresh-premieres",
    "true-story-sagas",
    "animation-vanguard",
    "sci-fi-frontier",
)


def get_blueprint(blueprint_id: str) -> DiscoveryBlueprint | None:
    """Return the blueprint if it exists."""

    key = slugify(blueprint_id)
    if not key:
        return None
    return DISCOVERY_BLUEPRINTS.get(key)


def blueprint_options(content_type: str) -> list[DiscoveryBlueprint]:
    """Return blueprints supporting the requested content type sorted by priority."""

    return sorted(
        (blueprint for blueprint in DISCOVERY_BLUEPRINTS.values() if blueprint.supports(content_type)),
        key=lambda entry: entry.priority,
        reverse=True,
    )


def blueprint_options_payload(content_type: str) -> list[dict[str, str]]:
    """Return a serialisable blueprint catalog for UI payloads."""

    payload: list[dict[str, str]] = []
    for blueprint in blueprint_options(content_type):
        payload.append(
            {
                "id": blueprint.id,
                "label": blueprint.label,
                "description": blueprint.description,
            }
        )
    return payload


def default_blueprint_ids(content_type: str) -> list[str]:
    """Return the default ordered selection for the content type."""

    if content_type == "movie":
        return list(DEFAULT_MOVIE_BLUEPRINTS)
    if content_type == "series":
        return list(DEFAULT_SERIES_BLUEPRINTS)
    return []


def sanitize_blueprint_selection(
    selection: Iterable[str] | None, content_type: str
) -> list[str]:
    """Normalise user provided blueprint identifiers."""

    if selection is None:
        return []
    seen: set[str] = set()
    normalised: list[str] = []
    for raw in selection:
        if not isinstance(raw, str):
            continue
        key = slugify(raw)
        if not key or key in seen:
            continue
        blueprint = DISCOVERY_BLUEPRINTS.get(key)
        if blueprint is None or not blueprint.supports(content_type):
            continue
        seen.add(key)
        normalised.append(key)
    return normalised


def selection_from_payload(
    payload: Mapping[str, object] | None, content_type: str
) -> list[str]:
    """Return a cleaned selection from a persisted JSON payload."""

    if not isinstance(payload, Mapping):
        return default_blueprint_ids(content_type)
    key = "movie" if content_type == "movie" else "series"
    if key not in payload:
        return default_blueprint_ids(content_type)
    raw_value = payload.get(key)
    if raw_value is None:
        return default_blueprint_ids(content_type)
    if isinstance(raw_value, str):
        raw_items: Sequence[str] = [part.strip() for part in raw_value.split(",")]
    elif isinstance(raw_value, Sequence):
        raw_items = [str(item) for item in raw_value]
    else:
        raw_items = []
    cleaned = sanitize_blueprint_selection(raw_items, content_type)
    if cleaned:
        return cleaned
    # Explicit empty list should be respected; fall back only when payload was malformed.
    if isinstance(raw_value, Sequence) and not raw_value:
        return []
    return default_blueprint_ids(content_type)


def build_selection_payload(
    *, movie: Sequence[str], series: Sequence[str]
) -> dict[str, list[str]]:
    """Return a serialisable payload for persistence."""

    return {
        "movie": sanitize_blueprint_selection(movie, "movie"),
        "series": sanitize_blueprint_selection(series, "series"),
    }


def summarise_blueprints(
    blueprints: Sequence[DiscoveryBlueprint], content_type: str
) -> list[dict[str, str]]:
    """Return summary payloads suitable for prompt construction."""

    summary: list[dict[str, str]] = []
    for blueprint in blueprints:
        prompt = blueprint.prompt_for(content_type)
        if not prompt:
            continue
        summary.append(
            {
                "id": blueprint.id,
                "label": blueprint.label,
                "description": blueprint.description,
                "prompt": prompt,
            }
        )
    return summary
