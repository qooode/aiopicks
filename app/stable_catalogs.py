"""Stable catalog lane definitions for AI generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


ContentType = Literal["movie", "series"]


@dataclass(frozen=True)
class StableCatalogDefinition:
    """Describes a fixed catalog lane shown in Stremio."""

    key: str
    title: str
    description: str
    content_type: ContentType


STABLE_CATALOGS: tuple[StableCatalogDefinition, ...] = (
    StableCatalogDefinition(
        key="movies-for-you",
        title="Movies For You",
        description=(
            "Primary personalised movie lane scoring unseen films against your genre affinity,"
            " favourite talent, and active streaks to surface the highest-propensity"
            " recommendations."
        ),
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="series-for-you",
        title="Series For You",
        description=(
            "Primary personalised series lane ranking in-season runs and upcoming debuts by how"
            " closely they match your binge cadence, preferred talent, and franchise momentum."
        ),
        content_type="series",
    ),
    StableCatalogDefinition(
        key="because-you-watched",
        title="Because You Watched",
        description="Similar series to your recent watches, extending the moods you just binged.",
        content_type="series",
    ),
    StableCatalogDefinition(
        key="top-genre-picks",
        title="Your Top Genre Picks",
        description="Fresh films expanding on the genres you play most—thrillers, comedies, and more.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="actors-you-love",
        title="Actors You Love",
        description="Movies headlined by the performers you return to again and again.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="directors-you-return-to",
        title="Directors You Return To",
        description="Films from directors already in your rotation, including acclaimed deep cuts.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="franchises-you-started",
        title="Franchises You Started",
        description="Series sequels, prequels, and spin-offs tied to universes you've begun but not finished.",
        content_type="series",
    ),
    StableCatalogDefinition(
        key="hidden-gems",
        title="Hidden Gems (Last 5 Years)",
        description="Critically praised films from the past five years that align with your taste yet slipped by.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="classics-you-missed",
        title="Classics You Missed",
        description="70s–90s films that fit your profile but never made it into your history.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="best-of-last-decade",
        title="Best of the Last Decade",
        description="Standout 2010s films matching your vibe and still waiting in your queue.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="seasonal-picks",
        title="Seasonal Picks for You",
        description="Rotating films for the current season—holiday comfort, Halloween chills, or summer heat.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="international-favorites",
        title="International Favorites",
        description="Foreign films in your preferred genres that global fans rave about.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="cult-classics",
        title="Cult Classics in Your Taste",
        description="Famous cult films that match your sensibilities but never hit your watch history.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="indie-discoveries",
        title="Indie Discoveries",
        description="Independent films that mirror your taste with daring storytelling and strong buzz.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="mini-series-matches",
        title="Mini-Series Matches",
        description="Short, high-impact limited series tuned to your favourite tones and genres.",
        content_type="series",
    ),
    StableCatalogDefinition(
        key="docs-youll-like",
        title="Docs You’ll Like",
        description="Feature documentaries linked to the interests—crime, sports, history—you revisit often.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="animated-worlds",
        title="Animated Worlds",
        description="Animated and anime series that echo the flavours you already love.",
        content_type="series",
    ),
    StableCatalogDefinition(
        key="missed-while-binging",
        title="Missed While Binging",
        description="Films released while you were deep into other shows—worthy catch-ups for your queue.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="forgotten-favorites-expanded",
        title="Forgotten Favorites Expanded",
        description="Series related to movies or shows you adored years ago—spiritual sequels and continuations.",
        content_type="series",
    ),
)


STABLE_CATALOG_COUNT = len(STABLE_CATALOGS)
