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
        description="Movies that represent your overall taste profile across favourite genres and moods.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="series-for-you",
        title="Series For You",
        description="Series that represent your overall taste profile across favourite genres and moods.",
        content_type="series",
    ),
    StableCatalogDefinition(
        key="your-comfort-zone",
        title="Your Comfort Zone",
        description="Safe film picks that align perfectly with the patterns you already love to revisit.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="expand-your-horizons",
        title="Expand Your Horizons",
        description="Quality films just outside your normal rotation, ready to broaden your taste without losing your vibe.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="your-next-obsession",
        title="Your Next Obsession",
        description="Binge-ready series poised to become your newest favourites based on deep taste analysis.",
        content_type="series",
    ),
    StableCatalogDefinition(
        key="you-missed-these",
        title="You Missed These",
        description="Noteworthy films that slipped past you the first time but match what you already enjoy.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="critics-love-youll-love",
        title="Critics Love, You'll Love",
        description="Critically adored movies curated to mirror your personal preferences and pacing.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="international-picks",
        title="International Picks",
        description="Foreign films matched to your favourite genres and storytelling moods.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="your-guilty-pleasures-extended",
        title="Your Guilty Pleasures Extended",
        description="More of the indulgent movies you watch on repeat—even if you never mention them.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="starring-your-favorite-actors",
        title="Starring Your Favorite Actors",
        description="Films featuring the actors who dominate your watch history and never disappoint.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="visually-stunning-for-you",
        title="Visually Stunning For You",
        description="Cinematography showcases that match your genre preferences and appetite for lush visuals.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="background-watching",
        title="Background Watching",
        description="Easy-flowing series perfect for multitasking without losing the narrative thread.",
        content_type="series",
    ),
    StableCatalogDefinition(
        key="same-universe-different-story",
        title="Same Universe, Different Story",
        description="Spin-offs and related series expanding the franchises you already follow.",
        content_type="series",
    ),
    StableCatalogDefinition(
        key="animation-worth-your-time",
        title="Animation Worth Your Time",
        description="Animated series that transcend age brackets while still fitting your preferred tones.",
        content_type="series",
    ),
    StableCatalogDefinition(
        key="documentaries-youll-like",
        title="Documentaries You’ll Like",
        description="Feature documentaries tied to the crime, sports, and history stories you revisit often.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="your-top-genre",
        title="Your Top Genre",
        description="Essential films from the genre you stream most, dialled in to your signature moods.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="your-second-genre",
        title="Your Second Genre",
        description="Series highlights from the runner-up genre in your history, tuned to familiar beats.",
        content_type="series",
    ),
    StableCatalogDefinition(
        key="your-third-genre",
        title="Your Third Genre",
        description="Hand-picked films exploring the third pillar of your taste profile with fresh twists.",
        content_type="movie",
    ),
    StableCatalogDefinition(
        key="franchises-you-started",
        title="Franchises You Started",
        description="Series sequels, prequels, and spin-offs tied to universes you've begun but not finished.",
        content_type="series",
    ),
    StableCatalogDefinition(
        key="independent-films",
        title="Independent Films That Mirror Your Taste",
        description="Indie standouts with daring storytelling and strong buzz that align with your preferences.",
        content_type="movie",
    ),
)


STABLE_CATALOG_COUNT = len(STABLE_CATALOGS)
