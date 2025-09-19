from app.services.trakt import TraktClient


def _history_entry(
    *,
    title: str,
    genre: str,
    language: str,
    watched_at: str = "2024-01-01T00:00:00.000Z",
) -> dict:
    return {
        "movie": {
            "title": title,
            "genres": [genre],
            "language": language,
            "runtime": 100,
            "year": 2024,
        },
        "watched_at": watched_at,
    }


def test_summarize_history_marks_fatigued_and_curiosity_signals() -> None:
    """High frequency genres are flagged as fatigued while rarer picks fuel curiosity."""

    history = [
        _history_entry(title=f"Action Pulse {idx}", genre="action", language="en")
        for idx in range(5)
    ]
    history.extend(
        [
            _history_entry(title="Night Puzzle", genre="mystery", language="ja"),
            _history_entry(title="Silent Whispers", genre="mystery", language="ja"),
            _history_entry(title="City of Clay", genre="documentary", language="es"),
        ]
    )

    summary = TraktClient.summarize_history(history, key="movie")

    assert summary["fatigued_genres"] == ["action"]
    assert summary["fatigued_languages"] == ["en"]
    # Curiosity surfaces the least-played genres and languages that still appear in history.
    assert summary["curiosity_genres"][:2] == ["documentary", "mystery"]
    assert summary["curiosity_languages"][:2] == ["es", "ja"]
    assert summary["total"] == len(history)


def test_summarize_history_handles_empty_batches() -> None:
    """Empty history returns zeroed aggregates without crashing."""

    summary = TraktClient.summarize_history([], key="movie")

    assert summary["total"] == 0
    assert summary["fatigued_genres"] == []
    assert summary["curiosity_genres"] == []
