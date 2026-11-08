"""Load Spotify Extended Streaming History into the local SongRadio database."""

import argparse
import logging
from pathlib import Path

from config_utils import keyword_parameters, load_config

logger = logging.getLogger("song_radio.history_loader")


def _find_export_directory(export_dir):
    """Return the directory containing Spotify audio history files."""
    path = Path(export_dir)
    if not path.is_dir():
        raise FileNotFoundError(f"Export directory not found: {path}")

    if any(path.glob("Streaming_History_Audio_*.json")):
        return path

    nested = path / "Spotify Extended Streaming History"
    if nested.is_dir() and any(nested.glob("Streaming_History_Audio_*.json")):
        return nested

    raise FileNotFoundError(
        f"No Streaming_History_Audio_*.json files found in {path} "
        f"or {nested}."
    )


def main(export_dir, timezone_name=None, half_life_days=365.0):
    """Parse an export and atomically replace its stored history aggregates."""
    from SongRadio import SongRadio
    from StreamingHistory import StreamingHistoryParser

    config = load_config(
        "config",
        allowed_keys=keyword_parameters(SongRadio) | {"timezone_name"},
        required_keys={"client_id", "client_secret", "redirect_uri", "lastfm_api_key"},
    )

    effective_timezone = timezone_name or config.get("timezone_name", "Europe/Berlin")
    song_radio_config = {
        key: value for key, value in config.items()
        if key != "timezone_name"
    }

    if "seed_queries" not in song_radio_config:
        song_radio_config["seed_queries"] = {}

    radio = SongRadio(**song_radio_config)
    try:
        source_dir = _find_export_directory(export_dir)
        print(f"Parsing streaming history from: {source_dir}")
        print(f"  Timezone: {effective_timezone}")
        print(f"  Recency half-life: {half_life_days:g} days")

        parser = StreamingHistoryParser(
            str(source_dir),
            half_life_days=half_life_days,
            timezone_name=effective_timezone,
        )
        stats = parser.parse()

        if not stats.artists or not stats.tracks:
            raise RuntimeError(
                "The export contains no usable music artists or tracks. "
                "The import was aborted; stored history was not replaced."
            )

        print(
            f"\nParsed {stats.total_records_processed} record(s):\n"
            f"  Music:      {stats.total_music_plays} plays, "
            f"{len(stats.artists)} artist(s), {len(stats.tracks)} track(s)\n"
            f"  Podcasts:   {stats.total_podcast_plays} plays, "
            f"{len(stats.podcast_shows)} show(s)\n"
            f"  Audiobooks: {stats.total_audiobook_plays} plays, "
            f"{len(stats.audiobooks)} title(s)\n"
            f"  Skipped/unclassifiable: {stats.skipped_malformed_records}"
        )

        print("\nReplacing stored streaming history...")
        radio.load_streaming_history(stats)

        print(
            f"Done. Stored {len(stats.artists)} artist(s) and "
            f"{len(stats.tracks)} track(s) in "
            f"{config.get('cache_path', 'spotify_search_cache.db')}."
        )
        print(
            f"Time-bucket data stored for {len(stats.artist_time_buckets)} "
            f"artist(s) using {effective_timezone}."
        )
    finally:
        radio.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import Spotify Extended Streaming History into SongRadio."
    )
    parser.add_argument(
        "export_dir",
        metavar="DIR",
        help="Export directory, or its parent directory containing the Spotify export folder.",
    )
    parser.add_argument(
        "--timezone",
        help="IANA timezone for time buckets; defaults to timezone_name in config.json or Europe/Berlin.",
    )
    parser.add_argument(
        "--half-life",
        type=float,
        default=365.0,
        metavar="DAYS",
        help="Recency-decay half-life in days (default: 365).",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable INFO-level logging.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    main(args.export_dir, timezone_name=args.timezone, half_life_days=args.half_life)