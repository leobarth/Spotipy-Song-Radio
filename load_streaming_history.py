"""
load_streaming_history.py
=========================

Parses a Spotify Extended Streaming History export folder and loads the
resulting per-artist/per-track/per-show/per-audiobook statistics into the
local SongRadio SQLite cache (the same database used by generate_song_radio.py
and generate_daylist.py, configured via cache_path in config.json).

Run this once per new export - not on every SongRadio or Daylist run.
A new export always fully replaces whatever was stored before (a Spotify
history export is always cumulative, not a delta).

How to get your export
----------------------
1. Open Spotify → Settings → Privacy → "Download your data".
2. Check "Extended streaming history" (NOT just "Account data" - that
   only covers the last ~12 months and has no skip/reason fields).
3. Wait up to ~30 days (usually a few days to 2 weeks) for the email.
4. Unzip the downloaded archive.
5. Run:  python load_streaming_history.py /path/to/unzipped/folder

What is stored
--------------
Only aggregated statistics survive into the database - the raw per-stream
records (individual play events, IP addresses, etc.) are never written to
disk by this script. What ends up in the database:

  streaming_history_artists   one row per artist:
      play_count, skip_count, total_ms_played, weighted_score,
      last_played_ts, loaded_at

  streaming_history_tracks    one row per (artist, track) pair:
      same columns as above

  streaming_history_podcasts  one row per podcast show name:
      same columns (kept for future use, not used by Daylist currently)

  streaming_history_audiobooks  one row per audiobook title:
      same columns (kept for future use)

  streaming_history_artist_buckets  one row per (artist, slot, weekday):
      slot_score, weekday_score - used by Daylist to pick seeds that
      match the current time of day and day of week

UTC offset and recency weighting
---------------------------------
  --utc-offset HOURS   Integer offset from UTC for local-time
                       slot/weekday bucketing. Use 1 for CET (German
                       winter time) and 2 for CEST (German summer time,
                       i.e. after the clocks go forward in late March).
                       Only affects which time-of-day bucket a play is
                       assigned to, not the recency weights.
                       Default: auto-detected from the system timezone
                       (DST-aware via zoneinfo / dateutil / datetime
                       fallback chain). Override only if the script runs
                       on a machine in a different timezone than where
                       you actually listen to music.

  --half-life DAYS     Recency decay half-life in days. A play this many
                       days ago contributes half the weighted_score of a
                       play today. Default: 365 (one year). With ~6 years
                       of history this means the oldest plays contribute
                       roughly 1.5% of a fresh play's weight.
"""

import argparse
import json
import logging
import os


def load_config(name="config"):
    """Loads <name>.json, falling back to <name>_template.json.

    Args:
        name: Config file stem (without extension).

    Returns:
        dict.

    Raises:
        RuntimeError: if neither file is found or contains invalid JSON.
    """
    for filename in (f"{name}.json", f"{name}_template.json"):
        try:
            with open(filename, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            continue
        except json.JSONDecodeError as e:
            raise RuntimeError(f"{filename} exists but is not valid JSON: {e}")
    raise RuntimeError(
        f"Could not find '{name}.json' or '{name}_template.json' in this directory."
    )


def main(export_dir, utc_offset_hours=None, half_life_days=365.0):
    """Parses the export and loads it into the SongRadio cache.

    Args:
        export_dir: Path to the unzipped Spotify export folder.
        utc_offset_hours: Integer UTC offset for local-time bucketing,
            or None to auto-detect from the system timezone.
        half_life_days: Recency decay half-life in days.

    Returns:
        None.
    """
    from StreamingHistory import StreamingHistoryParser, local_utc_offset_hours
    from SongRadio import SongRadio

    config = load_config("config")
    radio = SongRadio(**config)

    try:
        # Resolve offset before constructing the parser so we can print
        # the actual value that will be used (auto-detected or explicit).
        effective_offset = (
            utc_offset_hours if utc_offset_hours is not None
            else local_utc_offset_hours()
        )
        print(f"Parsing streaming history from: {export_dir}")
        print(
            f"  UTC offset: {effective_offset:+d}h "
            f"({'explicit' if utc_offset_hours is not None else 'auto-detected'})  "
            f"|  Recency half-life: {half_life_days:.0f} days"
        )
        parser = StreamingHistoryParser(
            os.path.join(export_dir, "Spotify Extended Streaming History"),
            half_life_days=half_life_days,
            utc_offset_hours=effective_offset,
        )
        stats = parser.parse()

        print(
            f"\nParsed {stats.total_records_processed} record(s):\n"
            f"  Music:      {stats.total_music_plays} plays, "
            f"{len(stats.artists)} unique artist(s), "
            f"{len(stats.tracks)} unique track(s)\n"
            f"  Podcasts:   {stats.total_podcast_plays} plays, "
            f"{len(stats.podcast_shows)} show(s)\n"
            f"  Audiobooks: {stats.total_audiobook_plays} plays, "
            f"{len(stats.audiobooks)} title(s)\n"
            f"  Skipped/unclassifiable: {stats.skipped_malformed_records}"
        )

        print("\nLoading into cache...")
        radio.load_streaming_history(stats)

        print(
            f"Done. {len(stats.artists)} artist(s) and "
            f"{len(stats.tracks)} track(s) persisted to "
            f"{config.get('cache_path', 'spotify_search_cache.db')}."
        )
        if len(stats.artist_time_buckets) > 0:
            print(
                f"Time-bucket data persisted for {len(stats.artist_time_buckets)} artist(s) "
                f"(used by Daylist for time-aware seed selection)."
            )
    finally:
        radio.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Parse a Spotify Extended Streaming History export and load it "
            "into the SongRadio/Daylist local cache."
        )
    )
    parser.add_argument(
        "export_dir",
        metavar="DIR",
        help=(
            "Path to your unzipped Spotify Extended Streaming History export folder "
            "(the one containing Streaming_History_Audio_*.json files)."
        ),
    )
    parser.add_argument(
        "--utc-offset",
        metavar="HOURS",
        type=int,
        default=None,
        help=(
            "Integer UTC offset for local-time slot/weekday bucketing "
            "(e.g. 1 for CET, 2 for CEST). Default: auto-detected from "
            "the system timezone (DST-aware). Override only if the server "
            "runs in a different timezone than your listening history."
        ),
    )
    parser.add_argument(
        "--half-life",
        metavar="DAYS",
        type=float,
        default=365.0,
        help=(
            "Recency decay half-life in days. A play this many days ago contributes "
            "half the score of a play today. Default: 365."
        ),
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show INFO-level log output.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    main(
        export_dir=args.export_dir,
        utc_offset_hours=args.utc_offset,
        half_life_days=args.half_life,
    )