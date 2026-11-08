"""Parse Spotify Extended Streaming History exports into aggregated statistics."""

import dataclasses
import glob
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from normalization import normalize_artist_name, normalize_name, normalize_track_title

logger = logging.getLogger("song_radio.streaming_history")

TIME_SLOTS = [
    ("night", 0, 6),
    ("morning", 6, 12),
    ("afternoon", 12, 18),
    ("evening", 18, 24),
]
WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


def local_utc_offset_hours(at=None):
    """Return the local UTC offset in whole hours for a timezone-aware datetime."""
    at = at or datetime.now(timezone.utc)
    if at.tzinfo is None or at.utcoffset() is None:
        raise ValueError("at must be timezone-aware")

    offset = at.astimezone().utcoffset()
    if offset is None:
        raise RuntimeError("Could not determine the local UTC offset")
    return round(offset.total_seconds() / 3600)


@dataclasses.dataclass
class TimeBuckets:
    """Recency-weighted listening totals by time slot and weekday."""

    by_slot: dict = dataclasses.field(default_factory=dict)
    by_weekday: dict = dataclasses.field(default_factory=dict)
    by_slot_weekday: dict = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        if not self.by_slot:
            self.by_slot = {slot[0]: 0.0 for slot in TIME_SLOTS}
        if not self.by_weekday:
            self.by_weekday = {day: 0.0 for day in WEEKDAYS}
        if not self.by_slot_weekday:
            self.by_slot_weekday = {
                (slot[0], day): 0.0 for slot in TIME_SLOTS for day in WEEKDAYS
            }


@dataclasses.dataclass
class EntityStats:
    """Aggregated listening statistics for one artist, track, show, or audiobook."""

    play_count: int = 0
    skip_count: int = 0
    total_ms_played: int = 0
    weighted_score: float = 0.0
    last_played_ts: Optional[str] = None


@dataclasses.dataclass
class StreamingHistoryStats:
    """Aggregated statistics produced by StreamingHistoryParser."""

    artists: dict
    tracks: dict
    podcast_shows: dict
    audiobooks: dict
    total_records_processed: int = 0
    total_music_plays: int = 0
    total_podcast_plays: int = 0
    total_audiobook_plays: int = 0
    skipped_malformed_records: int = 0
    half_life_days: float = 365.0
    parsed_at: float = 0.0
    artist_display_names: dict = dataclasses.field(default_factory=dict)
    track_display_names: dict = dataclasses.field(default_factory=dict)
    artist_time_buckets: dict = dataclasses.field(default_factory=dict)
    overall_time_buckets: TimeBuckets = dataclasses.field(default_factory=TimeBuckets)


class StreamingHistoryParser:
    """Parse Spotify audio export files into recency-weighted aggregates."""

    AUDIO_FILE_GLOB = "Streaming_History_Audio_*.json"

    def __init__(self, export_dir, half_life_days=365.0, now=None, timezone_name="Europe/Berlin"):
        """Initialize the parser with an export directory and IANA timezone."""
        if half_life_days <= 0:
            raise ValueError("half_life_days must be greater than 0")

        reference_time = now or datetime.now(timezone.utc)
        if reference_time.tzinfo is None or reference_time.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if not isinstance(timezone_name, str) or not timezone_name.strip():
            raise ValueError("timezone_name must be a non-empty IANA timezone name")

        self.export_dir = export_dir
        self.half_life_days = float(half_life_days)
        self._now = reference_time.astimezone(timezone.utc)
        self.timezone_name = timezone_name
        self._timezone = ZoneInfo(timezone_name)

    def _find_audio_files(self):
        """Return sorted paths of matching Spotify audio export files."""
        pattern = os.path.join(self.export_dir, self.AUDIO_FILE_GLOB)
        return sorted(glob.glob(pattern))

    @staticmethod
    def _parse_timestamp(ts_str):
        """Parse a Spotify timestamp, treating timestamps without an offset as UTC."""
        if not isinstance(ts_str, str) or not ts_str.strip():
            return None

        value = ts_str.strip()
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None

        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _decay_weight(self, ts_str):
        """Return the recency-decay weight, or zero for an invalid/future timestamp."""
        played_at = self._parse_timestamp(ts_str)
        if played_at is None:
            return 0.0

        age_days = (self._now - played_at).total_seconds() / 86400
        if age_days < 0:
            logger.warning("Ignoring future streaming-history timestamp: %s", ts_str)
            return 0.0
        return 0.5 ** (age_days / self.half_life_days)

    def _slot_and_weekday(self, ts_str):
        """Return local time-slot and weekday labels for a UTC timestamp."""
        played_at = self._parse_timestamp(ts_str)
        if played_at is None:
            return None, None

        local_time = played_at.astimezone(self._timezone)
        slot = next(
            (label for label, start, end in TIME_SLOTS if start <= local_time.hour < end),
            None,
        )
        return slot, WEEKDAYS[local_time.weekday()]

    @staticmethod
    def _classify_record(record):
        """Return the record type, or None if it is not a supported type."""
        if record.get("master_metadata_track_name"):
            return "music"
        if record.get("episode_name"):
            return "podcast"
        if record.get("audiobook_title"):
            return "audiobook"
        return None

    @staticmethod
    def _safe_ms_played(record):
        """Return ms_played as a nonnegative integer."""
        value = record.get("ms_played", 0)
        if isinstance(value, bool):
            return 0
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError, OverflowError):
            return 0

    @classmethod
    def _accumulate(cls, stats_dict, key, record, weight):
        """Add one play to an aggregate entry."""
        entry = stats_dict.setdefault(key, EntityStats())
        entry.play_count += 1
        if record.get("skipped") is True:
            entry.skip_count += 1
        entry.total_ms_played += cls._safe_ms_played(record)
        entry.weighted_score += weight

        timestamp = record.get("ts")
        parsed = cls._parse_timestamp(timestamp)
        previous = cls._parse_timestamp(entry.last_played_ts)
        if parsed is not None and (previous is None or parsed > previous):
            entry.last_played_ts = timestamp

    @staticmethod
    def _add_bucket(buckets, slot, weekday, weight):
        """Add a play's weight to slot, weekday, and combined buckets."""
        buckets.by_slot[slot] = buckets.by_slot.get(slot, 0.0) + weight
        buckets.by_weekday[weekday] = buckets.by_weekday.get(weekday, 0.0) + weight
        pair = (slot, weekday)
        buckets.by_slot_weekday[pair] = buckets.by_slot_weekday.get(pair, 0.0) + weight

    def parse(self):
        """Parse matching export files and return aggregated statistics.

        Raises:
            FileNotFoundError: If the export directory or matching files are missing.
        """
        if not os.path.isdir(self.export_dir):
            raise FileNotFoundError(f"Export directory not found: {self.export_dir}")

        audio_files = self._find_audio_files()
        if not audio_files:
            raise FileNotFoundError(
                f"No files matching '{self.AUDIO_FILE_GLOB}' found in {self.export_dir}"
            )

        artists, tracks, podcast_shows, audiobooks = {}, {}, {}, {}
        artist_display_names, track_display_names = {}, {}
        artist_time_buckets = {}
        overall_time_buckets = TimeBuckets()
        total_records = total_music = total_podcast = total_audiobook = skipped_malformed = 0

        for file_path in audio_files:
            try:
                with open(file_path, "r", encoding="utf-8") as file:
                    records = json.load(file)
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"Could not read streaming-history file {file_path}: {exc}. "
                    "The import was aborted; stored history was not replaced."
                ) from exc

            if not isinstance(records, list):
                raise RuntimeError(
                    f"Invalid streaming-history file {file_path}: "
                    f"expected a JSON array, got {type(records).__name__}. "
                    "The import was aborted; stored history was not replaced."
                )

            for record in records:
                total_records += 1
                if not isinstance(record, dict):
                    skipped_malformed += 1
                    continue

                kind = self._classify_record(record)
                if kind is None:
                    skipped_malformed += 1
                    continue

                timestamp = record.get("ts")
                weight = self._decay_weight(timestamp)

                if kind == "music":
                    total_music += 1
                    artist_name = record.get("master_metadata_album_artist_name")
                    track_name = record.get("master_metadata_track_name")
                    if not isinstance(artist_name, str) or not artist_name.strip():
                        continue

                    artist_name = artist_name.strip()
                    artist_key = normalize_artist_name(artist_name)
                    if not artist_key:
                        continue

                    artist_display_names.setdefault(artist_key, artist_name)
                    self._accumulate(artists, artist_key, record, weight)

                    if isinstance(track_name, str) and track_name.strip():
                        track_name = track_name.strip()
                        track_key = (artist_key, normalize_track_title(track_name))
                        if track_key[1]:
                            track_display_names.setdefault(track_key, track_name)
                            self._accumulate(tracks, track_key, record, weight)

                    slot, weekday = self._slot_and_weekday(timestamp)
                    if slot is not None:
                        buckets = artist_time_buckets.setdefault(artist_key, TimeBuckets())
                        self._add_bucket(buckets, slot, weekday, weight)
                        self._add_bucket(overall_time_buckets, slot, weekday, weight)

                elif kind == "podcast":
                    total_podcast += 1
                    show_name = record.get("episode_show_name")
                    if isinstance(show_name, str) and show_name.strip():
                        self._accumulate(
                            podcast_shows, normalize_name(show_name), record, weight
                        )

                elif kind == "audiobook":
                    total_audiobook += 1
                    title = record.get("audiobook_title")
                    if isinstance(title, str) and title.strip():
                        self._accumulate(audiobooks, normalize_name(title), record, weight)

            del records

        logger.info(
            "Parsed %d records from %d file(s): %d music, %d podcasts, %d audiobooks, %d skipped.",
            total_records, len(audio_files), total_music, total_podcast, total_audiobook, skipped_malformed,
        )

        return StreamingHistoryStats(
            artists=artists,
            tracks=tracks,
            podcast_shows=podcast_shows,
            audiobooks=audiobooks,
            total_records_processed=total_records,
            total_music_plays=total_music,
            total_podcast_plays=total_podcast,
            total_audiobook_plays=total_audiobook,
            skipped_malformed_records=skipped_malformed,
            half_life_days=self.half_life_days,
            parsed_at=time.time(),
            artist_display_names=artist_display_names,
            track_display_names=track_display_names,
            artist_time_buckets=artist_time_buckets,
            overall_time_buckets=overall_time_buckets,
        )