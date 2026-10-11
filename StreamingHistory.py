"""
StreamingHistory
================

Standalone parser/extractor for Spotify's "Extended Streaming History"
data export (requested via the user's Spotify Account Privacy page,
"Download your data" -> "Extended streaming history"). Deliberately has
no dependency on SongRadio.py (or vice versa) - this module's only job is
to turn a folder of exported JSON files into a small, clean statistics
object; SongRadio.py is responsible for deciding what to do with that
object (currently just persisting it - see SongRadio.load_streaming_history).

File naming / scope
--------------------
Only files matching "Streaming_History_Audio_*.json" are read. This glob
is intentionally loose (prefix-only, no strict year/suffix pattern)
rather than hardcoding an exact "Streaming_History_Audio_<year>.json" or
"..._<year>_<index>.json" shape, since the precise suffix convention can
vary between export vintages and account histories (e.g. multiple
numbered parts for a heavy-listening year). "Streaming_History_Video_*
.json" files are never read, by design - video streaming history is out
of scope here.

Each JSON file is itself a flat array of records. A single record can
describe a music track play, a podcast episode play, or an audiobook
chapter play - these are NOT split across different files in the
"Audio" export; instead, each record is classified by which group of
metadata fields is populated (see _classify_record). All three kinds are
processed, matching the fields documented in Spotify's own
ReadMeFirst_ExtendedStreamingHistory.pdf, with one deliberate exception:
IP addresses and user-agent strings are never read into the output
statistics, even though they're present in the source files - nothing in
this module's job needs them, and there's no reason to carry that kind
of sensitive data any further than it has to.

Timestamp formats: both the ISO-8601-with-"Z" form seen in current
exports (e.g. "2020-10-07T05:07:18Z") and the space-separated form shown
in Spotify's own field documentation ("YYYY-MM-DD HH:MM:SS") are
accepted, tolerating either vintage.

Recency weighting
------------------
A multi-year listening history makes "how many times have I played this"
alone a poor signal on its own - a phase you were in three years ago
shouldn't count the same as what you're listening to now. Every record
therefore also contributes a recency-decayed weight (exponential decay,
configurable half-life - see StreamingHistoryParser.half_life_days)
toward each entity's `weighted_score`, on top of the plain counts.

Memory / privacy hygiene
-------------------------
StreamingHistoryParser.parse() processes one export file at a time and
only ever keeps the running per-artist/per-track/per-show/per-audiobook
aggregates across files - a given file's raw record list is eligible for
garbage collection as soon as the next file starts, and nothing from the
raw per-stream data (track-by-track play history, IP addresses, etc.)
survives in the returned StreamingHistoryStats or on this instance after
parse() returns. The original export files on disk are of course
untouched - only this module's own in-memory representation is
discarded, so re-parsing with different settings (e.g. a different
half_life_days, or an extended aggregation added later) is always
possible by simply calling parse() again.
"""

import dataclasses
import glob
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("song_radio.streaming_history")


def local_utc_offset_hours(at=None):
    """Returns the current UTC offset of the local system timezone, in hours.

    Used so callers don't have to hardcode +1/+2 in their configs and
    manually update it twice a year when DST changes.

    Tries, in order:
      1. zoneinfo (stdlib since Python 3.9) + the system's local zone
         as reported by `zoneinfo.ZoneInfo("localtime")` on Unix or
         the Windows registry equivalent via `tzlocal` if available.
      2. dateutil.tz.tzlocal (pip install python-dateutil) as a fallback
         for environments where zoneinfo's tzdata isn't available.
      3. datetime.astimezone() with no argument, which asks the OS
         directly - works on most platforms, no extra packages needed.
      4. Hard zero (UTC) if everything else fails, with a logged warning.

    Args:
        at: datetime to evaluate the offset at, timezone-aware.
            Defaults to now (UTC). Matters for DST correctness: the
            offset on a winter day differs from a summer day.

    Returns:
        Integer UTC offset (e.g. 1 for CET, 2 for CEST). Fractional
        offsets (e.g. India +5:30) are rounded to the nearest integer -
        sufficient for the coarse slot/weekday bucketing done here.
    """
    if at is None:
        at = datetime.now(timezone.utc)
    try:
        # stdlib path - works without any extra packages on Python ≥ 3.9
        # with an up-to-date tzdata (Linux/macOS ship this; Windows
        # needs `pip install tzdata` or the dateutil fallback below).
        import zoneinfo
        local_zone = zoneinfo.ZoneInfo("localtime")  # /etc/localtime on Unix
        local_dt = at.astimezone(local_zone)
        offset_seconds = local_dt.utcoffset().total_seconds()
        return round(offset_seconds / 3600)
    except Exception:
        pass
    try:
        # dateutil fallback - covers Windows and systems without tzdata
        from dateutil import tz
        local_dt = at.astimezone(tz.tzlocal())
        offset_seconds = local_dt.utcoffset().total_seconds()
        return round(offset_seconds / 3600)
    except Exception:
        pass
    try:
        # Last stdlib fallback: datetime.astimezone() with no arg uses
        # the OS's local timezone directly, no zoneinfo/dateutil needed.
        local_dt = at.astimezone()
        offset_seconds = local_dt.utcoffset().total_seconds()
        return round(offset_seconds / 3600)
    except Exception:
        pass
    logger.warning(
        "Could not determine local UTC offset automatically - falling back to UTC+0. "
        "Install 'tzdata' (pip install tzdata) or 'python-dateutil' to fix this."
    )
    return 0


# Tageszeit-Slots: (Label, start_hour_inclusive, end_hour_exclusive), all local-clock
# hour comparisons use the UTC hour from the "ts" field directly (Spotify exports
# timestamps in UTC) - callers that want local-time bucketing should pass a
# utc_offset_hours argument to StreamingHistoryParser (see __init__).
TIME_SLOTS = [
    ("night",     0,  6),   # 00:00 – 05:59
    ("morning",   6, 12),   # 06:00 – 11:59
    ("afternoon", 12, 18),  # 12:00 – 17:59
    ("evening",   18, 24),  # 18:00 – 23:59
]
WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


@dataclasses.dataclass
class TimeBuckets:
    """Listening-weight distribution across time slots and weekdays.

    Each cell is the sum of recency-decayed weights for all plays that
    fell into that slot/day, making it directly comparable to
    EntityStats.weighted_score. Stored per-artist (music only) and
    aggregated into StreamingHistoryStats.time_buckets, so a Daylist can
    ask "what do I typically listen to on Monday mornings?" without
    re-scanning every individual track.

    Attributes:
        by_slot: dict slot_label -> total weighted_score (keys from
            TIME_SLOTS[i][0]).
        by_weekday: dict weekday_label -> total weighted_score (keys
            from WEEKDAYS).
        by_slot_weekday: dict (slot_label, weekday_label) -> total
            weighted_score - the joint distribution, for cases where
            "Saturday night" is a meaningfully different context than
            "Monday night".
    """
    by_slot: dict = dataclasses.field(default_factory=dict)
    by_weekday: dict = dataclasses.field(default_factory=dict)
    by_slot_weekday: dict = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        if not self.by_slot:
            self.by_slot = {s[0]: 0.0 for s in TIME_SLOTS}
        if not self.by_weekday:
            self.by_weekday = {d: 0.0 for d in WEEKDAYS}
        if not self.by_slot_weekday:
            self.by_slot_weekday = {(s[0], d): 0.0 for s in TIME_SLOTS for d in WEEKDAYS}


@dataclasses.dataclass
class EntityStats:
    """Aggregated listening stats for one artist, track, podcast show, or
    audiobook.

    Attributes:
        play_count: Number of streams recorded for this entity.
        skip_count: Of those, how many had the "skipped" flag set.
        total_ms_played: Sum of ms_played across all streams (note: this
            includes very short/skipped plays, which contribute little -
            it is not the same as "time actually enjoyed").
        weighted_score: Sum of each play's recency-decayed weight (see
            StreamingHistoryParser.half_life_days) - a play from years
            ago contributes far less here than a recent one, unlike
            play_count/total_ms_played which treat every play equally
            regardless of age.
        last_played_ts: The most recent raw timestamp string seen for
            this entity (original format, not parsed) - lets a caller
            get "I haven't played this in ages" without this class
            having to make a formatting decision on their behalf.
    """
    play_count: int = 0
    skip_count: int = 0
    total_ms_played: int = 0
    weighted_score: float = 0.0
    last_played_ts: Optional[str] = None


@dataclasses.dataclass
class StreamingHistoryStats:
    """The complete, processed output of StreamingHistoryParser.parse().

    This is deliberately the *only* thing meant to outlive parsing - see
    the module docstring's "Memory / privacy hygiene" section.

    Attributes:
        artists: artist name (lowercased) -> EntityStats, music only.
        tracks: (artist name lowercased, track name lowercased/stripped)
            -> EntityStats, music only. Note: unlike SongRadio's own
            normalize_title, keys here are NOT stripped of version
            suffixes ("- Remastered 2021" etc.) - this module has no
            dependency on SongRadio.py, so a caller that wants that
            normalization applies it itself when matching these keys
            against SongRadio candidates.
        podcast_shows: show name (lowercased) -> EntityStats.
        audiobooks: audiobook title (lowercased) -> EntityStats.
        total_records_processed: Every record seen across all files,
            regardless of whether it was classifiable.
        total_music_plays / total_podcast_plays / total_audiobook_plays:
            Per-kind counts, should sum to
            (total_records_processed - skipped_malformed_records).
        skipped_malformed_records: Records that were neither valid dicts
            nor classifiable as music/podcast/audiobook (e.g. a fully
            empty entry) - counted but otherwise ignored.
        half_life_days: The decay half-life used to compute every
            weighted_score in this object (see StreamingHistoryParser).
        parsed_at: time.time() when parse() produced this object.
    """
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
    # Tageszeit/Wochentag-Verteilung.
    # artist_time_buckets: artist_name (lowercased) -> TimeBuckets,
    #   music plays only - lets Daylist ask "what artists do I play
    #   on Friday evenings?" weighted by recency, not raw counts.
    # overall_time_buckets: aggregated across all music artists -
    #   useful for "what genres dominate Saturday mornings overall?"
    artist_time_buckets: dict = dataclasses.field(default_factory=dict)
    overall_time_buckets: TimeBuckets = dataclasses.field(default_factory=TimeBuckets)


class StreamingHistoryParser:
    """Parses a Spotify Extended Streaming History export folder into
    recency-weighted per-artist/per-track/per-show/per-audiobook
    statistics. See the module docstring for the full design rationale.

    Usage:
        parser = StreamingHistoryParser("/path/to/unzipped/export")
        stats = parser.parse()
        # stats is a StreamingHistoryStats - hand it to
        # SongRadio.load_streaming_history(stats) to persist it.
    """

    AUDIO_FILE_GLOB = "Streaming_History_Audio_*.json"
    # Intentionally never read - see the module docstring.
    VIDEO_FILE_GLOB = "Streaming_History_Video_*.json"

    def __init__(self, export_dir, half_life_days=365.0, now=None, utc_offset_hours=None):
        """
        Args:
            export_dir: Path to the folder containing the unzipped
                Spotify Extended Streaming History export (where the
                Streaming_History_Audio_*.json files live, alongside
                ReadMeFirst_ExtendedStreamingHistory.pdf and friends -
                only files matching AUDIO_FILE_GLOB are ever read).
            half_life_days: Recency-decay half-life, in days - a play
                this many days ago contributes half the weighted_score
                of a play today; twice that long ago, a quarter; and so
                on. Default 365 (one year) means roughly six-year-old
                listening contributes only about 1.5% of a fresh play's
                weight (0.5 ** (6*365/365) = 0.5**6 ~= 0.016) - a
                deliberately steep falloff, since several-year-old taste
                is meant to matter very little here.
            now: Reference point for the recency decay, as a
                timezone-aware datetime. Defaults to the current time.
                Exposed mainly so tests can get deterministic weights
                without depending on wall-clock time.
            utc_offset_hours: Integer offset applied to the UTC "ts"
                timestamps when assigning them to TIME_SLOTS / WEEKDAYS
                buckets - e.g. +1 for CET, +2 for CEST. Has no effect
                on the recency-decay weights (those are always computed
                from the raw UTC timestamp). Defaults to None, in which
                case local_utc_offset_hours() is called to detect the
                system timezone automatically (including DST). Pass an
                explicit integer to override (e.g. for reproducible
                tests or when running on a server in a different
                timezone than your listening history).

        Returns:
            None.
        """
        self.export_dir = export_dir
        self.half_life_days = half_life_days
        self._now = now or datetime.now(timezone.utc)
        if utc_offset_hours is None:
            self.utc_offset_hours = local_utc_offset_hours(at=self._now)
            logger.info("StreamingHistoryParser: auto-detected UTC offset %+dh.", self.utc_offset_hours)
        else:
            self.utc_offset_hours = utc_offset_hours

    def _find_audio_files(self):
        """Lists every Streaming_History_Audio_*.json file in export_dir.

        Args:
            None.

        Returns:
            Sorted list of file paths (sorted mainly for deterministic,
            reproducible logging/iteration order - aggregation order
            doesn't otherwise affect the result).
        """
        pattern = os.path.join(self.export_dir, self.AUDIO_FILE_GLOB)
        return sorted(glob.glob(pattern))

    def _parse_timestamp(self, ts_str):
        """Parses a streaming-history timestamp, tolerating both formats
        seen across different vintages of Spotify's export (see the
        module docstring). Both are treated as UTC.

        Args:
            ts_str: The raw "ts" field from a record.

        Returns:
            A timezone-aware datetime, or None if ts_str is empty or
            couldn't be parsed in either known form.
        """
        if not ts_str:
            return None
        parsed = None
        try:
            parsed = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass
        if parsed is None:
            try:
                parsed = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                return None
        if parsed.tzinfo is None:
            # Python's fromisoformat (3.11+) also happily accepts the
            # space-separated form, just without any offset - treated as
            # UTC here either way, regardless of which of the two tries
            # above actually produced it.
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    def _decay_weight(self, ts_str):
        """Computes the recency-decay weight for a single play.

        Args:
            ts_str: The raw "ts" field from a record.

        Returns:
            Float in (0, 1] for a successfully parsed timestamp. 0.0 if
            ts_str couldn't be parsed - the play still counts toward
            play_count/total_ms_played, just not toward weighted_score,
            rather than being dropped outright over a timestamp issue.
        """
        played_at = self._parse_timestamp(ts_str)
        if played_at is None:
            return 0.0
        age_days = (self._now - played_at).total_seconds() / 86400
        age_days = max(age_days, 0.0)  # clock skew / future timestamp safety net
        return 0.5 ** (age_days / self.half_life_days)

    def _slot_and_weekday(self, ts_str):
        """Returns the (slot_label, weekday_label) for a timestamp.

        Args:
            ts_str: The raw "ts" field from a record.

        Returns:
            (slot_label, weekday_label) tuple, both str, or (None, None)
            if the timestamp could not be parsed.
        """
        played_at = self._parse_timestamp(ts_str)
        if played_at is None:
            return None, None
        local_hour = (played_at.hour + self.utc_offset_hours) % 24
        # Adjust date when offset crosses midnight
        day_offset = (played_at.hour + self.utc_offset_hours) // 24
        local_weekday = WEEKDAYS[(played_at.weekday() + day_offset) % 7]
        slot_label = None
        for label, start, end in TIME_SLOTS:
            if start <= local_hour < end:
                slot_label = label
                break
        return slot_label, local_weekday

    @staticmethod
    def _classify_record(record):
        """Determines what kind of stream one record describes, based on
        which group of metadata fields is populated (see the module
        docstring - all three kinds share the same "Audio" file schema).

        Args:
            record: One decoded JSON record (dict).

        Returns:
            "music", "podcast", "audiobook", or None if the record
            matches none of these (e.g. a fully empty/corrupted entry).
        """
        if record.get("master_metadata_track_name"):
            return "music"
        if record.get("episode_name"):
            return "podcast"
        if record.get("audiobook_title"):
            return "audiobook"
        return None

    @staticmethod
    def _accumulate(stats_dict, key, record, weight):
        """Folds one record into its EntityStats entry, creating it if
        this is the first time `key` has been seen.

        Args:
            stats_dict: The target dict (e.g. the running `artists` dict).
            key: Dict key to update (str, or (str, str) for tracks).
            record: The raw record being folded in (for ms_played/skipped/ts).
            weight: This record's recency-decay weight (see _decay_weight).

        Returns:
            None. Mutates stats_dict in place.
        """
        entry = stats_dict.get(key)
        if entry is None:
            entry = EntityStats()
            stats_dict[key] = entry
        entry.play_count += 1
        if record.get("skipped"):
            entry.skip_count += 1
        entry.total_ms_played += record.get("ms_played") or 0
        entry.weighted_score += weight
        ts = record.get("ts")
        # Plain string comparison is safe here for both timestamp formats
        # this module accepts: ISO-8601 sorts lexicographically the same
        # as chronologically, and so does the zero-padded space-separated
        # form - no need to parse just to find the latest one.
        if ts and (entry.last_played_ts is None or ts > entry.last_played_ts):
            entry.last_played_ts = ts

    def parse(self):
        """Reads and aggregates every Streaming_History_Audio_*.json file
        in export_dir into a StreamingHistoryStats.

        Files are processed one at a time; a given file's decoded record
        list is only reachable for the duration of that file's loop
        iteration (see the module docstring's "Memory / privacy hygiene"
        section) - peak memory is bounded by the single largest export
        file, not the sum of all of them, and nothing beyond the four
        aggregate dicts and the summary counters survives into the
        returned object.

        A malformed/unreadable file (bad JSON, wrong top-level type) is
        logged and skipped rather than aborting the whole run - losing
        one file out of several years of exports shouldn't lose
        everything else. Individual records that aren't dicts or don't
        match a known kind are counted in skipped_malformed_records and
        otherwise ignored.

        Args:
            None.

        Returns:
            StreamingHistoryStats.

        Raises:
            FileNotFoundError: If export_dir doesn't exist, or contains
                no files matching AUDIO_FILE_GLOB.
        """
        if not os.path.isdir(self.export_dir):
            raise FileNotFoundError(f"Export directory not found: {self.export_dir}")

        audio_files = self._find_audio_files()
        if not audio_files:
            raise FileNotFoundError(
                f"No files matching '{self.AUDIO_FILE_GLOB}' found in {self.export_dir}"
            )

        artists = {}
        tracks = {}
        podcast_shows = {}
        audiobooks = {}
        artist_time_buckets = {}   # artist_name (lowercased) -> TimeBuckets
        overall_time_buckets = TimeBuckets()
        total_records = 0
        total_music = 0
        total_podcast = 0
        total_audiobook = 0
        skipped_malformed = 0

        for file_path in audio_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    records = json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                logger.warning("Skipping unreadable export file %s: %s", file_path, e)
                continue
            if not isinstance(records, list):
                logger.warning(
                    "Skipping %s: expected a JSON array at the top level, got %s",
                    file_path, type(records).__name__,
                )
                continue

            for record in records:
                total_records += 1
                if not isinstance(record, dict):
                    skipped_malformed += 1
                    continue

                kind = self._classify_record(record)
                if kind is None:
                    skipped_malformed += 1
                    continue

                weight = self._decay_weight(record.get("ts"))

                if kind == "music":
                    total_music += 1
                    artist_name = record.get("master_metadata_album_artist_name")
                    track_name = record.get("master_metadata_track_name")
                    if artist_name:
                        self._accumulate(artists, artist_name.lower(), record, weight)
                    if artist_name and track_name:
                        track_key = (artist_name.lower(), track_name.lower().strip())
                        self._accumulate(tracks, track_key, record, weight)
                    # Time-bucket accumulation for music plays only - podcasts/
                    # audiobooks are excluded because their listening pattern
                    # doesn't carry genre/taste signal the Daylist cares about.
                    slot, weekday = self._slot_and_weekday(record.get("ts"))
                    if slot is not None and artist_name:
                        akey = artist_name.lower()
                        if akey not in artist_time_buckets:
                            artist_time_buckets[akey] = TimeBuckets()
                        artist_time_buckets[akey].by_slot[slot] = (
                            artist_time_buckets[akey].by_slot.get(slot, 0.0) + weight
                        )
                        artist_time_buckets[akey].by_weekday[weekday] = (
                            artist_time_buckets[akey].by_weekday.get(weekday, 0.0) + weight
                        )
                        pair = (slot, weekday)
                        artist_time_buckets[akey].by_slot_weekday[pair] = (
                            artist_time_buckets[akey].by_slot_weekday.get(pair, 0.0) + weight
                        )
                        overall_time_buckets.by_slot[slot] = (
                            overall_time_buckets.by_slot.get(slot, 0.0) + weight
                        )
                        overall_time_buckets.by_weekday[weekday] = (
                            overall_time_buckets.by_weekday.get(weekday, 0.0) + weight
                        )
                        overall_time_buckets.by_slot_weekday[pair] = (
                            overall_time_buckets.by_slot_weekday.get(pair, 0.0) + weight
                        )
                elif kind == "podcast":
                    total_podcast += 1
                    show_name = record.get("episode_show_name")
                    if show_name:
                        self._accumulate(podcast_shows, show_name.lower(), record, weight)
                elif kind == "audiobook":
                    total_audiobook += 1
                    title = record.get("audiobook_title")
                    if title:
                        self._accumulate(audiobooks, title.lower(), record, weight)
            # `records` goes out of scope (eligible for GC) as soon as the
            # next file's `with open(...)` reassigns it - only the four
            # aggregate dicts above persist across the loop.

        logger.info(
            "Parsed %d streaming-history record(s) from %d file(s): "
            "%d music, %d podcast, %d audiobook, %d skipped/unclassifiable.",
            total_records, len(audio_files), total_music, total_podcast,
            total_audiobook, skipped_malformed,
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
            artist_time_buckets=artist_time_buckets,
            overall_time_buckets=overall_time_buckets,
        )