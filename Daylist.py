"""Build and refresh a time-aware Spotify discovery playlist."""

import json
import logging
import os
import random
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from zoneinfo import ZoneInfo
from bisect import bisect_left, bisect_right

from normalization import normalize_artist_name
from SongRadio import SongRadio
from StreamingHistory import TIME_SLOTS, WEEKDAYS

logger = logging.getLogger("song_radio.daylist")


def _current_slot_and_weekday(timezone_name="Europe/Berlin", now=None):
    """Return the local time-slot and weekday for a timezone-aware moment."""
    zone = ZoneInfo(timezone_name)
    moment = now or datetime.now(timezone.utc)
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("now must be timezone-aware")

    local_time = moment.astimezone(zone)
    slot = next(
        (label for label, start, end in TIME_SLOTS if start <= local_time.hour < end),
        None,
    )
    return slot, WEEKDAYS[local_time.weekday()]


class Daylist:
    """Manage one persistent playlist with time-aware seeds and discovery scoring."""

    PLAYLIST_NAME = "SongRadio Daylist"

    def __init__(
        self,
        radio_kwargs,
        daylist_num_seeds=2,
        daylist_familiarity_penalty=0.3,
        daylist_seed_pool_size=20,
        timezone_name="Europe/Berlin",
        state_file="daylist_state.json",
    ):
        """Initialize Daylist and its SongRadio instance."""
        if not isinstance(radio_kwargs, dict):
            raise TypeError("radio_kwargs must be a dictionary")
        if isinstance(daylist_num_seeds, bool) or not isinstance(daylist_num_seeds, int) or daylist_num_seeds < 1:
            raise ValueError("daylist_num_seeds must be a positive integer")
        if isinstance(daylist_seed_pool_size, bool) or not isinstance(daylist_seed_pool_size, int) or daylist_seed_pool_size < 1:
            raise ValueError("daylist_seed_pool_size must be a positive integer")
        if not isinstance(daylist_familiarity_penalty, (int, float)) or not 0 <= daylist_familiarity_penalty <= 1:
            raise ValueError("daylist_familiarity_penalty must be between 0 and 1")

        self.daylist_num_seeds = daylist_num_seeds
        self.daylist_familiarity_penalty = float(daylist_familiarity_penalty)
        self.daylist_seed_pool_size = daylist_seed_pool_size
        self.timezone_name = timezone_name
        self.timezone = ZoneInfo(timezone_name)
        self.state_file = Path(state_file)
        self.radio = SongRadio(**radio_kwargs)

        self._run_slot = None
        self._run_weekday = None
        self._selected_seeds = []
        self._dominant_genres = []
        self._familiarity_penalty_stats = {}

    def _load_playlist_id(self):
        """Load and validate the saved Spotify playlist ID."""
        try:
            with self.state_file.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except FileNotFoundError:
            return None
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Could not read Daylist state file %s: %s", self.state_file, exc)
            return None

        if not isinstance(data, dict):
            logger.warning("Ignoring malformed Daylist state file %s.", self.state_file)
            return None
        playlist_id = data.get("playlist_id")
        return playlist_id if isinstance(playlist_id, str) and playlist_id.strip() else None

    def _save_playlist_id(self, playlist_id):
        """Atomically save the managed playlist ID."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.state_file.parent,
                prefix=f".{self.state_file.name}.",
                suffix=".tmp",
                delete=False,
            ) as file:
                temp_path = Path(file.name)
                json.dump({"playlist_id": playlist_id}, file, indent=2)
                file.write("\n")
                file.flush()
                os.fsync(file.fileno())
            os.replace(temp_path, self.state_file)
        except OSError:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)
            raise
        logger.info("Saved Daylist playlist ID to %s.", self.state_file)

    def _ensure_playlist(self):
        """Return the saved playlist ID, creating a private playlist when necessary."""
        playlist_id = self._load_playlist_id()
        if playlist_id and self.radio.playlist_exists(playlist_id):
            logger.info("Using existing Daylist playlist %s.", playlist_id)
            return playlist_id

        if playlist_id:
            logger.warning("Saved Daylist playlist %s is unavailable; creating a replacement.", playlist_id)
        else:
            logger.info("No Daylist playlist is saved; creating one.")

        playlist_id = self.radio.create_empty_playlist(
            self.PLAYLIST_NAME,
            description="Your SongRadio Daylist — updated automatically.",
        )
        self._save_playlist_id(playlist_id)
        logger.info("Created Daylist playlist https://open.spotify.com/playlist/%s", playlist_id)
        return playlist_id

    def close(self):
        """Shut down the wrapped SongRadio instance."""
        self.radio.close()

    @staticmethod
    def _scale_scores(scores):
        """Scale positive scores to [0, 1] using the maximum in the supplied mapping."""
        positive = [value for value in scores.values() if value > 0]
        maximum = max(positive, default=0.0)
        if maximum <= 0:
            return {}
        return {key: max(0.0, value / maximum) for key, value in scores.items()}

    def _select_seeds_for_now(self):
        """Select seed artists with valid stored tracks for the current local time."""
        self._run_slot, self._run_weekday = _current_slot_and_weekday(self.timezone_name)
        artists = self.radio.get_streaming_history_artists()
        tracks_by_artist = self.radio.get_top_tracks_by_artist()

        if not artists:
            logger.warning("No streaming history is loaded.")
            return []

        buckets = self.radio.get_time_bucket_scores(self._run_slot, self._run_weekday)
        base_scores = {artist_key: float(score) for artist_key, _name, score in artists}
        slot_scores = {key: value["slot_score"] for key, value in buckets.items()}
        weekday_scores = {key: value["weekday_score"] for key, value in buckets.items()}
        joint_scores = {key: value["joint_score"] for key, value in buckets.items()}

        base_scaled = self._scale_scores(base_scores)
        slot_scaled = self._scale_scores(slot_scores)
        weekday_scaled = self._scale_scores(weekday_scores)
        joint_scaled = self._scale_scores(joint_scores)

        eligible = []
        for artist_key, artist_display, _base_score in artists:
            track_pair = tracks_by_artist.get(artist_key)
            if not track_pair:
                continue
            stored_artist_name, track_display = track_pair
            if not stored_artist_name or not track_display:
                continue

            base = base_scaled.get(artist_key, 0.0)
            slot = slot_scaled.get(artist_key, 0.0)
            weekday = weekday_scaled.get(artist_key, 0.0)
            joint = joint_scaled.get(artist_key, 0.0)
            relevance = base * (1 + slot) * (1 + weekday) * (1 + joint)
            if relevance > 0:
                eligible.append((artist_key, artist_display or stored_artist_name, track_display, relevance))

        eligible.sort(key=lambda item: item[3], reverse=True)
        pool = eligible[:self.daylist_seed_pool_size]
        count = min(self.daylist_num_seeds, len(pool))
        selected = random.sample(pool, count)
        self._selected_seeds = selected

        logger.info(
            "Selected %d seed(s) for %s/%s: %s",
            len(selected),
            self._run_slot,
            self._run_weekday,
            [artist for _key, artist, _track, _score in selected],
        )
        return selected

    def _apply_familiarity_penalty(self, scored):
        """Adjust candidate scores before the final Daylist selection.

        Familiarity percentiles are calculated across the scored tracks.
        Tracks with no known familiarity receive no penalty.
        """
        self._familiarity_penalty_stats = {}

        if not scored or self.daylist_familiarity_penalty == 0:
            return

        familiarity = self.radio.get_familiarity_scores()
        if not familiarity:
            return

        entries = []

        for item in scored:
            artists = self.radio._track_artist_names(item["track"])

            if not artists:
                entries.append((item, 0.0, "Unknown artist"))
                continue

            artist_name = artists[0]
            artist_key = normalize_artist_name(artist_name)
            score = float(familiarity.get(artist_key, 0.0))
            entries.append((item, score, artist_name))

        sorted_scores = sorted(score for _item, score, _artist in entries)
        count = len(sorted_scores)
        stats = {}

        for item, score, artist_name in entries:
            if count < 2 or score <= 0:
                percentile = 0.0
            else:
                below = bisect_left(sorted_scores, score)
                tied = bisect_right(sorted_scores, score) - below

                percentile = (below + (tied - 1) / 2) / (count - 1)

            penalty = self.daylist_familiarity_penalty * percentile
            item["similarity"] = max(
                0.0,
                item["similarity"] * (1 - penalty),
            )

            stats[artist_name] = {
                "familiarity_score": score,
                "percentile": percentile,
                "penalty_applied": penalty,
            }

        self._familiarity_penalty_stats = stats

    def build_description(self):
        """Build a concise playlist description within Spotify's 300-character limit."""
        parts = [f"{self._run_weekday or '?'} {self._run_slot or '?'}"]

        seed_names = [artist for _key, artist, _track, _score in self._selected_seeds]
        if seed_names:
            parts.append(f"Seeds: {', '.join(seed_names)}")
        if self._dominant_genres:
            parts.append(f"Genres: {', '.join(self._dominant_genres[:5])}")

        listener_counts = [
            pick["track_listeners"] for pick in self.radio.picks
            if pick.get("track_listeners") is not None
        ]
        if listener_counts:
            typical = int(median(listener_counts))
            if typical < 10_000:
                label = "deep cuts"
            elif typical < 50_000:
                label = "hidden gems"
            elif typical < 200_000:
                label = "rising"
            else:
                label = "known"
            parts.append(f"Obscurity: {label} (~{typical:,} listeners)")

        penalized = sum(
            data["penalty_applied"] > 0.05
            for data in self._familiarity_penalty_stats.values()
        )
        if penalized:
            parts.append(f"Discovery boost: {penalized} familiar artist(s) de-ranked")
        parts.append("— SongRadio")
        return " · ".join(parts)[:300]

    def run(self):
        """Generate a time-aware recommendation set and update the persistent playlist."""
        seeds = self._select_seeds_for_now()
        if not seeds:
            raise RuntimeError(
                "No usable streaming-history seeds were found. Import streaming history "
                "with at least one artist that has a stored track."
            )

        seed_queries = {artist: track for _key, artist, track, _score in seeds}
        result = self.radio.run_pipeline(seed_queries=seed_queries, score_adjuster=self._apply_familiarity_penalty)
        self._dominant_genres = [
            genre for genre, _count in result["genres"].most_common(5)
        ]

        picked_artist_keys = {
            normalize_artist_name(artists[0])
            for pick in self.radio.picks
            if (artists := self.radio._track_artist_names(pick["track"]))
        }

        self._familiarity_penalty_stats = {
            artist_name: data
            for artist_name, data in self._familiarity_penalty_stats.items()
            if normalize_artist_name(artist_name) in picked_artist_keys
        }

        self.radio.print_results()
        logger.info("%s", self.radio.lastfm_health_summary())

        description = self.build_description()
        playlist_id = self._ensure_playlist()
        self.radio.update_playlist(playlist_id, description=description)
        return {
            "playlist_id": playlist_id,
            "description": description,
            "seeds": seed_queries,
            "picks": self.radio.picks,
        }