"""
Daylist
=======

Builds and refreshes a single, persistent Spotify playlist whose content
reflects what the user typically listens to at the current time of day and
day of week - but skewed toward lesser-known tracks, like SongRadio, rather
than replaying the familiar catalog.

Architecture
------------
Daylist is not a subclass of SongRadio - it wraps a SongRadio instance via
composition (owns one, creates it internally). This keeps the two concerns
separated (SongRadio: generic radio generation; Daylist: time-aware seed
selection + familiarity penalty + persistent playlist management) and avoids
touching SongRadio's internals.

Pipeline
--------
1.  _select_seeds_for_now(): reads streaming_history_artists from the
    persisted cache (via radio.get_familiarity_scores()), weights each
    artist by (a) how strongly they appear in the current time slot and
    weekday bucket, and (b) recency (weighted_score from the streaming
    history), then samples a small set as seeds for this run.
2.  Runs the standard SongRadio pipeline (resolve_seed_track_ids ->
    get_seed_artist_ids_and_genres -> build_candidate_pool ->
    filter_and_rank) on those seeds.
3.  _apply_familiarity_penalty(): adjusts the scored candidates' similarity
    scores downward for artists the user already knows well, so the final
    playlist leans toward discovery rather than replaying the familiar.
4.  build_description(): assembles a human-readable playlist description
    from all available run metadata (time slot, genres, seeds, obscurity
    level, familiarity-penalty stats, attribution).
5.  radio.update_playlist(): atomically replaces the playlist's tracks and
    description.

Config
------
Daylist reads from daylist_config.json (falling back to
daylist_config_template.json). Required extra keys beyond SongRadio's own
config (all under the top-level dict):

  playlist_id (str): The Spotify playlist ID to update. Create an empty
      playlist manually once, copy its ID from the URL, put it here.
  daylist_num_seeds (int, default 2): How many artists to sample as seeds
      from the time-weighted history for each run.
  daylist_familiarity_penalty (float, default 0.3): How strongly to penalize
      familiar artists. 0 = no penalty, 1 = familiar artists' similarity
      scores are zeroed out. The penalty is applied proportionally to the
      artist's familiarity percentile within this run's scored pool.
  daylist_seed_pool_size (int, default 20): How many top time-weighted
      artists to sample seeds from - larger means more variety across runs.
  utc_offset_hours (int, default 0): Passed to StreamingHistoryParser; shifts
      UTC timestamps into local time before slot/weekday bucketing.
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timezone

from StreamingHistory import TIME_SLOTS, WEEKDAYS, local_utc_offset_hours
from SongRadio import SongRadio

logger = logging.getLogger("song_radio.daylist")


def _current_slot_and_weekday(utc_offset_hours=None):
    """Returns the (slot_label, weekday_label) for the current moment.

    Args:
        utc_offset_hours: Integer offset added to the current UTC hour to
            get local time. Defaults to None, in which case the system
            timezone is detected automatically via local_utc_offset_hours()
            (DST-aware). Pass an explicit integer to override.

    Returns:
        (slot_label, weekday_label) tuple, both str.
    """
    now = datetime.now(timezone.utc)
    if utc_offset_hours is None:
        utc_offset_hours = local_utc_offset_hours(at=now)
    local_hour = (now.hour + utc_offset_hours) % 24
    day_offset = (now.hour + utc_offset_hours) // 24
    weekday = WEEKDAYS[(now.weekday() + day_offset) % 7]
    slot = next(
        (label for label, start, end in TIME_SLOTS if start <= local_hour < end),
        TIME_SLOTS[-1][0],
    )
    return slot, weekday


class Daylist:
    """Time-aware, familiarity-penalized Spotify playlist manager.

    Wraps a SongRadio instance (created internally from the same config
    dict) and extends its pipeline with:
    - Time-of-day / day-of-week seed selection from streaming history.
    - Familiarity penalty to push the final picks toward discovery.
    - Persistent playlist update (no new playlist created each run).
    - Auto-generated playlist description with full run metadata.
    """

    PLAYLIST_NAME = "SongRadio Daylist"

    def __init__(
        self,
        radio_kwargs,
        daylist_num_seeds=2,
        daylist_familiarity_penalty=0.3,
        daylist_seed_pool_size=20,
        utc_offset_hours=None,
        state_file="daylist_state.json",
    ):
        """Initialises Daylist and the internal SongRadio instance.

        The Daylist manages its own persistent Spotify playlist: on first
        run it creates a new private playlist and saves its ID to
        state_file; on subsequent runs it loads that ID, checks it still
        exists, and re-creates it if it was deleted. No manual playlist
        setup or playlist_id config key is required.

        Args:
            radio_kwargs: Dict of kwargs forwarded verbatim to SongRadio().
                Must include all required SongRadio fields (client_id,
                client_secret, redirect_uri, lastfm_api_key, seed_queries).
                seed_queries is overridden at runtime by _select_seeds_for_now,
                but SongRadio requires it at construction time - pass any
                valid placeholder (e.g. {"Placeholder": "Placeholder"}).
            daylist_num_seeds: Number of artists sampled as seeds from the
                time-weighted streaming history for each run. More seeds
                broaden the genre neighbourhood; fewer keep the playlist
                more focused. Default 2.
            daylist_familiarity_penalty: Strength of the familiarity penalty
                applied after filter_and_rank (see _apply_familiarity_penalty).
                0.0 = no penalty, 1.0 = fully zeroes out similarity scores
                for the most-familiar artists. Practical range: 0.1-0.5.
                Default 0.3.
            daylist_seed_pool_size: How many top time-weighted artists form
                the candidate pool from which daylist_num_seeds are randomly
                sampled. Larger values increase run-to-run variety at the
                cost of occasionally sampling weaker time-signal artists.
                Default 20.
            utc_offset_hours: Integer UTC offset for slot/weekday bucketing
                (e.g. +1 for CET, +2 for CEST). Defaults to None, which
                triggers automatic DST-aware detection via
                local_utc_offset_hours(). Pass an explicit integer to
                override (e.g. when the server runs in a different
                timezone than the user).
            state_file: Path to the JSON file where the managed playlist's
                Spotify ID is persisted between runs. Defaults to
                "daylist_state.json" next to the script. If the file is
                missing (first run) or the playlist it references no
                longer exists, a new playlist is created automatically.

        Returns:
            None.
        """
        self.daylist_num_seeds = daylist_num_seeds
        self.daylist_familiarity_penalty = daylist_familiarity_penalty
        self.daylist_seed_pool_size = daylist_seed_pool_size
        self.utc_offset_hours = utc_offset_hours
        self.state_file = state_file
        self.radio = SongRadio(**radio_kwargs)
        # Populated during run(); read by build_description()
        self._run_slot = None
        self._run_weekday = None
        self._selected_seeds = []
        self._dominant_genres = []
        self._familiarity_penalty_stats = {}

    def _load_playlist_id(self):
        """Reads the persisted playlist ID from state_file, if it exists.

        Args:
            None.

        Returns:
            str or None.
        """
        try:
            with open(self.state_file, "r") as f:
                return json.load(f).get("playlist_id")
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def _save_playlist_id(self, playlist_id):
        """Persists the playlist ID to state_file.

        Args:
            playlist_id: Spotify playlist ID string.

        Returns:
            None.
        """
        with open(self.state_file, "w") as f:
            json.dump({"playlist_id": playlist_id}, f, indent=2)
        logger.info("Daylist playlist ID saved to %s.", self.state_file)

    def _ensure_playlist(self):
        """Returns a valid playlist ID, creating a new playlist if needed.

        Checks state_file for a previously-created playlist ID. If found,
        verifies it still exists on Spotify (in case it was deleted). If
        missing or gone, creates a new private playlist named
        PLAYLIST_NAME, saves the ID, and returns it.

        Args:
            None.

        Returns:
            Spotify playlist ID (str).
        """
        playlist_id = self._load_playlist_id()
        if playlist_id:
            if self.radio.playlist_exists(playlist_id):
                logger.info("Daylist: using existing playlist %s.", playlist_id)
                return playlist_id
            print(
                f"Daylist playlist {playlist_id} no longer exists on Spotify - "
                "creating a new one."
            )
        else:
            print("No Daylist playlist found - creating one now.")

        playlist_id = self.radio.create_empty_playlist(
            self.PLAYLIST_NAME,
            description="Your SongRadio Daylist — updated automatically.",
        )
        self._save_playlist_id(playlist_id)
        print(
            f"Created Daylist playlist '{self.PLAYLIST_NAME}': "
            f"https://open.spotify.com/playlist/{playlist_id}"
        )
        return playlist_id

    def close(self):
        """Shuts down the internal SongRadio instance.

        Args:
            None.

        Returns:
            None.
        """
        self.radio.close()

    def _select_seeds_for_now(self):
        """Samples seed artists from streaming history for the current slot.

        Reads per-artist weighted_scores and time-bucket data from the
        persisted streaming history cache, multiplies them to form a
        combined "relevance now" score, and samples daylist_num_seeds
        artists from the top daylist_seed_pool_size. The random sample
        provides run-to-run variety without ever straying far from what
        the history says is relevant right now.

        Falls back to a random sample from all known artists if no
        time-bucket data is available (e.g. first run before any streaming
        history has been loaded).

        Args:
            None.

        Returns:
            List of (artist_name_original_case, score) tuples, length
            daylist_num_seeds, or fewer if the history is too small.
            Also sets self._run_slot and self._run_weekday.
        """
        self._run_slot, self._run_weekday = _current_slot_and_weekday(self.utc_offset_hours)
        logger.info("Daylist slot: %s / %s", self._run_slot, self._run_weekday)

        # Fetch per-artist base scores
        rows = self.radio._search_db.execute(
            "SELECT artist_name, weighted_score FROM streaming_history_artists ORDER BY weighted_score DESC"
        ).fetchall()
        if not rows:
            logger.warning(
                "No streaming history found in cache - run load_streaming_history() first. "
                "Falling back to empty seed selection."
            )
            return []

        # Fetch time-bucket scores for current slot and weekday
        slot_rows = self.radio._search_db.execute(
            "SELECT artist_name, slot_score, weekday_score FROM streaming_history_artist_buckets "
            "WHERE slot_label = ? AND weekday_label = ?",
            (self._run_slot, self._run_weekday),
        ).fetchall()
        bucket_by_artist = {r[0]: (r[1], r[2]) for r in slot_rows}

        # Combine: base_score * (1 + slot_weight) * (1 + weekday_weight)
        # This gives a strong boost to artists that are both recently-listened
        # and time-relevant, without completely excluding artists with no
        # time-bucket data (they get a neutral multiplier of 1*1 = 1).
        scored = []
        for artist_name, base_score in rows:
            slot_w, weekday_w = bucket_by_artist.get(artist_name, (0.0, 0.0))
            combined = base_score * (1 + slot_w) * (1 + weekday_w)
            scored.append((artist_name, combined))

        scored.sort(key=lambda x: x[1], reverse=True)
        pool = scored[: self.daylist_seed_pool_size]
        n = min(self.daylist_num_seeds, len(pool))
        selected = random.sample(pool, n)
        self._selected_seeds = selected
        logger.info(
            "Selected %d seed(s): %s",
            len(selected), [s[0] for s in selected],
        )
        return selected

    def _apply_familiarity_penalty(self):
        """Penalizes familiar artists in radio.picks after filter_and_rank.

        Reads per-artist familiarity scores (weighted_scores from the
        streaming history) and computes each pick's familiarity percentile
        within this run's scored pool - so the penalty scales relative to
        what's familiar *to this user*, not against a global popularity
        metric. Artists at the top percentile receive the full
        daylist_familiarity_penalty multiplier applied to their similarity
        score; unfamiliar artists are unaffected.

        Also records per-pick familiarity data into self._familiarity_penalty_stats
        for use in build_description.

        Args:
            None.

        Returns:
            None. Mutates radio.picks in place.
        """
        if not self.radio.picks:
            return

        familiarity = self.radio.get_familiarity_scores()
        if not familiarity:
            return

        picks_with_scores = []
        for pick in self.radio.picks:
            artist = pick["track"]["artists"][0]["name"].lower()
            fam_score = familiarity.get(artist, 0.0)
            picks_with_scores.append((pick, fam_score))

        # Percentile within this run's pool only
        all_scores = sorted(p[1] for p in picks_with_scores)
        n = len(all_scores)

        stats = {}
        for pick, fam_score in picks_with_scores:
            if n > 1 and fam_score > 0:
                percentile = all_scores.index(fam_score) / (n - 1)
            else:
                percentile = 0.0
            penalty = self.daylist_familiarity_penalty * percentile
            pick["similarity"] = max(0.0, pick["similarity"] * (1 - penalty))
            artist = pick["track"]["artists"][0]["name"]
            stats[artist] = {
                "familiarity_score": fam_score,
                "percentile": percentile,
                "penalty_applied": penalty,
            }

        self._familiarity_penalty_stats = stats
        # Re-sort after penalty adjustment
        self.radio.picks.sort(key=lambda p: p["similarity"], reverse=True)

    def build_description(self):
        """Assembles a playlist description from all available run metadata.

        Packs the current time slot, weekday, selected seed artists, dominant
        genres, obscurity level (median track-listener count from picks),
        familiarity-penalty summary, and a fixed attribution line into a
        single string. Truncated to 300 characters for Spotify's limit.

        The layout is intentionally information-dense rather than polished
        prose - see the module docstring regarding future LLM/template-based
        description generation.

        Args:
            None.

        Returns:
            str, at most 300 characters.
        """
        parts = []

        # Time context
        slot = self._run_slot or "?"
        weekday = self._run_weekday or "?"
        parts.append(f"{weekday.capitalize()} {slot}")

        # Seeds
        if self._selected_seeds:
            seed_names = ", ".join(s[0] for s in self._selected_seeds)
            parts.append(f"Seeds: {seed_names}")

        # Genres
        if self._dominant_genres:
            parts.append(f"Genres: {', '.join(self._dominant_genres[:5])}")

        # Obscurity (median track listeners)
        listener_counts = [
            p["track_listeners"] for p in self.radio.picks
            if p.get("track_listeners") is not None
        ]
        if listener_counts:
            median_listeners = sorted(listener_counts)[len(listener_counts) // 2]
            if median_listeners < 10_000:
                obscurity = "deep cuts"
            elif median_listeners < 50_000:
                obscurity = "hidden gems"
            elif median_listeners < 200_000:
                obscurity = "rising"
            else:
                obscurity = "known"
            parts.append(f"Obscurity: {obscurity} (~{median_listeners:,} listeners)")

        # Familiarity penalty summary
        if self._familiarity_penalty_stats:
            penalized = sum(
                1 for v in self._familiarity_penalty_stats.values()
                if v["penalty_applied"] > 0.05
            )
            if penalized:
                parts.append(f"Discovery boost: {penalized} familiar artist(s) de-ranked")

        # Attribution
        parts.append("— SongRadio")

        description = " · ".join(parts)
        return description[:300]

    def run(self):
        """Runs the full Daylist pipeline and updates the persistent playlist.

        1. Selects time-appropriate seeds from streaming history.
        2. Overrides radio.seed_queries with the selected seeds.
        3. Runs the SongRadio pipeline (resolve -> genres -> candidates -> rank).
        4. Applies the familiarity penalty.
        5. Builds a description.
        6. Calls radio.update_playlist() to replace the playlist's content.

        Args:
            None.

        Returns:
            None.
        """
        seeds = self._select_seeds_for_now()
        if not seeds:
            raise RuntimeError(
                "No seeds could be selected from streaming history. "
                "Make sure streaming history has been loaded via "
                "SongRadio.load_streaming_history() before running Daylist."
            )

        # Override seed_queries with the time-selected artists.
        # We use each artist's most-played track (by total_ms_played) as the
        # seed track, since SongRadio needs a (artist, track) pair.
        track_rows = self.radio._search_db.execute(
            "SELECT artist_name, track_name FROM streaming_history_tracks "
            "ORDER BY weighted_score DESC"
        ).fetchall()
        top_track_by_artist = {}
        for artist_name, track_name in track_rows:
            if artist_name not in top_track_by_artist:
                top_track_by_artist[artist_name] = track_name

        self.radio.seed_queries = {}
        for artist_name, _score in seeds:
            track_name = top_track_by_artist.get(artist_name)
            if track_name:
                # Use original (non-lowercased) form for Spotify search;
                # the history stores lowercased keys so we title-case as a
                # reasonable approximation - Spotify's fuzzy search handles
                # the rest.
                self.radio.seed_queries[artist_name.title()] = track_name.title()
            else:
                logger.warning(
                    "No track found for seed artist '%s' in streaming history - skipping.",
                    artist_name,
                )

        if not self.radio.seed_queries:
            raise RuntimeError("No valid (artist, track) seed pairs could be assembled.")

        # Reset per-run state that SongRadio carries across pipeline steps
        self.radio.seed_tracks = []
        self.radio.seed_artist_names = []
        self.radio.seed_genre_set = set()
        self.radio.picks = []
        self.radio._similar_artist_pool_names = None
        self.radio.seed_similar_match = {}

        print("Resolving seed tracks...")
        seed_track_ids = self.radio.resolve_seed_track_ids()
        if not seed_track_ids:
            raise RuntimeError("No seed tracks resolved from Spotify - aborting.")

        print("\nLoading artist data and genres...")
        _seed_artist_ids, genre_counter = self.radio.get_seed_artist_ids_and_genres(seed_track_ids)

        print("\n--- Detected Genres ---")
        for genre, count in genre_counter.most_common(10):
            print(f"  {genre}  (x{count})")

        # Store top genres for description
        self._dominant_genres = [g for g, _ in genre_counter.most_common(5)]

        print("\nBuilding candidate pool...")
        candidates = self.radio.build_candidate_pool(genre_counter)
        print(f"{len(candidates)} candidates found.\n")

        print("Selecting tracks...")
        self.radio.filter_and_rank(candidates)

        print("Applying familiarity penalty...")
        self._apply_familiarity_penalty()

        self.radio.print_results()
        print("\n" + self.radio.lastfm_health_summary())

        description = self.build_description()
        print(f"\nPlaylist description: {description}")

        playlist_id = self._ensure_playlist()
        self.radio.update_playlist(playlist_id, description=description)