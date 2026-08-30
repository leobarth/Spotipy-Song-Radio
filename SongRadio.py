"""
Song Radio (Low-Mainstream Edition)

Spotify's Development Mode API was heavily restricted in Feb 2026 (batch
endpoints, "popularity", and the old playlist endpoints were removed).
https://developer.spotify.com/documentation/web-api/tutorials/february-2026-migration-guide

Playlist creation/population therefore uses raw requests
(POST /me/playlists, POST /playlists/{id}/items), since spotipy still uses
the removed endpoints internally (/users/{id}/playlists, /playlists/{id}/tracks).

Last.fm API calls
------------------
Last.fm has no documented hard rate limit (unlike Spotify), so this module
treats it defensively rather than optimistically:
  - every request goes through exponential backoff with jitter, and a 429
    is treated as an early warning rather than a hard failure: it's retried
    (respecting Retry-After if present) instead of aborting outright.
  - a rolling-window monitor (LastFmRequestMonitor) tracks error/429 rates
    per API key and logs alerts when they rise, independent of whether any
    individual request ultimately succeeds.
  - concurrent fetching (ThreadPoolExecutor) is used to shorten wall-clock
    time, but concurrency is adaptive: it backs off when the monitor sees
    elevated 429s and only cautiously ramps back up after a run of clean
    batches.
  - Last.fm has no multi-artist "batch" endpoint, so "batching" here means
    firing several single-artist requests concurrently, not a single
    multi-entity request. Artist-level results (tags, listener counts, top
    tracks) are cached for the lifetime of the instance so no artist is
    ever queried twice.
"""

import logging
import random
import re
import json
import difflib
import threading
import time
from collections import Counter, deque
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from functools import lru_cache
from typing import cast

import requests
import spotipy
from langdetect import LangDetectException, detect
from spotipy.oauth2 import SpotifyOAuth

logger = logging.getLogger("song_radio.lastfm")


class LastFmRequestMonitor:
    """Tracks Last.fm request outcomes and raises alerts on elevated error/429 rates.

    Thread-safe. Keeps a rolling window of the most recent request outcomes
    (per API key) rather than a lifetime average, so a recent spike in
    errors or 429s is visible even if the instance has made thousands of
    clean requests earlier in the run.
    """

    def __init__(
        self, api_key, window_size=40, error_rate_alert=0.3, throttle_rate_alert=0.15,
        latency_alert_multiplier=2.0, baseline_sample_size=20,
    ):
        """Initializes the monitor.

        Args:
            api_key: The Last.fm API key being monitored (only its last 4
                characters are ever logged, to avoid leaking the key).
            window_size: Number of most recent outcomes considered when
                computing rolling error/throttle rates and recent latency.
            error_rate_alert: Rolling error rate (network errors, 5xx) at or
                above which an alert is logged.
            throttle_rate_alert: Rolling 429 rate at or above which an alert
                is logged and the caller is expected to reduce concurrency.
            latency_alert_multiplier: How many times slower than the
                established baseline the recent median response latency
                must become before an alert is logged. This is meant to
                catch server-side slowdown under load even when it never
                escalates to an outright 429 - Last.fm has no documented
                rate limit, so quietly getting slower is a plausible first
                symptom of approaching one.
            baseline_sample_size: Number of early, presumably-uncontended
                requests used to establish the "normal" latency baseline
                against which later slowdowns are measured.

        Returns:
            None.
        """
        self.api_key = api_key
        self.window_size = window_size
        self.error_rate_alert = error_rate_alert
        self.throttle_rate_alert = throttle_rate_alert
        self.latency_alert_multiplier = latency_alert_multiplier
        # Reentrant: summary() calls recent_median_latency while already
        # holding the lock, which a plain Lock would deadlock on.
        self._lock = threading.RLock()
        self._outcomes = deque(maxlen=window_size)  # "ok" | "error" | "throttled"
        self._latencies = deque(maxlen=window_size)  # seconds, successful requests only
        self._baseline_samples = []
        self._baseline_sample_size = baseline_sample_size
        self.baseline_latency = None  # set once, from the first baseline_sample_size successes
        self.total_requests = 0
        self.total_errors = 0
        self.total_throttled = 0
        self._alerted_error = False
        self._alerted_throttle = False
        self._alerted_latency = False

    def record(self, outcome, latency=None):
        """Records a single request outcome and logs alerts if thresholds are crossed.

        Args:
            outcome: One of "ok", "error", "throttled".
            latency: Response time in seconds, for successful ("ok")
                requests only. Errors/timeouts are excluded from latency
                tracking since their duration reflects the failure mode
                (e.g. a 5s connect timeout) rather than genuine server
                responsiveness, which would badly skew the baseline.

        Returns:
            None.
        """
        with self._lock:
            self.total_requests += 1
            if outcome == "error":
                self.total_errors += 1
            elif outcome == "throttled":
                self.total_throttled += 1
            self._outcomes.append(outcome)

            if latency is not None and outcome == "ok":
                if self.baseline_latency is None:
                    self._baseline_samples.append(latency)
                    if len(self._baseline_samples) >= self._baseline_sample_size:
                        ordered = sorted(self._baseline_samples)
                        self.baseline_latency = ordered[len(ordered) // 2]  # median
                        logger.info(
                            "Last.fm key ...%s: baseline latency established at %.3fs.",
                            self.api_key[-4:], self.baseline_latency,
                        )
                self._latencies.append(latency)

            if self.total_requests % 25 == 0:
                latency_note = (
                    f", recent median latency {self.recent_median_latency:.3f}s"
                    f" (baseline {self.baseline_latency:.3f}s)"
                    if self.baseline_latency is not None else ""
                )
                logger.info(
                    "Last.fm key ...%s: %d requests so far (%d errors, %d throttled)%s.",
                    self.api_key[-4:], self.total_requests, self.total_errors, self.total_throttled,
                    latency_note,
                )

            if len(self._outcomes) >= self.window_size:
                error_rate = sum(1 for o in self._outcomes if o == "error") / len(self._outcomes)
                throttle_rate = sum(1 for o in self._outcomes if o == "throttled") / len(self._outcomes)

                if error_rate >= self.error_rate_alert and not self._alerted_error:
                    logger.warning(
                        "ALERT: Last.fm error rate %.0f%% over the last %d requests (key ...%s).",
                        error_rate * 100, self.window_size, self.api_key[-4:],
                    )
                    self._alerted_error = True
                elif error_rate < self.error_rate_alert * 0.5:
                    self._alerted_error = False  # recovered; allow re-alerting if it climbs again

                if throttle_rate >= self.throttle_rate_alert and not self._alerted_throttle:
                    logger.warning(
                        "ALERT: Last.fm 429 rate %.0f%% over the last %d requests (key ...%s) "
                        "- concurrency should be reduced.",
                        throttle_rate * 100, self.window_size, self.api_key[-4:],
                    )
                    self._alerted_throttle = True
                elif throttle_rate < self.throttle_rate_alert * 0.5:
                    self._alerted_throttle = False

            # Latency alert: a rolling window is used here too (rather than
            # requiring it to be full of "ok" outcomes) so a real slowdown
            # is caught even if the window is being diluted by unrelated
            # errors/throttles that don't themselves carry a latency value.
            if self.baseline_latency is not None and len(self._latencies) >= min(10, self.window_size):
                ordered = sorted(self._latencies)
                recent_median = ordered[len(ordered) // 2]
                slowdown_threshold = self.baseline_latency * self.latency_alert_multiplier

                if recent_median >= slowdown_threshold and not self._alerted_latency:
                    logger.warning(
                        "ALERT: Last.fm response latency has risen to %.3fs (baseline %.3fs, key ...%s) "
                        "- this can be a silent precursor to throttling even with no 429s seen.",
                        recent_median, self.baseline_latency, self.api_key[-4:],
                    )
                    self._alerted_latency = True
                elif recent_median < slowdown_threshold * 0.7:
                    self._alerted_latency = False  # recovered; allow re-alerting if it climbs again

    @property
    def recent_throttle_rate(self):
        """Current rolling 429 rate.

        Args:
            None.

        Returns:
            Float in [0, 1]. 0.0 if fewer than window_size requests have
            been recorded yet.
        """
        with self._lock:
            if not self._outcomes:
                return 0.0
            return sum(1 for o in self._outcomes if o == "throttled") / len(self._outcomes)

    @property
    def recent_median_latency(self):
        """Median response latency (seconds) over the current rolling window.

        Args:
            None.

        Returns:
            Float, or None if no successful requests have been recorded yet.
        """
        with self._lock:
            if not self._latencies:
                return None
            ordered = sorted(self._latencies)
            return ordered[len(ordered) // 2]

    def summary(self):
        """Human-readable one-line summary of request health for this run.

        Intended to be printed at the end of a run as an explicit check,
        rather than relying solely on alerts having fired mid-run.

        Args:
            None.

        Returns:
            str.
        """
        with self._lock:
            baseline = f"{self.baseline_latency:.3f}s" if self.baseline_latency is not None else "n/a"
            recent = self.recent_median_latency
            recent_str = f"{recent:.3f}s" if recent is not None else "n/a"
            drift = ""
            if self.baseline_latency is not None and recent is not None:
                ratio = recent / self.baseline_latency
                drift = f" ({ratio:.1f}x baseline)"
            return (
                f"Last.fm key ...{self.api_key[-4:]}: {self.total_requests} requests total, "
                f"{self.total_errors} errors, {self.total_throttled} throttled (429). "
                f"Latency baseline {baseline}, recent median {recent_str}{drift}."
            )


class SongRadio:
    LASTFM_JUNK_TAGS = {
        "seen live", "favorites", "favourite", "favourites", "amazing", "awesome",
        "love", "beautiful", "usa", "american", "uk", "british", "australian",
        "male vocalists", "female vocalists", "instrumental", "cover", "covers",
        "guilty pleasure", "guilty pleasures", "classic", "classics", "legend",
        "legendary", "under 2000 listeners", "spotify",
    }
    # Typical suffixes that mark a different "version" of the same song
    VERSION_SUFFIX_PATTERNS = [
        r"\s*-\s*\d{4}\s*remaster(ed)?.*$", r"\s*-\s*remaster(ed)?.*$",
        r"\s*\(\s*\d{4}\s*remaster(ed)?.*?\)", r"\s*\(remaster(ed)?.*?\)",
        r"\s*-\s*live.*$", r"\s*\(live.*?\)",
        r"\s*-\s*radio edit.*$", r"\s*\(radio edit\)",
        r"\s*-\s*single version.*$", r"\s*\(single version\)",
        r"\s*-\s*mono.*$", r"\s*-\s*stereo.*$",
        r"\s*-\s*deluxe.*$", r"\s*\(deluxe.*?\)",
        r"\s*-\s*bonus track.*$", r"\s*\(bonus track\)",
        r"\s*-\s*acoustic.*$", r"\s*\(acoustic.*?\)",
        r"\s*-\s*instrumental.*$", r"\s*\(instrumental\)",
        r"\s*\(explicit\)", r"\s*\(clean\)",
        r"\s*\(feat\..*?\)", r"\s*\(with .*?\)",
    ]

    MIN_TRACK_DURATION_MS = 60_000  # tracks shorter than this are filtered out by default
    RATE_LIMIT_AUTO_RETRY_THRESHOLD = 30  # seconds - above this: abort immediately instead of waiting

    def __init__(
        self,
        # --- Spotify / Last.fm connection ---
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        lastfm_api_key: str,
        # --- Seed configuration ---
        seed_queries: dict,
        include_seed_tracks: bool = True,
        # --- Output shaping ---
        result_limit: int = 15,
        similarity_oversample_factor: int = 3,
        # --- Candidate gathering ---
        genres_to_use: int = 5,
        genre_pool_multiplier: int = 2,
        results_per_turn: int = 10,
        max_results_per_genre: int = 300,
        max_results_per_seed_artist: int = 200,
        target_total_candidates: int = 150,
        min_fresh_fraction: float = 0.2,
        max_candidate_expansion_rounds: int = 5,
        # --- Filtering thresholds ---
        min_artist_listeners: int = 1_000,
        lastfm_listener_ceiling: int = 150_000,
        min_track_listeners: int = 0,
        artist_top_hit_exclude_n: int = 5,
        allowed_languages: list = ["en", "de"],
        excluded_artists: list = [],
        excluded_genres: list = [],
        # --- Local Spotify search cache ---
        cache_path: str = "spotify_search_cache.json",
        cache_ttl_days: float = 30,
        # --- Last.fm concurrency / resilience ---
        lastfm_max_workers: int = 4,
        lastfm_batch_size: int = 12,
        lastfm_max_retries: int = 4,
        lastfm_backoff_base: float = 1.0,
        lastfm_throttle_rate_alert: float = 0.15,
        lastfm_error_rate_alert: float = 0.3,
        lastfm_monitor_window: int = 40,
        lastfm_latency_alert_multiplier: float = 2.0,
        lastfm_baseline_sample_size: int = 20,
    ):
        """Initializes the Spotify/Last.fm clients and stores the configuration.

        Args:
            client_id: Spotify app client ID.
            client_secret: Spotify app client secret.
            redirect_uri: Spotify app redirect URI (for the OAuth flow).
            lastfm_api_key: Last.fm API key (genre/listener count data).
            seed_queries: Mapping {artist name: song title} used as the
                starting point for the recommendations.
            include_seed_tracks: Whether the seed tracks themselves should be
                included in the playlist created later.
            result_limit: Number of tracks returned at the end.
            similarity_oversample_factor: How many times result_limit worth
                of filter-passing candidates to gather before ranking by
                genre similarity to the seeds and keeping the best
                result_limit. Higher values give better similarity ranking
                at the cost of more Last.fm requests.
            genres_to_use: Number of (most specific) genres used for the
                genre search, per round.
            genre_pool_multiplier: Widens the genre candidate pool to
                genres_to_use * genre_pool_multiplier before randomly
                sampling genres_to_use from it, so the same seeds don't
                always search the exact same genres every run.
            results_per_turn: Results requested per query per round-robin
                turn (see build_candidate_pool).
            max_results_per_genre: Upper bound on search results per genre query.
            max_results_per_seed_artist: Upper bound on search results per
                seed artist query.
            target_total_candidates: Desired candidate pool size.
                build_candidate_pool treats this as a target to actively
                work toward - if the initial genre/seed-artist search comes
                up short, additional not-yet-tried genres are queried (up
                to max_candidate_expansion_rounds) before giving up and
                reporting a genuine shortfall.
            min_fresh_fraction: Minimum fraction (0-1) of the final
                candidate pool that must come from tracks fetched live from
                Spotify this run, as opposed to replayed from the local
                search cache. If the initial search comes in under this,
                additional not-yet-tried genres are queried (subject to the
                same max_candidate_expansion_rounds budget) to raise it.
            max_candidate_expansion_rounds: Safety cap on how many extra
                genre-search rounds build_candidate_pool will run while
                trying to reach target_total_candidates / min_fresh_fraction,
                so a persistently thin genre neighborhood can't loop
                indefinitely.
            min_artist_listeners: Minimum total listener count an artist must
                have on Last.fm to be considered a candidate.
            lastfm_listener_ceiling: Maximum listener count a single track may
                have on Last.fm to still count as a "hidden gem".
            min_track_listeners: Minimum Last.fm listener count a single
                track must have to be considered (filters out tracks with
                barely any listeners). 0 disables this floor.
            artist_top_hit_exclude_n: Number of an artist's most-listened
                tracks that count as "used up" and get excluded.
            allowed_languages: Set of allowed language codes (e.g. "en", "de")
                for the song title language detection.
            excluded_artists: Artist names that should never appear as candidates.
            excluded_genres: Genre tags that should never be used as a search
                term and whose artists get hard-excluded.
            cache_path: Path to the local JSON file used to persist Spotify
                search results across runs, so identical queries don't
                re-fetch already-seen offsets and instead explore new ones.
            cache_ttl_days: After how many days a cached query's exploration
                state (offsets_fetched/exhausted) is reset so it gets
                re-scanned from offset 0, to catch drift in Spotify's ranking.
                Set to 0 to disable expiration entirely.
            lastfm_max_workers: Maximum number of concurrent Last.fm requests
                in a batch. This is a ceiling, not a fixed value - the
                effective concurrency adapts downward if 429s are observed
                (see LastFmRequestMonitor) and only cautiously climbs back
                toward this ceiling afterward.
            lastfm_batch_size: Number of pool tracks processed together per
                concurrent batch in filter_and_rank. Larger batches use
                concurrency more efficiently but risk overshooting the
                early-stop target (oversample_target) by up to one batch's
                worth of unnecessary Last.fm lookups.
            lastfm_max_retries: Maximum attempts (including the first) for a
                single Last.fm request before giving up and treating it as
                failed (returns None/empty for that lookup).
            lastfm_backoff_base: Base delay in seconds for exponential
                backoff between retries (delay ~= base * 2**attempt, plus
                jitter). Also used as a fallback when a 429 response has no
                usable Retry-After header.
            lastfm_throttle_rate_alert: Rolling 429 rate (see
                LastFmRequestMonitor) at or above which concurrency is
                reduced and an alert is logged.
            lastfm_error_rate_alert: Rolling network/5xx error rate at or
                above which an alert is logged.
            lastfm_monitor_window: Number of most recent Last.fm requests
                considered when computing rolling error/429 rates and
                recent latency.
            lastfm_latency_alert_multiplier: How many times slower than the
                established baseline the recent median response latency
                must become before an alert is logged (see
                LastFmRequestMonitor). Catches server-side slowdown even
                when it never escalates to an outright 429.
            lastfm_baseline_sample_size: Number of early requests used to
                establish the "normal" latency baseline.

        Returns:
            None.
        """
        # --- Spotify / Last.fm connection ---
        self.sp = spotipy.Spotify(
            auth_manager=SpotifyOAuth(
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
                scope="user-library-read playlist-read-private playlist-modify-private playlist-modify-public",
            ),
            retries=0,  # otherwise spotipy would sleep the full Retry-After itself (possibly hours)
            requests_timeout=10,
        )
        self.lastfm_api_key = lastfm_api_key

        # --- Seed configuration ---
        self.seed_queries = seed_queries
        self.include_seed_tracks = include_seed_tracks
        self.seed_tracks = []  # full track objects, for playlist inclusion
        self.seed_artist_names = []
        self.seed_genre_set = set()  # populated by get_seed_artist_ids_and_genres

        # --- Output shaping ---
        self.result_limit = result_limit
        self.similarity_oversample_factor = similarity_oversample_factor
        self.picks = []

        # --- Candidate gathering ---
        self.genres_to_use = genres_to_use
        self.genre_pool_multiplier = genre_pool_multiplier
        self.results_per_turn = results_per_turn
        self.max_results_per_genre = max_results_per_genre
        self.max_results_per_seed_artist = max_results_per_seed_artist
        self.target_total_candidates = target_total_candidates
        self.min_fresh_fraction = min_fresh_fraction
        self.max_candidate_expansion_rounds = max_candidate_expansion_rounds

        # --- Filtering thresholds ---
        self.min_artist_listeners = min_artist_listeners
        self.lastfm_listener_ceiling = lastfm_listener_ceiling
        self.min_track_listeners = min_track_listeners
        self.artist_top_hit_exclude_n = artist_top_hit_exclude_n
        self.allowed_languages = set(allowed_languages)
        self.excluded_artists = {a.lower() for a in excluded_artists}
        self.excluded_genres = {g.lower() for g in excluded_genres}

        # --- Local Spotify search cache ---
        self.cache_path = cache_path
        self.cache_ttl_days = cache_ttl_days
        self._search_cache = self._load_search_cache()
        self.search_limit = 10  # Spotify maximum since Feb 2026
        self.request_sleep = 0.2  # a bit generous, to avoid bursts
        self._max_offset = 990  # safety cap for offset pagination

        # --- Last.fm-side lookup caches (artist tags/listeners/top-tracks) ---
        self._artist_stats_cache = {}
        self._top_tracks_cache = {}
        self._tags_cache = {}
        self._artist_info_cache = {}
        self._cache_lock = threading.Lock()

        # --- Last.fm concurrency / resilience ---
        self.lastfm_max_workers = lastfm_max_workers
        self.lastfm_batch_size = lastfm_batch_size
        self.lastfm_max_retries = lastfm_max_retries
        self.lastfm_backoff_base = lastfm_backoff_base
        # Created once and reused for every batch (see _run_concurrent) rather
        # than per-batch, to avoid repeated thread creation/teardown overhead
        # and to keep total OS thread count bounded by lastfm_max_workers for
        # the whole run - both for real overhead and for readable profiler output.
        self._lastfm_executor = ThreadPoolExecutor(max_workers=lastfm_max_workers, thread_name_prefix="lastfm")
        self._lastfm_monitor = LastFmRequestMonitor(
            api_key=lastfm_api_key,
            window_size=lastfm_monitor_window,
            error_rate_alert=lastfm_error_rate_alert,
            throttle_rate_alert=lastfm_throttle_rate_alert,
            latency_alert_multiplier=lastfm_latency_alert_multiplier,
            baseline_sample_size=lastfm_baseline_sample_size,
        )

    def lastfm_health_summary(self):
        """Human-readable summary of Last.fm request health for this run.

        Prints total requests/errors/429s and how the recent median
        response latency compares to the baseline established early in the
        run - an explicit, always-available check for server-side
        slowdown, rather than relying solely on alerts having fired.

        Args:
            None.

        Returns:
            str.
        """
        return self._lastfm_monitor.summary()

    # --- Last.fm helpers -----------------------------------------------

    def _lastfm_get(self, method, **params):
        """Performs a GET request against the Last.fm API with retry/backoff.

        There's no confirmed hard rate limit on the Last.fm API, so a 429 is
        treated as an early warning rather than a hard wall: it's retried
        with exponential backoff (respecting a Retry-After header if
        present) rather than failing immediately. Every outcome (ok, error,
        throttled) is recorded in self._lastfm_monitor, which is what the
        concurrent fetch layer in filter_and_rank uses to decide whether to
        reduce concurrency.

        Args:
            method: Name of the Last.fm API method (e.g. "artist.getInfo").
            **params: Additional query parameters for the request
                (e.g. artist=..., track=..., limit=...).

        Returns:
            The JSON-decoded response as a dict, or an empty dict if every
            attempt failed.
        """
        max_attempts = self.lastfm_max_retries
        base_delay = self.lastfm_backoff_base

        for attempt in range(max_attempts):
            is_last_attempt = attempt == max_attempts - 1
            request_start = time.monotonic()

            try:
                resp = requests.get(
                    "https://ws.audioscrobbler.com/2.0/",
                    params={"method": method, "api_key": self.lastfm_api_key, "format": "json", **params},
                    timeout=5,
                )
            except requests.RequestException as e:
                self._lastfm_monitor.record("error")
                if is_last_attempt:
                    logger.warning("Last.fm request failed after %d attempts (%s): %s", max_attempts, method, e)
                    return {}
                time.sleep(base_delay * (2 ** attempt) + random.uniform(0, base_delay))
                continue

            if resp.status_code == 429:
                self._lastfm_monitor.record("throttled")
                if is_last_attempt:
                    logger.warning(
                        "Last.fm kept returning 429 for %s after %d attempts, giving up on this call.",
                        method, max_attempts,
                    )
                    return {}
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        delay = base_delay * (2 ** attempt)
                else:
                    delay = base_delay * (2 ** attempt) + random.uniform(0, base_delay)
                time.sleep(delay)
                continue

            if resp.status_code >= 500:
                self._lastfm_monitor.record("error")
                if is_last_attempt:
                    logger.warning("Last.fm returned %d for %s after %d attempts, giving up.",
                                    resp.status_code, method, max_attempts)
                    return {}
                time.sleep(base_delay * (2 ** attempt) + random.uniform(0, base_delay))
                continue

            try:
                data = resp.json()
            except ValueError as e:
                self._lastfm_monitor.record("error")
                logger.warning("Last.fm response for %s was not valid JSON: %s", method, e)
                return {}

            elapsed = time.monotonic() - request_start
            self._lastfm_monitor.record("ok", latency=elapsed)
            return data

        return {}

    def lastfm_top_tags(self, artist_name, limit=8):
        """Fetches the most-assigned Last.fm tags (genre/style) for an artist.

        Cached per (artist_name, limit) for the lifetime of the instance.

        Args:
            artist_name: Name of the artist.
            limit: Maximum number of tags returned.

        Returns:
            List of tag names (lowercased), in descending order of frequency.
            Empty list if no tags were found or the request failed.
        """
        cache_key = (artist_name, limit)
        with self._cache_lock:
            cached = self._tags_cache.get(cache_key)
        if cached is not None:
            return cached

        data = self._lastfm_get("artist.getTopTags", artist=artist_name)
        assert data is not None
        tags_raw = data.get("toptags", {}).get("tag", [])
        tags = [t["name"].lower() for t in tags_raw[:limit] if t.get("name")]

        with self._cache_lock:
            self._tags_cache[cache_key] = tags
        return tags

    def lastfm_track_listeners(self, artist_name, track_name):
        """Fetches the listener count of a single track from Last.fm.

        Args:
            artist_name: Name of the artist.
            track_name: Title of the track.

        Returns:
            Number of listeners as an int, or None if no Last.fm entry was
            found or the request failed.
        """
        data = self._lastfm_get("track.getInfo", artist=artist_name, track=track_name)
        assert data is not None
        try:
            return int(data["track"]["listeners"])
        except (KeyError, ValueError, TypeError):
            return None

    def lastfm_artist_listeners(self, artist_name):
        """Fetches an artist's total listener count from Last.fm (cached).

        Args:
            artist_name: Name of the artist.

        Returns:
            Total listener count as an int, or None if no Last.fm entry was
            found or the request failed. Results are cached per artist name
            in self._artist_stats_cache.
        """
        with self._cache_lock:
            if artist_name in self._artist_stats_cache:
                return self._artist_stats_cache[artist_name]

        data = self._lastfm_get("artist.getInfo", artist=artist_name)
        assert data is not None
        try:
            listeners = int(data["artist"]["stats"]["listeners"])
        except (KeyError, ValueError, TypeError):
            listeners = None

        with self._cache_lock:
            self._artist_stats_cache[artist_name] = listeners
        return listeners

    def lastfm_artist_top_track_names(self, artist_name):
        """Fetches the names of an artist's most-listened to tracks (cached).

        Args:
            artist_name: Name of the artist.

        Returns:
            Set of track names (lowercased), limited to
            self.artist_top_hit_exclude_n entries. Empty set if no data was
            found or the request failed. Results are cached per artist name
            in self._top_tracks_cache.
        """
        with self._cache_lock:
            if artist_name in self._top_tracks_cache:
                return self._top_tracks_cache[artist_name]

        data = self._lastfm_get("artist.getTopTracks", artist=artist_name, limit=self.artist_top_hit_exclude_n)
        assert data is not None
        tracks = data.get("toptracks", {}).get("track", [])
        names = {t["name"].lower() for t in tracks if t.get("name")}

        with self._cache_lock:
            self._top_tracks_cache[artist_name] = names
        return names

    def _fetch_artist_info_one(self, artist_name):
        """Fetches and caches the bundle of Last.fm lookups for one artist.

        Bundles the three per-artist lookups (tags, total listeners, top
        tracks) used by filter_and_rank's cascade, and caches the bundle in
        self._artist_info_cache so an artist is never looked up twice across
        the whole run. Safe to call from multiple threads: each underlying
        lookup already guards its own cache with self._cache_lock, and the
        bundle write below does too.

        Args:
            artist_name: Name of the artist.

        Returns:
            dict with keys "tags" (set), "listeners" (int or None),
            "top_tracks" (set).
        """
        with self._cache_lock:
            cached = self._artist_info_cache.get(artist_name)
        if cached is not None:
            return cached

        info = {
            "tags": set(self.lastfm_top_tags(artist_name, limit=20)),
            "listeners": self.lastfm_artist_listeners(artist_name),
            "top_tracks": self.lastfm_artist_top_track_names(artist_name),
        }

        with self._cache_lock:
            self._artist_info_cache[artist_name] = info
        return info

    def _run_concurrent(self, callables, max_workers):
        """Runs zero-arg callables on the shared, persistent Last.fm thread pool.

        Reuses self._lastfm_executor (created once in __init__) instead of
        spinning up a fresh ThreadPoolExecutor per call, so total OS thread
        count stays bounded by lastfm_max_workers for the whole run rather
        than growing with the number of batches. Concurrency below the
        pool's configured size (e.g. after an adaptive backoff) is achieved
        by submitting in chunks, not by resizing the pool.

        Args:
            callables: List of zero-argument callables to run.
            max_workers: Maximum number in flight at once for this call.

        Returns:
            List of concurrent.futures.Future objects, in the same order as
            `callables`, each already completed.
        """
        futures: "list[Future | None]" = [None] * len(callables)
        chunk_size = max(1, max_workers)
        for start in range(0, len(callables), chunk_size):
            chunk = callables[start : start + chunk_size]
            future_to_idx = {self._lastfm_executor.submit(fn): start + i for i, fn in enumerate(chunk)}
            for future in as_completed(future_to_idx):
                futures[future_to_idx[future]] = future
        # Every slot was filled above (one future per input callable across
        # all chunks) - the None in the element type only existed to satisfy
        # the initial placeholder list, not because a slot can stay empty.
        return cast("list[Future]", futures)

    def close(self):
        """Shuts down the shared Last.fm thread pool.

        Not strictly required for a one-shot script (process exit cleans
        this up), but good hygiene if an instance is ever reused.

        Args:
            None.

        Returns:
            None.
        """
        self._lastfm_executor.shutdown(wait=True)

    def _fetch_artist_infos_batch(self, artist_names, max_workers):
        """Concurrently fetches Last.fm info for a batch of artists.

        Last.fm has no multi-artist "batch" endpoint, so this fires up to
        max_workers single-artist requests concurrently rather than one at a
        time. Artists already present in self._artist_info_cache are
        skipped entirely (no request made, no thread spent).

        Args:
            artist_names: Iterable of artist names to fetch info for.
            max_workers: Maximum number of concurrent requests for this
                batch (the caller adapts this between batches based on
                observed 429 rates).

        Returns:
            dict artist_name -> info dict (see _fetch_artist_info_one).
            Artists whose fetch raised an exception get a safe empty-info
            placeholder rather than being omitted, so downstream filtering
            doesn't need special-casing for missing keys.
        """
        results = {}
        to_fetch = []
        with self._cache_lock:
            for name in artist_names:
                cached = self._artist_info_cache.get(name)
                if cached is not None:
                    results[name] = cached
                else:
                    to_fetch.append(name)

        if not to_fetch:
            return results

        futures = self._run_concurrent(
            [(lambda n=name: self._fetch_artist_info_one(n)) for name in to_fetch],
            max_workers=max_workers,
        )
        for name, future in zip(to_fetch, futures):
            try:
                results[name] = future.result()
            except Exception as e:
                    logger.warning("Artist info fetch failed for %s: %s", name, e)
                    results[name] = {"tags": set(), "listeners": None, "top_tracks": set()}

        return results

    @lru_cache(maxsize=None)
    def is_allowed_language(self, text, allowed_languages):
        """Checks via best-effort language detection whether a text is allowed.

        Args:
            text: Text to check (e.g. a song title).
            allowed_languages: Set of allowed language codes (e.g. {"en", "de"}).

        Returns:
            True if the detected language is contained in allowed_languages
            or detection fails (a too-short/uncertain text is let through
            rather than falsely blocked). Otherwise False.
        """
        try:
            return detect(text) in allowed_languages
        except LangDetectException:
            return True  # too short/uncertain -> let it through

    @classmethod
    def normalize_title(cls, title):
        """Normalizes a song title for version-duplicate matching.

        Removes remaster/live/radio-edit/feat. suffixes so that different
        versions of the same song map to the same key.

        Args:
            title: Raw song title.

        Returns:
            Normalized title: lowercased, version suffixes removed, only alphanumeric characters and single spaces.
        """
        t = title.lower()
        for pattern in cls.VERSION_SUFFIX_PATTERNS:
            t = re.sub(pattern, "", t, flags=re.IGNORECASE)
        return re.sub(r"[^a-z0-9]+", " ", t).strip()

    # --- Pipeline steps ----------------------------------------------

    def resolve_seed_track_ids(self):
        """Resolves self.seed_queries into real Spotify track IDs via search.

        Searches Spotify for each (artist, song title) pair and picks the
        result with the best artist/title match (instead of blindly trusting
        result #1, since Spotify's field search tends to match fuzzily).
        Uncertain matches are flagged with a console warning.

        Args:
            None.

        Returns:
            List of Spotify track IDs (str), one per successfully resolved
            seed. Seeds without a match are skipped.
        """
        track_ids = []
        for artist_name, track_name in self.seed_queries.items():
            results = self.sp.search(q=f"{track_name} {artist_name}", type="track", limit=10)
            if not results:
                print(f"Warning: No match found for '{track_name}' by {artist_name}.")
                continue
            items = results.get("tracks", {}).get("items", [])
            time.sleep(self.request_sleep)

            def match_score(item):
                item_artist = item["artists"][0]["name"].lower()
                artist_ok = artist_name.lower() in item_artist or item_artist in artist_name.lower()
                title_sim = difflib.SequenceMatcher(None, item["name"].lower(), track_name.lower()).ratio()
                return (artist_ok, title_sim)

            best = max(items, key=match_score)
            artist_ok, title_sim = match_score(best)
            if not artist_ok or title_sim < 0.6:
                print(f"Warning: uncertain match for '{track_name}' -> '{best['name']}' ({title_sim:.2f}).")

            track_ids.append(best["id"])
            print(f"  Found: {best['name']} by {best['artists'][0]['name']} ({best['id']})")
        return track_ids

    def get_seed_artist_ids_and_genres(self, seed_track_ids):
        """Fetches artist data and genres for the seed tracks.

        Loads each seed track individually (populating self.seed_tracks and
        self.seed_artist_names) and determines genres primarily via Last.fm
        (artist.getTopTags), since Spotify's own "genres" field on artist
        objects is frequently empty. Only if Last.fm returns no tags does it
        fall back to Spotify's artist.genres. Also stores the resulting genre
        set as self.seed_genre_set for later genre-similarity ranking in
        filter_and_rank.

        Args:
            seed_track_ids: List of Spotify track IDs of the seed tracks.

        Returns:
            Tuple of:
                - List of Spotify artist IDs (str), one per seed track.
                - Counter mapping genre tag -> frequency across all seed artists.
        """
        artist_ids = []
        for track_id in seed_track_ids:
            track = self.sp.track(track_id)
            assert track is not None, f"track {track_id} could not be resolved from its ID"
            self.seed_tracks.append(track)
            artist_ids.append(track["artists"][0]["id"])
            self.seed_artist_names.append(track["artists"][0]["name"])
            time.sleep(self.request_sleep)

        genre_counter = Counter()
        for artist_name in self.seed_artist_names:
            # limit=8 (default) leaves too few tags after blocklist filtering
            # for genre_pool_multiplier to have anything meaningful to sample
            # from. Fetch more raw tags instead.
            genre_counter.update(self.lastfm_top_tags(artist_name, limit=20))
            time.sleep(self.request_sleep)

        if not genre_counter:
            for artist_id in artist_ids:
                artist = self.sp.artist(artist_id)
                assert artist is not None, f"artist {artist_id} could not be resolved from its ID"
                genre_counter.update(artist.get("genres", []))
                time.sleep(self.request_sleep)

        self.seed_genre_set = set(genre_counter)
        return artist_ids, genre_counter

    def _load_search_cache(self):
        """Loads the persistent Spotify search cache from disk.

        Args:
            None.

        Returns:
            dict with "tracks" (id -> track object, deduplicated globally)
            and "queries" (query string -> {"offsets_fetched": list[int],
            "exhausted": bool, "track_ids": list[str]}). Returns a fresh
            empty structure if the file doesn't exist or is corrupted.
        """
        try:
            with open(self.cache_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"tracks": {}, "queries": {}}

    def _save_search_cache(self):
        """Persists the current search cache to disk.

        Args:
            None.

        Returns:
            None.
        """
        with open(self.cache_path, "w") as f:
            json.dump(self._search_cache, f)

    def _paginated_search(self, query, max_results, fresh_ids=None):
        """Pages through Spotify search results via offset.

        Increases offset in steps of self.search_limit until either
        max_results is reached or Spotify returns an empty results page (end
        of the filtered database). Handles 429 responses: short wait times
        (<= RATE_LIMIT_AUTO_RETRY_THRESHOLD seconds) are waited out and the
        offset retried; long/unknown wait times cause an immediate abort
        (raise), so the lockout isn't extended by further requests.

        Reuses previously cached results for this exact query (see
        self._search_cache) instead of re-fetching them, and only issues live
        Spotify requests for offsets not yet visited by any prior run. If the
        cached entry is older than self.cache_ttl_days, its exploration state
        (offsets_fetched/exhausted) is reset so it gets re-scanned from
        offset 0 - this catches drift in Spotify's ranking over time, at the
        cost of re-fetching the top results once. Previously found tracks are
        never discarded. The cache is saved to disk after every fetched
        offset, so progress isn't lost if the run aborts (e.g. on a 429).

        Args:
            query: Spotify search query (e.g. 'genre:"funk rock"').
            max_results: Upper bound on results collected for this query
                (cached + newly fetched).
            fresh_ids: Optional set that track IDs get added to when they're
                obtained via a live Spotify request in this call, as opposed
                to being replayed from the cache's existing track_ids. Lets
                a caller measure what fraction of a candidate pool came from
                genuinely new requests this run (see build_candidate_pool).

        Returns:
            List of Spotify track objects (dicts).

        Raises:
            spotipy.exceptions.SpotifyException: On a 429 with a long/unknown
                wait time, or when spotipy re-raises the exception (via `raise`).
        """
        entry = self._search_cache["queries"].setdefault(
            query, {"offsets_fetched": [], "exhausted": False, "track_ids": [], "last_fetched_at": None}
        )

        if self.cache_ttl_days and entry["last_fetched_at"]:
            age_days = (time.time() - entry["last_fetched_at"]) / 86400
            if age_days > self.cache_ttl_days:
                entry["offsets_fetched"] = []
                entry["exhausted"] = False

        tracks_by_id = self._search_cache["tracks"]
        collected = [tracks_by_id[tid] for tid in entry["track_ids"] if tid in tracks_by_id]

        if entry["exhausted"] or len(collected) >= max_results:
            return collected

        offset = (max(entry["offsets_fetched"]) + self.search_limit) if entry["offsets_fetched"] else 0
        while offset < max_results and offset <= self._max_offset:
            try:
                results = self.sp.search(q=query, type="track", limit=self.search_limit, offset=offset)
            except spotipy.exceptions.SpotifyException as e:
                # Check for 429 rate limit
                if getattr(e, "http_status", None) == 429:
                    # Safe access to the header, in case headers is None
                    headers = getattr(e, "headers", {}) or {}
                    raw_retry = headers.get("Retry-After")
                    retry_after = None
                    if raw_retry is not None:
                        try:
                            retry_after = int(raw_retry)
                        except ValueError:
                            print(f"Spotify sent an HTTP date instead of seconds: {raw_retry}")
                            pass  # stays None, aborted below
                    if retry_after is not None and retry_after <= self.RATE_LIMIT_AUTO_RETRY_THRESHOLD:
                        print(f"Short rate limit ({retry_after}s) - waiting and retrying...")
                        time.sleep(retry_after + 1)
                        continue
                    wait_msg = f"{retry_after}s" if retry_after is not None else f"unknown ({raw_retry})"
                    print(f"Rate limit reached (wait time: {wait_msg}) - aborting completely.")
                    raise  # hard-stops the script so it doesn't proceed as if it "succeeded"
                else:
                    # A DIFFERENT Spotify error occurred (e.g. 500, 400, 401)
                    print(f"Unexpected API error for query '{query}': {e}")
                    break  # skips THIS search, but continues with the next artist
            except Exception as e:
                # Catches general network timeouts (requests.exceptions.ReadTimeout)
                print(f"Network or system error: {e}")
                break
            time.sleep(self.request_sleep)
            assert results is not None
            items = results.get("tracks", {}).get("items", [])
            entry["offsets_fetched"].append(offset)
            entry["last_fetched_at"] = time.time()
            if not items:
                entry["exhausted"] = True
                self._save_search_cache()
                break  # reached the end of the filtered database
            for track in items:
                if track and track.get("id"):
                    tracks_by_id[track["id"]] = track
                    if track["id"] not in entry["track_ids"]:
                        entry["track_ids"].append(track["id"])
                    collected.append(track)
                    if fresh_ids is not None:
                        fresh_ids.add(track["id"])  # obtained via a live request, not the cache replay above
            self._save_search_cache()  # persist after every offset, not just at the end
            offset += self.search_limit
        return collected

    def build_candidate_pool(self, genre_counter):
        """Gathers candidate tracks via genre and seed artist search.

        Combines two sources, each fetched round-robin (results_per_turn new
        results per query per turn, cycling through queries) so that no
        single genre or seed artist drains its full budget before the others
        get a turn: (1) search by a random sample of genres_to_use genres
        drawn from the genres_to_use * genre_pool_multiplier most specific
        genres in genre_counter, for diversity across other artists and
        across runs; (2) a targeted search for the seed artists themselves,
        to weight their catalog (deep cuts) more heavily. Deduplicates via
        (artist, normalized title) so different versions of the same song
        don't appear multiple times, and excludes the seed tracks themselves
        as well as excluded_artists / disallowed languages.

        target_total_candidates and min_fresh_fraction are treated as goals
        to actively work toward rather than passive caps: when sampling
        genres, not-yet-exhausted ones (see self._search_cache) are
        preferred over ones already known to have no more Spotify results
        left, since a query that can only replay cached data can't help
        either goal. If the initial round still falls short of either
        target, additional not-yet-tried genres are queried in further
        rounds - each of which necessarily pulls live, fresh results, since
        a genre only gets picked here if it hasn't been queried yet this run
        - up to max_candidate_expansion_rounds, after which a genuine
        shortfall is reported rather than silently returned as if nothing
        were missing.

        Args:
            genre_counter: Counter mapping genre tag -> frequency (e.g. from
                get_seed_artist_ids_and_genres).

        Returns:
            List of Spotify track objects (dicts), one per unique
            (artist, normalized title) key.
        """
        candidates = {}  # dedup_key -> track
        fresh_track_ids = set()  # track IDs obtained via a live Spotify request this run

        def add_track(track):
            if not track or not track.get("id"):
                return
            artist_name = track["artists"][0]["name"]
            if artist_name.lower() in self.excluded_artists:
                return
            key = (artist_name.lower(), self.normalize_title(track["name"]))
            if key in candidates:
                return
            if not self.is_allowed_language(track["name"], frozenset(self.allowed_languages)):
                return
            candidates[key] = track

        def fetch_round_robin(query_to_artist, per_query_cap):
            """Cycles through queries, requesting results_per_turn new
            results per query per round, until every query is exhausted or
            at its per_query_cap, or target_total_candidates is reached."""
            requested = {q: 0 for q in query_to_artist}
            active = set(query_to_artist)
            while active and len(candidates) < self.target_total_candidates:
                for q in list(active):
                    if len(candidates) >= self.target_total_candidates:
                        break
                    target = min(requested[q] + self.results_per_turn, per_query_cap)
                    results = self._paginated_search(q, target, fresh_ids=fresh_track_ids)
                    requested[q] = target
                    artist_name = query_to_artist[q]
                    for track in results:
                        if len(candidates) >= self.target_total_candidates:
                            break
                        if artist_name and not any(
                            a["name"].lower() == artist_name.lower() for a in track["artists"]
                        ):
                            continue
                        add_track(track)
                    if len(results) < target or target >= per_query_cap:
                        active.discard(q)

        def genre_query_exhausted(genre):
            entry = self._search_cache["queries"].get(f'genre:"{genre}"')
            return bool(entry and entry.get("exhausted"))

        def current_pool():
            return [t for t in candidates.values() if t is not None]

        def fresh_fraction(pool):
            return sum(1 for t in pool if t["id"] in fresh_track_ids) / len(pool) if pool else 0.0

        # Never let the seed tracks themselves count as a "new recommendation"
        for seed_track in self.seed_tracks:
            key = (seed_track["artists"][0]["name"].lower(), self.normalize_title(seed_track["name"]))
            candidates[key] = None  # placeholder, filtered out below

        # 1) Genre search for diversity across other artists. Genres still
        # known to have unexplored Spotify results are preferred over
        # already-exhausted ones, so a random draw isn't wasted on a query
        # that can only replay cached data.
        blocklist = self.LASTFM_JUNK_TAGS | self.excluded_genres
        specific_genres = [g for g in genre_counter if g not in blocklist]
        specific_genres.sort(key=lambda g: (-g.count(" "), -genre_counter[g]))
        non_exhausted = [g for g in specific_genres if not genre_query_exhausted(g)]
        exhausted = [g for g in specific_genres if genre_query_exhausted(g)]
        ordered_genres = non_exhausted + exhausted

        pool_size = min(len(ordered_genres), self.genres_to_use * self.genre_pool_multiplier)
        genre_pool = ordered_genres[:pool_size]
        if genre_pool:
            top_genres = random.sample(genre_pool, min(self.genres_to_use, len(genre_pool)))
        else:
            top_genres = [g for g, _ in genre_counter.most_common(self.genres_to_use)] or ["pop"]
        print(f"Genres used: {top_genres}")

        fetch_round_robin(
            {f'genre:"{g}"': None for g in top_genres},
            self.max_results_per_genre,
        )

        # 2) Targeted search for the seed artists, to weight their catalog
        #    (deep cuts) more heavily
        fetch_round_robin(
            {f'artist:"{a}"': a for a in self.seed_artist_names},
            self.max_results_per_seed_artist,
        )

        # 3) Expansion rounds: if the initial search left the pool short of
        # target_total_candidates or min_fresh_fraction, reach for genres
        # that haven't been tried yet this run instead of quietly returning
        # less than asked for. Bounded by max_candidate_expansion_rounds so
        # a persistently thin genre neighborhood can't loop indefinitely.
        tried_genres = set(top_genres)
        remaining_genres = [g for g in ordered_genres if g not in tried_genres]
        expansion_round = 0

        while (
            remaining_genres
            and expansion_round < self.max_candidate_expansion_rounds
            and (
                len(current_pool()) < self.target_total_candidates
                or fresh_fraction(current_pool()) < self.min_fresh_fraction
            )
        ):
            expansion_round += 1
            batch = remaining_genres[: self.genres_to_use]
            remaining_genres = remaining_genres[self.genres_to_use :]
            tried_genres.update(batch)
            print(f"Expanding candidate search (round {expansion_round}): {batch}")
            fetch_round_robin({f'genre:"{g}"': None for g in batch}, self.max_results_per_genre)

        pool = current_pool()
        final_fresh_fraction = fresh_fraction(pool)

        if len(pool) < self.target_total_candidates:
            print(
                f"Note: Candidate pool reached {len(pool)}/{self.target_total_candidates} requested "
                f"after {expansion_round} expansion round(s) - no further un-exhausted genres were available."
            )
        if final_fresh_fraction < self.min_fresh_fraction:
            print(
                f"Note: Only {final_fresh_fraction:.0%} of candidates came from fresh Spotify requests "
                f"this run (target: {self.min_fresh_fraction:.0%})."
            )

        return pool

    def _genre_similarity(self, artist_tags):
        """Computes Jaccard similarity between an artist's tags and the seeds.

        Args:
            artist_tags: Set of Last.fm tag names (lowercased) for a candidate's artist.

        Returns:
            Float in [0, 1]: |intersection| / |union| with self.seed_genre_set.
            0.0 if either set is empty.
        """
        if not self.seed_genre_set or not artist_tags:
            return 0.0
        intersection = self.seed_genre_set & artist_tags
        union_size = len(self.seed_genre_set) + len(artist_tags) - len(intersection)
        return len(intersection) / union_size if union_size else 0.0

    def filter_and_rank(self, candidates):
        """Filters candidates and ranks them by genre similarity to the seeds.

        Filter cascade per candidate (same as before):
            1. Track duration > MIN_TRACK_DURATION_MS (no data needed, cheapest check first).
            2. Last.fm genre tags against excluded_genres.
            3. Artist's total listeners >= min_artist_listeners (lower bound
               only, large artists are allowed as long as the song itself is obscure).
            4. Track is not one of the artist's top hits.
            5. Track listener count <= lastfm_listener_ceiling.
            6. Track listener count >= min_track_listeners.

        Instead of stopping at the first result_limit passing candidates,
        gathers up to result_limit * similarity_oversample_factor of them,
        scores each by Jaccard similarity (see _genre_similarity) between its
        artist's Last.fm tags and self.seed_genre_set, and keeps the
        result_limit most similar ones.

        Candidates are processed in windowed batches (self.lastfm_batch_size
        tracks at a time) rather than one at a time or all at once: within a
        batch, Last.fm lookups run concurrently (self.lastfm_max_workers
        ceiling, adaptively reduced if 429s are observed - see
        LastFmRequestMonitor), but the early-stop check still runs between
        batches, so gathering stops as soon as oversample_target passing
        candidates are found. This trades a small, bounded overfetch (up to
        one batch's worth of extra lookups) for concurrency within each
        batch - a fixed one-at-a-time loop can't be parallelized without
        losing the early-stop property entirely.

        Args:
            candidates: List of Spotify track objects (dicts), e.g. from
                build_candidate_pool.

        Returns:
            List of dicts with the keys "track" (Spotify track object),
            "track_listeners" (int or None), "artist_listeners" (int or
            None), and "similarity" (float in [0, 1]). At most
            self.result_limit entries, sorted by similarity descending. Also
            stored in self.picks.
        """
        pool = list(candidates)
        random.shuffle(pool)

        oversample_target = self.result_limit * self.similarity_oversample_factor
        scored = []

        workers = self.lastfm_max_workers  # adaptive: shrinks/grows between batches
        batch_size = self.lastfm_batch_size
        consecutive_clean_batches = 0

        i = 0
        while i < len(pool) and len(scored) < oversample_target:
            batch = pool[i : i + batch_size]
            i += batch_size

            # Cheap, sequential pre-filter: no network call needed.
            batch = [t for t in batch if t.get("duration_ms", 0) > self.MIN_TRACK_DURATION_MS]
            if not batch:
                continue

            unique_artists = {t["artists"][0]["name"] for t in batch}
            artist_infos = self._fetch_artist_infos_batch(unique_artists, max_workers=workers)

            # Artist-level filters first (cheap, no extra request) to avoid
            # spending a track.getInfo request on a track whose artist would
            # be excluded anyway.
            survivors = []
            for track in batch:
                artist_name = track["artists"][0]["name"]
                info = artist_infos.get(artist_name, {"tags": set(), "listeners": None, "top_tracks": set()})

                if self.excluded_genres and info["tags"] & self.excluded_genres:
                    continue
                if info["listeners"] is not None and info["listeners"] < self.min_artist_listeners:
                    continue
                if track["name"].lower() in info["top_tracks"]:
                    continue
                survivors.append((track, info))

            track_listener_results = {}
            if survivors:
                callables = [
                    (lambda track=track: self.lastfm_track_listeners(track["artists"][0]["name"], track["name"]))
                    for track, _info in survivors
                ]
                futures = self._run_concurrent(callables, max_workers=workers)
                for idx, future in enumerate(futures):
                    try:
                        track_listener_results[idx] = future.result()
                    except Exception as e:
                        logger.warning("Track listener fetch failed: %s", e)
                        track_listener_results[idx] = None

            for idx, (track, info) in enumerate(survivors):
                if len(scored) >= oversample_target:
                    break

                track_listeners = track_listener_results.get(idx)
                if track_listeners is not None and track_listeners > self.lastfm_listener_ceiling:
                    continue
                if track_listeners is not None and track_listeners < self.min_track_listeners:
                    continue

                similarity = self._genre_similarity(info["tags"])
                scored.append({
                    "track": track,
                    "track_listeners": track_listeners,
                    "artist_listeners": info["listeners"],
                    "similarity": similarity,
                })

            # Adaptive concurrency: back off hard if this batch saw
            # meaningful 429s, and only cautiously ramp back up after a run
            # of clean batches (additive increase / multiplicative decrease).
            throttle_rate = self._lastfm_monitor.recent_throttle_rate
            if throttle_rate >= self._lastfm_monitor.throttle_rate_alert:
                workers = max(1, workers // 2)
                cooldown = min(30, self.lastfm_backoff_base * (2 ** (self.lastfm_max_workers - workers)))
                logger.warning(
                    "Backing off Last.fm concurrency to %d worker(s), cooling down %.1fs.", workers, cooldown,
                )
                time.sleep(cooldown)
                consecutive_clean_batches = 0
            else:
                consecutive_clean_batches += 1
                if consecutive_clean_batches >= 3 and workers < self.lastfm_max_workers:
                    workers += 1
                    consecutive_clean_batches = 0

        scored.sort(key=lambda p: p["similarity"], reverse=True)
        picks = scored[: self.result_limit]

        if len(picks) < self.result_limit:
            print(f"Note: Only {len(picks)} matching tracks found (out of {len(pool)} candidates).")

        self.picks = picks
        return picks

    # --- Output -----------------------------------------------------------

    def print_results(self):
        """Prints the recommendations stored in self.picks to the console.

        Shows title, artist, track and artist listener counts, and the
        Spotify link for each track.

        Args:
            None.

        Returns:
            None.
        """
        print("\n--- Recommended Tracks ---")
        for i, pick in enumerate(self.picks, 1):
            track = pick["track"]
            artist_name = track["artists"][0]["name"]
            url = track["external_urls"]["spotify"]
            track_listeners = pick["track_listeners"] if pick["track_listeners"] is not None else "?"
            artist_listeners = pick["artist_listeners"] if pick["artist_listeners"] is not None else "?"
            similarity = pick.get("similarity")
            similarity_str = f"{similarity:.2f}" if similarity is not None else "?"
            print(f"{i}. {track['name']} by {artist_name}")
            print(f"   Track listeners: {track_listeners} | Artist listeners: {artist_listeners} | "
                  f"Genre similarity: {similarity_str} | Link: {url}")

    # --- Playlist (raw requests, see module docstring) --------------------

    def _bearer_headers(self):
        """Builds HTTP headers with a valid bearer token for raw API requests.

        Args:
            None.

        Returns:
            dict with "Authorization" and "Content-Type" headers.
        """
        assert self.sp.auth_manager is not None
        token = self.sp.auth_manager.get_access_token(as_dict=False)
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _existing_playlist_names(self):
        """Reads the names of all playlists of the current user.

        Pages through all pages of GET /me/playlists.

        Args:
            None.

        Returns:
            Set of existing playlist names (str).
        """
        names = set()
        results = self.sp.current_user_playlists(limit=50)
        while results:
            names.update(p["name"] for p in results["items"])
            results = self.sp.next(results) if results.get("next") else None
        return names

    def _unique_playlist_name(self, base_name):
        """Generates a unique playlist name via numbering.

        Checks base_name against the user's existing playlist names and
        appends " #2", " #3", etc. on a collision, until the name is free.

        Args:
            base_name: Desired base name of the playlist.

        Returns:
            Unique playlist name (str): either base_name itself, or base_name
            with a number appended.
        """
        existing = self._existing_playlist_names()
        if base_name not in existing:
            return base_name
        n = 2
        while f"{base_name} #{n}" in existing:
            n += 1
        return f"{base_name} #{n}"

    def save_as_playlist(self, base_name="Song Radio (Low-Mainstream)"):
        """Creates a private playlist with the recommendations (+ optionally seeds).

        Uses raw HTTP requests instead of spotipy methods for creation and
        population, since the endpoints spotipy uses internally
        (/users/{id}/playlists, /playlists/{id}/tracks) were removed as of
        Feb 2026 (see module docstring). The playlist name is deduplicated
        via numbering through _unique_playlist_name.

        Args:
            base_name: Desired base name of the playlist.

        Returns:
            None.

        Raises:
            requests.exceptions.HTTPError: If creating the playlist or adding
                the tracks fails.
        """
        if not self.picks:
            print("No tracks available to save.")
            return

        uris = []
        if self.include_seed_tracks:
            uris.extend(t["uri"] for t in self.seed_tracks)
        uris.extend(pick["track"]["uri"] for pick in self.picks)

        name = self._unique_playlist_name(base_name)

        create_resp = requests.post(
            "https://api.spotify.com/v1/me/playlists",
            headers=self._bearer_headers(),
            json={"name": name, "public": False},
            timeout=10,
        )
        create_resp.raise_for_status()
        playlist = create_resp.json()

        add_resp = requests.post(
            f"https://api.spotify.com/v1/playlists/{playlist['id']}/items",
            headers=self._bearer_headers(),
            json={"uris": uris},
            timeout=10,
        )
        add_resp.raise_for_status()

        print(f"Playlist '{name}' created: {playlist['external_urls']['spotify']}")