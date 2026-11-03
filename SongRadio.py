"""Spotify and Last.fm recommendation pipeline with persistent SQLite caches."""

import difflib
import enum
import json
import logging
import random
import re
import sqlite3
import threading
import time
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from functools import lru_cache
from typing import cast

import requests
import spotipy
from langdetect import LangDetectException, detect
from requests.adapters import HTTPAdapter
from spotipy.oauth2 import SpotifyOAuth

from normalization import normalize_artist_name, normalize_track_title
from StreamingHistory import StreamingHistoryStats

logger = logging.getLogger("song_radio")


class _CacheMiss(enum.Enum):
    TOKEN = enum.auto()


_CACHE_MISS = _CacheMiss.TOKEN


class LastFmRequestMonitor:
    """Thread-safe rolling monitor for Last.fm request outcomes and latency."""

    def __init__(
        self,
        api_key,
        window_size=40,
        error_rate_alert=0.3,
        throttle_rate_alert=0.15,
        latency_alert_multiplier=2.0,
        baseline_sample_size=20,
    ):
        if window_size < 1 or baseline_sample_size < 1:
            raise ValueError("Monitor window and baseline sample size must be positive")
        self.api_key = api_key
        self.window_size = window_size
        self.error_rate_alert = error_rate_alert
        self.throttle_rate_alert = throttle_rate_alert
        self.latency_alert_multiplier = latency_alert_multiplier
        self._lock = threading.RLock()
        self._outcomes = deque(maxlen=window_size)
        self._latencies = deque(maxlen=window_size)
        self._baseline_samples = []
        self._baseline_sample_size = baseline_sample_size
        self.baseline_latency = None
        self.total_requests = 0
        self.total_errors = 0
        self.total_throttled = 0
        self._alerted_error = False
        self._alerted_throttle = False
        self._alerted_latency = False

    def record(self, outcome, latency=None):
        """Record one request outcome and emit alerts when thresholds are crossed."""
        if outcome not in {"ok", "error", "throttled"}:
            raise ValueError(f"Unknown Last.fm request outcome: {outcome}")

        with self._lock:
            self.total_requests += 1
            self.total_errors += outcome == "error"
            self.total_throttled += outcome == "throttled"
            self._outcomes.append(outcome)

            if outcome == "ok" and latency is not None and latency >= 0:
                if self.baseline_latency is None:
                    self._baseline_samples.append(latency)
                    if len(self._baseline_samples) >= self._baseline_sample_size:
                        ordered = sorted(self._baseline_samples)
                        self.baseline_latency = ordered[len(ordered) // 2]
                        logger.info(
                            "Last.fm key ...%s: baseline latency %.3fs.",
                            self.api_key[-4:],
                            self.baseline_latency,
                        )
                self._latencies.append(latency)

            if len(self._outcomes) >= self.window_size:
                count = len(self._outcomes)
                error_rate = sum(value == "error" for value in self._outcomes) / count
                throttle_rate = sum(value == "throttled" for value in self._outcomes) / count

                if error_rate >= self.error_rate_alert and not self._alerted_error:
                    logger.warning(
                        "Last.fm error rate %.0f%% over the last %d requests (key ...%s).",
                        error_rate * 100,
                        count,
                        self.api_key[-4:],
                    )
                    self._alerted_error = True
                elif error_rate < self.error_rate_alert * 0.5:
                    self._alerted_error = False

                if throttle_rate >= self.throttle_rate_alert and not self._alerted_throttle:
                    logger.warning(
                        "Last.fm 429 rate %.0f%% over the last %d requests (key ...%s).",
                        throttle_rate * 100,
                        count,
                        self.api_key[-4:],
                    )
                    self._alerted_throttle = True
                elif throttle_rate < self.throttle_rate_alert * 0.5:
                    self._alerted_throttle = False

            minimum_latency_samples = min(10, self.window_size)
            if self.baseline_latency and len(self._latencies) >= minimum_latency_samples:
                ordered = sorted(self._latencies)
                recent = ordered[len(ordered) // 2]
                threshold = self.baseline_latency * self.latency_alert_multiplier
                if recent >= threshold and not self._alerted_latency:
                    logger.warning(
                        "Last.fm latency rose to %.3fs (baseline %.3fs, key ...%s).",
                        recent,
                        self.baseline_latency,
                        self.api_key[-4:],
                    )
                    self._alerted_latency = True
                elif recent < threshold * 0.7:
                    self._alerted_latency = False

    @property
    def recent_throttle_rate(self):
        """Return the throttle rate in the rolling window."""
        with self._lock:
            if not self._outcomes:
                return 0.0
            return sum(value == "throttled" for value in self._outcomes) / len(self._outcomes)

    @property
    def recent_median_latency(self):
        """Return the median successful-request latency, or None."""
        with self._lock:
            if not self._latencies:
                return None
            ordered = sorted(self._latencies)
            return ordered[len(ordered) // 2]

    def summary(self):
        """Return a concise request-health summary."""
        with self._lock:
            baseline = f"{self.baseline_latency:.3f}s" if self.baseline_latency is not None else "n/a"
            recent = self.recent_median_latency
            recent_text = f"{recent:.3f}s" if recent is not None else "n/a"
            drift = f" ({recent / self.baseline_latency:.1f}x baseline)" if recent is not None and self.baseline_latency else ""
            return (
                f"Last.fm key ...{self.api_key[-4:]}: {self.total_requests} requests, "
                f"{self.total_errors} errors, {self.total_throttled} throttled; "
                f"latency baseline {baseline}, recent median {recent_text}{drift}."
            )


class SongRadio:
    """Generate and rank Spotify recommendations using Last.fm metadata."""

    LASTFM_JUNK_TAGS = {
        "seen live", "favorites", "favourite", "favourites", "amazing", "awesome",
        "love", "beautiful", "usa", "american", "uk", "british", "australian",
        "male vocalists", "female vocalists", "instrumental", "cover", "covers",
        "guilty pleasure", "guilty pleasures", "classic", "classics", "legend",
        "legendary", "under 2000 listeners", "spotify", "all",
    }
    MIN_TRACK_DURATION_MS = 60_000
    RATE_LIMIT_AUTO_RETRY_THRESHOLD = 30
    MAX_UNKNOWN_429_RETRIES = 2
    MAX_RETRY_AFTER_SECONDS = 60
    PERSISTED_TAGS_LIMIT = 50
    PERSISTED_TOP_TRACKS_LIMIT = 20
    PERSISTED_SIMILAR_LIMIT = 30

    _NON_LATIN_SCRIPT_RE = re.compile(
        "[\u0400-\u04FF\u0370-\u03FF\u0590-\u05FF\u0600-\u06FF"
        "\u0900-\u097F\u0E00-\u0E7F\u3040-\u30FF\u3400-\u9FFF\uAC00-\uD7A3]"
    )
    _NON_LATIN_LANGUAGES = {
        "ru", "uk", "bg", "sr", "el", "he", "yi", "ar", "fa", "ur",
        "hi", "mr", "ne", "th", "ja", "zh", "zh-cn", "zh-tw", "ko",
    }
    _STOPWORD_PATTERNS = {
        "en": re.compile(r"\b(the|and|you|your|love|with|for|of|in|on|to|me|my|we)\b", re.I),
        "de": re.compile(r"\b(und|nicht|ich|der|die|das|mit|für|auf|ist|ein|eine|kein|dich|mich|wir)\b", re.I),
    }
    _ARTIST_FIELD_COLUMNS = {
        "tags": ("tags_json", "tags_fetched_at"),
        "listeners": ("listeners", "listeners_fetched_at"),
        "top_tracks": ("top_tracks_json", "top_tracks_fetched_at"),
        "similar": ("similar_json", "similar_fetched_at"),
    }

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        lastfm_api_key: str,
        seed_queries: dict,
        include_seed_tracks: bool = True,
        result_limit: int = 15,
        similarity_oversample_factor: int = 3,
        genres_to_use: int = 5,
        genre_pool_multiplier: int = 2,
        results_per_turn: int = 10,
        max_results_per_genre: int = 300,
        max_results_per_seed_artist: int = 200,
        max_results_per_similar_artist: int = 15,
        target_total_candidates: int = 150,
        min_fresh_fraction: float = 0.2,
        max_candidate_expansion_rounds: int = 5,
        min_discovered_genre_count: int = 2,
        lastfm_similar_artist_count: int = 8,
        thin_tag_genre_threshold: int = 4,
        thin_tag_similar_artist_count: int = 25,
        similarity_blend_weight_range: tuple = (0.2, 0.5),
        popularity_bias: float = 0.0,
        min_artist_listeners: int = 1_000,
        lastfm_listener_ceiling: int = 150_000,
        min_track_listeners: int = 0,
        artist_top_hit_exclude_n: int = 5,
        allowed_languages=None,
        excluded_artists=None,
        excluded_genres=None,
        cache_path: str = "spotify_search_cache.db",
        cache_ttl_days: float = 30,
        lastfm_cache_ttl_days: float = 14,
        lastfm_max_workers: int = 4,
        lastfm_batch_size: int = 12,
        lastfm_max_retries: int = 4,
        lastfm_backoff_base: float = 1.0,
        lastfm_throttle_rate_alert: float = 0.15,
        lastfm_error_rate_alert: float = 0.3,
        lastfm_monitor_window: int = 40,
        lastfm_latency_alert_multiplier: float = 2.0,
        lastfm_baseline_sample_size: int = 20,
        max_tracks_per_artist: int = 2,
        max_2hop_artists: int = 20,
    ):
        """Initialize clients, configuration, worker pool, and local caches."""
        positive_ints = {
            "result_limit": result_limit,
            "similarity_oversample_factor": similarity_oversample_factor,
            "lastfm_max_workers": lastfm_max_workers,
            "lastfm_batch_size": lastfm_batch_size,
            "lastfm_max_retries": lastfm_max_retries,
            "max_tracks_per_artist": max_tracks_per_artist,
        }
        for name, value in positive_ints.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if isinstance(max_2hop_artists, bool) or not isinstance(max_2hop_artists, int) or max_2hop_artists < 0:
            raise ValueError("max_2hop_artists must be a nonnegative integer")
        if not 0 <= min_fresh_fraction <= 1:
            raise ValueError("min_fresh_fraction must be between 0 and 1")
        if len(similarity_blend_weight_range) != 2 or not 0 <= similarity_blend_weight_range[0] <= similarity_blend_weight_range[1] <= 1:
            raise ValueError("similarity_blend_weight_range must satisfy 0 <= min <= max <= 1")
        if lastfm_backoff_base < 0:
            raise ValueError("lastfm_backoff_base must be nonnegative")
        if lastfm_cache_ttl_days < 0 or cache_ttl_days < 0:
            raise ValueError("Cache TTL values must be nonnegative")
        if not isinstance(seed_queries, dict):
            raise TypeError("seed_queries must be a dictionary")

        self.sp = spotipy.Spotify(
            auth_manager=SpotifyOAuth(
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
                scope="user-library-read playlist-read-private playlist-modify-private playlist-modify-public",
                show_dialog=True,
            ),
            retries=0,
            requests_timeout=10,
        )
        self.lastfm_api_key = lastfm_api_key
        self.seed_queries = dict(seed_queries)
        self.include_seed_tracks = include_seed_tracks
        self.seed_tracks = []
        self.seed_artist_names = []
        self.seed_genre_set = set()
        self.picks = []

        self.result_limit = result_limit
        self.similarity_oversample_factor = similarity_oversample_factor
        self.genres_to_use = genres_to_use
        self.genre_pool_multiplier = genre_pool_multiplier
        self.results_per_turn = results_per_turn
        self.max_results_per_genre = max_results_per_genre
        self.max_results_per_seed_artist = max_results_per_seed_artist
        self.max_results_per_similar_artist = max_results_per_similar_artist
        self.target_total_candidates = target_total_candidates
        self.min_fresh_fraction = min_fresh_fraction
        self.max_candidate_expansion_rounds = max_candidate_expansion_rounds
        self.min_discovered_genre_count = min_discovered_genre_count
        self.lastfm_similar_artist_count = lastfm_similar_artist_count
        self.thin_tag_genre_threshold = thin_tag_genre_threshold
        self.thin_tag_similar_artist_count = thin_tag_similar_artist_count
        self.similarity_blend_weight_range = similarity_blend_weight_range
        self.popularity_bias = popularity_bias
        self.min_artist_listeners = min_artist_listeners
        self.lastfm_listener_ceiling = lastfm_listener_ceiling
        self.min_track_listeners = min_track_listeners
        self.artist_top_hit_exclude_n = artist_top_hit_exclude_n
        self.allowed_languages = set(allowed_languages if allowed_languages is not None else ["en", "de"])
        self.excluded_artists = {normalize_artist_name(name) for name in (excluded_artists or [])}
        self.excluded_genres = {str(genre).casefold() for genre in (excluded_genres or [])}
        self.max_tracks_per_artist = max_tracks_per_artist
        self.max_2hop_artists = max_2hop_artists

        self.cache_path = cache_path
        self.cache_ttl_days = cache_ttl_days
        self.lastfm_cache_ttl_days = lastfm_cache_ttl_days
        self._search_db = self._init_search_db(cache_path)
        self._lastfm_db = sqlite3.connect(cache_path, check_same_thread=False, timeout=30)
        self._lastfm_db.execute("PRAGMA journal_mode=WAL")
        self._cache_lock = threading.RLock()

        self._artist_stats_cache = {}
        self._top_tracks_cache = {}
        self._tags_cache = {}
        self._track_listeners_cache = {}
        self._similar_artists_cache = {}
        self._artist_info_cache = {}
        self._similar_artist_pool_names = None
        self._two_hop_artist_pool_names = None
        self.seed_similar_match = {}

        self.lastfm_max_workers = lastfm_max_workers
        self.lastfm_batch_size = lastfm_batch_size
        self.lastfm_max_retries = lastfm_max_retries
        self.lastfm_backoff_base = lastfm_backoff_base
        self._lastfm_executor = ThreadPoolExecutor(
            max_workers=lastfm_max_workers,
            thread_name_prefix="lastfm",
        )
        self._session_local = threading.local()
        self._sessions = []
        self._sessions_lock = threading.Lock()
        self._lastfm_monitor = LastFmRequestMonitor(
            api_key=lastfm_api_key,
            window_size=lastfm_monitor_window,
            error_rate_alert=lastfm_error_rate_alert,
            throttle_rate_alert=lastfm_throttle_rate_alert,
            latency_alert_multiplier=lastfm_latency_alert_multiplier,
            baseline_sample_size=lastfm_baseline_sample_size,
        )
        self.search_limit = 10
        self.request_sleep = 0.2
        self._max_offset = 990

    def _get_lastfm_session(self):
        """Return a persistent requests session owned by the current thread."""
        session = getattr(self._session_local, "session", None)
        if session is None:
            session = requests.Session()
            pool_size = max(self.lastfm_max_workers, 10)
            session.mount("https://", HTTPAdapter(pool_connections=1, pool_maxsize=pool_size))
            self._session_local.session = session
            with self._sessions_lock:
                self._sessions.append(session)
        return session

    def lastfm_health_summary(self):
        """Return a summary of Last.fm request health for this run."""
        return self._lastfm_monitor.summary()

    _LASTFM_FAILURE = object()

    def _lastfm_get(self, method, **params):
        """Request a Last.fm API method with bounded retries and backoff."""
        for attempt in range(self.lastfm_max_retries):
            started = time.monotonic()
            try:
                response = self._get_lastfm_session().get(
                    "https://ws.audioscrobbler.com/2.0/",
                    params={"method": method, "api_key": self.lastfm_api_key, "format": "json", **params},
                    timeout=5,
                )
            except requests.RequestException as exc:
                self._lastfm_monitor.record("error")
                if attempt + 1 == self.lastfm_max_retries:
                    logger.warning("Last.fm request failed for %s: %s", method, exc)
                    return self._LASTFM_FAILURE
                time.sleep(self.lastfm_backoff_base * (2 ** attempt) + random.uniform(0, self.lastfm_backoff_base))
                continue

            if response.status_code == 429:
                self._lastfm_monitor.record("throttled")
                if attempt + 1 == self.lastfm_max_retries:
                    logger.warning("Last.fm returned 429 for %s after %d attempts.", method, self.lastfm_max_retries)
                    return self._LASTFM_FAILURE
                try:
                    delay = float(response.headers.get("Retry-After", ""))
                except (TypeError, ValueError):
                    delay = self.lastfm_backoff_base * (2 ** attempt) + random.uniform(0, self.lastfm_backoff_base)
                time.sleep(min(max(delay, 0.0), self.MAX_RETRY_AFTER_SECONDS))
                continue

            if response.status_code >= 500:
                self._lastfm_monitor.record("error")
                if attempt + 1 == self.lastfm_max_retries:
                    logger.warning("Last.fm returned HTTP %d for %s.", response.status_code, method)
                    return self._LASTFM_FAILURE
                time.sleep(self.lastfm_backoff_base * (2 ** attempt) + random.uniform(0, self.lastfm_backoff_base))
                continue

            try:
                data = response.json()
            except ValueError as exc:
                self._lastfm_monitor.record("error")
                logger.warning("Last.fm returned invalid JSON for %s: %s", method, exc)
                return self._LASTFM_FAILURE

            if response.status_code >= 400 or not isinstance(data, dict) or "error" in data:
                code = data.get("error") if isinstance(data, dict) else None
                if code == 29 and attempt + 1 < self.lastfm_max_retries:
                    self._lastfm_monitor.record("throttled")
                    time.sleep(min(self.lastfm_backoff_base * (2 ** attempt), self.MAX_RETRY_AFTER_SECONDS))
                    continue
                self._lastfm_monitor.record("error")
                logger.warning(
                    "Last.fm API error for %s (HTTP %d, code %s): %s",
                    method, response.status_code, code, data.get("message") if isinstance(data, dict) else response.reason,
                )
                return self._LASTFM_FAILURE

            self._lastfm_monitor.record("ok", time.monotonic() - started)
            return data

        return self._LASTFM_FAILURE

    def _load_persisted_artist_field(self, artist_name, field):
        """Load one artist field from SQLite, respecting its independent TTL."""
        value_col, timestamp_col = self._ARTIST_FIELD_COLUMNS[field]
        key = normalize_artist_name(artist_name)
        with self._cache_lock:
            row = self._lastfm_db.execute(
                f"SELECT {value_col}, {timestamp_col} FROM lastfm_artists WHERE artist_name = ?",
                (key,),
            ).fetchone()

        if row is None:
            return _CACHE_MISS

        value, fetched_at = row
        if fetched_at is None:
            return _CACHE_MISS
        if self.lastfm_cache_ttl_days and time.time() - fetched_at > self.lastfm_cache_ttl_days * 86400:
            return _CACHE_MISS
        if field == "listeners":
            return value
        return json.loads(value) if value else []

    def _save_persisted_artist_field(self, artist_name, field, value):
        """Save one Last.fm artist field without changing other cached fields."""
        value_col, timestamp_col = self._ARTIST_FIELD_COLUMNS[field]
        key = normalize_artist_name(artist_name)
        stored = value if field == "listeners" else json.dumps(value, ensure_ascii=False)
        display_name = artist_name.strip()
        with self._cache_lock:
            self._lastfm_db.execute(
                f"INSERT INTO lastfm_artists (artist_name, display_name, {value_col}, {timestamp_col}) "
                f"VALUES (?, ?, ?, ?) ON CONFLICT(artist_name) DO UPDATE SET "
                f"display_name=excluded.display_name, {value_col}=excluded.{value_col}, "
                f"{timestamp_col}=excluded.{timestamp_col}",
                (key, display_name, stored, time.time()),
            )
            self._lastfm_db.commit()

    def _load_persisted_track_listeners(self, artist_name, track_name):
        """Load a track listener count from SQLite, respecting its TTL."""
        artist_key = normalize_artist_name(artist_name)
        track_key = normalize_track_title(track_name)
        with self._cache_lock:
            row = self._lastfm_db.execute(
                "SELECT listeners, fetched_at FROM lastfm_tracks WHERE artist_name = ? AND track_name = ?",
                (artist_key, track_key),
            ).fetchone()

        if row is None or row[1] is None:
            return _CACHE_MISS
        if self.lastfm_cache_ttl_days and time.time() - row[1] > self.lastfm_cache_ttl_days * 86400:
            return _CACHE_MISS
        return row[0]

    def _save_persisted_track_listeners(self, artist_name, track_name, listeners):
        """Save a track listener count using normalized artist/title keys."""
        artist_key = normalize_artist_name(artist_name)
        track_key = normalize_track_title(track_name)
        with self._cache_lock:
            self._lastfm_db.execute(
                "INSERT INTO lastfm_tracks (artist_name, track_name, display_name, listeners, fetched_at) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT(artist_name, track_name) DO UPDATE SET "
                "display_name=excluded.display_name, listeners=excluded.listeners, fetched_at=excluded.fetched_at",
                (artist_key, track_key, track_name.strip(), listeners, time.time()),
            )
            self._lastfm_db.commit()

    @staticmethod
    def _as_list(value):
        """Normalize an API field that may contain either one object or a list."""
        if value is None:
            return []
        return value if isinstance(value, list) else [value]

    def lastfm_top_tags(self, artist_name, limit=8):
        """Return an artist's most-used Last.fm tags."""
        if limit <= 0:
            return []
        key = normalize_artist_name(artist_name)
        cache_key = (key, limit)
        with self._cache_lock:
            cached = self._tags_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        persisted = self._load_persisted_artist_field(artist_name, "tags")
        if persisted is not _CACHE_MISS:
            tags = persisted[:limit]
        else:
            data = self._lastfm_get("artist.getTopTags", artist=artist_name)
            if data is self._LASTFM_FAILURE:
                return []
            raw = data.get("toptags", {}).get("tag", [])
            tags = [
                str(tag["name"]).casefold()
                for tag in self._as_list(raw)[:self.PERSISTED_TAGS_LIMIT]
                if isinstance(tag, dict) and tag.get("name")
            ]
            self._save_persisted_artist_field(artist_name, "tags", tags)
            tags = tags[:limit]

        with self._cache_lock:
            self._tags_cache[cache_key] = list(tags)
        return tags

    def lastfm_track_listeners(self, artist_name, track_name):
        """Return the Last.fm listener count for a track, or None if unavailable."""
        artist_key = normalize_artist_name(artist_name)
        track_key = normalize_track_title(track_name)
        cache_key = (artist_key, track_key)
        with self._cache_lock:
            cached = self._track_listeners_cache.get(cache_key, _CACHE_MISS)
        if cached is not _CACHE_MISS:
            return cached

        persisted = self._load_persisted_track_listeners(artist_name, track_name)
        if persisted is not _CACHE_MISS:
            with self._cache_lock:
                self._track_listeners_cache[cache_key] = persisted
            return persisted

        data = self._lastfm_get("track.getInfo", artist=artist_name, track=track_name)
        if data is self._LASTFM_FAILURE:
            return None

        try:
            listeners = int(data["track"]["listeners"])
        except (KeyError, TypeError, ValueError):
            listeners = None

        self._save_persisted_track_listeners(artist_name, track_name, listeners)
        with self._cache_lock:
            self._track_listeners_cache[cache_key] = listeners
        return listeners

    def lastfm_artist_listeners(self, artist_name):
        """Return an artist's Last.fm listener count, or None if unavailable."""
        key = normalize_artist_name(artist_name)
        with self._cache_lock:
            cached = self._artist_stats_cache.get(key, _CACHE_MISS)
        if cached is not _CACHE_MISS:
            return cached

        persisted = self._load_persisted_artist_field(artist_name, "listeners")
        if persisted is not _CACHE_MISS:
            with self._cache_lock:
                self._artist_stats_cache[key] = persisted
            return persisted

        data = self._lastfm_get("artist.getInfo", artist=artist_name)
        if data is self._LASTFM_FAILURE:
            return None

        try:
            listeners = int(data["artist"]["stats"]["listeners"])
        except (KeyError, TypeError, ValueError):
            listeners = None

        self._save_persisted_artist_field(artist_name, "listeners", listeners)
        with self._cache_lock:
            self._artist_stats_cache[key] = listeners
        return listeners

    def lastfm_artist_top_track_names(self, artist_name):
        """Return the configured number of an artist's most-played track titles."""
        key = normalize_artist_name(artist_name)
        with self._cache_lock:
            cached = self._top_tracks_cache.get(key)
        if cached is not None:
            return set(cached)

        persisted = self._load_persisted_artist_field(artist_name, "top_tracks")
        if persisted is _CACHE_MISS:
            data = self._lastfm_get(
                "artist.getTopTracks", artist=artist_name, limit=self.PERSISTED_TOP_TRACKS_LIMIT,
            )
            if data is self._LASTFM_FAILURE:
                return set()
            raw = data.get("toptracks", {}).get("track", [])
            names = [
                normalize_track_title(track["name"])
                for track in self._as_list(raw)[:self.PERSISTED_TOP_TRACKS_LIMIT]
                if isinstance(track, dict) and track.get("name")
            ]
            self._save_persisted_artist_field(artist_name, "top_tracks", names)
        else:
            names = persisted

        result = set(names[:self.artist_top_hit_exclude_n])
        with self._cache_lock:
            self._top_tracks_cache[key] = set(result)
        return result

    def lastfm_similar_artists(self, artist_name, limit=20):
        """Return Last.fm's similar artists as (name, match) pairs."""
        if limit <= 0:
            return []
        key = normalize_artist_name(artist_name)
        cache_key = (key, limit)
        with self._cache_lock:
            cached = self._similar_artists_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        persisted = self._load_persisted_artist_field(artist_name, "similar")
        if persisted is _CACHE_MISS:
            data = self._lastfm_get(
                "artist.getSimilar", artist=artist_name, limit=self.PERSISTED_SIMILAR_LIMIT,
            )
            if data is self._LASTFM_FAILURE:
                return []
            raw = data.get("similarartists", {}).get("artist", [])
            similar = []
            for item in self._as_list(raw)[:self.PERSISTED_SIMILAR_LIMIT]:
                if not isinstance(item, dict) or not item.get("name"):
                    continue
                try:
                    match = max(0.0, min(1.0, float(item.get("match", 0.0))))
                except (TypeError, ValueError):
                    match = 0.0
                similar.append([item["name"], match])
            self._save_persisted_artist_field(artist_name, "similar", similar)
        else:
            similar = persisted

        result = [tuple(item) for item in similar[:limit]]
        with self._cache_lock:
            self._similar_artists_cache[cache_key] = list(result)
        return result

    @staticmethod
    def _init_search_db(cache_path):
        """Create or migrate the SQLite cache schema."""
        if cache_path == ":memory:":
            raise ValueError("cache_path must be a file path because SongRadio uses two SQLite connections")

        db = sqlite3.connect(cache_path, timeout=30)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA busy_timeout=30000")
        db.executescript("""
            CREATE TABLE IF NOT EXISTS tracks (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS queries (
                query TEXT PRIMARY KEY,
                offsets_fetched TEXT NOT NULL DEFAULT '[]',
                exhausted INTEGER NOT NULL DEFAULT 0,
                last_fetched_at REAL,
                capped_at_max_offset INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS query_tracks (
                query TEXT NOT NULL,
                track_id TEXT NOT NULL,
                PRIMARY KEY (query, track_id)
            );
            CREATE INDEX IF NOT EXISTS idx_query_tracks_query ON query_tracks(query);
            CREATE TABLE IF NOT EXISTS lastfm_artists (
                artist_name TEXT PRIMARY KEY,
                display_name TEXT,
                tags_json TEXT,
                tags_fetched_at REAL,
                listeners INTEGER,
                listeners_fetched_at REAL,
                top_tracks_json TEXT,
                top_tracks_fetched_at REAL,
                similar_json TEXT,
                similar_fetched_at REAL
            );
            CREATE TABLE IF NOT EXISTS lastfm_tracks (
                artist_name TEXT NOT NULL,
                track_name TEXT NOT NULL,
                display_name TEXT,
                listeners INTEGER,
                fetched_at REAL,
                PRIMARY KEY (artist_name, track_name)
            );
            CREATE TABLE IF NOT EXISTS streaming_history_artists (
                artist_name TEXT PRIMARY KEY,
                display_name TEXT NOT NULL DEFAULT '',
                play_count INTEGER NOT NULL,
                skip_count INTEGER NOT NULL,
                total_ms_played INTEGER NOT NULL,
                weighted_score REAL NOT NULL,
                last_played_ts TEXT,
                loaded_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS streaming_history_tracks (
                artist_name TEXT NOT NULL,
                track_name TEXT NOT NULL,
                display_name TEXT NOT NULL DEFAULT '',
                play_count INTEGER NOT NULL,
                skip_count INTEGER NOT NULL,
                total_ms_played INTEGER NOT NULL,
                weighted_score REAL NOT NULL,
                last_played_ts TEXT,
                loaded_at REAL NOT NULL,
                PRIMARY KEY (artist_name, track_name)
            );
            CREATE TABLE IF NOT EXISTS streaming_history_podcasts (
                show_name TEXT PRIMARY KEY,
                play_count INTEGER NOT NULL,
                skip_count INTEGER NOT NULL,
                total_ms_played INTEGER NOT NULL,
                weighted_score REAL NOT NULL,
                last_played_ts TEXT,
                loaded_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS streaming_history_audiobooks (
                title TEXT PRIMARY KEY,
                play_count INTEGER NOT NULL,
                skip_count INTEGER NOT NULL,
                total_ms_played INTEGER NOT NULL,
                weighted_score REAL NOT NULL,
                last_played_ts TEXT,
                loaded_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS streaming_history_artist_buckets (
                artist_name TEXT NOT NULL,
                slot_label TEXT NOT NULL,
                weekday_label TEXT NOT NULL,
                slot_score REAL NOT NULL,
                weekday_score REAL NOT NULL,
                joint_score REAL NOT NULL DEFAULT 0,
                loaded_at REAL NOT NULL,
                PRIMARY KEY (artist_name, slot_label, weekday_label)
            );
        """)

        migrations = {
            "queries": {
                "capped_at_max_offset": "INTEGER NOT NULL DEFAULT 0",
            },
            "lastfm_artists": {
                "display_name": "TEXT",
                "similar_json": "TEXT",
                "similar_fetched_at": "REAL",
            },
            "lastfm_tracks": {
                "display_name": "TEXT",
            },
            "streaming_history_artists": {
                "display_name": "TEXT NOT NULL DEFAULT ''",
            },
            "streaming_history_tracks": {
                "display_name": "TEXT NOT NULL DEFAULT ''",
            },
            "streaming_history_artist_buckets": {
                "joint_score": "REAL NOT NULL DEFAULT 0",
            },
        }

        for table, columns in migrations.items():
            existing = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
            for column, definition in columns.items():
                if column not in existing:
                    db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

        db.commit()
        return db

    @staticmethod
    def normalize_title(title):
        """Return the shared normalized title key used for version matching."""
        return normalize_track_title(title)

    @staticmethod
    def _read_query_state(db, query):
        """Read a cached Spotify query's pagination state."""
        row = db.execute(
            "SELECT offsets_fetched, exhausted, last_fetched_at, capped_at_max_offset "
            "FROM queries WHERE query = ?",
            (query,),
        ).fetchone()
        if row is None:
            return {"offsets_fetched": [], "exhausted": False, "last_fetched_at": None, "capped_at_max_offset": False}
        offsets, exhausted, fetched_at, capped = row
        try:
            offsets = json.loads(offsets)
        except (TypeError, json.JSONDecodeError):
            offsets = []
        return {
            "offsets_fetched": offsets if isinstance(offsets, list) else [],
            "exhausted": bool(exhausted),
            "last_fetched_at": fetched_at,
            "capped_at_max_offset": bool(capped),
        }

    def _get_query_state(self, query):
        """Read a cached Spotify query's pagination state."""
        return self._read_query_state(self._search_db, query)

    def _save_query_state(self, query, state):
        """Persist a Spotify query's pagination state."""
        self._search_db.execute(
            "INSERT INTO queries (query, offsets_fetched, exhausted, last_fetched_at, capped_at_max_offset) "
            "VALUES (?, ?, ?, ?, ?) ON CONFLICT(query) DO UPDATE SET "
            "offsets_fetched=excluded.offsets_fetched, exhausted=excluded.exhausted, "
            "last_fetched_at=excluded.last_fetched_at, capped_at_max_offset=excluded.capped_at_max_offset",
            (
                query,
                json.dumps(state["offsets_fetched"]),
                int(state["exhausted"]),
                state["last_fetched_at"],
                int(state["capped_at_max_offset"]),
            ),
        )
        self._search_db.commit()

    def load_streaming_history(self, stats):
        """Atomically replace persisted streaming-history aggregates."""
        if not isinstance(stats, StreamingHistoryStats):
            raise TypeError("stats must be a StreamingHistoryStats instance")

        now = time.time()
        artist_rows = [
            (
                key,
                stats.artist_display_names.get(key, key),
                item.play_count,
                item.skip_count,
                item.total_ms_played,
                item.weighted_score,
                item.last_played_ts,
                now,
            )
            for key, item in stats.artists.items()
        ]
        track_rows = [
            (
                artist_key,
                track_key,
                stats.track_display_names.get((artist_key, track_key), track_key),
                item.play_count,
                item.skip_count,
                item.total_ms_played,
                item.weighted_score,
                item.last_played_ts,
                now,
            )
            for (artist_key, track_key), item in stats.tracks.items()
        ]
        podcast_rows = [
            (key, item.play_count, item.skip_count, item.total_ms_played, item.weighted_score, item.last_played_ts, now)
            for key, item in stats.podcast_shows.items()
        ]
        audiobook_rows = [
            (key, item.play_count, item.skip_count, item.total_ms_played, item.weighted_score, item.last_played_ts, now)
            for key, item in stats.audiobooks.items()
        ]
        bucket_rows = []
        for artist_key, buckets in (stats.artist_time_buckets or {}).items():
            for (slot, weekday), joint_score in buckets.by_slot_weekday.items():
                slot_score = buckets.by_slot.get(slot, 0.0)
                weekday_score = buckets.by_weekday.get(weekday, 0.0)
                bucket_rows.append((artist_key, slot, weekday, slot_score, weekday_score, joint_score, now))

        db = self._search_db
        try:
            db.execute("BEGIN")
            for table in (
                "streaming_history_artists",
                "streaming_history_tracks",
                "streaming_history_podcasts",
                "streaming_history_audiobooks",
                "streaming_history_artist_buckets",
            ):
                db.execute(f"DELETE FROM {table}")

            db.executemany(
                "INSERT INTO streaming_history_artists "
                "(artist_name, display_name, play_count, skip_count, total_ms_played, weighted_score, last_played_ts, loaded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                artist_rows,
            )
            db.executemany(
                "INSERT INTO streaming_history_tracks "
                "(artist_name, track_name, display_name, play_count, skip_count, total_ms_played, weighted_score, last_played_ts, loaded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                track_rows,
            )
            db.executemany(
                "INSERT INTO streaming_history_podcasts "
                "(show_name, play_count, skip_count, total_ms_played, weighted_score, last_played_ts, loaded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                podcast_rows,
            )
            db.executemany(
                "INSERT INTO streaming_history_audiobooks "
                "(title, play_count, skip_count, total_ms_played, weighted_score, last_played_ts, loaded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                audiobook_rows,
            )
            db.executemany(
                "INSERT INTO streaming_history_artist_buckets "
                "(artist_name, slot_label, weekday_label, slot_score, weekday_score, joint_score, loaded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                bucket_rows,
            )
            db.commit()
        except Exception:
            db.rollback()
            raise

        logger.info(
            "Loaded streaming history: %d artists, %d tracks, %d podcast shows, %d audiobooks.",
            len(artist_rows), len(track_rows), len(podcast_rows), len(audiobook_rows),
        )

    def get_familiarity_scores(self):
        """Return artist familiarity scores keyed by normalized artist name."""
        rows = self._search_db.execute(
            "SELECT artist_name, weighted_score FROM streaming_history_artists"
        ).fetchall()
        return {artist: score for artist, score in rows}

    def get_streaming_history_artists(self):
        """Return stored artists with display names and weighted scores."""
        return self._search_db.execute(
            "SELECT artist_name, display_name, weighted_score "
            "FROM streaming_history_artists ORDER BY weighted_score DESC"
        ).fetchall()

    def get_top_tracks_by_artist(self):
        """Return each artist's most-played track using original display names."""
        rows = self._search_db.execute(
            "SELECT t.artist_name, a.display_name, t.display_name, t.track_name "
            "FROM streaming_history_tracks AS t "
            "JOIN streaming_history_artists AS a ON a.artist_name = t.artist_name "
            "ORDER BY t.artist_name, t.weighted_score DESC"
        ).fetchall()
        result = {}
        for artist_key, artist_display, track_display, track_key in rows:
            result.setdefault(artist_key, (artist_display or artist_key, track_display or track_key))
        return result

    def get_time_bucket_scores(self, slot, weekday):
        """Return per-artist slot and weekday scores for the requested context."""
        rows = self._search_db.execute(
            "SELECT artist_name, slot_score, weekday_score, joint_score "
            "FROM streaming_history_artist_buckets "
            "WHERE slot_label = ? AND weekday_label = ?",
            (slot, weekday),
        ).fetchall()
        return {
            artist: {"slot_score": slot_score, "weekday_score": weekday_score, "joint_score": joint_score}
            for artist, slot_score, weekday_score, joint_score in rows
        }

    def _get_cached_tracks_for_query(self, query):
        """Return tracks previously associated with a Spotify search query."""
        rows = self._search_db.execute(
            "SELECT t.data FROM query_tracks qt JOIN tracks t ON t.id = qt.track_id WHERE qt.query = ?",
            (query,),
        ).fetchall()
        tracks = []
        for (data,) in rows:
            try:
                track = json.loads(data)
            except (TypeError, json.JSONDecodeError):
                logger.warning("Ignoring invalid cached track JSON for query %r.", query)
                continue
            if isinstance(track, dict):
                tracks.append(track)
        return tracks

    def _store_track_for_query(self, query, track):
        """Store a Spotify track and associate it with a search query."""
        if not isinstance(track, dict) or not track.get("id"):
            return
        self._search_db.execute(
            "INSERT INTO tracks (id, data) VALUES (?, ?) "
            "ON CONFLICT(id) DO UPDATE SET data=excluded.data",
            (track["id"], json.dumps(track, ensure_ascii=False)),
        )
        self._search_db.execute(
            "INSERT OR IGNORE INTO query_tracks (query, track_id) VALUES (?, ?)",
            (query, track["id"]),
        )

    def _is_query_exhausted(self, query):
        """Return whether Spotify pagination for this query is exhausted or capped."""
        row = self._search_db.execute(
            "SELECT exhausted, capped_at_max_offset FROM queries WHERE query = ?",
            (query,),
        ).fetchone()
        return bool(row and (row[0] or row[1]))

    def _paginated_search(self, query, max_results, fresh_ids=None):
        """Fetch Spotify search pages while reusing cached tracks and pagination state."""
        if max_results <= 0:
            return []

        state = self._get_query_state(query)
        if self.cache_ttl_days and state["last_fetched_at"]:
            age_days = (time.time() - state["last_fetched_at"]) / 86400
            if age_days > self.cache_ttl_days:
                state.update(offsets_fetched=[], exhausted=False, capped_at_max_offset=False)

        collected = self._get_cached_tracks_for_query(query)
        if state["exhausted"] or len(collected) >= max_results:
            return collected[:max_results]

        offset = max(state["offsets_fetched"]) + self.search_limit if state["offsets_fetched"] else 0
        unknown_429_retries = 0

        while offset < max_results and offset <= self._max_offset:
            try:
                results = self.sp.search(q=query, type="track", limit=self.search_limit, offset=offset)
            except spotipy.exceptions.SpotifyException as exc:
                if getattr(exc, "http_status", None) == 429:
                    headers = getattr(exc, "headers", None) or {}
                    raw_retry = headers.get("Retry-After")
                    try:
                        retry_after = float(raw_retry) if raw_retry is not None else None
                    except (TypeError, ValueError):
                        retry_after = None

                    if retry_after is not None and 0 <= retry_after <= self.RATE_LIMIT_AUTO_RETRY_THRESHOLD:
                        time.sleep(retry_after + 1)
                        continue
                    if retry_after is None and unknown_429_retries < self.MAX_UNKNOWN_429_RETRIES:
                        unknown_429_retries += 1
                        delay = self.request_sleep * (2 ** unknown_429_retries)
                        logger.warning("Spotify returned a 429 without Retry-After; retrying in %.1fs.", delay)
                        time.sleep(delay)
                        continue

                    self._save_query_state(query, state)
                    raise

                logger.exception("Spotify search failed for query %r.", query)
                self._save_query_state(query, state)
                break
            except requests.RequestException:
                logger.exception("Network error during Spotify search for query %r.", query)
                self._save_query_state(query, state)
                break

            time.sleep(self.request_sleep)
            unknown_429_retries = 0
            items = (results or {}).get("tracks", {}).get("items", [])
            state["offsets_fetched"].append(offset)
            state["last_fetched_at"] = time.time()

            if not items:
                state["exhausted"] = True
                self._save_query_state(query, state)
                break

            for track in items:
                if not isinstance(track, dict) or not track.get("id"):
                    continue
                self._store_track_for_query(query, track)
                collected.append(track)
                if fresh_ids is not None:
                    fresh_ids.add(track["id"])

            self._save_query_state(query, state)
            offset += self.search_limit

        if not state["exhausted"] and offset > self._max_offset:
            state["capped_at_max_offset"] = True
            self._save_query_state(query, state)

        return collected[:max_results]

    @staticmethod
    def _track_artist_names(track):
        """Return nonempty artist names from a Spotify track object."""
        artists = track.get("artists") if isinstance(track, dict) else None
        if not isinstance(artists, list):
            return []
        return [artist["name"] for artist in artists if isinstance(artist, dict) and artist.get("name")]

    def resolve_seed_track_ids(self):
        """Resolve configured seed artist/title pairs to Spotify track IDs."""
        track_ids = []
        for artist_name, track_name in self.seed_queries.items():
            if not isinstance(artist_name, str) or not isinstance(track_name, str):
                logger.warning("Skipping invalid seed entry: %r -> %r.", artist_name, track_name)
                continue

            response = self.sp.search(q=f"{track_name} {artist_name}", type="track", limit=10)
            time.sleep(self.request_sleep)
            items = (response or {}).get("tracks", {}).get("items", [])
            items = [item for item in items if isinstance(item, dict) and item.get("id")]
            if not items:
                logger.warning("No Spotify track results for %r by %r.", track_name, artist_name)
                continue

            expected_artist = normalize_artist_name(artist_name)
            expected_title = normalize_track_title(track_name)

            def score(item):
                names = [normalize_artist_name(name) for name in self._track_artist_names(item)]
                artist_match = any(
                    expected_artist == name or expected_artist in name or name in expected_artist
                    for name in names if expected_artist and name
                )
                title = normalize_track_title(item.get("name", ""))
                title_match = difflib.SequenceMatcher(None, expected_title, title).ratio()
                return artist_match, title_match

            best = max(items, key=score)
            artist_match, title_match = score(best)
            if not artist_match or title_match < 0.6:
                logger.warning(
                    "Uncertain seed match for %r by %r: Spotify returned %r (title similarity %.2f).",
                    track_name, artist_name, best.get("name"), title_match,
                )
            track_ids.append(best["id"])
            logger.info("Resolved seed %r by %r to Spotify track %s.", track_name, artist_name, best["id"])
        return track_ids

    def get_seed_artist_ids_and_genres(self, seed_track_ids):
        """Load seed tracks, artist names, and their Last.fm genre tags."""
        self.seed_tracks = []
        self.seed_artist_names = []
        self.seed_genre_set = set()
        artist_ids = []

        for track_id in seed_track_ids:
            track = self.sp.track(track_id)
            if not isinstance(track, dict):
                logger.warning("Spotify did not return seed track %s.", track_id)
                continue
            artists = self._track_artist_names(track)
            artist_objects = track.get("artists", [])
            if not artists or not artist_objects or not artist_objects[0].get("id"):
                logger.warning("Seed track %s has no usable primary artist.", track_id)
                continue
            self.seed_tracks.append(track)
            self.seed_artist_names.append(artists[0])
            artist_ids.append(artist_objects[0]["id"])
            time.sleep(self.request_sleep)

        genre_counter = Counter()
        for artist_name in self.seed_artist_names:
            genre_counter.update(self.lastfm_top_tags(artist_name, limit=20))
            time.sleep(self.request_sleep)

        if not genre_counter:
            for artist_id in artist_ids:
                artist = self.sp.artist(artist_id)
                if isinstance(artist, dict):
                    genre_counter.update(
                        str(genre).casefold()
                        for genre in artist.get("genres", [])
                        if isinstance(genre, str)
                    )
                time.sleep(self.request_sleep)

        self.seed_genre_set = set(genre_counter)
        return artist_ids, genre_counter

    def _ensure_seed_similar_artists(self):
        """Load first-hop and bounded second-hop Last.fm similar artists."""
        if self._similar_artist_pool_names is not None:
            return

        usable_tags = self.seed_genre_set - self.LASTFM_JUNK_TAGS - self.excluded_genres
        thin_tags = len(usable_tags) < self.thin_tag_genre_threshold
        count = self.thin_tag_similar_artist_count if thin_tags else self.lastfm_similar_artist_count

        first_hop = []
        first_hop_seen = set()
        match_by_artist = {}
        seed_keys = {normalize_artist_name(name) for name in self.seed_artist_names}

        for seed_artist in self.seed_artist_names:
            for name, match in self.lastfm_similar_artists(seed_artist, limit=count):
                key = normalize_artist_name(name)
                if not key:
                    continue
                match_by_artist[key] = max(match_by_artist.get(key, 0.0), match)
                if key not in first_hop_seen and key not in seed_keys and key not in self.excluded_artists:
                    first_hop_seen.add(key)
                    first_hop.append(name)
            time.sleep(self.request_sleep)

        second_hop = []
        second_hop_seen = set(first_hop_seen | seed_keys)
        remaining_budget = self.max_2hop_artists

        for parent in first_hop:
            if remaining_budget <= 0:
                break
            for name, match in self.lastfm_similar_artists(parent, limit=min(count, remaining_budget)):
                key = normalize_artist_name(name)
                if not key:
                    continue
                match_by_artist[key] = max(match_by_artist.get(key, 0.0), match)
                if key in second_hop_seen or key in self.excluded_artists:
                    continue
                second_hop_seen.add(key)
                second_hop.append(name)
                remaining_budget -= 1
                if remaining_budget <= 0:
                    break
            time.sleep(self.request_sleep)

        self._similar_artist_pool_names = first_hop
        self._two_hop_artist_pool_names = second_hop
        self.seed_similar_match = match_by_artist

        if thin_tags:
            logger.info(
                "Seed tags are sparse; using %d first-hop and %d second-hop similar artists.",
                len(first_hop), len(second_hop),
            )

    def _is_allowed_artist(self, artist_name):
        """Return whether an artist is absent from the configured exclusion list."""
        return normalize_artist_name(artist_name) not in self.excluded_artists

    def _run_concurrent(self, callables, max_workers):
        """Run callables with a bounded number of tasks in flight."""
        if not callables:
            return []

        limit = min(len(callables), max(1, max_workers), self.lastfm_max_workers)
        futures = [None] * len(callables)
        pending = {}
        next_index = 0

        def submit_next():
            nonlocal next_index
            if next_index < len(callables):
                future = self._lastfm_executor.submit(callables[next_index])
                pending[future] = next_index
                next_index += 1

        for _ in range(limit):
            submit_next()

        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                index = pending.pop(future)
                futures[index] = future
                submit_next()

        return cast(list[Future], futures)

    def _fetch_artist_infos_batch(self, artist_names, max_workers):
        """Fetch listeners first, then fetch tags and top tracks for eligible artists."""
        names = list(dict.fromkeys(name for name in artist_names if isinstance(name, str) and name.strip()))
        results = {}
        missing = []

        with self._cache_lock:
            for name in names:
                key = normalize_artist_name(name)
                cached = self._artist_info_cache.get(key)
                if cached is None:
                    missing.append((key, name))
                else:
                    results[name] = cached

        if not missing:
            return results

        listener_futures = self._run_concurrent(
            [lambda name=name: self.lastfm_artist_listeners(name) for _, name in missing],
            max_workers,
        )
        listeners = {}
        for (key, name), future in zip(missing, listener_futures):
            try:
                listeners[key] = future.result()
            except Exception:
                logger.exception("Failed to fetch Last.fm listener count for %s.", name)
                listeners[key] = None

        survivors = [
            (key, name) for key, name in missing
            if listeners[key] is None or listeners[key] >= self.min_artist_listeners
        ]
        tasks = (
            [("tags", key, name) for key, name in survivors]
            + [("top_tracks", key, name) for key, name in survivors]
        )
        futures = self._run_concurrent(
            [
                (lambda kind=kind, name=name: self.lastfm_top_tags(name, limit=20))
                if kind == "tags"
                else (lambda name=name: self.lastfm_artist_top_track_names(name))
                for kind, _key, name in tasks
            ],
            max_workers,
        )

        fetched = {}
        for (kind, key, _name), future in zip(tasks, futures):
            try:
                value = future.result()
            except Exception:
                logger.exception("Failed to fetch Last.fm %s for %s.", kind, _name)
                value = [] if kind == "tags" else set()
            fetched.setdefault(key, {})[kind] = value

        for key, name in missing:
            data = fetched.get(key, {})
            info = {
                "tags": set(data.get("tags", [])),
                "listeners": listeners[key],
                "top_tracks": set(data.get("top_tracks", set())),
            }
            with self._cache_lock:
                self._artist_info_cache[key] = info
            results[name] = info

        return results

    @classmethod
    def _fast_language_hint(cls, text, allowed_languages):
        """Return a confident language decision, or None when detection is needed."""
        letters = sum(char.isalpha() for char in text)
        if letters < 4:
            return None

        non_latin = len(cls._NON_LATIN_SCRIPT_RE.findall(text))
        if non_latin / letters > 0.5:
            if not allowed_languages.intersection(cls._NON_LATIN_LANGUAGES):
                return False
            return None

        for language in allowed_languages:
            pattern = cls._STOPWORD_PATTERNS.get(language)
            if pattern and pattern.search(text):
                return True
        return None

    @lru_cache(maxsize=20_000)
    def is_allowed_language(self, text, allowed_languages):
        """Return whether a title is in an allowed language; uncertain short titles pass."""
        hint = self._fast_language_hint(text, allowed_languages)
        if hint is not None:
            return hint
        try:
            return detect(text) in allowed_languages
        except LangDetectException:
            return True

    def _genre_similarity(self, artist_tags):
        """Return Jaccard similarity between candidate and seed genre tags."""
        tags = {str(tag).casefold() for tag in artist_tags}
        seeds = {str(tag).casefold() for tag in self.seed_genre_set}
        if not tags or not seeds:
            return 0.0
        union = tags | seeds
        return len(tags & seeds) / len(union) if union else 0.0

    def _candidate_artist_name(self, track):
        """Return the primary artist name for a valid Spotify track, or None."""
        names = self._track_artist_names(track)
        return names[0] if names else None

    def build_candidate_pool(self, genre_counter):
        """Build a deduplicated candidate pool using genres and one- and two-hop artists."""
        candidates = {}
        fresh_track_ids = set()
        discovered_genres = Counter()
        processed_artists = set()
        seed_track_keys = {
            (
                normalize_artist_name(self._candidate_artist_name(track) or ""),
                normalize_track_title(track.get("name", "")),
            )
            for track in self.seed_tracks
        }

        def add_track(track, expected_artist=None):
            if not isinstance(track, dict) or not track.get("id") or not track.get("name"):
                return
            artist = self._candidate_artist_name(track)
            if not artist or not self._is_allowed_artist(artist):
                return
            if expected_artist and not any(
                normalize_artist_name(name) == normalize_artist_name(expected_artist)
                for name in self._track_artist_names(track)
            ):
                return
            title_key = normalize_track_title(track["name"])
            artist_key = normalize_artist_name(artist)
            if not title_key or (artist_key, title_key) in seed_track_keys:
                return
            if not self.is_allowed_language(track["name"], frozenset(self.allowed_languages)):
                return
            candidates.setdefault((artist_key, title_key), track)

        def fresh_fraction():
            ids = {track["id"] for track in candidates.values()}
            return len(ids & fresh_track_ids) / len(ids) if ids else 0.0

        def fetch_round_robin(query_to_artist, per_query_cap):
            requested = {query: 0 for query in query_to_artist}
            active = set(query_to_artist)
            while active and len(candidates) < self.target_total_candidates:
                for query in list(active):
                    if len(candidates) >= self.target_total_candidates:
                        break
                    target = min(requested[query] + self.results_per_turn, per_query_cap)
                    results = self._paginated_search(query, target, fresh_ids=fresh_track_ids)
                    requested[query] = target
                    expected_artist = query_to_artist[query]
                    for track in results:
                        add_track(track, expected_artist)
                        if len(candidates) >= self.target_total_candidates:
                            break
                    if len(results) < target or target >= per_query_cap:
                        active.discard(query)

        blocklist = self.LASTFM_JUNK_TAGS | self.excluded_genres
        genres = [
            genre for genre in genre_counter
            if isinstance(genre, str) and genre.casefold() not in blocklist
        ]
        genres.sort(key=lambda genre: (-genre.count(" "), -genre_counter[genre]))
        active_genres = [genre for genre in genres if not self._is_query_exhausted(f'genre:"{genre}"')]
        exhausted_genres = [genre for genre in genres if self._is_query_exhausted(f'genre:"{genre}"')]
        ordered_genres = active_genres + exhausted_genres
        pool_limit = max(0, self.genres_to_use * self.genre_pool_multiplier)
        genre_pool = ordered_genres[:pool_limit]
        selected_genres = random.sample(genre_pool, min(self.genres_to_use, len(genre_pool)))
        if not selected_genres and not genre_counter:
            selected_genres = ["pop"]

        fetch_round_robin(
            {f'genre:"{genre}"': None for genre in selected_genres},
            self.max_results_per_genre,
        )
        fetch_round_robin(
            {f'artist:"{artist}"': artist for artist in self.seed_artist_names},
            self.max_results_per_seed_artist,
        )

        self._ensure_seed_similar_artists()
        first_hop = self._similar_artist_pool_names or []
        second_hop = self._two_hop_artist_pool_names or []
        if first_hop:
            fetch_round_robin(
                {f'artist:"{artist}"': artist for artist in first_hop},
                self.max_results_per_similar_artist,
            )
        if second_hop:
            fetch_round_robin(
                {f'artist:"{artist}"': artist for artist in second_hop},
                self.max_results_per_similar_artist,
            )

        tried_genres = set(selected_genres)
        remaining_genres = [genre for genre in ordered_genres if genre not in tried_genres]
        for expansion_round in range(1, max(0, self.max_candidate_expansion_rounds) + 1):
            if len(candidates) >= self.target_total_candidates and fresh_fraction() >= self.min_fresh_fraction:
                break

            multiplier = expansion_round + 1
            fetch_round_robin(
                {f'artist:"{artist}"': artist for artist in self.seed_artist_names},
                self.max_results_per_seed_artist * multiplier,
            )
            for artists in (first_hop, second_hop):
                if artists:
                    fetch_round_robin(
                        {f'artist:"{artist}"': artist for artist in artists},
                        self.max_results_per_similar_artist * multiplier,
                    )

            if not remaining_genres:
                artists = {
                    self._candidate_artist_name(track)
                    for track in candidates.values()
                } - processed_artists
                artists.discard(None)
                processed_artists.update(artists)
                infos = self._fetch_artist_infos_batch(artists, self.lastfm_max_workers)
                for artist, info in infos.items():
                    if self._genre_similarity(info["tags"]) <= 0:
                        continue
                    for tag in info["tags"]:
                        tag = str(tag).casefold()
                        if tag not in blocklist:
                            discovered_genres[tag] += 1
                remaining_genres = [
                    genre for genre, count in discovered_genres.most_common()
                    if genre not in tried_genres
                    and count >= self.min_discovered_genre_count
                    and not self._is_query_exhausted(f'genre:"{genre}"')
                ]

            batch = remaining_genres[:max(1, self.genres_to_use)]
            remaining_genres = remaining_genres[len(batch):]
            tried_genres.update(batch)
            if batch:
                fetch_round_robin(
                    {f'genre:"{genre}"': None for genre in batch},
                    self.max_results_per_genre * multiplier,
                )

        final_fraction = fresh_fraction()
        if len(candidates) < self.target_total_candidates:
            logger.warning(
                "Candidate pool reached %d/%d tracks after expansion.",
                len(candidates), self.target_total_candidates,
            )
        if final_fraction < self.min_fresh_fraction:
            logger.warning(
                "Only %.0f%% of candidates were newly fetched; target is %.0f%%.",
                final_fraction * 100, self.min_fresh_fraction * 100,
            )
        return list(candidates.values())

    def _apply_popularity_bias(self, scored):
        """Adjust similarity using tied popularity percentiles within this run."""
        if self.popularity_bias == 0 or len(scored) < 2:
            return

        known = [item for item in scored if item["track_listeners"] is not None]
        if len(known) < 2:
            return
        known.sort(key=lambda item: item["track_listeners"])

        n = len(known)
        index = 0
        while index < n:
            end = index + 1
            while end < n and known[end]["track_listeners"] == known[index]["track_listeners"]:
                end += 1
            percentile = ((index + end - 1) / 2) / (n - 1)
            adjustment = self.popularity_bias * (percentile - 0.5) * 2
            for item in known[index:end]:
                item["similarity"] = max(0.0, min(1.0, item["similarity"] + adjustment))
            index = end

    def filter_and_rank(self, candidates):
        """Filter candidates, score them, enforce the per-artist cap, and save picks."""
        pool = list(candidates)
        random.shuffle(pool)
        self._ensure_seed_similar_artists()
        weight_min, weight_max = self.similarity_blend_weight_range
        blend_weight = random.uniform(weight_min, weight_max)
        oversample_target = self.result_limit * self.similarity_oversample_factor
        scored = []
        workers = self.lastfm_max_workers
        clean_batches = 0

        for start in range(0, len(pool), self.lastfm_batch_size):
            if len(scored) >= oversample_target:
                break

            batch = []
            for track in pool[start:start + self.lastfm_batch_size]:
                if not isinstance(track, dict) or not track.get("name") or not track.get("id"):
                    continue
                if not isinstance(track.get("duration_ms"), int) or track["duration_ms"] <= self.MIN_TRACK_DURATION_MS:
                    continue
                if not self._candidate_artist_name(track):
                    continue
                batch.append(track)
            if not batch:
                continue

            artist_names = list(dict.fromkeys(self._candidate_artist_name(track) for track in batch))
            artist_infos = self._fetch_artist_infos_batch(artist_names, workers)
            survivors = []

            for track in batch:
                artist = self._candidate_artist_name(track)
                info = artist_infos.get(artist, {"tags": set(), "listeners": None, "top_tracks": set()})
                artist_tags = {str(tag).casefold() for tag in info["tags"]}
                if artist_tags & self.excluded_genres:
                    continue
                listeners = info["listeners"]
                if listeners is not None and listeners < self.min_artist_listeners:
                    continue
                if normalize_track_title(track["name"]) in info["top_tracks"]:
                    continue
                survivors.append((track, info))

            listener_futures = self._run_concurrent(
                [
                    lambda track=track: self.lastfm_track_listeners(
                        self._candidate_artist_name(track), track["name"]
                    )
                    for track, _info in survivors
                ],
                workers,
            )
            track_listeners = []
            for future in listener_futures:
                try:
                    track_listeners.append(future.result())
                except Exception:
                    logger.exception("Failed to fetch track listener count.")
                    track_listeners.append(None)

            for (track, info), listeners in zip(survivors, track_listeners):
                if len(scored) >= oversample_target:
                    break
                if listeners is not None and listeners > self.lastfm_listener_ceiling:
                    continue
                if listeners is not None and listeners < self.min_track_listeners:
                    continue

                similarity = self._genre_similarity(info["tags"])
                artist_key = normalize_artist_name(self._candidate_artist_name(track))
                lastfm_match = self.seed_similar_match.get(artist_key)
                if lastfm_match is not None:
                    similarity = (1 - blend_weight) * similarity + blend_weight * lastfm_match

                scored.append({
                    "track": track,
                    "track_listeners": listeners,
                    "artist_listeners": info["listeners"],
                    "similarity": max(0.0, min(1.0, similarity)),
                })

            throttle_rate = self._lastfm_monitor.recent_throttle_rate
            if throttle_rate >= self._lastfm_monitor.throttle_rate_alert:
                workers = max(1, workers // 2)
                cooldown = min(30.0, self.lastfm_backoff_base * (2 ** (self.lastfm_max_workers - workers)))
                logger.warning("Reducing Last.fm concurrency to %d worker(s); cooling down %.1fs.", workers, cooldown)
                time.sleep(cooldown)
                clean_batches = 0
            else:
                clean_batches += 1
                if clean_batches >= 3 and workers < self.lastfm_max_workers:
                    workers += 1
                    clean_batches = 0

        self._apply_popularity_bias(scored)
        scored.sort(key=lambda item: item["similarity"], reverse=True)

        picks = []
        artist_counts = Counter()
        for item in scored:
            artist = self._candidate_artist_name(item["track"])
            key = normalize_artist_name(artist)
            if artist_counts[key] >= self.max_tracks_per_artist:
                continue
            picks.append(item)
            artist_counts[key] += 1
            if len(picks) >= self.result_limit:
                break

        if len(picks) < self.result_limit:
            logger.warning("Only %d of %d requested tracks passed filtering.", len(picks), self.result_limit)
        self.picks = picks
        return picks

    def run_pipeline(self, seed_queries=None):
        """Run the recommendation pipeline and return structured run metadata."""
        if seed_queries is not None:
            if not isinstance(seed_queries, dict) or not seed_queries:
                raise ValueError("seed_queries must be a non-empty dictionary")
            self.seed_queries = dict(seed_queries)

        self.seed_tracks = []
        self.seed_artist_names = []
        self.seed_genre_set = set()
        self.picks = []
        self._similar_artist_pool_names = None
        self._two_hop_artist_pool_names = None
        self.seed_similar_match = {}
        self._artist_info_cache.clear()

        seed_track_ids = self.resolve_seed_track_ids()
        if not seed_track_ids:
            raise RuntimeError("No seed tracks resolved from Spotify.")

        seed_artist_ids, genre_counter = self.get_seed_artist_ids_and_genres(seed_track_ids)
        candidates = self.build_candidate_pool(genre_counter)
        picks = self.filter_and_rank(candidates)

        return {
            "seed_track_ids": list(seed_track_ids),
            "seed_artist_ids": list(seed_artist_ids),
            "seed_artists": list(self.seed_artist_names),
            "genres": genre_counter,
            "candidates": candidates,
            "picks": picks,
            "lastfm_health": self.lastfm_health_summary(),
        }

    def print_results(self):
        """Print the current recommendations to stdout."""
        print("\n--- Recommended Tracks ---")
        for index, pick in enumerate(self.picks, 1):
            track = pick["track"]
            artists = self._track_artist_names(track)
            artist = ", ".join(artists) if artists else "Unknown artist"
            url = track.get("external_urls", {}).get("spotify", "")
            track_listeners = pick["track_listeners"]
            artist_listeners = pick["artist_listeners"]
            print(f"{index}. {track['name']} by {artist}")
            print(
                f"   Track listeners: {track_listeners if track_listeners is not None else '?'} | "
                f"Artist listeners: {artist_listeners if artist_listeners is not None else '?'} | "
                f"Similarity: {pick['similarity']:.2f} | Link: {url}"
            )

    def _bearer_headers(self):
        """Return authorization headers for direct Spotify Web API requests."""
        manager = getattr(self.sp, "auth_manager", None)
        if manager is None:
            raise RuntimeError("Spotify authentication manager is unavailable")
        token = manager.get_access_token(as_dict=False)
        if not token:
            raise RuntimeError("Spotify did not provide an access token")
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _existing_playlist_names(self):
        """Return names of the current user's playlists."""
        names = set()
        page = self.sp.current_user_playlists(limit=50)
        while page:
            names.update(item["name"] for item in page.get("items", []) if item.get("name"))
            page = self.sp.next(page) if page.get("next") else None
        return names

    def _unique_playlist_name(self, base_name):
        """Return an unused playlist name prefixed with the first seed title."""
        if not self.seed_queries:
            prefix = "SongRadio"
        else:
            prefix = next(iter(self.seed_queries.values()))
        base = f"{prefix}: {base_name}"
        existing = self._existing_playlist_names()
        if base not in existing:
            return base
        suffix = 2
        while f"{base} #{suffix}" in existing:
            suffix += 1
        return f"{base} #{suffix}"

    def _playlist_uris(self):
        """Return valid, unique Spotify URIs for the current playlist contents."""
        uris = []
        seen = set()
        if self.include_seed_tracks:
            tracks = self.seed_tracks
        else:
            tracks = []
        tracks += [pick["track"] for pick in self.picks]

        for track in tracks:
            uri = track.get("uri") if isinstance(track, dict) else None
            if uri and uri not in seen:
                seen.add(uri)
                uris.append(uri)
        return uris

    @staticmethod
    def _raise_spotify_error(response, action):
        """Raise an HTTP error with context when a Spotify request fails."""
        try:
            response.raise_for_status()
        except requests.RequestException:
            logger.exception("Spotify request failed while %s (HTTP %s).", action, response.status_code)
            raise

    def _create_playlist(self, name, description=None):
        """Create a private playlist and return its Spotify response."""
        body = {"name": name, "public": False}
        if description is not None:
            body["description"] = description[:300]

        response = requests.post(
            "https://api.spotify.com/v1/me/playlists",
            headers=self._bearer_headers(),
            json=body,
            timeout=15,
        )
        self._raise_spotify_error(response, "creating a playlist")
        playlist = response.json()
        if not isinstance(playlist, dict) or not playlist.get("id"):
            raise RuntimeError("Spotify returned a playlist response without an ID.")
        return playlist

    def _write_playlist_items(self, playlist_id, uris):
        """Replace playlist contents and append further items in API-sized batches."""
        if not uris:
            raise ValueError("Cannot write an empty playlist.")

        url = f"https://api.spotify.com/v1/playlists/{playlist_id}/items"
        headers = self._bearer_headers()
        response = requests.put(url, headers=headers, json={"uris": uris[:100]}, timeout=15)
        self._raise_spotify_error(response, "replacing playlist items")

        for start in range(100, len(uris), 100):
            response = requests.post(
                url,
                headers=headers,
                json={"uris": uris[start:start + 100]},
                timeout=15,
            )
            self._raise_spotify_error(response, "adding playlist items")

    def save_as_playlist(self, base_name="Song Radio"):
        """Create a private playlist containing the current picks and optional seeds."""
        uris = self._playlist_uris()
        if not uris:
            raise RuntimeError("No tracks are available to save.")

        name = self._unique_playlist_name(base_name)
        playlist = self._create_playlist(name)
        self._write_playlist_items(playlist["id"], uris)
        url = playlist.get("external_urls", {}).get("spotify")
        logger.info("Created playlist %r: %s", name, url or playlist["id"])
        return playlist["id"]

    def playlist_exists(self, playlist_id):
        """Return whether the playlist exists and is accessible to this account."""
        if not isinstance(playlist_id, str) or not playlist_id.strip():
            raise ValueError("playlist_id must be a non-empty string")

        response = requests.get(
            f"https://api.spotify.com/v1/playlists/{playlist_id}",
            headers=self._bearer_headers(),
            params={"fields": "id"},
            timeout=15,
        )
        if response.status_code == 404:
            return False
        self._raise_spotify_error(response, f"checking playlist {playlist_id}")
        return True

    def create_empty_playlist(self, name, description=None):
        """Create an empty private playlist and return its Spotify ID."""
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Playlist name must be a non-empty string")
        playlist = self._create_playlist(name.strip(), description)
        return playlist["id"]

    def update_playlist(self, playlist_id, description=None):
        """Replace playlist contents and optionally update its description."""
        if not isinstance(playlist_id, str) or not playlist_id.strip():
            raise ValueError("playlist_id must be a non-empty string")

        uris = self._playlist_uris()
        if not uris:
            raise RuntimeError("No tracks are available; the playlist was not updated.")

        self._write_playlist_items(playlist_id, uris)

        if description is not None:
            response = requests.put(
                f"https://api.spotify.com/v1/playlists/{playlist_id}",
                headers=self._bearer_headers(),
                json={"description": description[:300]},
                timeout=15,
            )
            self._raise_spotify_error(response, f"updating playlist {playlist_id} description")

        logger.info("Updated playlist https://open.spotify.com/playlist/%s", playlist_id)

    def close(self):
        """Shut down the worker pool, close HTTP sessions, and close SQLite connections."""
        if getattr(self, "_closed", False):
            return
        self._closed = True

        self._lastfm_executor.shutdown(wait=True, cancel_futures=True)
        with self._sessions_lock:
            sessions = list(self._sessions)
            self._sessions.clear()
        for session in sessions:
            session.close()

        self._lastfm_db.close()
        self._search_db.close()