"""
Song Radio (Low-Mainstream Edition)

Spotify's Development Mode API was heavily restricted in Feb 2026 (batch
endpoints, "popularity", and the old playlist endpoints were removed).
https://developer.spotify.com/documentation/web-api/tutorials/february-2026-migration-guide

Playlist creation/population therefore uses raw requests
(POST /me/playlists, POST /playlists/{id}/items), since spotipy still uses
the removed endpoints internally (/users/{id}/playlists, /playlists/{id}/tracks).
"""

import re
import json
import difflib
import random
import time
from collections import Counter

import requests
import spotipy
from langdetect import LangDetectException, detect
from spotipy.oauth2 import SpotifyOAuth


class SongRadio:
    GENERIC_GENRE_BLOCKLIST = {
        "pop", "rock", "soul", "funk", "hip hop", "rap", "dance", "r&b",
        "classic rock", "alternative", "indie", "singer-songwriter",
    }
    LASTFM_JUNK_TAGS = {
        "seen live", "favorites", "favourite", "favourites", "amazing", "awesome",
        "love", "beautiful", "usa", "american", "uk", "british", "australian",
        "male vocalists", "female vocalists", "instrumental", "cover", "covers",
        "guilty pleasure", "guilty pleasures", "classic", "classics", "legend",
        "legendary", "00s", "10s", "20s", "30s", "40s", "50s", "60s", "70s",
        "80s", "90s", "under 2000 listeners", "spotify",
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

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        lastfm_api_key: str,
        seed_queries: dict,
        result_limit: int = 15,
        genres_to_use: int = 5,
        min_artist_listeners: int = 1_000,
        lastfm_listener_ceiling: int = 150_000,
        artist_top_hit_exclude_n: int = 5,
        allowed_languages: list = ["en", "de"],
        excluded_artists: list = [],
        excluded_genres: list = [],
        max_results_per_genre: int = 300,
        max_results_per_seed_artist: int = 200,
        max_total_candidates: int = 150,
        include_seed_tracks: bool = True,
        cache_path: str = "spotify_search_cache.json",
        cache_ttl_days: float = 30,
        genre_pool_multiplier: int = 2,
        min_track_listeners: int = 0,
    ):
        """Initializes the Spotify/Last.fm clients and stores the configuration.

        Args:
            client_id: Spotify app client ID.
            client_secret: Spotify app client secret.
            redirect_uri: Spotify app redirect URI (for the OAuth flow).
            lastfm_api_key: Last.fm API key (genre/listener count data).
            seed_queries: Mapping {artist name: song title} used as the
                starting point for the recommendations.
            result_limit: Number of tracks returned at the end.
            genres_to_use: Number of (most specific) genres used for the
                genre search.
            min_artist_listeners: Minimum total listener count an artist must
                have on Last.fm to be considered a candidate.
            lastfm_listener_ceiling: Maximum listener count a single track may
                have on Last.fm to still count as a "hidden gem".
            artist_top_hit_exclude_n: Number of an artist's most-listened
                tracks that count as "used up" and get excluded.
            allowed_languages: Set of allowed language codes (e.g. "en", "de")
                for the song title language detection.
            excluded_artists: Artist names that should never appear as candidates.
            excluded_genres: Genre tags that should never be used as a search
                term and whose artists get hard-excluded.
            max_results_per_genre: Upper bound on search results per genre query.
            max_results_per_seed_artist: Upper bound on search results per
                seed artist query.
            max_total_candidates: Overall candidate cap at which candidate
                gathering is stopped early.
            include_seed_tracks: Whether the seed tracks themselves should be
                included in the playlist created later.
            cache_path: Path to the local JSON file used to persist Spotify
                search results across runs, so identical queries don't
                re-fetch already-seen offsets and instead explore new ones.
            cache_ttl_days: After how many days a cached query's exploration
                state (offsets_fetched/exhausted) is reset so it gets
                re-scanned from offset 0, to catch drift in Spotify's ranking.
                Set to 0 to disable expiration entirely.
            genre_pool_multiplier: Widens the genre candidate pool to
                genres_to_use * genre_pool_multiplier before randomly
                sampling genres_to_use from it, so the same seeds don't
                always search the exact same genres every run.
            min_track_listeners: Minimum Last.fm listener count a single
                track must have to be considered (filters out tracks with
                barely any listeners). 0 disables this floor.

        Returns:
            None.
        """
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
        self.seed_queries = seed_queries
        self.result_limit = result_limit
        self.genres_to_use = genres_to_use
        self.min_artist_listeners = min_artist_listeners
        self.lastfm_listener_ceiling = lastfm_listener_ceiling
        self.artist_top_hit_exclude_n = artist_top_hit_exclude_n
        self.allowed_languages = set(allowed_languages)
        self.excluded_artists = {a.lower() for a in excluded_artists}
        self.excluded_genres = {g.lower() for g in excluded_genres}
        self.max_results_per_genre = max_results_per_genre
        self.max_results_per_seed_artist = max_results_per_seed_artist
        self.max_total_candidates = max_total_candidates
        self.include_seed_tracks = include_seed_tracks

        self.search_limit = 10  # Spotify maximum since Feb 2026
        self.request_sleep = 0.2  # a bit generous, to avoid bursts
        self._max_offset = 990  # safety cap for offset pagination

        self._artist_stats_cache = {}
        self._top_tracks_cache = {}
        self.seed_tracks = []  # full track objects, for playlist inclusion
        self.seed_artist_names = []
        self.picks = []

        self.cache_path = cache_path
        self._search_cache = self._load_search_cache()
        self.cache_ttl_days = cache_ttl_days
        self.genre_pool_multiplier = genre_pool_multiplier
        self.min_track_listeners = min_track_listeners

    # --- Last.fm helpers -----------------------------------------------

    def _lastfm_get(self, method, **params):
        """Performs a GET request against the Last.fm API.

        Last.fm has no known lockout risk like Spotify's rate limiting, so a
        transient network hiccup (e.g. a read timeout) is retried once after
        a short pause before giving up. On repeated failure, an empty dict is
        returned rather than raising, so a single flaky Last.fm call doesn't
        crash an entire run and lose an already-built candidate pool.

        Args:
            method: Name of the Last.fm API method (e.g. "artist.getInfo").
            **params: Additional query parameters for the request
                (e.g. artist=..., track=..., limit=...).

        Returns:
            The JSON-decoded response as a dict, or an empty dict if both
            attempts failed.
        """
        for attempt in range(2):
            try:
                resp = requests.get(
                    "https://ws.audioscrobbler.com/2.0/",
                    params={"method": method, "api_key": self.lastfm_api_key, "format": "json", **params},
                    timeout=5,
                )
                return resp.json()
            except requests.RequestException as e:
                if attempt == 0:
                    print(f"Last.fm request failed ({e}), retrying once...")
                    time.sleep(1)
                else:
                    print(f"Last.fm request failed again, giving up for this call: {e}")
                    return {}

    def lastfm_top_tags(self, artist_name, limit=8):
        """Fetches the most-assigned Last.fm tags (genre/style) for an artist.

        Args:
            artist_name: Name of the artist.
            limit: Maximum number of tags returned.

        Returns:
            List of tag names (lowercased), in descending order of frequency.
            Empty list if no tags were found or the request failed.
        """
        data = self._lastfm_get("artist.getTopTags", artist=artist_name)
        tags = data.get("toptags", {}).get("tag", [])
        return [t["name"].lower() for t in tags[:limit] if t.get("name")]

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
        if artist_name in self._artist_stats_cache:
            return self._artist_stats_cache[artist_name]
        data = self._lastfm_get("artist.getInfo", artist=artist_name)
        try:
            listeners = int(data["artist"]["stats"]["listeners"])
        except (KeyError, ValueError, TypeError):
            listeners = None
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
        if artist_name in self._top_tracks_cache:
            return self._top_tracks_cache[artist_name]
        data = self._lastfm_get("artist.getTopTracks", artist=artist_name, limit=self.artist_top_hit_exclude_n)
        tracks = data.get("toptracks", {}).get("track", [])
        names = {t["name"].lower() for t in tracks if t.get("name")}
        self._top_tracks_cache[artist_name] = names
        return names

    @staticmethod
    def is_allowed_language(text, allowed_languages):
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
        fall back to Spotify's artist.genres.

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
            genre_counter.update(self.lastfm_top_tags(artist_name))
            time.sleep(self.request_sleep)

        if not genre_counter:
            for artist_id in artist_ids:
                artist = self.sp.artist(artist_id)
                assert artist is not None, f"artist {artist_id} could not be resolved from its ID"
                genre_counter.update(artist.get("genres", []))
                time.sleep(self.request_sleep)

        return artist_ids, genre_counter

    RATE_LIMIT_AUTO_RETRY_THRESHOLD = 30  # seconds - above this: abort immediately instead of waiting

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

    def _paginated_search(self, query, max_results):
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
            self._save_search_cache()  # persist after every offset, not just at the end
            offset += self.search_limit
        return collected

    def build_candidate_pool(self, genre_counter):
        """Gathers candidate tracks via genre and seed artist search.

        Combines two sources: (1) search by a random sample of genres_to_use
        genres drawn from the genres_to_use * genre_pool_multiplier most
        specific genres in genre_counter, for diversity across other artists
        and across runs; (2) a targeted search for the seed artists themselves, to weight their catalog
        (deep cuts) more heavily. Deduplicates via (artist, normalized title)
        so different versions of the same song don't appear multiple times,
        and excludes the seed tracks themselves as well as excluded_artists /
        disallowed languages.

        Args:
            genre_counter: Counter mapping genre tag -> frequency (e.g. from
                get_seed_artist_ids_and_genres).

        Returns:
            List of Spotify track objects (dicts), one per unique
            (artist, normalized title) key.
        """
        candidates = {}  # dedup_key -> track

        def add_track(track):
            if not track or not track.get("id"):
                return
            artist_name = track["artists"][0]["name"]
            if artist_name.lower() in self.excluded_artists:
                return
            if not self.is_allowed_language(track["name"], self.allowed_languages):
                return
            key = (artist_name.lower(), self.normalize_title(track["name"]))
            candidates.setdefault(key, track)

        # Never let the seed tracks themselves count as a "new recommendation"
        for seed_track in self.seed_tracks:
            key = (seed_track["artists"][0]["name"].lower(), self.normalize_title(seed_track["name"]))
            candidates[key] = None  # placeholder, filtered out below

        # 1) Genre search for diversity across other artists
        blocklist = self.GENERIC_GENRE_BLOCKLIST | self.LASTFM_JUNK_TAGS | self.excluded_genres
        specific_genres = [g for g in genre_counter if g not in blocklist]
        specific_genres.sort(key=lambda g: (-len(g.split()), -genre_counter[g]))
        pool_size = min(len(specific_genres), self.genres_to_use * self.genre_pool_multiplier)
        genre_pool = specific_genres[:pool_size]
        if genre_pool:
            top_genres = random.sample(genre_pool, min(self.genres_to_use, len(genre_pool)))
        else:
            top_genres = [g for g, _ in genre_counter.most_common(self.genres_to_use)] or ["pop"]
        print(f"Genres used: {top_genres}")

        for genre in top_genres:
            if len(candidates) >= self.max_total_candidates:
                break
            for track in self._paginated_search(f'genre:"{genre}"', self.max_results_per_genre):
                add_track(track)

        # 2) Targeted search for the seed artists, to weight their catalog
        #    (deep cuts) more heavily
        for artist_name in self.seed_artist_names:
            if len(candidates) >= self.max_total_candidates:
                break
            for track in self._paginated_search(f'artist:"{artist_name}"', self.max_results_per_seed_artist):
                if any(a["name"].lower() == artist_name.lower() for a in track["artists"]):
                    add_track(track)

        candidates.pop(None, None)
        return [t for t in candidates.values() if t is not None]

    def filter_and_rank(self, candidates):
        """Filters and selects the final recommendations from the candidate pool.

        Filter cascade per candidate:
            1. Last.fm genre tags against excluded_genres.
            2. Artist's total listeners >= min_artist_listeners (lower bound
               only, large artists are allowed as long as the song itself is obscure).
            3. Track is not one of the artist's top hits.
            4. Track listener count <= lastfm_listener_ceiling.
            5. Track listener count >= min_track_listeners.

        Args:
            candidates: List of Spotify track objects (dicts), e.g. from
                build_candidate_pool.

        Returns:
            List of dicts with the keys "track" (Spotify track object),
            "track_listeners" (int or None), and "artist_listeners" (int or
            None). At most self.result_limit entries. Also stored in
            self.picks.
        """
        pool = list(candidates)
        random.shuffle(pool)

        picks = []
        for track in pool:
            if len(picks) >= self.result_limit:
                break

            artist_name = track["artists"][0]["name"]
            track_name = track["name"]

            if self.excluded_genres and set(self.lastfm_top_tags(artist_name, limit=10)) & self.excluded_genres:
                continue

            artist_listeners = self.lastfm_artist_listeners(artist_name)
            if artist_listeners is not None and artist_listeners < self.min_artist_listeners:
                continue

            if track_name.lower() in self.lastfm_artist_top_track_names(artist_name):
                continue

            track_listeners = self.lastfm_track_listeners(artist_name, track_name)
            if track_listeners is not None and track_listeners > self.lastfm_listener_ceiling:
                continue
            if track_listeners is not None and track_listeners < self.min_track_listeners:
                continue

            picks.append({"track": track, "track_listeners": track_listeners, "artist_listeners": artist_listeners})

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
            print(f"{i}. {track['name']} by {artist_name}")
            print(f"   Track listeners: {track_listeners} | Artist listeners: {artist_listeners} | Link: {url}")

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