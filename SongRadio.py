"""
Song Radio (Low-Mainstream Edition)

Spotifys Development-Mode-API wurde Feb 2026 stark eingeschraenkt (u.a.
Batch-Endpoints und "popularity" entfernt), daher kommen Genre + Popularitaet
primaer von Last.fm: https://developer.spotify.com/documentation/web-api/references/changes/february-2026
"""

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

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        lastfm_api_key: str,
        seed_queries: dict,
        candidate_limit: int = 15,
        genres_to_use: int = 5,
        min_artist_listeners: int = 1_000,
        lastfm_listener_ceiling: int = 150_000,
        artist_top_hit_exclude_n: int = 5,
        allowed_languages: list = ("en", "de"),
        excluded_artists: list = (),
        excluded_genres: list = (),
    ):
        self.sp = spotipy.Spotify(
            auth_manager=SpotifyOAuth(
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
                scope="user-library-read playlist-modify-private playlist-modify-public",
            )
        )
        self.lastfm_api_key = lastfm_api_key
        self.seed_queries = seed_queries
        self.candidate_limit = candidate_limit
        self.genres_to_use = genres_to_use
        self.min_artist_listeners = min_artist_listeners
        self.lastfm_listener_ceiling = lastfm_listener_ceiling
        self.artist_top_hit_exclude_n = artist_top_hit_exclude_n
        self.allowed_languages = set(allowed_languages)
        self.excluded_artists = {a.lower() for a in excluded_artists}
        self.excluded_genres = {g.lower() for g in excluded_genres}

        self.search_limit = 10  # Spotify-Maximum seit Feb 2026
        self.search_offsets = [0, 10, 20, 30]
        self.request_sleep = 0.05

        self._artist_stats_cache = {}
        self._top_tracks_cache = {}
        self.picks = []  # gefuellt von filter_and_rank: dicts mit track/listeners

    # --- Last.fm Helfer -----------------------------------------------

    def _lastfm_get(self, method, **params):
        try:
            resp = requests.get(
                "https://ws.audioscrobbler.com/2.0/",
                params={"method": method, "api_key": self.lastfm_api_key, "format": "json", **params},
                timeout=5,
            )
            return resp.json()
        except requests.RequestException:
            return {}

    def lastfm_top_tags(self, artist_name, limit=8):
        data = self._lastfm_get("artist.getTopTags", artist=artist_name)
        tags = data.get("toptags", {}).get("tag", [])
        return [t["name"].lower() for t in tags[:limit] if t.get("name")]

    def lastfm_track_listeners(self, artist_name, track_name):
        data = self._lastfm_get("track.getInfo", artist=artist_name, track=track_name)
        try:
            return int(data["track"]["listeners"])
        except (KeyError, ValueError, TypeError):
            return None

    def lastfm_artist_listeners(self, artist_name):
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
        if artist_name in self._top_tracks_cache:
            return self._top_tracks_cache[artist_name]
        data = self._lastfm_get("artist.getTopTracks", artist=artist_name, limit=self.artist_top_hit_exclude_n)
        tracks = data.get("toptracks", {}).get("track", [])
        names = {t["name"].lower() for t in tracks if t.get("name")}
        self._top_tracks_cache[artist_name] = names
        return names

    @staticmethod
    def is_allowed_language(text, allowed_languages):
        try:
            return detect(text) in allowed_languages
        except LangDetectException:
            return True  # zu kurz/unsicher -> durchlassen

    # --- Pipeline-Schritte ----------------------------------------------

    def resolve_seed_track_ids(self):
        track_ids = []
        for artist_name, track_name in self.seed_queries.items():
            results = self.sp.search(q=f"{track_name} {artist_name}", type="track", limit=10)
            items = results.get("tracks", {}).get("items", [])
            time.sleep(self.request_sleep)
            if not items:
                print(f"Warnung: Kein Treffer fuer '{track_name}' von {artist_name}.")
                continue

            def match_score(item):
                item_artist = item["artists"][0]["name"].lower()
                artist_ok = artist_name.lower() in item_artist or item_artist in artist_name.lower()
                title_sim = difflib.SequenceMatcher(None, item["name"].lower(), track_name.lower()).ratio()
                return (artist_ok, title_sim)

            best = max(items, key=match_score)
            artist_ok, title_sim = match_score(best)
            if not artist_ok or title_sim < 0.6:
                print(f"Warnung: unsicherer Treffer fuer '{track_name}' -> '{best['name']}' ({title_sim:.2f}).")

            track_ids.append(best["id"])
            print(f"  Gefunden: {best['name']} by {best['artists'][0]['name']} ({best['id']})")
        return track_ids

    def get_seed_artist_ids_and_genres(self, seed_track_ids):
        artist_ids = []
        artist_names = []
        for track_id in seed_track_ids:
            track = self.sp.track(track_id)
            artist_ids.append(track["artists"][0]["id"])
            artist_names.append(track["artists"][0]["name"])
            time.sleep(self.request_sleep)

        genre_counter = Counter()
        for artist_name in artist_names:
            genre_counter.update(self.lastfm_top_tags(artist_name))
            time.sleep(self.request_sleep)

        if not genre_counter:
            for artist_id in artist_ids:
                artist = self.sp.artist(artist_id)
                genre_counter.update(artist.get("genres", []))
                time.sleep(self.request_sleep)

        return artist_ids, genre_counter

    def build_candidate_pool(self, genre_counter, exclude_artist_ids):
        candidates = {}

        blocked = self.GENERIC_GENRE_BLOCKLIST | self.LASTFM_JUNK_TAGS | self.excluded_genres
        specific_genres = [g for g in genre_counter if g not in blocked]
        specific_genres.sort(key=lambda g: (-len(g.split()), -genre_counter[g]))
        top_genres = specific_genres[: self.genres_to_use]
        if not top_genres:
            top_genres = [g for g, _ in genre_counter.most_common(self.genres_to_use)] or ["pop"]

        print(f"Verwendete Genres: {top_genres}")

        for genre in top_genres:
            query = f'genre:"{genre}"'
            for offset in self.search_offsets:
                try:
                    results = self.sp.search(q=query, type="track", limit=self.search_limit, offset=offset)
                except spotipy.exceptions.SpotifyException:
                    raise KeyError("Some error with spotify. Most likely just invalid parameters.")
                time.sleep(self.request_sleep)

                items = results.get("tracks", {}).get("items", [])
                if not items:
                    break

                for track in items:
                    if not track or not track.get("id"):
                        continue
                    if track["artists"][0]["id"] in exclude_artist_ids:
                        continue
                    if track["artists"][0]["name"].lower() in self.excluded_artists:
                        continue
                    if not self.is_allowed_language(track["name"], self.allowed_languages):
                        continue
                    candidates[track["id"]] = track

        return list(candidates.values())

    def filter_and_rank(self, candidates):
        """
        Filterkaskade pro Kandidat:
          1. Last.fm-Genre-Tags gegen excluded_genres
          2. Kuenstler-Gesamthoerer >= min_artist_listeners (nur Untergrenze)
          3. Track ist nicht einer der Top-Hits des Kuenstlers
          4. Track-Hoererzahl <= lastfm_listener_ceiling
        """
        pool = list(candidates)
        random.shuffle(pool)

        picks = []
        for track in pool:
            if len(picks) >= self.candidate_limit:
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

            picks.append({"track": track, "track_listeners": track_listeners, "artist_listeners": artist_listeners})

        if len(picks) < self.candidate_limit:
            print(f"Hinweis: Nur {len(picks)} passende Tracks gefunden (von {len(pool)} Kandidaten).")

        self.picks = picks
        return picks

    # --- Ausgabe & Playlist -----------------------------------------------

    def print_results(self):
        print("\n--- Empfohlene Tracks ---")
        for i, pick in enumerate(self.picks, 1):
            track = pick["track"]
            artist_name = track["artists"][0]["name"]
            url = track["external_urls"]["spotify"]
            track_listeners = pick["track_listeners"] if pick["track_listeners"] is not None else "?"
            artist_listeners = pick["artist_listeners"] if pick["artist_listeners"] is not None else "?"
            print(f"{i}. {track['name']} by {artist_name}")
            print(f"   Song-Hoerer: {track_listeners} | Kuenstler-Hoerer: {artist_listeners} | Link: {url}")

    def save_as_playlist(self, name="Song Radio (Low-Mainstream)"):
        if not self.picks:
            print("Keine Tracks zum Speichern vorhanden.")
            return
        uris = [pick["track"]["uri"] for pick in self.picks]
        playlist = self.sp.current_user_playlist_create(name=name, public=False)
        self.sp.playlist_add_items(playlist["id"], uris)
        print(f"Playlist '{name}' erstellt: {playlist['external_urls']['spotify']}")

    # --- Ablauf -----------------------------------------------------------

    def run(self):
        print("Loese Seed-Tracks auf...")
        seed_track_ids = self.resolve_seed_track_ids()
        if not seed_track_ids:
            print("Keine Seed-Tracks gefunden - Abbruch.")
            return

        print("\nLade Kuenstlerdaten und Genres...")
        seed_artist_ids, genre_counter = self.get_seed_artist_ids_and_genres(seed_track_ids)

        print("\n--- Erkannte Genres ---")
        for genre, count in genre_counter.most_common(10):
            print(f"  {genre}  (x{count})")

        print("\nBaue Kandidaten-Pool...")
        candidates = self.build_candidate_pool(genre_counter, set(seed_artist_ids))
        print(f"{len(candidates)} Kandidaten gefunden.\n")

        print("Waehle Tracks aus...")
        self.filter_and_rank(candidates)
        self.print_results()

        answer = input("\nAls Playlist in die Bibliothek speichern? [Y/n] ").strip().lower()
        if answer in ("", "y", "yes"):
            self.save_as_playlist()
        else:
            print("Playlist wird nicht gespeichert.")