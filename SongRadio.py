"""
Song Radio (Low-Mainstream Edition)

Spotifys Development-Mode-API wurde Feb 2026 stark eingeschraenkt (u.a.
Batch-Endpoints, "popularity" und die alten Playlist-Endpoints entfernt).
https://developer.spotify.com/documentation/web-api/tutorials/february-2026-migration-guide

Playlist-Erstellung/-Befuellung laeuft daher ueber rohe Requests
(POST /me/playlists, POST /playlists/{id}/items), da spotipy intern noch
die entfernten Endpoints (/users/{id}/playlists, /playlists/{id}/tracks)
nutzt.
"""

import re
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
    # Typische Zusaetze, die eine andere "Version" desselben Songs markieren
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
    ):
        """Initialisiert Spotify-/Last.fm-Clients und speichert die Konfiguration.

        Args:
            client_id: Spotify-App Client-ID.
            client_secret: Spotify-App Client-Secret.
            redirect_uri: Spotify-App Redirect-URI (fuer den OAuth-Flow).
            lastfm_api_key: Last.fm API-Key (Genre-/Hoererzahlen-Daten).
            seed_queries: Mapping {Kuenstlername: Songtitel} als Ausgangspunkt
                fuer die Empfehlungen.
            result_limit: Anzahl der Tracks, die am Ende zurueckgegeben werden.
            genres_to_use: Anzahl der (spezifischsten) Genres, die fuer die
                Genre-Suche verwendet werden.
            min_artist_listeners: Mindest-Gesamthoererzahl eines Kuenstlers auf
                Last.fm, um als Kandidat zu gelten.
            lastfm_listener_ceiling: Maximale Hoererzahl eines einzelnen Tracks
                auf Last.fm, um noch als "Hidden Gem" zu gelten.
            artist_top_hit_exclude_n: Anzahl der meistgehoerten Tracks eines
                Kuenstlers, die als "verbraucht" gelten und ausgeschlossen werden.
            allowed_languages: Menge erlaubter Sprachcodes (z.B. "en", "de") fuer
                die Spracherkennung der Songtitel.
            excluded_artists: Kuenstlernamen, die nie als Kandidaten vorkommen sollen.
            excluded_genres: Genre-Tags, die nie als Suchbegriff genutzt und deren
                Kuenstler hart ausgeschlossen werden.
            max_results_per_genre: Obergrenze an Suchergebnissen pro Genre-Query.
            max_results_per_seed_artist: Obergrenze an Suchergebnissen pro
                Seed-Kuenstler-Query.
            max_total_candidates: Obergrenze an Kandidaten insgesamt, ab der die
                Kandidatensuche vorzeitig abgebrochen wird.
            include_seed_tracks: Ob die Seed-Tracks selbst mit in die spaeter
                erstellte Playlist aufgenommen werden sollen.

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
            retries=0,  # spotipy wuerde sonst selbst den vollen Retry-After abschlafen (ggf. Stunden)
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

        self.search_limit = 10  # Spotify-Maximum seit Feb 2026
        self.request_sleep = 0.2  # etwas grosszuegiger, um Bursts zu vermeiden
        self._max_offset = 990  # Sicherheitsgrenze fuer offset-Pagination

        self._artist_stats_cache = {}
        self._top_tracks_cache = {}
        self.seed_tracks = []  # volle Track-Objekte, fuer Playlist-Aufnahme
        self.seed_artist_names = []
        self.picks = []

    # --- Last.fm Helfer -----------------------------------------------

    def _lastfm_get(self, method, **params):
        """Fuehrt einen GET-Request gegen die Last.fm-API aus.

        Args:
            method: Name der Last.fm-API-Methode (z.B. "artist.getInfo").
            **params: Zusaetzliche Query-Parameter fuer den Request
                (z.B. artist=..., track=..., limit=...).

        Returns:
            Die per JSON dekodierte Antwort als dict, oder ein leeres dict
            bei einem Request-Fehler.
        """
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
        """Holt die meistvergebenen Last.fm-Tags (Genre/Style) eines Kuenstlers.

        Args:
            artist_name: Name des Kuenstlers.
            limit: Maximale Anzahl zurueckgegebener Tags.

        Returns:
            Liste von Tag-Namen (kleingeschrieben), absteigend nach Haeufigkeit.
            Leere Liste, falls keine Tags gefunden wurden oder der Request fehlschlug.
        """
        data = self._lastfm_get("artist.getTopTags", artist=artist_name)
        tags = data.get("toptags", {}).get("tag", [])
        return [t["name"].lower() for t in tags[:limit] if t.get("name")]

    def lastfm_track_listeners(self, artist_name, track_name):
        """Holt die Hoererzahl eines einzelnen Tracks von Last.fm.

        Args:
            artist_name: Name des Kuenstlers.
            track_name: Titel des Tracks.

        Returns:
            Anzahl der Hoerer als int, oder None falls kein Last.fm-Eintrag
            gefunden wurde oder der Request fehlschlug.
        """
        data = self._lastfm_get("track.getInfo", artist=artist_name, track=track_name)
        try:
            return int(data["track"]["listeners"])
        except (KeyError, ValueError, TypeError):
            return None

    def lastfm_artist_listeners(self, artist_name):
        """Holt die Gesamthoererzahl eines Kuenstlers von Last.fm (gecacht).

        Args:
            artist_name: Name des Kuenstlers.

        Returns:
            Gesamthoererzahl als int, oder None falls kein Last.fm-Eintrag
            gefunden wurde oder der Request fehlschlug. Ergebnisse werden pro
            Kuenstlername in self._artist_stats_cache gecacht.
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
        """Holt die Namen der meistgehoerten Tracks eines Kuenstlers (gecacht).

        Args:
            artist_name: Name des Kuenstlers.

        Returns:
            Menge (set) von Tracknamen (kleingeschrieben), begrenzt auf
            self.artist_top_hit_exclude_n Eintraege. Leere Menge, falls keine
            Daten gefunden wurden oder der Request fehlschlug. Ergebnisse
            werden pro Kuenstlername in self._top_tracks_cache gecacht.
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
        """Prueft per Best-effort-Spracherkennung, ob ein Text erlaubt ist.

        Args:
            text: Zu pruefender Text (z.B. ein Songtitel).
            allowed_languages: Menge erlaubter Sprachcodes (z.B. {"en", "de"}).

        Returns:
            True, wenn die erkannte Sprache in allowed_languages enthalten ist
            oder die Erkennung fehlschlaegt (zu kurzer/unsicherer Text wird
            durchgelassen statt faelschlich blockiert). Sonst False.
        """
        try:
            return detect(text) in allowed_languages
        except LangDetectException:
            return True  # zu kurz/unsicher -> durchlassen

    @classmethod
    def normalize_title(cls, title):
        """Normalisiert einen Songtitel fuer den Versions-Duplikat-Abgleich.

        Entfernt Remaster-/Live-/Radio-Edit-/feat.-Zusaetze, damit verschiedene
        Versionen desselben Songs auf denselben Schluessel abbilden.

        Args:
            title: Roher Songtitel.

        Returns:
            Normalisierter Titel: kleingeschrieben, Versions-Zusaetze entfernt,
            nur alphanumerische Zeichen und einzelne Leerzeichen.
        """
        t = title.lower()
        for pattern in cls.VERSION_SUFFIX_PATTERNS:
            t = re.sub(pattern, "", t, flags=re.IGNORECASE)
        return re.sub(r"[^a-z0-9]+", " ", t).strip()

    # --- Pipeline-Schritte ----------------------------------------------

    def resolve_seed_track_ids(self):
        """Loest self.seed_queries per Suche in echte Spotify-Track-IDs auf.

        Sucht pro (Kuenstler, Songtitel)-Paar bei Spotify und waehlt aus den
        Ergebnissen den Treffer mit der besten Kuenstler-/Titel-Uebereinstimmung
        (statt blind Treffer #1 zu vertrauen, da Spotifys Feldsuche eher fuzzy
        matcht). Unsichere Treffer werden per Warnung auf der Konsole markiert.

        Args:
            Keine.

        Returns:
            Liste von Spotify-Track-IDs (str), eine pro erfolgreich
            aufgeloestem Seed. Seeds ohne Treffer werden uebersprungen.
        """
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
        """Holt Kuenstlerdaten und Genres zu den Seed-Tracks.

        Laedt jeden Seed-Track einzeln (fuellt dabei self.seed_tracks und
        self.seed_artist_names) und ermittelt Genres primaer ueber Last.fm
        (artist.getTopTags), da Spotifys eigenes "genres"-Feld auf
        Artist-Objekten haeufig leer ist. Nur falls Last.fm keine Tags liefert,
        wird ersatzweise Spotifys artist.genres verwendet.

        Args:
            seed_track_ids: Liste von Spotify-Track-IDs der Seed-Tracks.

        Returns:
            Tuple aus:
                - Liste von Spotify-Artist-IDs (str), eine pro Seed-Track.
                - Counter mit Genre-Tag -> Haeufigkeit ueber alle Seed-Kuenstler.
        """
        artist_ids = []
        for track_id in seed_track_ids:
            track = self.sp.track(track_id)
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
                genre_counter.update(artist.get("genres", []))
                time.sleep(self.request_sleep)

        return artist_ids, genre_counter

    RATE_LIMIT_AUTO_RETRY_THRESHOLD = 30  # Sekunden - darueber: sofort abbrechen statt warten

    def _paginated_search(self, query, max_results):
        """Blaettert per offset durch Spotify-Suchergebnisse.

        Erhoeht offset in Schritten von self.search_limit, bis entweder
        max_results erreicht ist oder Spotify eine leere Ergebnisseite liefert
        (Ende der gefilterten Datenbank). Behandelt 429-Antworten: kurze
        Wartezeiten (<= RATE_LIMIT_AUTO_RETRY_THRESHOLD Sekunden) werden selbst
        abgewartet und der Offset erneut versucht; lange/unbekannte Wartezeiten
        fuehren zum sofortigen Abbruch (raise), um die Sperre nicht durch
        weitere Requests zu verlaengern.

        Args:
            query: Spotify-Suchquery (z.B. 'genre:"funk rock"').
            max_results: Obergrenze an gesammelten Ergebnissen fuer diese Query.

        Returns:
            Liste von Spotify-Track-Objekten (dicts).

        Raises:
            spotipy.exceptions.SpotifyException: Bei einem 429 mit langer/
                unbekannter Wartezeit, oder wenn spotipy die Exception erneut
                wirft (via `raise`).
        """
        collected = []
        offset = 0
        while offset < max_results and offset <= self._max_offset:
            try:
                results = self.sp.search(q=query, type="track", limit=self.search_limit, offset=offset)
            except spotipy.exceptions.SpotifyException as e:
                if getattr(e, "http_status", None) == 429:
                    retry_after = None
                    try:
                        retry_after = int(e.headers.get("Retry-After"))
                    except (AttributeError, TypeError, ValueError):
                        pass

                    if retry_after is not None and retry_after <= self.RATE_LIMIT_AUTO_RETRY_THRESHOLD:
                        print(f"Kurzes Rate-Limit ({retry_after}s) - warte und versuche erneut...")
                        time.sleep(retry_after + 1)
                        continue  # denselben offset nochmal versuchen

                    # Grosser/unbekannter Retry-After -> keine sinnvolle Wartezeit
                    # fuer ein Skript, sofort abbrechen statt stundenlang zu haengen.
                    wait_msg = f"{retry_after}s" if retry_after is not None else "unbekannt"
                    print(f"Rate-Limit erreicht (Wartezeit: {wait_msg}) - breche komplett ab.")
                    raise
                break  # anderer Fehler (z.B. ungueltige Genre-Query) -> naechstes Genre/Artist probieren
            time.sleep(self.request_sleep)
            items = results.get("tracks", {}).get("items", [])
            if not items:
                break  # Ende der gefilterten Datenbank erreicht
            collected.extend(items)
            offset += self.search_limit
        return collected

    def build_candidate_pool(self, genre_counter, exclude_artist_ids=None):
        """Sammelt Kandidaten-Tracks per Genre- und Seed-Kuenstler-Suche.

        Kombiniert zwei Quellen: (1) Suche nach den spezifischsten Genres aus
        genre_counter, fuer Vielfalt an anderen Kuenstlern; (2) gezielte Suche
        nach den Seed-Kuenstlern selbst, um deren Katalog (Deep Cuts) staerker
        zu gewichten. Dedupliziert dabei ueber (Kuenstler, normalisierter
        Titel), damit verschiedene Versionen desselben Songs nicht mehrfach
        vorkommen, und schliesst die Seed-Tracks selbst sowie excluded_artists/
        nicht erlaubte Sprachen aus.

        Args:
            genre_counter: Counter mit Genre-Tag -> Haeufigkeit (z.B. von
                get_seed_artist_ids_and_genres).
            exclude_artist_ids: Nicht mehr verwendet (Seed-Kuenstler werden
                bewusst NICHT mehr komplett ausgeschlossen, da ihre Deep Cuts
                vorkommen sollen). Nur fuer Abwaertskompatibilitaet vorhanden.

        Returns:
            Liste von Spotify-Track-Objekten (dicts), einer pro einzigartigem
            (Kuenstler, normalisierter Titel)-Schluessel.
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

        # Seed-Tracks selbst nie als "neue Empfehlung" durchgehen lassen
        for seed_track in self.seed_tracks:
            key = (seed_track["artists"][0]["name"].lower(), self.normalize_title(seed_track["name"]))
            candidates[key] = None  # Platzhalter, wird unten rausgefiltert

        # 1) Genre-Suche fuer Vielfalt an anderen Kuenstlern
        blocklist = self.GENERIC_GENRE_BLOCKLIST | self.LASTFM_JUNK_TAGS | self.excluded_genres
        specific_genres = [g for g in genre_counter if g not in blocklist]
        specific_genres.sort(key=lambda g: (-len(g.split()), -genre_counter[g]))
        top_genres = specific_genres[: self.genres_to_use] or \
            [g for g, _ in genre_counter.most_common(self.genres_to_use)] or ["pop"]
        print(f"Verwendete Genres: {top_genres}")

        for genre in top_genres:
            if len(candidates) >= self.max_total_candidates:
                break
            for track in self._paginated_search(f'genre:"{genre}"', self.max_results_per_genre):
                add_track(track)

        # 2) Gezielte Suche nach den Seed-Kuenstlern, um deren Katalog
        #    (Deep Cuts) staerker zu gewichten
        for artist_name in self.seed_artist_names:
            if len(candidates) >= self.max_total_candidates:
                break
            for track in self._paginated_search(f'artist:"{artist_name}"', self.max_results_per_seed_artist):
                if any(a["name"].lower() == artist_name.lower() for a in track["artists"]):
                    add_track(track)

        candidates.pop(None, None)
        return [t for t in candidates.values() if t is not None]

    def filter_and_rank(self, candidates):
        """Filtert und waehlt die finalen Empfehlungen aus dem Kandidaten-Pool.

        Filterkaskade pro Kandidat:
            1. Last.fm-Genre-Tags gegen excluded_genres.
            2. Kuenstler-Gesamthoerer >= min_artist_listeners (nur Untergrenze,
               grosse Kuenstler sind erlaubt, solange der Song selbst obskur ist).
            3. Track ist nicht einer der Top-Hits des Kuenstlers.
            4. Track-Hoererzahl <= lastfm_listener_ceiling.

        Args:
            candidates: Liste von Spotify-Track-Objekten (dicts), z.B. von
                build_candidate_pool.

        Returns:
            Liste von dicts mit den Schluesseln "track" (Spotify-Track-Objekt),
            "track_listeners" (int oder None) und "artist_listeners" (int oder
            None). Maximal self.result_limit Eintraege. Wird zusaetzlich in
            self.picks gespeichert.
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

            picks.append({"track": track, "track_listeners": track_listeners, "artist_listeners": artist_listeners})

        if len(picks) < self.result_limit:
            print(f"Hinweis: Nur {len(picks)} passende Tracks gefunden (von {len(pool)} Kandidaten).")

        self.picks = picks
        return picks

    # --- Ausgabe -----------------------------------------------------------

    def print_results(self):
        """Gibt die in self.picks gespeicherten Empfehlungen auf der Konsole aus.

        Zeigt pro Track Titel, Kuenstler, Song- und Kuenstler-Hoererzahl sowie
        den Spotify-Link.

        Args:
            Keine.

        Returns:
            None.
        """
        print("\n--- Empfohlene Tracks ---")
        for i, pick in enumerate(self.picks, 1):
            track = pick["track"]
            artist_name = track["artists"][0]["name"]
            url = track["external_urls"]["spotify"]
            track_listeners = pick["track_listeners"] if pick["track_listeners"] is not None else "?"
            artist_listeners = pick["artist_listeners"] if pick["artist_listeners"] is not None else "?"
            print(f"{i}. {track['name']} by {artist_name}")
            print(f"   Song-Hoerer: {track_listeners} | Kuenstler-Hoerer: {artist_listeners} | Link: {url}")

    # --- Playlist (rohe Requests, siehe Modul-Docstring) --------------------

    def _bearer_headers(self):
        """Baut HTTP-Header mit gueltigem Bearer-Token fuer rohe API-Requests.

        Args:
            Keine.

        Returns:
            dict mit "Authorization"- und "Content-Type"-Headern.
        """
        token = self.sp.auth_manager.get_access_token(as_dict=False)
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _existing_playlist_names(self):
        """Liest die Namen aller Playlists des aktuellen Nutzers.

        Blaettert ueber alle Seiten von GET /me/playlists.

        Args:
            Keine.

        Returns:
            Menge (set) der vorhandenen Playlist-Namen (str).
        """
        names = set()
        results = self.sp.current_user_playlists(limit=50)
        while results:
            names.update(p["name"] for p in results["items"])
            results = self.sp.next(results) if results.get("next") else None
        return names

    def _unique_playlist_name(self, base_name):
        """Erzeugt einen eindeutigen Playlist-Namen per Nummerierung.

        Prueft base_name gegen die vorhandenen Playlist-Namen des Nutzers und
        haengt bei einer Kollision " #2", " #3" usw. an, bis der Name frei ist.

        Args:
            base_name: Gewuenschter Basis-Name der Playlist.

        Returns:
            Eindeutiger Playlist-Name (str): base_name selbst, oder base_name
            mit angehaengter Nummer.
        """
        existing = self._existing_playlist_names()
        if base_name not in existing:
            return base_name
        n = 2
        while f"{base_name} #{n}" in existing:
            n += 1
        return f"{base_name} #{n}"

    def save_as_playlist(self, base_name="Song Radio (Low-Mainstream)"):
        """Erstellt eine private Playlist mit den Empfehlungen (+ optional Seeds).

        Nutzt rohe HTTP-Requests statt spotipy-Methoden fuer Erstellung und
        Befuellung, da die von spotipy intern genutzten Endpoints
        (/users/{id}/playlists, /playlists/{id}/tracks) seit Feb 2026 entfernt
        sind (siehe Modul-Docstring). Der Playlist-Name wird ueber
        _unique_playlist_name deduplizierend nummeriert.

        Args:
            base_name: Gewuenschter Basis-Name der Playlist.

        Returns:
            None.

        Raises:
            requests.exceptions.HTTPError: Falls das Erstellen der Playlist
                oder das Hinzufuegen der Tracks fehlschlaegt.
        """
        if not self.picks:
            print("Keine Tracks zum Speichern vorhanden.")
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

        print(f"Playlist '{name}' erstellt: {playlist['external_urls']['spotify']}")